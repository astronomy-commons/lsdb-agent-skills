#!/usr/bin/env python
"""
attach_positions.py — give a table that has no coordinates its positions from a reference HATS catalog, by id.

Many survey products carry only an object id (variability or derived-parameter tables, detection tables keyed by
object). HATS needs a position per row, so this step materialises the source as Parquet files with
the reference catalog's `ra`/`dec` columns merged in by id. The import then reads those files
(`[source].files = ["positions/*.parquet"]`, `format = "parquet"`), and the verification loader sees the same rows.

Configuration (`catalog.toml`):
    [source.positions_from]
    catalog = "<collection root with an index table on `id`>"
    id = "object_id"
    ra = "ra"            # reference column names; they are written under [columns].ra / [columns].dec
    dec = "dec"

Steps:
    1. pass over the input files reading the id column only (through the configured reader: csv/ecsv arrays, fits,
       parquet, indexed inputs) and collect the distinct ids;
    2. fetch the positions once: up to --idsearch-max ids with chunked `Catalog.id_search` on the reference
       collection's index, above that with a column-pruned scan of the reference partitions (pyarrow, threads);
    3. pass over the input files again, merge the positions by id, count rows without a match (fail, or drop them when
       `[reader].drop_rows_without_coordinates` is true), and write one Parquet file per input file into
       <work_dir>/positions/ with the run's writer settings; append a `positions` entry to the run log.

Output names: the input's path relative to the common directory of all inputs, directory separators replaced by
`__`, the file-type and compression suffixes removed, plus `.parquet` (`north/x.part1.csv.gz` ->
`north__x.part1.parquet`). The step refuses to start when two inputs would get the same name. For the indexed
formats the inputs are the index files (one output per index file).

CPUs: --threads is capped at the CPUs left after pinning ([dask].max_cpus / HATS_PUBLISH_MAX_CPUS). The memory cap
(HATS_PUBLISH_MAX_MEM_GB) applies to Dask workers only; this step has none, so bound it with memwatch.sh on shared
nodes.

Usage:
    attach_positions.py [--config catalog.toml] [--dry-run] [--threads N] [--idsearch-max N] [--no-pypi]
"""
from __future__ import annotations

import argparse
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _common import (  # noqa: E402
    check_versions, input_files, load_config, pin_cpus, read_properties, runlog, parquet_write_kwargs,
)
from import_collection import file_reader  # noqa: E402


def as_table(chunk) -> pa.Table:
    return chunk if isinstance(chunk, pa.Table) else pa.Table.from_pandas(chunk, preserve_index=False)


def expand_inputs(cfg) -> list[Path]:
    """The reader's inputs: data files, or for the indexed formats the index files themselves (the indexed readers
    open the files listed in them)."""
    return input_files(cfg)


DATA_SUFFIXES = {".gz", ".bz2", ".xz", ".zst", ".zip", ".csv", ".tsv", ".ecsv", ".txt", ".dat", ".fits", ".fit", ".fz",
                 ".parquet", ".pq", ".hdf5", ".h5", ".index"}


def output_names(files: list[Path]) -> dict:
    """input -> output file name: path relative to the inputs' common directory, separators replaced by `__`, the
    file-type and compression suffixes stripped (`x.part1.csv.gz` -> `x.part1`), `.parquet` appended. Refuses names
    that collide."""
    import os

    paths = [Path(f).resolve() for f in files]
    root = Path(os.path.commonpath([str(p.parent) for p in paths])) if paths else Path(".")
    names = {}
    for f, p in zip(files, paths):
        rel = p.relative_to(root)
        stem = rel.name
        while Path(stem).suffix.lower() in DATA_SUFFIXES:
            stem = stem[: -len(Path(stem).suffix)]
        name = "__".join(list(rel.parent.parts) + [stem]) + ".parquet"
        names[f] = name
    seen: dict = {}
    for f, name in names.items():
        seen.setdefault(name, []).append(str(f))
    clashes = {k: v for k, v in seen.items() if len(v) > 1}
    if clashes:
        raise SystemExit(f"inputs would overwrite each other's output in positions/: {dict(list(clashes.items())[:5])}; "
                         "rename the inputs or list them so that their relative paths differ")
    return names


def _csv_ids_fast(path: Path, id_col: str) -> np.ndarray:
    """One column of a (gzipped, ECSV-headed) CSV with pyarrow's multithreaded reader: pandas would tokenise every
    field of every row just to return the id column."""
    import gzip
    from pyarrow import csv as pacsv

    opener = gzip.open if path.suffix == ".gz" else open
    skip = 0
    with opener(path, "rt") as fh:
        for line in fh:
            if not line.startswith("#"):
                break
            skip += 1
    table = pacsv.read_csv(str(path), read_options=pacsv.ReadOptions(skip_rows=skip),
                           convert_options=pacsv.ConvertOptions(include_columns=[id_col]))
    return table.column(id_col).to_numpy(zero_copy_only=False)


def _csv_table_fast(path: Path, cfg) -> pa.Table:
    """Whole ECSV-headed CSV file as an Arrow table with pyarrow's reader: scalar types from the header, array columns
    parsed into list columns. Same result as the hats-import reader chain, several times faster (no pandas)."""
    import gzip
    from pyarrow import csv as pacsv
    from readers import ECSV_ELEMENT_TYPES, ecsv_datatypes, parse_array_strings

    opener = gzip.open if path.suffix == ".gz" else open
    skip = 0
    with opener(path, "rt") as fh:
        for line in fh:
            if not line.startswith("#"):
                break
            skip += 1
    cols = ecsv_datatypes(path)
    arrays = {c["name"]: c["subtype"] for c in cols if c.get("subtype")}
    types = {c["name"]: (pa.string() if c["name"] in arrays else ECSV_ELEMENT_TYPES.get(c["datatype"], pa.string())) for c in cols}
    from readers import arrow_type

    for c, dt in (cfg["reader"].get("type_map") or {}).items():   # explicit overrides win (numpy or pandas names)
        if c in types and c not in arrays:
            types[c] = arrow_type(dt)
    table = pacsv.read_csv(str(path), read_options=pacsv.ReadOptions(skip_rows=skip),
                           convert_options=pacsv.ConvertOptions(column_types=types))
    for name, subtype in arrays.items():
        i = table.schema.get_field_index(name)
        table = table.set_column(i, name, parse_array_strings(table.column(name), subtype))
    return table


def collect_ids(make_reader, files: list[Path], id_col: str, threads: int, fast_csv: bool = False) -> np.ndarray:
    """Distinct ids over all input files; one reader per file (hats-import readers keep per-call state)."""
    def one(f):
        if fast_csv:
            return _csv_ids_fast(Path(f), id_col)
        reader = make_reader()
        parts = [as_table(c).column(id_col).to_numpy(zero_copy_only=False) for c in reader.read(str(f), read_columns=[id_col])]
        return np.concatenate(parts) if parts else np.array([], dtype=np.int64)
    with ThreadPoolExecutor(max_workers=threads) as ex:
        parts = list(ex.map(one, files))
    return np.unique(np.concatenate(parts)) if parts else np.array([], dtype=np.int64)


def main_catalog_dir(path: Path) -> Path:
    if (path / "dataset").exists():
        return path
    return path / read_properties(path / "collection.properties")["hats_primary_table_url"]


def fetch_positions_idsearch(catalog: str, id_col: str, ra: str, dec: str, ids: np.ndarray, chunk: int) -> pa.Table:
    import lsdb

    ref = lsdb.open_catalog(catalog, columns=[id_col, ra, dec])
    parts = []
    for i in range(0, len(ids), chunk):
        found = ref.id_search(values={id_col: ids[i:i + chunk].tolist()})[[id_col, ra, dec]].compute()
        parts.append(pa.Table.from_pandas(found.reset_index(drop=True), preserve_index=False))
        print(f"  id_search {min(i + chunk, len(ids)):,}/{len(ids):,} ids -> {sum(len(p) for p in parts):,} positions")
    return pa.concat_tables(parts) if parts else pa.table({id_col: [], ra: [], dec: []})


def fetch_positions_scan(catalog: str, id_col: str, ra: str, dec: str, ids: np.ndarray, threads: int) -> pa.Table:
    """Read [id, ra, dec] from every partition of the reference with an `isin` filter (row-group statistics prune the
    reads when the catalog is sorted by id)."""
    import hats
    from hats.io import paths

    main = main_catalog_dir(Path(catalog))
    cat = hats.read_hats(main)
    pixels = cat.get_healpix_pixels()
    wanted = pa.array(ids)

    def one(pix):
        # read the three columns, then one is_in per partition: a dataset-level `isin` filter with millions of values
        # is re-evaluated per row group and ran at a few MB/s
        t = pq.read_table(paths.pixel_catalog_file(main, pix), columns=[id_col, ra, dec])
        return t.filter(pc.is_in(t.column(id_col), value_set=wanted))

    out = []
    with ThreadPoolExecutor(max_workers=threads) as ex:
        for n, t in enumerate(ex.map(one, pixels), 1):
            if t.num_rows:
                out.append(t)
            if n % 200 == 0:
                print(f"  scanned {n}/{len(pixels)} partitions, {sum(x.num_rows for x in out):,} positions")
    return pa.concat_tables(out) if out else pa.table({id_col: [], ra: [], dec: []})


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--config", default="catalog.toml")
    ap.add_argument("--dry-run", action="store_true", help="first input file only; report the match statistics, write nothing")
    ap.add_argument("--threads", type=int, default=8, help="threads over the input files (both passes) and over the reference partitions")
    ap.add_argument("--idsearch-max", type=int, default=100_000, help="use id_search below this many ids (each call re-reads the index table), a column-pruned partition scan above")
    ap.add_argument("--idsearch-chunk", type=int, default=50_000)
    ap.add_argument("--no-pypi", action="store_true")
    a = ap.parse_args(argv)
    sys.stdout.reconfigure(line_buffering=True)   # progress lines reach the log as they happen
    cfg = load_config(a.config)
    versions = check_versions(cfg, warn_newer=not a.no_pypi)
    cpus = pin_cpus(cfg)
    if a.threads > cpus:
        print(f"--threads {a.threads} reduced to the {cpus} CPUs available")
        a.threads = cpus
    import dask

    dask.config.set(num_workers=cpus)   # lsdb id_search runs on Dask's threaded scheduler
    pf = cfg["source"].get("positions_from") or {}
    if not pf.get("catalog") or not pf.get("id"):
        raise SystemExit("[source.positions_from].catalog and .id must be set for this step")
    pf = dict(pf)
    if "://" not in pf["catalog"]:   # a relative reference path is taken from the work directory
        pf["catalog"] = str(cfg.path(pf["catalog"]))
    id_col, ra, dec = pf["id"], pf.get("ra", "ra"), pf.get("dec", "dec")
    ra_out, dec_out = cfg["columns"]["ra"], cfg["columns"]["dec"]
    files = expand_inputs(cfg)
    if not files:
        raise SystemExit("no input files")
    out_names = output_names(files)   # before any work: refuse colliding output names
    if a.dry_run:
        files = files[:1]
    # the source has no coordinates yet; [reader].conform_schema is applied by the import that reads positions/
    rcfg = cfg.derived(dict(cfg) | {"reader": dict(cfg["reader"]) | {"conform_schema": ""}})
    make_reader = lambda: file_reader(rcfg, [str(f) for f in files], coordinate_filter=False)
    out_dir = cfg.work_dir / "positions"
    ref_props = read_properties(main_catalog_dir(Path(pf["catalog"])) / "hats.properties")
    print(f"reference: {pf['catalog']} ({ref_props.get('hats_builder', '?')}, {int(ref_props.get('hats_nrows', 0)):,} rows); "
          f"{len(files)} input file(s); id column {id_col}")

    t0 = time.time()
    fast_csv = cfg["source"]["format"] == "csv" and not cfg["reader"].get("column_names") and not cfg["reader"].get("csv_kwargs", {}).get("sep")
    ids = collect_ids(make_reader, files, id_col, a.threads, fast_csv=fast_csv)
    print(f"pass 1: {len(ids):,} distinct ids in {time.time() - t0:.0f}s")
    t0 = time.time()
    if len(ids) <= a.idsearch_max:
        pos = fetch_positions_idsearch(pf["catalog"], id_col, ra, dec, ids, a.idsearch_chunk)
    else:
        pos = fetch_positions_scan(pf["catalog"], id_col, ra, dec, ids, a.threads)
    pos = pos.rename_columns([id_col, ra_out, dec_out]) if pos.column_names == [id_col, ra, dec] else pos.select([id_col, ra, dec]).rename_columns([id_col, ra_out, dec_out])
    n_dup = pos.num_rows - pc.count_distinct(pos.column(id_col)).as_py()
    if n_dup:
        pdf = pos.to_pandas()
        dup_ids = pdf.loc[pdf[id_col].duplicated(keep=False), id_col].unique()
        conflicting = pdf[pdf[id_col].isin(dup_ids)].groupby(id_col)[[ra_out, dec_out]].nunique().max(axis=1).gt(1)
        if conflicting.any():
            raise SystemExit(f"the reference catalog holds {int(conflicting.sum()):,} ids with DIFFERENT positions (e.g. "
                             f"{conflicting[conflicting].index[:5].tolist()}); refusing to guess which position is right")
        print(f"WARNING: the reference catalog holds {len(dup_ids):,} ids more than once with identical positions "
              f"(e.g. {dup_ids[:5].tolist()}); using one copy each. This is a defect of the reference catalog, record it.")
        pos = pa.Table.from_pandas(pdf.drop_duplicates(subset=[id_col]).reset_index(drop=True), preserve_index=False)
    matched = pos.num_rows
    print(f"positions: {matched:,} of {len(ids):,} ids found in {time.time() - t0:.0f}s; missing {len(ids) - matched:,}")

    drop_missing = bool(cfg["reader"].get("drop_rows_without_coordinates"))
    if matched < len(ids) and not drop_missing and not a.dry_run:
        raise SystemExit("ids without a position: set [reader].drop_rows_without_coordinates = true to drop those rows, "
                         "or fix the reference catalog")
    if not a.dry_run:
        out_dir.mkdir(parents=True, exist_ok=True)
    rows_in = rows_out = 0
    t0 = time.time()
    def one_file(f):
        if fast_csv and cfg["reader"].get("ecsv_header"):
            table = _csv_table_fast(Path(f), cfg)
        else:
            reader = make_reader()   # fresh per file: hats-import's CSV reader keeps the previous column request
            tables = [as_table(c) for c in reader.read(str(f))]
            table = pa.concat_tables(tables) if len(tables) > 1 else tables[0]
        # the column rules of the import route ([columns].drop / rename) apply to the materialised files too
        rules = cfg["columns"]
        drop = [c for c in (rules.get("drop") or []) if c in table.column_names]
        if drop:
            table = table.drop_columns(drop)
        if rules.get("rename"):
            table = table.rename_columns([rules["rename"].get(c, c) for c in table.column_names])
        # lookup instead of a hash join: Arrow joins reject list columns, and this keeps the row order
        idx = pc.index_in(table.column(id_col), value_set=pos.column(id_col))
        joined = table.append_column(ra_out, pc.take(pos.column(ra_out), idx)).append_column(dec_out, pc.take(pos.column(dec_out), idx))
        missing = pc.sum(pc.is_null(joined.column(ra_out))).as_py() or 0
        if missing and drop_missing:
            joined = joined.filter(pc.is_valid(joined.column(ra_out)))
        if a.dry_run:
            print(f"dry run: {f.name}: {table.num_rows:,} rows, {missing:,} without a position; would write {out_dir / out_names[f]}")
            print("schema:", [f"{n}: {t}" for n, t in zip(joined.schema.names, joined.schema.types)][:12], "...")
        else:
            pq.write_table(joined, out_dir / out_names[f], **parquet_write_kwargs(cfg, joined.schema))
        return table.num_rows, joined.num_rows
    with ThreadPoolExecutor(max_workers=a.threads) as ex:
        for n_in, n_out in ex.map(one_file, files):
            rows_in += n_in; rows_out += n_out
    if a.dry_run:
        return 0
    print(f"pass 2: {rows_in:,} rows read, {rows_out:,} written to {out_dir} ({len(files)} files) in {time.time() - t0:.0f}s")
    runlog(cfg, "end", "positions", "positions attached", status="ok", reference=pf["catalog"],
           reference_builder=ref_props.get("hats_builder"), ids=int(len(ids)), matched=int(matched), rows_in=int(rows_in),
           rows_out=int(rows_out), dropped=int(rows_in - rows_out), versions=versions)
    print("next: set in catalog.toml  [source].files = [\"positions/*.parquet\"]  and  [source].format = \"parquet\"  "
          "(keep [reader].drop_rows_without_coordinates), then import_collection.py")
    return 0


if __name__ == "__main__":
    sys.exit(main())
