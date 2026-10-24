#!/usr/bin/env python
"""
attach_nested.py — build the nested time-series product: join per-object list columns from a reference HATS catalog
onto the flat catalog by id, pack them into one nested column with lsdb, and write a new collection.

Configuration (`catalog.toml`):
    [nested]
    catalog = "<reference HATS collection root (or catalog) whose rows carry the list columns>"
    id = "object_id"                 # join column present in both catalogs; the join is spatial, so positions must agree
    columns = ["n_epochs", "obs_time", "flux", ...]   # taken from the reference
    list_columns = ["obs_time", "flux", ...]          # packed into the nested column (pyarrow list dtypes)
    name = "lightcurve"
    output = "<collection name written next to the flat one>"

lsdb's `join` is an inner spatial join: a row of the flat catalog gets its light curve only when the reference holds
the same id inside the same sky partition. Rows without a match are dropped; the count is reported and written to the
run log, and the verification notebook (section 9b) reconciles it. Scalars listed in `columns` but not in
`list_columns` stay as base columns next to the nested one.

The spatial join aligns the two pixel trees, so the joined catalog comes out at the FINER partitioning of the two
(thousands of small files for a few hundred thousand objects). The step therefore writes the joined catalog to a
temporary collection (`<output>_fine`) and re-partitions it with hats-import at `[partitioning].pixel_threshold`, with
the margin and index tables of the configuration, into `[nested].output`. `--keep-fine` keeps the temporary
collection; `--repartition-only PATH` skips the join and re-partitions an existing fine collection.

The re-partition runs on a minimal configuration built from catalog.toml (repartition_config): the fine files as
Parquet input with the joined row count as expected_total_rows, default reader settings (no companions, derive,
pack_nested, conform_schema or ECSV parsing), no column renames, and the run's partitioning, margins, index, properties,
writer and Dask settings. Both steps (join and write, re-partition) run on a local Dask cluster sized by
[dask] after the shared-node caps (HATS_PUBLISH_MAX_CPUS, HATS_PUBLISH_MAX_MEM_GB, [dask].max_cpus).

Usage:
    attach_nested.py [--config catalog.toml] [--dry-run] [--overwrite] [--keep-fine] [--repartition-only PATH] [--no-pypi]
"""
from __future__ import annotations

import argparse
import shutil
import sys
import time
from pathlib import Path

import lsdb

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _common import (  # noqa: E402
    apply_dask_resources, check_versions, load_config, pin_cpus, properties_dict, read_properties, row_group_kwargs, runlog,
    writer_kwargs,
)


def local_client(cfg):
    """A local Dask cluster from the capped [dask] section (apply_dask_resources has run)."""
    from dask.distributed import Client

    d = cfg["dask"]
    return Client(n_workers=int(d["n_workers"]), threads_per_worker=int(d["threads_per_worker"]),
                  memory_limit=d["memory_limit"], local_directory=d.get("dask_tmp") or None)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--config", default="catalog.toml")
    ap.add_argument("--dry-run", action="store_true", help="resolve the join and print the plan and the schema; write nothing")
    ap.add_argument("--overwrite", action="store_true")
    ap.add_argument("--keep-fine", action="store_true", help="keep the finely partitioned intermediate collection")
    ap.add_argument("--repartition-only", metavar="PATH", help="skip the join: re-partition this fine collection into [nested].output")
    ap.add_argument("--no-pypi", action="store_true")
    a = ap.parse_args(argv)
    sys.stdout.reconfigure(line_buffering=True)
    cfg = load_config(a.config)
    versions = check_versions(cfg, warn_newer=not a.no_pypi)
    apply_dask_resources(cfg, pin_cpus(cfg))
    n = cfg.get("nested") or {}
    for key in ("catalog", "id", "columns", "list_columns", "output"):
        if not n.get(key):
            raise SystemExit(f"[nested].{key} must be set for this step")
    n = dict(n)
    if "://" not in n["catalog"]:   # a relative reference path is taken from the work directory
        n["catalog"] = str(cfg.path(n["catalog"]))
    id_col, name = n["id"], n.get("name") or "nested"
    columns, list_columns = list(n["columns"]), list(n["list_columns"])
    missing = [c for c in list_columns if c not in columns]
    if missing:
        raise SystemExit(f"[nested].list_columns not in [nested].columns: {missing}")

    import pyarrow as pa

    base_path = cfg.collection_path
    base = lsdb.open_catalog(base_path, columns="all")
    ref = lsdb.open_catalog(n["catalog"], columns=[id_col] + columns)
    ref_main = Path(n["catalog"]) if (Path(n["catalog"]) / "dataset").exists() else \
        Path(n["catalog"]) / read_properties(Path(n["catalog"]) / "collection.properties")["hats_primary_table_url"]
    ref_props = read_properties(ref_main / "hats.properties")
    base_rows = base.hc_structure.catalog_info.total_rows
    print(f"flat catalog: {base_path} ({base_rows:,} rows); reference: {n['catalog']} "
          f"({ref_props.get('hats_builder', '?')}, {int(ref_props.get('hats_nrows', 0)):,} rows); join on {id_col}")

    def is_list(dtype) -> bool:
        t = getattr(dtype, "pyarrow_dtype", None)   # numpy / object dtypes have no Arrow type
        return t is not None and (pa.types.is_list(t) or pa.types.is_large_list(t))

    absent = [c for c in list_columns if c not in ref.dtypes.index]
    if absent:
        raise SystemExit(f"[nested].list_columns not in the reference catalog: {absent}")
    bad = {c: str(ref.dtypes[c]) for c in list_columns if not is_list(ref.dtypes[c])}
    if bad:
        raise SystemExit(f"reference columns without a pyarrow list dtype cannot be nested: {bad}")
    joined = base.join(ref, left_on=id_col, right_on=id_col, suffix_method="overlapping_columns", suffixes=("", "_ref"))
    joined = joined.drop(columns=[c for c in joined.columns if c.endswith("_ref")])
    nested = joined.nest_lists(list_columns=list_columns, name=name)
    print("partitions after the join:", len(nested.get_healpix_pixels()))
    print("schema:", [f"{c}: {str(t)[:40]}" for c, t in nested.dtypes.items()][:12], "...")
    if a.dry_run:
        return 0

    out = cfg.build_root / n["output"]
    fine = Path(a.repartition_only) if a.repartition_only else cfg.build_root / f"{n['output']}_fine"
    if out.exists():
        if not a.overwrite:
            raise SystemExit(f"{out} exists; pass --overwrite")
        shutil.rmtree(out)
    if a.repartition_only:
        return repartition(cfg, fine, out, n, versions, keep_fine=True)
    if fine.exists():
        shutil.rmtree(fine)
    runlog(cfg, "start", "nested", f"join {id_col} with {n['catalog']}, nest {len(list_columns)} columns as {name}")
    t0 = time.time()
    defaults = list(cfg["columns"].get("default") or [])
    nested_defaults = [d for d in defaults if d.split(".")[0] in nested.columns]
    props = properties_dict(cfg)
    props["obs_description"] = (props.get("obs_description", "") + f" Nested column '{name}' holds per-object list columns "
                                f"({', '.join(list_columns)}) joined by {id_col} from {ref_props.get('obs_title') or n['catalog']}.").strip()
    with local_client(cfg):   # the join and the write run here
        nested.write_catalog(
            fine, catalog_name=n["output"], as_collection=True, overwrite=True,
            default_columns=nested_defaults or None, addl_hats_properties=props,
            skymap_alt_orders=list(cfg["partitioning"].get("skymap_alt_orders") or []) or None,
            create_thumbnail=False, create_per_partition_statistics=True, create_summary=False,
            write_table_kwargs=writer_kwargs(cfg), row_group_kwargs=row_group_kwargs(cfg),   # lsdb drops row_group_size
        )
    reopened = lsdb.open_catalog(fine)
    rows = reopened.hc_structure.catalog_info.total_rows
    elapsed = round(time.time() - t0, 1)
    print(f"joined and nested: {rows:,} rows in {len(reopened.get_healpix_pixels())} fine partitions "
          f"({base_rows - rows:,} rows of the flat catalog had no match in the reference) in {elapsed} s")
    runlog(cfg, "end", "nested", "joined catalog written (fine partitioning)", status="ok", elapsed_s=elapsed, output=str(fine),
           rows=int(rows), rows_flat=int(base_rows), unmatched=int(base_rows - rows), reference=n["catalog"],
           reference_builder=ref_props.get("hats_builder"), nested_column=name, list_columns=list_columns, versions=versions)
    return repartition(cfg, fine, out, n, versions, keep_fine=a.keep_fine)


def repartition_config(cfg, n: dict, files: list[str], rows: int, schema_names: list[str]):
    """The minimal configuration for the re-partition: only what a Parquet-to-HATS import of the fine files needs.
    Kept from catalog.toml: [run] (route forced to "hats-import"), [partitioning] (with [nested].pixel_threshold),
    [margins], [index], [properties], [writer], [dask], [versions]. Replaced: [names] (the nested output), [source]
    (the fine files as Parquet, expected_total_rows = the joined row count), [reader] (defaults, chunksize kept),
    [columns] (ra, dec, id and sort kept; no rename or cast; the old spatial index dropped; default columns limited to
    those in the fine schema)."""
    cols = cfg["columns"]
    data = {
        "run": dict(cfg["run"]) | {"route": "hats-import"},
        "names": {"collection": n["output"], "catalog": n["output"], "public_name": n["output"]},
        "source": {"format": "parquet", "files": list(files), "urls": [], "expected_files": 0,
                   "expected_total_rows": int(rows)},
        "reader": {"chunksize": int(cfg["reader"].get("chunksize", 500_000))},
        "columns": {"ra": cols["ra"], "dec": cols["dec"], "id": cols.get("id") or [], "sort": cols.get("sort") or "",
                    "drop": ["_healpix_29"],
                    "default": [d for d in (cols.get("default") or []) if d.split(".")[0] in schema_names]},
        "partitioning": dict(cfg["partitioning"]),
        "margins": dict(cfg["margins"]),
        "index": dict(cfg["index"]),
        "properties": dict(cfg["properties"]),
        "writer": dict(cfg["writer"]),
        "dask": dict(cfg["dask"]),
        "versions": dict(cfg.get("versions") or {}),
    }
    if n.get("pixel_threshold"):   # rows with list columns weigh kilobytes: the nested product gets its own threshold
        data["partitioning"]["pixel_threshold"] = int(n["pixel_threshold"])
    props = data["properties"]
    props["obs_description"] = (props.get("obs_description", "") + f" Nested column '{n.get('name') or 'nested'}' holds per-object "
                                f"list columns ({', '.join(n['list_columns'])}) joined by {n['id']}.").strip()
    return cfg.derived(data)


def repartition(cfg, fine: Path, out: Path, n: dict, versions: dict, keep_fine: bool) -> int:
    """Re-partition the finely partitioned joined collection with hats-import at [partitioning].pixel_threshold, adding
    the configured margin and index tables, into [nested].output (same writer settings as every other table)."""
    import glob

    import hats
    from hats_import.pipeline import pipeline_with_client

    from import_collection import collection_args

    main = fine / n["output"]
    files = sorted(glob.glob(str(main / "dataset" / "Norder=*" / "Dir=*" / "Npix=*.parquet")))
    if not files:
        raise SystemExit(f"no data files under {main}")
    fine_cat = hats.read_hats(main)
    rows = int(fine_cat.catalog_info.total_rows)
    names = list(fine_cat.schema.names) if fine_cat.schema is not None else []
    c2 = repartition_config(cfg, n, files, rows, names)
    t0 = time.time()
    runlog(cfg, "start", "nested-repartition", f"{len(files)} fine files ({rows:,} rows) -> pixel_threshold "
           f"{c2['partitioning'].get('pixel_threshold')}")
    args = collection_args(c2, files, cfg.build_root, n["output"], n["output"])
    with local_client(cfg) as client:
        pipeline_with_client(args, client)
    reopened = lsdb.open_catalog(out)
    rows = reopened.hc_structure.catalog_info.total_rows
    elapsed = round(time.time() - t0, 1)
    print(f"written {out}: {rows:,} rows in {len(reopened.get_healpix_pixels())} partitions in {elapsed} s")
    runlog(cfg, "end", "nested-repartition", "nested product written", status="ok", elapsed_s=elapsed, output=str(out), rows=int(rows),
           partitions=len(reopened.get_healpix_pixels()), versions=versions)
    if not keep_fine:
        shutil.rmtree(fine); print(f"removed the intermediate collection {fine}")
    print("next: verify_structure.py --collection", out, " then build_verify_notebook.py (section 9b)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
