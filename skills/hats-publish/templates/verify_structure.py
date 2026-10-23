#!/usr/bin/env python
"""verify_structure.py — structural verification of a built HATS collection (no source data needed).

Three groups of checks, every one PASS / FAIL / WARN with a one-line reason:

  a. hats-import's own verification pipeline (``VerificationArguments``): valid catalog, file set vs ``_metadata``,
     row counts vs properties / ``_metadata`` / ``--truth-rows``, schema vs ``_common_metadata`` or ``--truth-schema``.
  b. HATS-note compliance: required files and properties per table type, both property files identical, property
     vocabularies, spatial-index column present and first, data thumbnail present with the partition schema,
     per-partition statistics present, VOParquet metadata consistent across partitions, margin/index
     ``hats_primary_table_url`` resolving, default columns existing, down-sampled skymaps present.
  c. Parquet encoding sweep over every data partition, margin partition and index part: codec per column chunk,
     page index present, encodings and row-group counts recorded, ``created_by`` recorded, writer level recorded
     as provenance (``--spot-check`` re-encodes one row group at the configured level and compares the size).

Usage:
    verify_structure.py --config catalog.toml [--collection PATH] [--truth-rows N] [--truth-schema PARQUET]
                        [--results-file PATH] [--spot-check] [--no-pypi]

Run it after the import, and again on every copy after publishing (``--collection <copy>``; give that run its own
``--results-file`` so the build's results are kept). Exit code 0 only when every check passed (WARN does not fail).
Results are appended to ``--results-file`` (default ``verify_results.csv`` in the work directory); hats-import's
own verifier CSV goes next to it as ``<results-file stem>_hats_import.csv`` (default
``hats_import_verifier_results.csv``).
"""
from __future__ import annotations

import argparse
import csv
import io
import logging
import random
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _common import (  # noqa: E402
    HATS_FRAMES,
    OBS_REGIMES,
    check_versions,
    data_files,
    index_files,
    load_config,
    read_properties,
    parquet_write_kwargs,
    runlog,
    writer_kwargs,
)

HATS_STATUS_WORDS = ({"private", "public"}, {"main", "mirror", "partial"}, {"clonable", "unclonable", "clonableOnce"})
VOPARQUET_KEY = b"IVOA.VOTable-Parquet.version"
REQUIRED_PROPS = {
    "object": ["dataproduct_type", "hats_col_ra", "hats_col_dec", "hats_nrows", "obs_collection"],
    "source": ["dataproduct_type", "hats_col_ra", "hats_col_dec", "hats_nrows", "obs_collection"],
    "margin": ["dataproduct_type", "obs_collection", "hats_nrows", "hats_margin_threshold", "hats_primary_table_url"],
    "index": ["dataproduct_type", "obs_collection", "hats_nrows", "hats_index_column", "hats_primary_table_url"],
}


class Results:
    def __init__(self):
        self.rows: list[dict] = []

    def add(self, status: str, test: str, target: str, description: str):
        self.rows.append({"datetime": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                          "passed": status == "PASS" or status == "WARN", "status": status, "test": test,
                          "target": str(target), "description": description})
        print(f"{status:4s} {test:38s} {target}: {description}")

    def ok(self, test, target, desc=""):
        self.add("PASS", test, target, desc)

    def fail(self, test, target, desc=""):
        self.add("FAIL", test, target, desc)

    def check_warn(self, ok, test, target, ok_desc="", warn_desc=""):
        """A recommendation: PASS with ok_desc when ok, otherwise WARN (not FAIL) with the reason warn_desc."""
        (self.ok if ok else self.warn)(test, target, ok_desc if ok else warn_desc)

    def warn(self, test, target, desc=""):
        self.add("WARN", test, target, desc)

    def check(self, cond: bool, test, target, ok_desc="", fail_desc=""):
        (self.ok if cond else self.fail)(test, target, ok_desc if cond else fail_desc)

    @property
    def failed(self) -> int:
        return sum(r["status"] == "FAIL" for r in self.rows)


# ----------------------------------------------------------------------------------------------------------------
# a. hats-import verification pipeline
# ----------------------------------------------------------------------------------------------------------------
def run_hats_import_verification(collection: Path, verifier_csv: Path, truth_rows: int | None, truth_schema: str | None,
                                 res: Results) -> None:
    from hats_import.verification.arguments import VerificationArguments
    from hats_import.verification.run_verification import run

    args = VerificationArguments(
        input_catalog_path=collection,
        output_path=verifier_csv.parent,
        output_filename=verifier_csv.name,
        truth_total_rows=truth_rows if truth_rows else None,
        truth_schema=truth_schema or None,
        verbose=False,
        write_mode="a",
    )
    verifier = run(args)
    for row in verifier.results_df.to_dict("records"):
        res.add("PASS" if row["passed"] else "FAIL", f"hats-import:{row['test']}", row["target"],
                str(row["description"]) + (f" bad_files={row['bad_files']}" if row.get("bad_files") else ""))


def footer_verification(catalog: Path, truth_rows: int | None, res: Results, workers: int = 32) -> None:
    """The checks of hats-import's verification pipeline for a catalog written without dataset/_metadata
    ([writer].create_metadata = false; the combined footer would exceed Parquet's 4 GiB limit): data files on disk vs
    partition_info.csv, every file's schema vs _common_metadata, footer row total vs hats_nrows and the truth."""
    from concurrent.futures import ThreadPoolExecutor

    import pandas as pd

    pinfo = pd.read_csv(catalog / "partition_info.csv")
    expected = {catalog / "dataset" / f"Norder={o}" / f"Dir={(n // 10000) * 10000}" / f"Npix={n}.parquet"
                for o, n in zip(pinfo["Norder"].astype(int), pinfo["Npix"].astype(int))}
    on_disk = set((catalog / "dataset").rglob("Npix=*.parquet"))
    res.check(expected == on_disk, "footers:file_sets", catalog.name,
              f"{len(on_disk)} data files match partition_info.csv",
              f"missing {len(expected - on_disk)}, unexpected {len(on_disk - expected)}")
    common = pq.read_schema(catalog / "dataset" / "_common_metadata")

    def one(f):
        md = pq.read_metadata(f)
        return md.num_rows, md.schema.to_arrow_schema().remove_metadata().equals(common.remove_metadata())

    with ThreadPoolExecutor(workers) as ex:
        out = list(ex.map(one, sorted(on_disk)))
    total = sum(r for r, _ in out); bad = sum(1 for _, ok in out if not ok)
    res.check(bad == 0, "footers:schemas", catalog.name, f"all {len(out)} file schemas equal _common_metadata",
              f"{bad} files differ from _common_metadata")
    nrows = int(_props(catalog).get("hats_nrows", -1))
    ok = total == nrows and (not truth_rows or total == truth_rows)
    res.check(ok, "footers:num_rows", catalog.name, f"footers {total:,} = hats_nrows {nrows:,}" + (f" = truth {truth_rows:,}" if truth_rows else ""),
              f"footers {total:,} | hats_nrows {nrows:,} | truth {truth_rows}")


class _Collect(logging.Handler):
    """Collects the warnings hats' validation logs, to report them as the reason of a failure."""

    def __init__(self):
        super().__init__(logging.WARNING)
        self.messages: list[str] = []

    def emit(self, record):
        self.messages.append(record.getMessage().splitlines()[0])


def hats_validation_without_metadata(collection: Path, res: Results) -> None:
    """hats' strict validation of every table when the collection was written without dataset/_metadata.

    For catalogs and margins, hats checks the properties, _common_metadata, partition_info.csv and the partition
    files on disk, and skips the _metadata comparison when the file is absent. Its strict check of an index table
    needs _metadata, so an index without one is reported as WARN with that reason instead of being skipped silently."""
    from hats.io.validation import is_valid_catalog

    props = read_properties(collection / "collection.properties") if (collection / "collection.properties").exists() else {}
    tables = [collection / props.get("hats_primary_table_url", "")] + [collection / m for m in (props.get("all_margins") or "").split()]
    tokens = (props.get("all_indexes") or "").split()
    indexes = [collection / i for i in (tokens[1::2] if len(tokens) % 2 == 0 else tokens)]
    for table, is_index in [(t, False) for t in tables] + [(i, True) for i in indexes]:
        if is_index and not (table / "dataset" / "_metadata").exists():
            res.warn("hats:is_valid_catalog", table.name,
                     "not validated: hats' strict check of an index table reads dataset/_metadata, which is absent "
                     "([writer].create_metadata = false); its file set and row count are covered by the sweep in c.")
            continue
        handler = _Collect()
        root = logging.getLogger()
        root.addHandler(handler)
        try:
            ok = bool(is_valid_catalog(table, strict=True))
        except Exception as exc:   # a crash in the validator is a failure to report, with its reason
            ok, handler.messages = False, handler.messages + [f"{type(exc).__name__}: {exc}"]
        finally:
            root.removeHandler(handler)
        reasons = [m for m in handler.messages if "_metadata file does not exist" not in m]
        res.check(ok, "hats:is_valid_catalog", table.name,
                  "strict validation passed (partition_info.csv, properties and files on disk; no _metadata to compare)",
                  "strict validation failed: " + ("; ".join(reasons[:5]) or "no reason logged"))


# ----------------------------------------------------------------------------------------------------------------
# b. HATS-note compliance
# ----------------------------------------------------------------------------------------------------------------
def _props(table: Path) -> dict:
    for name in ("hats.properties", "properties"):
        if (table / name).exists():
            return read_properties(table / name)
    return {}


def _table_type(props: dict) -> str:
    return props.get("dataproduct_type", "").strip()


def check_property_files(table: Path, res: Results) -> dict:
    a, b = table / "hats.properties", table / "properties"
    if not a.exists() and not b.exists():
        res.fail("properties-present", table.name, "neither hats.properties nor properties exists")
        return {}
    if a.exists() and b.exists():
        res.check(a.read_bytes() == b.read_bytes(), "properties-files-identical", table.name,
                  "hats.properties == properties", "hats.properties and properties differ (edit both)")
    else:
        res.warn("properties-files-identical", table.name, f"only {'hats.properties' if a.exists() else 'properties'} present")
    return _props(table)


def check_table(table: Path, kind: str, collection: Path, res: Results, metadata_optional: bool = False) -> dict:
    """Required files and properties for a catalog, margin or index table."""
    props = check_property_files(table, res)
    ttype = _table_type(props) or kind
    required_files = {
        "catalog": ["partition_info.csv", "dataset/_metadata", "dataset/_common_metadata", "skymap.fits"],
        "margin": ["dataset/_metadata", "dataset/_common_metadata"],
        "index": ["dataset/_metadata", "dataset/_common_metadata"],
    }[kind]
    for rel in required_files:
        if rel == "dataset/_metadata" and metadata_optional and not (table / rel).exists():
            # [writer].create_metadata = false: hats-import writes no _metadata for the catalog, the margins or the
            # indexes (it passes the flag to every table); the footer checks in a. stand in for the catalog
            res.warn("required-file", f"{table.name}/{rel}", "absent by design ([writer].create_metadata = false)")
            continue
        res.check((table / rel).exists(), "required-file", f"{table.name}/{rel}", "present", "missing")
    req = REQUIRED_PROPS.get(ttype if kind == "catalog" else kind, REQUIRED_PROPS["object"])
    missing = [k for k in req if not props.get(k)]
    res.check(not missing, "required-properties", table.name, f"type {ttype}: all present", f"type {ttype}: missing {missing}")

    for key, vocab in (("obs_regime", OBS_REGIMES), ("hats_frame", HATS_FRAMES)):
        if props.get(key):
            res.check(props[key] in vocab, f"vocabulary:{key}", table.name, props[key], f"{props[key]!r} not in {sorted(vocab)}")
    if props.get("hats_status"):
        words = props["hats_status"].split()
        ok = all(any(w in group for w in words) for group in HATS_STATUS_WORDS) and all(any(w in g for g in HATS_STATUS_WORDS) for w in words)
        res.check(ok, "vocabulary:hats_status", table.name, props["hats_status"],
                  f"{props['hats_status']!r}: expected one word from each of {HATS_STATUS_WORDS}")

    schema = None
    cm = table / "dataset" / "_common_metadata"
    if cm.exists():
        schema = pq.read_schema(cm)
    if kind in ("catalog", "margin") and schema is not None:
        hcol = props.get("hats_col_healpix")
        if hcol:
            res.check(hcol in schema.names, "healpix-column-present", table.name, hcol, f"{hcol} not in schema")
            if hcol in schema.names:
                res.check_warn(schema.names[0] == hcol, "healpix-column-first", table.name, f"{hcol} is the first column",
                               f"first column is {schema.names[0]}; the HATS note recommends {hcol} first")
        else:
            res.warn("healpix-column-present", table.name, "hats_col_healpix not declared")
        defaults = (props.get("hats_cols_default") or "").split()
        if defaults:
            top = [d.split(".")[0] for d in defaults]
            missing = [d for d in top if d not in schema.names]
            res.check(not missing, "default-columns-exist", table.name, f"{len(defaults)} columns", f"not in schema: {missing}")
        for key, name in (("hats_cols_survey_id", "survey-id-columns-exist"), ("hats_cols_sort", "sort-columns-exist")):
            cols = (props.get(key) or "").split()
            if cols:
                missing = [c for c in cols if c.split(".")[0] not in schema.names]
                res.check(not missing, name, table.name, " ".join(cols), f"{key} names columns not in schema: {missing}")
    if kind == "catalog" and ttype in ("object", "source"):
        thumb = table / "data_thumbnail.parquet"
        if thumb.exists() and schema is not None:
            ts = pq.read_schema(thumb)
            same = [f.name for f in ts] == schema.names and all(ts.field(n).type == schema.field(n).type for n in schema.names)
            res.check(same, "thumbnail-schema", table.name, f"{pq.read_metadata(thumb).num_rows} rows, schema matches",
                      "data_thumbnail.parquet schema differs from _common_metadata")
        else:
            (res.fail if ttype == "object" else res.warn)("thumbnail-present", table.name,
                                                           "data_thumbnail.parquet missing (RECOMMENDED for object catalogs)")
        res.check((table / "per_partition_statistics.parquet").exists(), "per-partition-statistics", table.name, "present",
                  "per_partition_statistics.parquet missing (RECOMMENDED)")
        for order in (props.get("hats_skymap_alt_orders") or "").split():
            res.check((table / f"skymap.{order}.fits").exists(), "skymap-alt-order", f"{table.name}/skymap.{order}.fits", "present", "missing")
    if kind in ("margin", "index"):
        url = props.get("hats_primary_table_url", "")
        if url.startswith("/"):
            target = Path(url)
        elif url:
            target = (table / url) if (table / url).is_dir() else (collection / url)   # relative to the table (../catalog) or to the collection
        else:
            target = None
        if url.startswith(("http://", "https://", "s3://")):
            res.warn("primary-table-url", table.name, f"remote URL {url}")
        elif target is not None and target.exists():
            res.ok("primary-table-url", table.name, f"resolves ({'absolute' if url.startswith('/') else 'relative'})")
            if url.startswith("/"):
                res.warn("primary-table-url-absolute", table.name, "absolute build path; rewrite to a relative path on every copy")
        else:
            res.fail("primary-table-url", table.name, f"{url!r} does not resolve")
    return props


def check_voparquet(tables: list[Path], res: Results) -> None:
    for table in tables:
        files = data_files(table) or index_files(table)
        if not files:
            continue
        has = [VOPARQUET_KEY in (pq.read_schema(f).metadata or {}) for f in files]
        if not any(has):
            res.ok("voparquet-consistent", table.name, "absent in all partitions")
        else:
            res.check(all(has), "voparquet-consistent", table.name, "present in all partitions",
                      f"present in {sum(has)}/{len(has)} partitions (must be all or none)")


def check_collection(collection: Path, res: Results) -> tuple[Path, list[Path], list[Path]]:
    cp = collection / "collection.properties"
    res.check(cp.exists(), "required-file", "collection.properties", "present", "missing")
    props = read_properties(cp) if cp.exists() else {}
    for key in ("obs_collection", "hats_primary_table_url"):
        res.check(bool(props.get(key)), "required-properties", f"collection:{key}", props.get(key, ""), "missing")
    margins = (props.get("all_margins") or "").split()
    default_margin = props.get("default_margin", "")
    if margins:
        res.check(not default_margin or default_margin in margins, "default-margin-listed", default_margin or "(none)",
                  "in all_margins", f"default_margin {default_margin!r} not in all_margins {margins}")
    tokens = (props.get("all_indexes") or "").split()   # "column dir column dir ..." pairs
    indexes = tokens[1::2] if len(tokens) % 2 == 0 else tokens
    main = collection / props.get("hats_primary_table_url", "")
    res.check(main.is_dir(), "primary-table", main.name, "directory present", f"{main} missing")
    margin_paths = [collection / m for m in margins]
    index_paths = [collection / i for i in indexes]
    return main, margin_paths, index_paths


# ----------------------------------------------------------------------------------------------------------------
# c. Parquet encoding sweep
# ----------------------------------------------------------------------------------------------------------------
def sweep(files: list[Path], label: str, want: dict, res: Results, codec_only: bool = False) -> dict:
    """Check codec/page-index/encodings over files; return a summary dict."""
    want_codec = want["compression"].upper()
    want_index = bool(want.get("write_page_index", True))
    want_enc = want.get("column_encoding") or {}
    dict_override = set(want["use_dictionary"]) if isinstance(want.get("use_dictionary"), list) else set()
    bad_codec, bad_index, bad_enc, encodings, created_by, row_groups = [], [], [], set(), set(), []
    for f in files:
        md = pq.read_metadata(f)
        created_by.add(md.created_by)
        row_groups.append(md.num_row_groups)
        for rg_i in range(md.num_row_groups):
            rg = md.row_group(rg_i)
            for c_i in range(rg.num_columns):
                cc = rg.column(c_i)
                if cc.compression.upper() != want_codec:
                    bad_codec.append(f"{f.name}:{cc.path_in_schema}={cc.compression}")
                if not codec_only:
                    # pyarrow writes no column index for chunks whose statistics carry no min/max (e.g. all-NaN
                    # floats in a tiny partition); the offset index is always written with write_page_index.
                    st = cc.statistics
                    needs_column = bool(st is not None and st.has_min_max)
                    if want_index and (not cc.has_offset_index or (needs_column and not cc.has_column_index)):
                        bad_index.append(f"{f.name}:{cc.path_in_schema}")
                    if not want_index and (cc.has_offset_index or cc.has_column_index):
                        bad_index.append(f"{f.name}:{cc.path_in_schema}")
                    # hats defaults: BYTE_STREAM_SPLIT on float leaves, DELTA_BINARY_PACKED on _healpix_29; margin and
                    # index schemas differ from the main one, so the rule is applied by physical type, not by name
                    path = cc.path_in_schema
                    expect = want_enc.get(path)
                    if expect is None and path not in dict_override:
                        if cc.physical_type in ("FLOAT", "DOUBLE"):
                            expect = "BYTE_STREAM_SPLIT"
                        elif path == "_healpix_29":
                            expect = "DELTA_BINARY_PACKED"
                    if expect and expect not in cc.encodings:
                        bad_enc.append(f"{f.name}:{path}={'/'.join(cc.encodings)}")
                encodings.add(tuple(sorted(cc.encodings)))
    if not files:
        res.warn(f"encoding:{label}", label, "no files")
        return {}
    res.check(not bad_codec, f"encoding-codec:{label}", f"{len(files)} files", f"all column chunks {want_codec}",
              f"{len(bad_codec)} column chunks not {want_codec}: {bad_codec[:5]}")
    if not codec_only:
        res.check(not bad_index, f"encoding-page-index:{label}", f"{len(files)} files",
                  "offset+column index " + ("present" if want_index else "absent") + " everywhere",
                  f"{len(bad_index)} column chunks without the expected page index: {bad_index[:5]}")
        res.check(not bad_enc, f"encoding-columns:{label}", f"{len(files)} files",
                  "float leaves BYTE_STREAM_SPLIT, _healpix_29 DELTA_BINARY_PACKED (hats >= 0.11.1 defaults + [writer])",
                  f"{len(bad_enc)} column chunks with another encoding (written by hats < 0.11.1 or without the hats "
                  f"defaults; files are ~1.4x larger): {bad_enc[:5]}")
        res.ok(f"encoding-row-groups:{label}", f"{len(files)} files",
               f"row groups per file min/max {min(row_groups)}/{max(row_groups)}; encodings {sorted(encodings)}")
    res.ok(f"encoding-created-by:{label}", f"{len(files)} files", "; ".join(sorted(created_by)))
    return {"files": len(files), "created_by": sorted(created_by), "encodings": [list(e) for e in sorted(encodings)]}


def spot_check_level(files: list[Path], want: dict, res: Results) -> None:
    """Re-encode one random row group with the configured codec/level and compare compressed sizes."""
    if not files:
        return
    f = random.choice(files)
    pf = pq.ParquetFile(f)
    rg_i = random.randrange(pf.num_row_groups)
    table = pf.read_row_group(rg_i)
    on_disk = sum(pf.metadata.row_group(rg_i).column(c).total_compressed_size for c in range(pf.metadata.row_group(rg_i).num_columns))
    buf = io.BytesIO()
    pq.write_table(table, buf, **{k: v for k, v in want.items() if k != "row_group_size"})
    md = pq.read_metadata(io.BytesIO(buf.getvalue()))
    reenc = sum(md.row_group(0).column(c).total_compressed_size for c in range(md.row_group(0).num_columns))
    ratio = reenc / on_disk if on_disk else float("nan")
    (res.ok if abs(ratio - 1) <= 0.05 else res.warn)("encoding-level-spot-check", f"{f.name} rg{rg_i}",
                                                     f"re-encoded at level {want.get('compression_level')}: size ratio {ratio:.3f} (±5% expected)")


# ----------------------------------------------------------------------------------------------------------------
def builder_hats_version(builder: str) -> str | None:
    """The version in the `hats v<version>` token of hats_builder ("lsdb v0.11.0, hats v0.11.0" -> "0.11.0").
    Exact token match: "hats-import v..." is a different package and does not count."""
    m = re.search(r"(?:^|[,;\s])hats\s+v?([0-9][^,;\s]*)", builder)
    return m.group(1) if m else None


def check_builder_versions(builder: str, installed: str, target: str, res: Results) -> None:
    """PASS when the copy was built with the installed hats; WARN otherwise. A copy built with another hats
    version is not wrong (verifying an existing or published copy is a normal use), but the result says so."""
    built = builder_hats_version(builder)
    if built is None:
        res.warn("builder-versions", target, f"hats_builder {builder!r} names no hats version")
    elif built == installed:
        res.ok("builder-versions", target, f"built with hats {built} (installed)")
    else:
        res.warn("builder-versions", target, f"built with hats {built}, installed hats is {installed} "
                 f"(expected when verifying an existing copy; a new build should use the installed version)")


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--config", default="catalog.toml")
    ap.add_argument("--collection", help="collection to verify (default: the build path from catalog.toml)")
    ap.add_argument("--truth-rows", type=int, default=0, help="expected total rows (default [source].expected_total_rows)")
    ap.add_argument("--truth-schema", help="parquet file whose schema is the truth (default: the catalog's _common_metadata)")
    ap.add_argument("--results-file", metavar="PATH",
                    help="CSV the results are appended to (default: verify_results.csv in the work directory); "
                         "a re-check of a hosted copy should pass its own file so the build's results are kept")
    ap.add_argument("--spot-check", action="store_true", help="re-encode one row group at the configured level and compare sizes")
    ap.add_argument("--no-pypi", action="store_true", help="skip the newer-release lookup")
    a = ap.parse_args(argv)

    cfg = load_config(a.config)
    versions = check_versions(cfg, warn_newer=not a.no_pypi)
    collection = Path(a.collection).resolve() if a.collection else cfg.collection_path
    if not collection.is_dir():
        raise SystemExit(f"collection not found: {collection}")
    work_dir = cfg.work_dir
    work_dir.mkdir(parents=True, exist_ok=True)
    truth_rows = a.truth_rows or int(cfg["source"].get("expected_total_rows") or 0)
    # results of this run, and hats-import's own CSV next to them (named after --results-file, so a re-check of a
    # hosted copy with its own results file keeps the build's two files intact)
    out = Path(a.results_file).resolve() if a.results_file else work_dir / "verify_results.csv"
    out.parent.mkdir(parents=True, exist_ok=True)
    verifier_csv = out.with_name(f"{out.stem}_hats_import.csv") if a.results_file else work_dir / "hats_import_verifier_results.csv"
    res = Results()
    runlog(cfg, "start", "verify-structure", f"collection {collection.name}")

    main_catalog = collection / cfg["names"]["catalog"]
    if (main_catalog / "dataset" / "_metadata").exists():
        print(f"== a. hats-import verification pipeline on {collection}")
        try:
            run_hats_import_verification(collection, verifier_csv, truth_rows, a.truth_schema, res)
        except Exception as exc:  # the pipeline raising is itself a failure to report
            res.fail("hats-import:run", collection.name, f"{type(exc).__name__}: {exc}")
    else:   # hats-import's pipeline reads _metadata; the same checks from the file footers
        print(f"== a. no dataset/_metadata in {main_catalog.name}: hats validation, then file-set, schema and row checks from the file footers")
        res.warn("hats-import:run", main_catalog.name,
                 "hats-import's verification pipeline not run: it reads dataset/_metadata, which is absent; "
                 "hats strict validation and the footer checks below stand in for it")
        hats_validation_without_metadata(collection, res)
        footer_verification(main_catalog, truth_rows, res)

    print("== b. HATS-note compliance")
    main, margins, indexes = check_collection(collection, res)
    no_metadata = not cfg["writer"].get("create_metadata", True)
    main_props = check_table(main, "catalog", collection, res, metadata_optional=no_metadata)
    for m in margins:
        check_table(m, "margin", collection, res, metadata_optional=no_metadata)
    for i in indexes:
        check_table(i, "index", collection, res, metadata_optional=no_metadata)
    check_voparquet([main, *margins], res)
    check_builder_versions(main_props.get("hats_builder", ""), versions.get("hats", ""), main.name, res)

    print("== c. Parquet encoding sweep")
    main_files = data_files(main)
    want = parquet_write_kwargs(cfg, pq.read_schema(main_files[0]) if main_files else pa.schema([]))
    res.ok("encoding-level-provenance", main.name,
           f"writer: {want['compression']} level {want.get('compression_level')} page_index={want.get('write_page_index')} "
           f"overrides={writer_kwargs(cfg)} "
           "(level is not stored in Parquet metadata; enforced at write time)")
    sweep(data_files(main), main.name, want, res)
    for m in margins:
        sweep(data_files(m), m.name, want, res)
    for i in indexes:
        sweep(index_files(i), i.name, want, res)
    ancillary = [p for p in (main / "data_thumbnail.parquet", main / "per_partition_statistics.parquet") if p.exists()]
    if ancillary:
        sweep(ancillary, "ancillary", want, res, codec_only=True)
    if a.spot_check:
        spot_check_level(data_files(main), want, res)

    new_file = not out.exists()
    with out.open("a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["datetime", "passed", "status", "test", "target", "description"])
        if new_file:
            w.writeheader()
        w.writerows(res.rows)
    n_pass = sum(r["status"] == "PASS" for r in res.rows)
    n_warn = sum(r["status"] == "WARN" for r in res.rows)
    print(f"\n{n_pass} PASS, {n_warn} WARN, {res.failed} FAIL -> {out}")
    runlog(cfg, "end", "verify-structure", "ok" if res.failed == 0 else "fail", status="ok" if res.failed == 0 else "fail",
           collection=str(collection.name), passed=n_pass, warned=n_warn, failed=res.failed, results=str(out.name),
           failed_checks=[f"{r['test']} {r['target']}" for r in res.rows if r["status"] == "FAIL"][:20])
    return 0 if res.failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
