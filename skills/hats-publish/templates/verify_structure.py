#!/usr/bin/env python
"""verify_structure.py — structural verification of a built HATS collection (no source data needed).

Three groups of checks, every one PASS / FAIL / WARN with a one-line reason:

  a. hats-import's own verification pipeline (``VerificationArguments``): valid catalog, file set vs ``_metadata``,
     row counts vs properties / ``_metadata`` / ``--truth-rows``, schema vs ``_common_metadata`` or ``--truth-schema``.
  b. HATS-note compliance: required files and properties per table type, both property files identical, property
     vocabularies, spatial-index column present and first, data thumbnail present with the partition schema,
     per-partition statistics present, VOParquet metadata consistent across partitions, margin/index
     ``hats_primary_table_url`` resolving, default columns existing, down-sampled skymaps present; then defects
     found on served catalogs: stray hive-key columns (Norder/Dir/Npix, _hipscat_index, __index_level_*), files
     that fail a hive-partitioned read, pandas index metadata, float32 coordinates, margin schema and pixel set vs
     the catalog, default margin declared, point-map order and sum, and on a sample of up to 3 partitions (first,
     median size, last): duplicate ids, all-null columns, sort order, coordinate ranges and the recomputed
     ``_healpix_29``.
  c. Parquet encoding sweep over every data partition, margin partition and index part: codec per column chunk,
     page index present, encodings and row-group counts recorded, ``created_by`` recorded, writer level recorded
     as provenance (``--spot-check`` re-encodes one row group at the configured level and compares the size).

Usage:
    verify_structure.py --config catalog.toml [--collection PATH] [--truth-rows N] [--truth-schema PARQUET]
                        [--results-file PATH] [--spot-check] [--no-pypi] [--trial]

``--trial`` is for a trial build (``import_collection.py --trial-file``: no margins, indexes or ancillary files): the
thumbnail, per-partition statistics, alternate skymap orders and the margin/index presence checks become WARN with
"(trial build)"; every data check (stray columns, coordinates, duplicates, encodings) keeps its full strength.
The row truth is then ``--truth-rows`` only (not ``[source].expected_total_rows``, which counts every input file).
The main catalog is the collection's ``hats_primary_table_url`` (``[names].catalog`` only when collection.properties
is absent), so a trial collection ``trial_<collection>/trial_<catalog>`` is found without extra arguments.

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
import json
import logging
import random
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
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
# hive keys and pandas index columns that pre-0.5 hipscat builds wrote into the files (often all null); with them a
# hive-partitioned read fails with "Field Norder has incompatible types"
STRAY_COLUMNS = {"Norder", "Dir", "Npix", "_hipscat_index"}
MARGIN_PIXEL_COLUMNS = {"margin_Norder", "margin_Dir", "margin_Npix"}
TRIAL_SUFFIX = " (trial build)"


class Results:
    def __init__(self, trial: bool = False):
        self.rows: list[dict] = []
        self.trial = trial   # --trial: checks of files a trial build does not write report WARN instead of FAIL

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

    def trial_fail(self, test, target, desc=""):
        """FAIL, or WARN with "(trial build)" under --trial: for files and tables a trial build does not write."""
        if self.trial:
            self.warn(test, target, desc + TRIAL_SUFFIX)
        else:
            self.fail(test, target, desc)

    def check_trial(self, cond: bool, test, target, ok_desc="", fail_desc=""):
        if cond:
            self.ok(test, target, ok_desc)
        else:
            self.trial_fail(test, target, fail_desc)

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
        same = a.read_bytes() == b.read_bytes()
        drift = ""
        if not same:
            pa_, pb_ = read_properties(a), read_properties(b)
            only_a = sorted(set(pa_) - set(pb_))
            only_b = sorted(set(pb_) - set(pa_))
            changed = sorted(k for k in set(pa_) & set(pb_) if pa_[k] != pb_[k])
            drift = (f"; only in hats.properties: {only_a}" if only_a else "") + \
                    (f"; only in properties: {only_b}" if only_b else "") + \
                    (f"; different values: {changed}" if changed else "")
            drift = drift or "; same keys and values (comments, order or whitespace differ)"
        res.check(same, "properties-files-identical", table.name,
                  "hats.properties == properties", "hats.properties and properties differ (edit both)" + drift)
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

    if props.get("obs_regime"):   # a space-separated list is allowed ("Infrared Optical"); every word must be known
        words = props["obs_regime"].split()
        unknown = [w for w in words if w not in OBS_REGIMES]
        res.check(not unknown, "vocabulary:obs_regime", table.name,
                  props["obs_regime"] if len(words) == 1 else f"multiple regimes: {' '.join(words)}",
                  f"{props['obs_regime']!r}: {unknown} not in {sorted(OBS_REGIMES)}")
    if props.get("hats_frame"):
        res.check(props["hats_frame"] in HATS_FRAMES, "vocabulary:hats_frame", table.name, props["hats_frame"],
                  f"{props['hats_frame']!r} not in {sorted(HATS_FRAMES)}")
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
            (res.trial_fail if ttype == "object" else res.warn)("thumbnail-present", table.name,
                                                                 "data_thumbnail.parquet missing (RECOMMENDED for object catalogs)")
        res.check_trial((table / "per_partition_statistics.parquet").exists(), "per-partition-statistics", table.name,
                        "present", "per_partition_statistics.parquet missing (RECOMMENDED)")
        for order in (props.get("hats_skymap_alt_orders") or "").split():
            res.check_trial((table / f"skymap.{order}.fits").exists(), "skymap-alt-order",
                            f"{table.name}/skymap.{order}.fits", "present", "missing")
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


def collection_properties(collection: Path) -> dict:
    cp = collection / "collection.properties"
    return read_properties(cp) if cp.exists() else {}


def primary_table(collection: Path, cfg) -> Path:
    """The main catalog: collection.properties' hats_primary_table_url (relative to the collection, or absolute);
    [names].catalog only when collection.properties is absent or names no table. A trial build is written as
    trial_<collection>/trial_<catalog>, so the configured name alone does not find it."""
    url = collection_properties(collection).get("hats_primary_table_url", "").strip()
    if url:
        return Path(url) if url.startswith("/") else collection / url
    return collection / cfg["names"]["catalog"]


def _expected_margins(cfg) -> int:
    m = cfg["margins"]
    return (1 if float(m.get("default_arcsec") or 0) > 0 else 0) + len(m.get("extra_arcsec") or [])


def check_collection(collection: Path, cfg, res: Results) -> tuple[Path, list[Path], list[Path]]:
    cp = collection / "collection.properties"
    res.check(cp.exists(), "required-file", "collection.properties", "present", "missing")
    props = collection_properties(collection)
    for key in ("obs_collection", "hats_primary_table_url"):
        res.check(bool(props.get(key)), "required-properties", f"collection:{key}", props.get(key, ""), "missing")
    margins = (props.get("all_margins") or "").split()
    default_margin = props.get("default_margin", "")
    if margins:
        res.check(not default_margin or default_margin in margins, "default-margin-listed", default_margin or "(none)",
                  "in all_margins", f"default_margin {default_margin!r} not in all_margins {margins}")
        res.check(bool(default_margin), "default-margin-declared", collection.name, f"default_margin={default_margin}",
                  f"all_margins lists {len(margins)} margin(s) but no default_margin: lsdb never loads a margin")
    else:
        res.ok("default-margin-declared", collection.name, "no all_margins (nothing to declare)")
    tokens = (props.get("all_indexes") or "").split()   # "column dir column dir ..." pairs
    indexes = tokens[1::2] if len(tokens) % 2 == 0 else tokens
    want_margins, want_indexes = _expected_margins(cfg), len(cfg["index"].get("columns") or [])
    res.check_trial(len(margins) >= want_margins, "margins-present", collection.name,
                    f"{len(margins)} margin(s) listed, {want_margins} configured",
                    f"{len(margins)} margin(s) listed in all_margins, {want_margins} configured in [margins]")
    res.check_trial(len(indexes) >= want_indexes, "indexes-present", collection.name,
                    f"{len(indexes)} index table(s) listed, {want_indexes} configured",
                    f"{len(indexes)} index table(s) listed in all_indexes, {want_indexes} configured in [index].columns")
    main = primary_table(collection, cfg)
    res.check(main.is_dir(), "primary-table", main.name, "directory present", f"{main} missing")
    margin_paths = [collection / m for m in margins]
    index_paths = [collection / i for i in indexes]
    return main, margin_paths, index_paths


# ----------------------------------------------------------------------------------------------------------------
# b2. Defects found on served catalogs (data.lsdb.io audit 2026-10-23): schema, margins, point map, sampled values
# ----------------------------------------------------------------------------------------------------------------
def _one_line(exc: Exception) -> str:
    text = f"{type(exc).__name__}: {exc}".strip().splitlines()
    return text[0][:300] if text else type(exc).__name__


def guarded(res: Results, test: str, target: str, fn, *args, **kwargs):
    """Run one check; an exception becomes a WARN "check skipped: <reason>" instead of ending the run."""
    try:
        return fn(*args, **kwargs)
    except Exception as exc:
        res.warn(test, target, f"check skipped: {_one_line(exc)}")
        return None


def _stray(names) -> list[str]:
    return [n for n in names if n in STRAY_COLUMNS or n.startswith("__index_level")]


def _leaf_file(partition: Path) -> Path:
    """A partition is a file or a directory of files; its first file stands for the schema."""
    return partition if partition.is_file() else sorted(partition.rglob("*.parquet"))[0]


def _size(partition: Path) -> int:
    return partition.stat().st_size if partition.is_file() else sum(f.stat().st_size for f in partition.rglob("*.parquet"))


def _read(partition: Path, columns=None) -> pa.Table:
    # partitioning=None: no hive discovery, so files carrying Norder/Dir/Npix columns read cleanly
    return pq.read_table(partition, columns=columns, partitioning=None)


def _table_schema(table: Path) -> pa.Schema:
    cm = table / "dataset" / "_common_metadata"
    if cm.exists():
        return pq.read_schema(cm)
    return pq.read_schema(_leaf_file(data_files(table)[0]))


def _pixels(table: Path) -> set[tuple[int, int]]:
    pi = table / "partition_info.csv"
    if pi.exists():
        import pandas as pd
        df = pd.read_csv(pi)
        return set(zip(df["Norder"].astype(int), df["Npix"].astype(int)))
    pat = re.compile(r"Norder=(\d+)/Dir=\d+/Npix=(\d+)")
    return {(int(m.group(1)), int(m.group(2))) for f in data_files(table) if (m := pat.search(f.as_posix()))}


def sample_partitions(table: Path, n: int = 3) -> list[Path]:
    """Up to n partitions: the first, the median-size one and the last (path order)."""
    files = data_files(table)
    if len(files) <= n:
        return files
    by_size = sorted(files, key=_size)
    picks = [files[0], by_size[len(by_size) // 2], files[-1]]
    return list(dict.fromkeys(picks))


def check_stray_columns(tables: list[Path], sample: list[Path], res: Results) -> None:
    for table in tables:
        sources = {"_common_metadata": _table_schema(table).names}
        if table == tables[0]:
            sources.update({f.name: pq.read_schema(_leaf_file(f)).names for f in sample})
        found = {src: _stray(names) for src, names in sources.items() if _stray(names)}
        res.check(not found, "stray-columns", table.name,
                  f"no Norder/Dir/Npix/_hipscat_index/__index_level_* columns in {len(sources)} schema(s)",
                  "stray columns (pre-0.5 hipscat hive keys or pandas index; break hive-partitioned reads): "
                  + "; ".join(f"{src}: {cols}" for src, cols in found.items()))


def check_hive_read(table: Path, sample: list[Path], res: Results) -> None:
    """pq.read_table(file) with default arguments, and the same file read as a member of the hive-partitioned
    dataset (as a reader of the dataset directory sees it): a Norder/Dir/Npix column in the file clashes with the
    hive key ("Field Norder has incompatible types")."""
    import pyarrow.dataset as ds

    f = _leaf_file(sample[0])
    first = pq.read_schema(f).names[0]
    errors = []
    try:
        pq.read_table(f, columns=[first])
    except Exception as exc:
        errors.append(f"pq.read_table: {_one_line(exc)}")
    try:
        ds.dataset([str(f)], format="parquet", partitioning="hive",
                   partition_base_dir=str(table / "dataset")).to_table(columns=[first])
    except Exception as exc:
        errors.append(f"hive dataset: {_one_line(exc)}")
    res.check(not errors, "hive-read", f"{table.name}/{f.name}",
              "reads with default arguments and as a hive-partitioned dataset member", "; ".join(errors))


def check_pandas_index(table: Path, sample: list[Path], res: Results) -> None:
    sources = {"_common_metadata": _table_schema(table)}
    sources.update({f.name: pq.read_schema(_leaf_file(f)) for f in sample})
    found = {}
    for src, schema in sources.items():
        raw = (schema.metadata or {}).get(b"pandas")
        if raw:
            cols = [c for c in json.loads(raw).get("index_columns", []) if isinstance(c, str)]
            if cols:
                found[src] = cols
    res.check_warn(not found, "pandas-index-metadata", table.name,
                   f"no non-range pandas index declared in {len(sources)} schema(s)",
                   "pandas metadata declares index column(s) (to_pandas sets them as the index): "
                   + "; ".join(f"{src}: {cols}" for src, cols in found.items()))


def check_coordinate_dtype(table: Path, props: dict, res: Results) -> None:
    schema = _table_schema(table)
    for key in ("hats_col_ra", "hats_col_dec"):
        col = props.get(key, "")
        if not col or col not in schema.names:
            res.fail("coordinate-dtype", f"{table.name}:{key}", f"{key}={col!r} not in the schema")
            continue
        t = schema.field(col).type
        if pa.types.is_float64(t):
            res.ok("coordinate-dtype", f"{table.name}:{col}", "double")
        elif pa.types.is_floating(t):
            top = 360 if key == "hats_col_ra" else 90
            step = float(np.spacing(np.array(top, dtype=t.to_pandas_dtype()))) * 3600
            res.fail("coordinate-dtype", f"{table.name}:{col}",
                     f"{t.bit_width}-bit float: value spacing {step:.3f} arcsec at {top} deg; sub-arcsecond precision lost (need double)")
        else:
            res.warn("coordinate-dtype", f"{table.name}:{col}", f"type {t}, expected double")


def check_margin_schema(main: Path, margin: Path, res: Results) -> None:
    cat, mar = _table_schema(main), _table_schema(margin)
    cn = cat.names
    mn = [n for n in mar.names if n not in MARGIN_PIXEL_COLUMNS]
    only_cat = [n for n in cn if n not in set(mn)]
    only_mar = [n for n in mn if n not in set(cn)]
    if only_cat or only_mar:
        res.fail("margin-schema-matches", margin.name,
                 f"column names differ: {len(only_cat)} only in catalog {only_cat[:5]}, "
                 f"{len(only_mar)} only in margin {only_mar[:5]}")
        return
    types = [f"{n}: {cat.field(n).type} vs {mar.field(n).type}" for n in cn if cat.field(n).type != mar.field(n).type]
    if types:
        res.warn("margin-schema-matches", margin.name, f"{len(types)} column(s) differ in type (catalog vs margin): {types[:5]}")
    elif cn != mn:
        res.warn("margin-schema-matches", margin.name, f"same {len(cn)} names and types, different order")
    else:
        res.ok("margin-schema-matches", margin.name, f"{len(cn)} columns, names and types equal the catalog's")


def check_margin_pixels(main: Path, margin: Path, res: Results) -> None:
    cpix, mpix = _pixels(main), _pixels(margin)
    extra = sorted(mpix - cpix)
    res.check_warn(not extra, "margin-pixels-subset", margin.name,
                   f"all {len(mpix):,} margin pixels are catalog pixels ({len(cpix):,})",
                   f"{len(extra):,} of {len(mpix):,} margin pixels not among the catalog's {len(cpix):,} "
                   f"(margin rows for empty catalog pixels; harmless) e.g. {extra[:3]}")


def check_point_map(table: Path, props: dict, res: Results) -> None:
    """open_catalog builds the MOC from point_map.fits on every open (order 10 = 100 MB, order 12 ~95 s); skymap.fits is
    not read on open, so its order is recorded only. Both maps must sum to hats_nrows."""
    from hats.io.file_io import read_fits_image

    names = [n for n in ("point_map.fits", "skymap.fits") if (table / n).exists()]
    if not names:
        res.warn("point-map-order", table.name, "neither point_map.fits nor skymap.fits present")
        return
    nrows = int(props.get("hats_nrows") or -1)
    for name in names:
        target = f"{table.name}/{name}"
        img = read_fits_image(table / name)
        order = int(round(np.log(len(img) / 12) / np.log(4)))
        mb = (table / name).stat().st_size / 1e6
        if name == "point_map.fits":
            res.check_warn(order <= 8, "point-map-order", target, f"order {order} ({len(img):,} cells, {mb:.1f} MB file)",
                           f"order {order} ({len(img):,} cells, {mb:.1f} MB file): open_catalog expands it and rebuilds "
                           "the MOC on every open; order <= 8 recommended")
        else:
            res.ok("point-map-order", target, f"order {order} ({len(img):,} cells, {mb:.1f} MB file; not read by open_catalog)")
        total = int(img.sum())
        res.check(total == nrows, "point-map-sum", target, f"sum {total:,} = hats_nrows",
                  f"sum {total:,} != hats_nrows {nrows:,} (diff {total - nrows:+,})")


def _id_columns(props: dict, cfg) -> list[str]:
    ids = (props.get("hats_cols_survey_id") or "").split()
    if not ids:
        ids = cfg["columns"].get("id") or []
        ids = [ids] if isinstance(ids, str) else list(ids)
    return [i for i in ids if i]


def check_sampled_values(table: Path, props: dict, cfg, sample: list[Path], res: Results) -> None:
    """Duplicate ids, sort order and coordinates on the sampled partitions, reading only the needed columns."""
    target = f"{table.name} ({len(sample)} sampled)"
    schema = _table_schema(table)
    ra, dec = props.get("hats_col_ra", ""), props.get("hats_col_dec", "")
    hpx = props.get("hats_col_healpix") or "_healpix_29"
    hpx = hpx if hpx in schema.names else None
    ids = _id_columns(props, cfg)
    missing_ids = [i for i in ids if i not in schema.names]
    columns = list(dict.fromkeys([c for c in (ra, dec, hpx, *ids) if c and c in schema.names]))
    parts = {f: _read(f, columns) for f in sample}

    def duplicates():
        if not ids or missing_ids:
            res.warn("sampled-duplicate-ids", target, f"no id column to test (ids {ids}, missing {missing_ids})")
            return
        t = pa.concat_tables([p.select(ids) for p in parts.values()])
        unique = t.group_by(ids).aggregate([]).num_rows
        dup = t.num_rows - unique
        share = dup / t.num_rows if t.num_rows else 0.0
        msg = f"{dup:,} duplicate {'+'.join(ids)} rows of {t.num_rows:,} sampled ({share:.3%})"
        if dup == 0:
            res.ok("sampled-duplicate-ids", target, msg)
        elif _table_type(props) == "object" and share > 0.001:
            res.fail("sampled-duplicate-ids", target, msg + "; object catalog, limit 0.1%")
        else:
            res.warn("sampled-duplicate-ids", target, msg + (" (source table: repeats may be legitimate)"
                                                             if _table_type(props) != "object" else ""))

    def sorted_check():
        if not hpx:
            res.warn("sampled-sorted", target, "no spatial index column")
            return
        bad = []
        for f, t in parts.items():
            h = t[hpx].to_numpy()
            descents = int(np.count_nonzero(h[1:] < h[:-1]))
            if descents:
                bad.append(f"{f.name}: {descents:,} descents")
        res.check_warn(not bad, "sampled-sorted", target, f"{hpx} non-decreasing in every sampled partition",
                       f"{hpx} not sorted within {len(bad)} partition(s): {bad}")

    def coordinates():
        from hats.pixel_math.spatial_index import compute_spatial_index

        if not (ra in schema.names and dec in schema.names):
            res.fail("sampled-coordinates", target, f"hats_col_ra/dec {ra!r}/{dec!r} not in the schema")
            return
        r = np.concatenate([t[ra].to_numpy(zero_copy_only=False).astype(np.float64) for t in parts.values()])
        d = np.concatenate([t[dec].to_numpy(zero_copy_only=False).astype(np.float64) for t in parts.values()])
        finite = np.isfinite(r) & np.isfinite(d)
        out = finite & ((r < 0) | (r >= 360) | (d < -90) | (d > 90))
        good = finite & ~out
        n_nonfinite, n_out = int((~finite).sum()), int(out.sum())
        msg = f"{len(r):,} rows: {n_nonfinite:,} non-finite, {n_out:,} out of range"
        mismatch_share = 0.0
        if hpx:
            stored = np.concatenate([t[hpx].to_numpy(zero_copy_only=False) for t in parts.values()]).astype(np.int64)
            recomputed = np.asarray(compute_spatial_index(r[good], d[good]), dtype=np.int64)
            mismatch = int(np.count_nonzero(recomputed != stored[good]))
            mismatch_share = mismatch / max(int(good.sum()), 1)
            msg += f", {hpx} recomputed differs for {mismatch:,} ({mismatch_share:.4%}; limit 0.01%)"
        else:
            msg += ", no spatial index column to recompute"
        res.check(n_nonfinite == 0 and n_out == 0 and mismatch_share <= 1e-4, "sampled-coordinates", target, msg, msg)

    guarded(res, "sampled-duplicate-ids", target, duplicates)
    guarded(res, "sampled-sorted", target, sorted_check)
    guarded(res, "sampled-coordinates", target, coordinates)


def check_all_null(table: Path, sample: list[Path], res: Results) -> None:
    """Columns null in every row of every sampled partition (one full partition in memory at a time)."""
    nulls: dict[str, int] = {}
    types: dict[str, pa.DataType] = {}
    rows = 0
    for f in sample:
        t = _read(f)
        rows += t.num_rows
        for name in t.column_names:
            nulls[name] = nulls.get(name, 0) + t[name].null_count
            types[name] = t.schema.field(name).type
        del t
    names = [n + ("[null type]" if pa.types.is_null(types[n]) else "") for n, c in nulls.items() if rows and c == rows]
    target = f"{table.name} ({len(sample)} sampled)"
    res.check_warn(not names, "sampled-all-null-columns", target,
                   f"no column entirely null in {rows:,} sampled rows ({len(nulls)} columns)",
                   f"{len(names)} of {len(nulls)} columns null in all {rows:,} sampled rows: {names[:15]}"
                   + (f" (+{len(names) - 15} more)" if len(names) > 15 else ""))


def check_served_defects(main: Path, margins: list[Path], props: dict, cfg, res: Results) -> None:
    """Checks for the defects found on served catalogs; each one is skipped with a WARN if it cannot run."""
    sample = guarded(res, "sample-partitions", main.name, sample_partitions, main)
    if sample:
        res.ok("sample-partitions", main.name, "; ".join(f"{f.parent.parent.name}/{f.parent.name}/{f.name} "
                                                       f"{_size(f) / 1e6:.1f} MB" for f in sample))
    elif sample is not None:
        res.warn("sample-partitions", main.name, "no data partitions found")
    sample = sample or []
    guarded(res, "stray-columns", main.name, check_stray_columns, [main, *margins], sample, res)
    if sample:
        guarded(res, "hive-read", main.name, check_hive_read, main, sample, res)
    guarded(res, "pandas-index-metadata", main.name, check_pandas_index, main, sample, res)
    guarded(res, "coordinate-dtype", main.name, check_coordinate_dtype, main, props, res)
    for m in margins:
        guarded(res, "margin-schema-matches", m.name, check_margin_schema, main, m, res)
        guarded(res, "margin-pixels-subset", m.name, check_margin_pixels, main, m, res)
    guarded(res, "point-map-order", main.name, check_point_map, main, props, res)
    if sample:
        guarded(res, "sampled-values", main.name, check_sampled_values, main, props, cfg, sample, res)
        guarded(res, "sampled-all-null-columns", main.name, check_all_null, main, sample, res)


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
    offset_only = 0
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
                    # pyarrow omits the column index when page statistics cannot be formed (pages full of NaN,
                    # e.g. nested float leaves); the offset index is always written with write_page_index, so
                    # only a missing offset index fails
                    if want_index and not cc.has_offset_index:
                        bad_index.append(f"{f.name}:{cc.path_in_schema}")
                    elif want_index and not cc.has_column_index:
                        offset_only += 1
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
                  ("offset+column index present everywhere" if not offset_only else
                   f"offset index present everywhere; {offset_only} chunks with offset index only (NaN pages)")
                  if want_index else "offset+column index absent everywhere",
                  (f"{len(bad_index)} column chunks without an offset index: {bad_index[:5]}" if want_index else
                   f"{len(bad_index)} column chunks with a page index although write_page_index is off: {bad_index[:5]}"))
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
    ap.add_argument("--truth-rows", type=int, default=0, help="expected total rows (default [source].expected_total_rows; none under --trial)")
    ap.add_argument("--truth-schema", help="parquet file whose schema is the truth (default: the catalog's _common_metadata)")
    ap.add_argument("--results-file", metavar="PATH",
                    help="CSV the results are appended to (default: verify_results.csv in the work directory); "
                         "a re-check of a hosted copy should pass its own file so the build's results are kept")
    ap.add_argument("--spot-check", action="store_true", help="re-encode one row group at the configured level and compare sizes")
    ap.add_argument("--no-pypi", action="store_true", help="skip the newer-release lookup")
    ap.add_argument("--trial", action="store_true",
                    help="trial build (import_collection.py --trial-file): thumbnail, per-partition statistics, "
                         "alternate skymaps and margin/index presence become WARN; data checks keep full strength")
    a = ap.parse_args(argv)

    cfg = load_config(a.config)
    versions = check_versions(cfg, warn_newer=not a.no_pypi)
    collection = Path(a.collection).resolve() if a.collection else cfg.collection_path
    if not collection.is_dir():
        raise SystemExit(f"collection not found: {collection}")
    work_dir = cfg.work_dir
    work_dir.mkdir(parents=True, exist_ok=True)
    # a trial build holds one input file: [source].expected_total_rows is not its truth, so footers are checked against
    # the properties / _metadata only, unless --truth-rows is given
    truth_rows = a.truth_rows or (0 if a.trial else int(cfg["source"].get("expected_total_rows") or 0))
    # results of this run, and hats-import's own CSV next to them (named after --results-file, so a re-check of a
    # hosted copy with its own results file keeps the build's two files intact)
    out = Path(a.results_file).resolve() if a.results_file else work_dir / "verify_results.csv"
    out.parent.mkdir(parents=True, exist_ok=True)
    verifier_csv = out.with_name(f"{out.stem}_hats_import.csv") if a.results_file else work_dir / "hats_import_verifier_results.csv"
    res = Results(trial=a.trial)
    runlog(cfg, "start", "verify-structure", f"collection {collection.name}")

    main_catalog = primary_table(collection, cfg)
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
    main, margins, indexes = check_collection(collection, cfg, res)
    no_metadata = not cfg["writer"].get("create_metadata", True)
    main_props = check_table(main, "catalog", collection, res, metadata_optional=no_metadata)
    for m in margins:
        check_table(m, "margin", collection, res, metadata_optional=no_metadata)
    for i in indexes:
        check_table(i, "index", collection, res, metadata_optional=no_metadata)
    check_voparquet([main, *margins], res)
    check_builder_versions(main_props.get("hats_builder", ""), versions.get("hats", ""), main.name, res)
    check_served_defects(main, margins, main_props, cfg, res)

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
