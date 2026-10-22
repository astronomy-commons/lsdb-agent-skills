#!/usr/bin/env python
"""estimate_threshold.py — suggest hats-import partitioning parameters from a sample of the input files.

Rule (hats-import docs, notebooks/estimate_pixel_threshold): keep catalog parquet files between about 200 and
800 MB on disk and never more than a couple of GB. This script writes a sample of the input to parquet with the run's
writer settings, measures the bytes per row on disk, and converts the target file size into a `pixel_threshold`
(rows per partition). MB here is 10^6 bytes ([partitioning].target_file_mb uses the same unit).

The sample is read with the import's own reader chain (import_collection.file_reader: keep-list, column rules,
string stripping, conform_schema), so the measured row width is that of the rows the import will write.

Usage:
    estimate_threshold.py [--config catalog.toml] [--sample-rows 200000] [--files N]
Prints the suggestion and the resulting number of partitions at a few HEALPix orders; with --write it stores
`pixel_threshold` into catalog.toml ([partitioning]) when it is still 0.
"""
from __future__ import annotations

import argparse
import math
import re
import sys
import tempfile
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _common import check_versions, input_files, load_config, pin_cpus, runlog, writer_kwargs  # noqa: E402

MB = 10**6   # file sizes in this script and in [partitioning].target_file_mb


def iter_source_chunks(cfg, files: list[Path], max_rows: int):
    """Yield pyarrow tables from the first input file(s) using the import's reader for the configured format (for
    the indexed formats `files` are index files, as the import reads them)."""
    from import_collection import file_reader

    chunksize = min(int(cfg["reader"].get("chunksize", 500_000)), max_rows)
    reader = file_reader(cfg, files, coordinate_filter=False, chunksize=chunksize)
    got = 0
    for f in files:
        for chunk in reader.read(str(f)):
            table = chunk if isinstance(chunk, pa.Table) else pa.Table.from_pandas(chunk, preserve_index=False)
            yield table
            got += len(table)
            if got >= max_rows:
                return


# Adaptive binning leaves most partitions between a quarter of the threshold and the threshold, so a catalog ends up
# with more partitions than total_rows / threshold: measured 2.4x on a wide catalog with tens of thousands of
# partitions and 2.2x on an all-sky catalog with about two thousand. Parquet cannot serialize a footer over 4 GiB
# (a binary limit, hence GiB below), and dataset/_metadata is one footer holding every row group of every file.
PARTITION_FACTOR = 2.4
FOOTER_LIMIT = 4 * 2**30


def expected_partitions(total_rows: int, threshold: int) -> int:
    """The partition count to plan with: total_rows / threshold x PARTITION_FACTOR."""
    return math.ceil(total_rows / threshold * PARTITION_FACTOR)


def metadata_estimate(cfg, sample: pa.Table, total_rows: int, threshold: int) -> dict | None:
    """Predict the size of dataset/_metadata (catalog) and of a margin's, from the footer bytes one row group adds
    with this schema and writer, the expected number of partitions and the row-group strategy."""
    if not total_rows or not threshold or len(sample) < 8:
        return None
    with tempfile.TemporaryDirectory() as tmp:
        sizes = {}
        for groups in (1, 4):
            out = Path(tmp) / f"rg{groups}.parquet"
            pq.write_table(sample, out, row_group_size=math.ceil(len(sample) / groups), **writer_kwargs(cfg))
            sizes[groups] = pq.read_metadata(out).serialized_size
    per_group = max((sizes[4] - sizes[1]) / 3, 1)
    partitions = expected_partitions(total_rows, threshold)
    rg = (cfg["partitioning"].get("row_groups") or {})
    strategy, value = rg.get("strategy", "subtile_order_delta"), rg.get("value", 1)
    if strategy == "subtile_order_delta":
        groups_per_file = 4 ** int(value)
    elif strategy == "num_rows":
        groups_per_file = max(1, math.ceil(total_rows / partitions / int(value)))
    else:
        groups_per_file = 1
    catalog_bytes = partitions * groups_per_file * per_group
    margin_bytes = partitions * per_group   # margin files are small: one row group each
    create = bool(cfg["writer"].get("create_metadata", True))
    lines = [f"dataset/_metadata estimate: {per_group / 1e3:.1f} kB per row group x ~{groups_per_file} row groups x "
             f"~{partitions:,} partitions = ~{catalog_bytes / 2**30:.2f} GiB (margin: ~{margin_bytes / 2**30:.2f} GiB); "
             f"Parquet's limit is 4 GiB"]
    if catalog_bytes > 0.6 * FOOTER_LIMIT:   # the partition count is known to ~20%, so warn early
        if create:
            lines.append("  WARNING: the catalog's _metadata would come close to or over the 4 GiB limit and the finishing step "
                         "would fail after the whole import. Set [writer].create_metadata = false (verification then works "
                         "from the file footers), or use fewer row groups per file ([partitioning].row_groups).")
        else:
            lines.append("  [writer].create_metadata = false: no _metadata will be written; this is the right setting here.")
    elif not create:
        lines.append("  [writer].create_metadata = false, but _metadata would fit: consider writing it.")
    return {"report": "\n".join(lines), "footer_bytes_per_row_group": round(per_group), "predicted_partitions": partitions,
            "predicted_metadata_gib": round(catalog_bytes / 2**30, 2), "predicted_margin_metadata_gib": round(margin_bytes / 2**30, 2)}


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--config", default="catalog.toml")
    ap.add_argument("--sample-rows", type=int, default=200_000)
    ap.add_argument("--files", type=int, default=2, help="how many input files to sample")
    ap.add_argument("--total-rows", type=int, default=0, help="override [source].expected_total_rows")
    ap.add_argument("--write", action="store_true", help="store the suggested pixel_threshold into catalog.toml if unset")
    ap.add_argument("--no-pypi", action="store_true", help="skip the newer-version lookup")
    a = ap.parse_args(argv)

    cfg = load_config(a.config)
    check_versions(cfg, warn_newer=not a.no_pypi)
    pin_cpus(cfg)   # one process, no Dask cluster: pinning is the only cap that applies
    all_files = input_files(cfg)
    step = max(1, len(all_files) // max(a.files, 1))
    files = all_files[::step][: a.files]   # spread over the inputs: sky-ordered files make the first ones unrepresentative
    tables = list(iter_source_chunks(cfg, files, a.sample_rows))
    sample = pa.concat_tables(tables)
    with tempfile.TemporaryDirectory() as tmp:
        out = Path(tmp) / "sample.parquet"
        pq.write_table(sample, out, **writer_kwargs(cfg))
        on_disk = out.stat().st_size
    bytes_per_row = on_disk / len(sample)
    in_mem = sample.nbytes / len(sample)
    lo_mb, hi_mb = cfg["partitioning"].get("target_file_mb", [200, 800])
    lo_rows, hi_rows = int(lo_mb * MB / bytes_per_row), int(hi_mb * MB / bytes_per_row)
    suggestion = int(round((lo_rows + hi_rows) / 2, -3)) or hi_rows
    total = a.total_rows or int(cfg["source"].get("expected_total_rows") or 0)

    print(f"sampled {len(sample):,} rows from {len(files)} file(s): {bytes_per_row:.1f} B/row on disk "
          f"({in_mem:.1f} B/row in memory, {sample.num_columns} columns)")
    print(f"target {lo_mb}-{hi_mb} MB per file -> pixel_threshold between {lo_rows:,} and {hi_rows:,} rows; suggested {suggestion:,}")
    threshold = int(cfg["partitioning"].get("pixel_threshold") or 0) or suggestion
    if total:
        print(f"expected partitions at pixel_threshold {threshold:,}: ~{expected_partitions(total, threshold):,} "
              f"(total rows {total:,} / threshold x {PARTITION_FACTOR} for adaptive binning)")
        for order in (3, 4, 5, 6, 7):
            per_pixel = total / (12 * 4**order) * 2  # ~half the sky populated is a common case
            print(f"  order {order}: ~{per_pixel:,.0f} rows per populated pixel if the catalog covers half the sky")
    print("set [partitioning].highest_order so that the densest region can still split below the threshold; "
          "a Binning-stage error during import means it needs to be higher.")
    md = metadata_estimate(cfg, sample, total, threshold)
    if md:
        print(md["report"])
    runlog(cfg, "event", "plan", "threshold estimate", bytes_per_row=round(bytes_per_row, 1), suggested_pixel_threshold=suggestion,
           lo_rows=lo_rows, hi_rows=hi_rows, sampled_rows=len(sample),
           **({k: v for k, v in md.items() if k != "report"} if md else {}))
    if a.write and not int(cfg["partitioning"].get("pixel_threshold") or 0):
        text = Path(a.config).read_text()
        text, n = re.subn(r"(?m)^pixel_threshold\s*=\s*0\b", f"pixel_threshold = {suggestion}", text, count=1)
        if n:
            Path(a.config).write_text(text)
            print(f"wrote pixel_threshold = {suggestion} into {a.config}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
