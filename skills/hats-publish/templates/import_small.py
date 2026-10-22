#!/usr/bin/env python
"""import_small.py — the in-memory route for SMALL catalogs (fewer than about a million rows and under 1 GB
(10^9 bytes) in memory): load the source with catalog_source.load_source, build with lsdb.from_dataframe, write a
collection with the run's properties and [writer] settings.

Usage:
    import_small.py [--config catalog.toml] [--dry-run] [--overwrite]

Use import_collection.py for anything larger; lsdb documents from_dataframe for small tables only, and the margin
generation here is single-process. Index tables are not produced on this route (hats-import's index pipeline can
be run afterwards if one is needed).

Differences from the hats-import route (lsdb 0.11 write_catalog):
- [partitioning].row_groups: `num_rows` is applied (pyarrow row_group_size); `subtile_order_delta` is not available
  here, so each file is written with pyarrow's default row groups (one group for partitions under ~1 M rows);
  `single` is the same default.
- [writer].create_metadata is applied (create_parquet_metadata) to the catalog and its margin.
- Only the default margin is built ([margins].default_arcsec; 0 = no margin); extra margins are not.
- Fixed-size FITS array columns are written as Arrow fixed-size lists, as hats-import's FitsReader with
  flatten_tensors writes them.
"""
from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _common import (  # noqa: E402
    check_versions, load_config, pin_cpus, properties_dict, runlog, validate_properties, writer_kwargs,
)
from catalog_source import load_source  # noqa: E402

SMALL_ROWS, SMALL_BYTES = 1_000_000, 10**9   # 1 GB = 10^9 bytes


def arrays_to_arrow(df: pd.DataFrame) -> list[str]:
    """Object columns holding one numpy array per row (FITS array columns from catalog_source) -> Arrow-backed list
    columns in native byte order: fixed-size lists when every row has the same length (hats-import's FitsReader
    with flatten_tensors), variable-length lists otherwise; a missing row is a null list. lsdb.from_dataframe
    rejects the big-endian object arrays. Returns the converted column names."""
    converted = []
    for c in df.columns:
        if df[c].dtype != object:
            continue
        vals = df[c].to_numpy(dtype=object)
        present = np.array([isinstance(v, np.ndarray) for v in vals])
        if not present.any() or not all(v is None or isinstance(v, np.ndarray) or (isinstance(v, float) and np.isnan(v))
                                        for v in vals):
            continue
        arrays = [np.asarray(v).reshape(-1) for v in vals[present]]
        dtype = arrays[0].dtype.newbyteorder("=")
        arrays = [a.astype(dtype, copy=False) for a in arrays]
        lengths = {len(a) for a in arrays}
        if len(lengths) == 1:
            k = lengths.pop()
            flat = np.zeros((len(vals), k), dtype=dtype)
            flat[present] = np.stack(arrays)
            arr = pa.FixedSizeListArray.from_arrays(pa.array(flat.reshape(-1)), k, mask=pa.array(~present))
        else:
            it = iter(arrays)
            arr = pa.array([next(it) if ok else None for ok in present], type=pa.list_(pa.from_numpy_dtype(dtype)))
        df[c] = pd.array(arr, dtype=pd.ArrowDtype(arr.type))
        converted.append(c)
    return converted


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--config", default="catalog.toml")
    ap.add_argument("--dry-run", action="store_true", help="load, check and show the partition plan; do not write")
    ap.add_argument("--overwrite", action="store_true")
    ap.add_argument("--force-large", action="store_true", help="proceed even above the small-route limits")
    ap.add_argument("--no-pypi", action="store_true")
    a = ap.parse_args(argv)

    cfg = load_config(a.config)
    versions = check_versions(cfg, warn_newer=not a.no_pypi)
    problems = validate_properties(cfg)
    if problems:
        raise SystemExit("fix [properties] first: " + "; ".join(problems))
    cpus = pin_cpus(cfg)
    import dask

    dask.config.set(num_workers=cpus)   # lsdb computes with Dask's threaded scheduler here: one thread per pinned CPU

    import lsdb

    t0 = time.time()
    df, schema_expect, meta = load_source(cfg)
    mem = int(df.memory_usage(deep=True).sum())
    print(f"loaded {len(df):,} rows x {df.shape[1]} columns in {time.time()-t0:.0f}s ({mem/1e9:.2f} GB in memory); "
          f"strip changed: { {k: v for k, v in meta['strip_changed'].items() if v} }")
    if (len(df) > SMALL_ROWS or mem > SMALL_BYTES) and not a.force_large:
        raise SystemExit(f"{len(df):,} rows / {mem/1e9:.1f} GB exceeds the small route ({SMALL_ROWS:,} rows, 1 GB); "
                         "use import_collection.py (hats-import) or pass --force-large")

    cols = cfg["columns"]
    for c in (cols["ra"], cols["dec"]):
        if c not in df.columns:
            raise SystemExit(f"coordinate column {c!r} ([columns].ra/dec) is not in the loaded columns")
    ra, dec = df[cols["ra"]].to_numpy(dtype=float), df[cols["dec"]].to_numpy(dtype=float)
    if not (np.isfinite(ra).all() and np.isfinite(dec).all()):
        raise SystemExit(f"{int((~(np.isfinite(ra) & np.isfinite(dec))).sum()):,} rows have non-finite coordinates; "
                         "set [reader].drop_rows_without_coordinates = true to drop and count them")
    bad = ~((ra >= 0) & (ra < 360) & (dec >= -90) & (dec <= 90))
    if bad.any():
        raise SystemExit(f"{int(bad.sum()):,} rows have coordinates out of range; "
                         "set [reader].drop_rows_without_coordinates = true to drop and count them")
    ids = cols.get("id") or []
    ids = ids if isinstance(ids, list) else [ids]
    if len(ids) == 1 and ids[0] in df.columns and not df[ids[0]].is_unique:
        print(f"note: {ids[0]} is not unique ({df[ids[0]].nunique():,} distinct)")
    missing = [c for c in cols.get("default") or [] if c not in df.columns]
    if missing:
        raise SystemExit(f"[columns].default names columns that do not exist: {missing}")
    expected = int(cfg["source"].get("expected_total_rows") or 0)
    if expected and expected != len(df):
        raise SystemExit(f"row count {len(df):,} != [source].expected_total_rows {expected:,}")

    converted = arrays_to_arrow(df)
    if converted:
        print(f"array columns written as Arrow lists: {converted}")

    p, m = cfg["partitioning"], cfg["margins"]
    default_margin = m.get("default_arcsec", 10)
    default_margin = int(default_margin) if float(default_margin).is_integer() else float(default_margin)  # int -> "_Narcs"
    if not default_margin or default_margin <= 0:
        default_margin = None   # 0 = no margin (lsdb builds none for margin_threshold=None)
    threshold = int(p.get("pixel_threshold") or 0) or 100_000
    t_fd = time.time()
    cat = lsdb.from_dataframe(
        df, ra_column=cols["ra"], dec_column=cols["dec"],
        lowest_order=int(p.get("lowest_order", 0)), highest_order=min(int(p.get("highest_order", 10)), 7),   # lsdb's in-memory partitioner is capped at order 7 here
        drop_empty_siblings=bool(p.get("drop_empty_siblings", True)),
        partition_rows=threshold, margin_threshold=default_margin,
        catalog_name=cfg["names"]["catalog"],
    )
    pixels = cat.get_healpix_pixels()
    orders = pd.Series([px.order for px in pixels]).value_counts().sort_index().to_dict()
    margin_name = cat.margin.hc_structure.catalog_info.catalog_name if cat.margin is not None else None
    print(f"partition plan: {len(pixels)} partitions, orders {orders}; margin {margin_name or 'none'} "
          f"(from_dataframe {time.time() - t_fd:.0f}s)")
    if a.dry_run:
        return 0

    out = cfg.collection_path
    if out.exists() and not a.overwrite:
        raise SystemExit(f"{out} exists; pass --overwrite")
    runlog(cfg, "start", "import", "small route (lsdb.from_dataframe)", rows=len(df), partitions=len(pixels), orders=orders)
    t1 = time.time()
    wk = writer_kwargs(cfg)
    rg = p.get("row_groups") or {}
    if rg.get("strategy") == "num_rows":
        wk["row_group_size"] = int(rg.get("value", 200_000))
    elif rg.get("strategy", "subtile_order_delta") == "subtile_order_delta":
        print("note: [partitioning].row_groups subtile_order_delta is not available on the small route; "
              "files get pyarrow's default row groups")
    cat.write_catalog(
        out, catalog_name=cfg["names"]["catalog"], as_collection=True, overwrite=a.overwrite,
        default_columns=cols.get("default") or None,
        addl_hats_properties=properties_dict(cfg),
        skymap_alt_orders=list(p.get("skymap_alt_orders") or []) or None,
        create_parquet_metadata=bool(cfg["writer"].get("create_metadata", True)),
        create_thumbnail=False, create_per_partition_statistics=True, create_summary=False,
        write_table_kwargs=wk,
    )
    print(f"write_catalog (data, margin, metadata, per-partition statistics): {time.time() - t1:.0f}s")
    # extra margins on the small route: not produced (hats-import's margin pipeline can add them later)
    if m.get("extra_arcsec"):
        print(f"note: extra margins {m['extra_arcsec']} are not built on the small route")
    # lsdb's margin properties do not declare the spatial-index column; add it to both property files
    from _common import both_property_files, write_properties

    if margin_name:
        for prop_file in both_property_files(out / margin_name):
            write_properties(prop_file, {"hats_col_healpix": "_healpix_29", "hats_col_healpix_order": "29"})
    t_ro = time.time()
    reopened = lsdb.open_catalog(out)
    n = reopened.hc_structure.catalog_info.total_rows
    print(f"reopen: {time.time() - t_ro:.0f}s")
    if n != len(df):
        raise SystemExit(f"round trip: {n} rows in the written catalog, {len(df)} loaded")
    elapsed = round(time.time() - t1, 1)
    runlog(cfg, "end", "import", "written and reopened", status="ok", elapsed_s=elapsed, output=str(out),
           margin=reopened.margin.hc_structure.catalog_info.catalog_name if reopened.margin else None, versions=versions)
    print(f"done in {elapsed} s -> {out}; next: write_ancillary.py then verify_structure.py")
    return 0


if __name__ == "__main__":
    sys.exit(main())
