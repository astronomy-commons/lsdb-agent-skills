#!/usr/bin/env python
"""compare_versions.py — compare a newly built HATS catalog with an existing HATS version of the same data
(a re-import, a mirror, or a validation import against the team's published copy).

What it reports:
  * schema: columns only in one side, columns with different Arrow types, each type change classified as a
    narrowing (same kind, fewer bits: double -> float, int64 -> int32), a widening, a sign change (int32 <-> uint32)
    or a kind change (anything else);
  * partitioning and writer: partition counts and orders, rows per partition, row groups, codec, page index,
    builder versions, default columns (old side: only the partitions that overlap the compared region);
  * rows: for the compared sky region (the NEW catalog's partitions, restricted by --sample-pixels / --max-rows),
    the number of rows on each side and the id sets (ids only in one side). The old side is read with a fine pixel
    search, so only its rows inside the compared pixels count, whatever its partitioning;
  * values: for the rows present on both sides (joined on the id column), per-column exact equality for
    integers/strings/booleans and, for floats, exact equality plus the maximum absolute and relative difference
    (so a float64 -> float32 cast shows up as a bounded relative difference rather than a failure);
  * a verdict line: "identical"; "identical up to type narrowing" (every type change is a narrowing, no column
    is missing on either side, the row sets agree and every value difference is a float narrowing below 1e-6
    relative); otherwise "DIFFERENT" with the reasons.

Usage:
    compare_versions.py --new <collection or catalog> --old <collection, catalog or URL> --key source_id
                        [--columns a,b,c] [--sample-pixels N] [--max-rows N] [--out compare_versions.csv]
                        [--config catalog.toml]
The OLD side may be far larger than the new one: only the partitions overlapping the compared pixels are read.
The report CSV (one row per column: schema change and value comparison) goes to --out, relative to the work
directory of --config when that file exists, else to the current directory.
Exit code: 0 identical, 3 identical up to type narrowing, 1 different (2 is argparse's usage error).
"""
from __future__ import annotations

import argparse
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _common import check_versions, load_config  # noqa: E402

EXIT_IDENTICAL, EXIT_DIFFERENT, EXIT_NARROWED = 0, 1, 3


def open_main(path: str):
    import hats
    import lsdb

    hc = hats.read_hats(path)
    main = hc.main_catalog if hasattr(hc, "main_catalog") else hc
    cat = lsdb.open_catalog(path, columns="all")
    return main, cat


def schema_of(main) -> dict:
    return {f.name: f.type for f in main.schema}


def _kind(t: pa.DataType) -> str | None:
    if pa.types.is_floating(t):
        return "float"
    if pa.types.is_signed_integer(t):
        return "int"
    if pa.types.is_unsigned_integer(t):
        return "uint"
    return None


def type_change(old: pa.DataType, new: pa.DataType) -> str:
    """How the type changed from the old to the new side: narrowing, widening, sign change or kind change."""
    ko, kn = _kind(old), _kind(new)
    if ko and kn and ko == kn:
        return "narrowing" if new.bit_width < old.bit_width else "widening"
    if {ko, kn} == {"int", "uint"} and old.bit_width == new.bit_width:
        return "sign change"
    return "kind change"


def _footer(path):
    """Parquet footer of one partition file (local path or URL)."""
    from hats.io.file_io import read_parquet_metadata

    return read_parquet_metadata(path)


def _partition_files(main, pixels) -> list:
    from hats.io import paths

    suffix = getattr(main.catalog_info, "npix_suffix", ".parquet") or ".parquet"
    return [paths.pixel_catalog_file(main.catalog_base_dir, pix, npix_suffix=suffix) for pix in pixels]


def writer_summary(main, pixels) -> dict:
    """Writer settings and partition sizes from the footers of the given partitions only."""
    files = _partition_files(main, pixels)
    if not files:
        return {}
    with ThreadPoolExecutor(8) as ex:
        mds = list(ex.map(_footer, files))
    md = mds[0]
    cc = md.row_group(0).column(0)
    rows = [m.num_rows for m in mds]
    return {"partitions_read": len(files), "rows_min": int(min(rows)), "rows_median": int(np.median(rows)),
            "rows_max": int(max(rows)), "row_groups_first_file": md.num_row_groups, "codec": cc.compression,
            "page_index": bool(cc.has_offset_index), "created_by": md.created_by}


def partition_rows(main, pixels) -> dict:
    """(order, pixel) -> rows for the given partitions: from dataset/_metadata when the catalog has one, otherwise
    from the partition footers (catalogs written with create_metadata = false)."""
    from hats.io import paths

    meta = paths.get_parquet_metadata_pointer(main.catalog_base_dir)
    wanted = {(p.order, p.pixel) for p in pixels}
    out: dict = {}
    if meta.exists():
        md = _footer(meta)
        for i in range(md.num_row_groups):
            rg = md.row_group(i)
            m = re.search(r"Norder=(\d+).*Npix=(\d+)", rg.column(0).file_path or "")
            if m and (int(m.group(1)), int(m.group(2))) in wanted:
                key = (int(m.group(1)), int(m.group(2)))
                out[key] = out.get(key, 0) + rg.num_rows
        return out
    with ThreadPoolExecutor(8) as ex:
        counts = list(ex.map(lambda f: _footer(f).num_rows, _partition_files(main, pixels)))
    return {(p.order, p.pixel): n for p, n in zip(pixels, counts)}


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--new", required=True)
    ap.add_argument("--old", required=True)
    ap.add_argument("--key", required=True, help="id column(s) present on both sides; comma-separated for a composite key")
    ap.add_argument("--columns", help="comma-separated subset of columns to compare (default: all common)")
    ap.add_argument("--sample-pixels", type=int, default=0, help="compare only the first N of the new catalog's partitions")
    ap.add_argument("--max-rows", type=int, default=5_000_000,
                    help="row budget for the new side (default 5 000 000; 0 = no budget): partitions are taken in order "
                         "until the budget is reached, so the two frames fit in memory whatever the column count")
    ap.add_argument("--out", default="compare_versions.csv",
                    help="report CSV (default compare_versions.csv in the work directory of --config, else the current directory)")
    ap.add_argument("--config", default="catalog.toml", help="run configuration, used for the work directory and the version check")
    a = ap.parse_args(argv)

    work_dir = Path.cwd()
    if Path(a.config).exists():
        cfg = load_config(a.config)
        work_dir = cfg.work_dir
        check_versions(cfg, need=("hats", "lsdb"), warn_newer=False)
    out_path = Path(a.out) if Path(a.out).is_absolute() else work_dir / a.out
    out_path.parent.mkdir(parents=True, exist_ok=True)

    t0 = time.time()
    new_main, new_cat = open_main(a.new)
    old_main, old_cat = open_main(a.old)
    s_new, s_old = schema_of(new_main), schema_of(old_main)
    only_new = sorted(set(s_new) - set(s_old)); only_old = sorted(set(s_old) - set(s_new))
    common = [c for c in s_new if c in s_old]
    type_diff = {c: (str(s_old[c]), str(s_new[c]), type_change(s_old[c], s_new[c])) for c in common if s_old[c] != s_new[c]}
    print(f"schema: {len(common)} common columns; only in new: {only_new}; only in old: {only_old}")
    for c, (o, n, change) in type_diff.items():
        print(f"  type differs {c}: old {o} -> new {n}  ({change})")

    # the compared region: the new catalog's partitions, restricted by --sample-pixels and --max-rows
    new_pixels = list(new_cat.get_healpix_pixels())
    if a.sample_pixels:
        new_pixels = new_pixels[: a.sample_pixels]
    if a.max_rows:
        # keep the comparison inside a row budget: both sides are held in memory with every common column
        rows_per_pixel = partition_rows(new_main, new_pixels)
        kept, total = [], 0
        for pix in new_pixels:
            n = rows_per_pixel.get((pix.order, pix.pixel), 0)
            if kept and total + n > a.max_rows:
                break
            kept.append(pix); total += n
        if len(kept) < len(new_pixels):
            print(f"row budget {a.max_rows:,}: comparing {len(kept)} of {len(new_pixels)} partitions ({total:,} rows); raise --max-rows for more")
        new_pixels = kept
    new_sel = new_cat.pixel_search(new_pixels)
    # fine=True: the old side keeps only its rows inside the compared pixels, even when its partitions are coarser
    old_sel = old_cat.pixel_search(new_pixels, fine=True)

    new_props = new_main.catalog_info.extra_dict() | {"hats_col_healpix": getattr(new_main.catalog_info, "healpix_column", None)}
    old_props = old_main.catalog_info.extra_dict() | {"hats_col_healpix": getattr(old_main.catalog_info, "healpix_column", None)}
    for label, main_, sel, props in (("new", new_main, new_sel, new_props), ("old", old_main, old_sel, old_props)):
        pixels = list(sel.get_healpix_pixels())
        try:
            summary = writer_summary(main_, pixels)
        except Exception as exc:   # a remote old side without footer access still gets a comparison
            summary = f"footers not readable ({type(exc).__name__})"
        print(f"{label}: builder {props.get('hats_builder')} | max_rows {props.get('hats_max_rows')} | "
              f"order {props.get('hats_order')} | {len(main_.get_healpix_pixels())} partitions, {len(pixels)} in the compared region | "
              f"default cols {len((props.get('hats_cols_default') or '').split())} | writer {summary}")

    keys = [k.strip() for k in a.key.split(",")]
    missing_keys = [k for k in keys if k not in s_new or k not in s_old]
    if missing_keys:
        raise SystemExit(f"--key column(s) {missing_keys} are not on both sides")
    # the spatial-index column is the frame index in lsdb, not a selectable column (older catalogs: _hipscat_index)
    index_cols = {"_healpix_29", "_hipscat_index"} | {p.get("hats_col_healpix") for p in (new_props, old_props) if p.get("hats_col_healpix")}
    cols = keys + [c for c in (a.columns.split(",") if a.columns else common) if c not in keys and c in common and c not in index_cols]
    new_df = new_sel[cols].compute()
    old_df = old_sel[cols].compute()
    print(f"rows in the compared region ({len(new_pixels)} partitions of the new catalog): new {len(new_df):,} | old {len(old_df):,}")
    k_new = set(map(tuple, new_df[keys].to_numpy().tolist())); k_old = set(map(tuple, old_df[keys].to_numpy().tolist()))
    print(f"ids only in new: {len(k_new - k_old):,} | only in old (within the region): {len(k_old - k_new):,} | in both: {len(k_new & k_old):,}")
    dup_new = int(new_df.duplicated(subset=keys).sum()); dup_old = int(old_df.duplicated(subset=keys).sum())
    print(f"duplicate ids within the region: new {dup_new:,} | old {dup_old:,}")

    both = new_df.merge(old_df, on=keys, suffixes=("_new", "_old"))
    rows = []
    for c in cols[len(keys):]:
        n, o = both[f"{c}_new"], both[f"{c}_old"]
        t = s_new[c]
        if pa.types.is_floating(t) or pa.types.is_floating(s_old[c]):
            x = n.to_numpy(dtype=np.float64, na_value=np.nan); y = o.to_numpy(dtype=np.float64, na_value=np.nan)
            eq = (x == y) | (np.isnan(x) & np.isnan(y))
            d = np.abs(x - y); rel = d / np.maximum(np.abs(y), 1e-300)
            rows.append({"column": c, "n": len(both), "n_differ": int((~eq).sum()), "max_abs_diff": float(np.nanmax(d)) if (~eq).any() else 0.0,
                         "max_rel_diff": float(np.nanmax(rel[~eq])) if (~eq).any() else 0.0, "nan_new": int(np.isnan(x).sum()), "nan_old": int(np.isnan(y).sum())})
        else:
            x = n.to_numpy(dtype=object, na_value=None); y = o.to_numpy(dtype=object, na_value=None)
            if pa.types.is_list(t) or pa.types.is_fixed_size_list(t) or pa.types.is_large_list(t):
                # array columns: element-wise, NaN equal to NaN
                def _same(u, v):
                    if u is None or v is None:
                        return u is None and v is None
                    u, v = np.asarray(u), np.asarray(v)
                    if u.shape != v.shape:
                        return False
                    if np.issubdtype(u.dtype, np.floating) or np.issubdtype(v.dtype, np.floating):
                        uf, vf = u.astype(np.float64), v.astype(np.float64)
                        return bool(np.all((uf == vf) | (np.isnan(uf) & np.isnan(vf))))
                    return bool(np.array_equal(u, v))
                ne = int(sum(1 for i in range(len(x)) if not _same(x[i], y[i])))
            else:
                ne = int(sum(1 for i in range(len(x)) if x[i] != y[i]))
            rows.append({"column": c, "n": len(both), "n_differ": ne, "max_abs_diff": np.nan, "max_rel_diff": np.nan,
                         "nan_new": int(n.isna().sum()), "nan_old": int(o.isna().sum())})
    rep = pd.DataFrame(rows, columns=["column", "n", "n_differ", "max_abs_diff", "max_rel_diff", "nan_new", "nan_old"])
    rep["type_old"] = [str(s_old[c]) for c in rep.column]
    rep["type_new"] = [str(s_new[c]) for c in rep.column]
    rep["type_change"] = [type_diff.get(c, (None, None, ""))[2] for c in rep.column]
    one_side = pd.DataFrame([{"column": c, "type_new": str(s_new[c]), "type_old": "", "type_change": "only in new"} for c in only_new]
                            + [{"column": c, "type_new": "", "type_old": str(s_old[c]), "type_change": "only in old"} for c in only_old])
    pd.concat([rep, one_side], ignore_index=True).to_csv(out_path, index=False)
    exact = rep[rep.n_differ == 0]; diff = rep[rep.n_differ > 0]
    print(f"values on {len(both):,} shared rows: {len(exact)}/{len(rep)} columns identical")
    narrow_cols = {c for c, (_, _, change) in type_diff.items() if change == "narrowing"}
    explained = diff[diff.column.isin(narrow_cols) & diff.column.map(lambda c: pa.types.is_floating(s_new[c])).astype(bool) & (diff.max_rel_diff < 1e-6)]
    if not diff.empty:
        print(diff.sort_values("n_differ", ascending=False).head(25).to_string(index=False))
        print(f"of which explained by float narrowing (rel diff < 1e-6): {len(explained)}")

    n_only_new, n_only_old = len(k_new - k_old), len(k_old - k_new)
    reasons = []
    if only_new or only_old:
        reasons.append(f"columns only in new {only_new}, only in old {only_old}")
    not_narrowing = {c: change for c, (_, _, change) in type_diff.items() if change != "narrowing"}
    if not_narrowing:
        reasons.append(f"type changes that are not narrowings: {not_narrowing}")
    unexplained = sorted(set(diff.column) - set(explained.column))
    if unexplained:
        reasons.append(f"values differ in {unexplained}")
    if n_only_new or n_only_old:
        reasons.append(f"row sets differ: {n_only_new:,} ids only in new, {n_only_old:,} only in old (within the compared region)")
    if dup_new != dup_old:
        reasons.append(f"duplicate ids differ: new {dup_new:,}, old {dup_old:,}")
    if reasons:
        verdict, code = "DIFFERENT: " + "; ".join(reasons), EXIT_DIFFERENT
    elif type_diff:
        verdict, code = f"identical up to type narrowing ({sorted(type_diff)})", EXIT_NARROWED
    else:
        verdict, code = "identical", EXIT_IDENTICAL
    print(f"VERDICT: {verdict} ({time.time()-t0:.0f}s); report -> {out_path}")
    return code


if __name__ == "__main__":
    sys.exit(main())
