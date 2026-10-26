#!/usr/bin/env python
"""build_verify_notebook.py — generate the value-level verification notebook for one import run.

The notebook compares the built HATS collection against the SOURCE files, row by row and column by column, and
checks everything the structural verification cannot: values, the spatial index recomputed from the positions,
margin completeness, skymap / MOC / per-partition statistics against the data, default columns, and copies.
It fails loud: every check is recorded PASS, FAIL or SKIP (a check the data cannot exercise, with the reason; never
counted as a pass), any cell that raises is a FAIL, and the last cell asserts that nothing failed.

Usage:
    build_verify_notebook.py --config catalog.toml [--collection PATH] [--staging PATH] [--public-url URL]
                             [--out verify_<catalog>.ipynb]
then execute it (run_verify.sh does both). At run time the notebook reads [verify], [columns], [names], [source]
and [margins] from catalog.toml and imports catalog_source.py / _common.py from the templates directory, so the
comparison uses exactly the loader the small import route used.

Assumption: the source table fits in memory (hats-import handles larger catalogs on the build side; for the
comparison, point [source].files at a subset or use --sample-rows to verify a sampled subset of rows).
"""
from __future__ import annotations

import argparse
import re
import os
import sys
from pathlib import Path

import nbformat as nbf

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _common import check_versions, load_config  # noqa: E402


def build(cfg, collection: str, staging: str, public_url: str, templates_dir: str, sample_rows: int, config_path: str):
    cells = []
    md = lambda s: cells.append(nbf.v4.new_markdown_cell(s))  # noqa: E731
    code = lambda s: cells.append(nbf.v4.new_code_cell(s))  # noqa: E731
    cat_name = cfg["names"]["catalog"]

    md(f"""# {cat_name} — source vs HATS verification

**Ground truth:** the source files listed in `catalog.toml` (`[source]`), loaded with `catalog_source.load_source`,
i.e. exactly as the small import route reads them (route "reimport" without `[source].files`: the partition files of
`[run].reimport_from`, read as the import reads them). **Under test:** the collection at `{collection}`.

Rules: no tolerances. Every stored column must be exact (integers and strings equal, floats bit-equal with NaN
matching NaN), the row set identical, and the spatial index, partitioning, skymap and margin must follow from the
source positions. Any FAIL, or any cell that raises, makes the last cell raise; a SKIP names the check the data
could not exercise and why.""")

    code(f'''# parameters (written by build_verify_notebook.py)
CONFIG_PATH = {config_path!r}
COLLECTION = {collection!r}
STAGING = {staging!r}          # another copy of the collection to hash against, or ""
PUBLIC_URL = {public_url!r}    # the served URL to open and compare, or ""
TEMPLATES_DIR = {templates_dir!r}
SAMPLE_ROWS = {sample_rows}      # 0 = all source rows''')

    code('''import tqdm.auto, tqdm.std
tqdm.auto.tqdm = tqdm.std.tqdm   # text progress bars survive a headless nbconvert run; must precede `import lsdb`
import hashlib, json, logging, re, sys, time, urllib.request, warnings
from pathlib import Path
import numpy as np, pandas as pd, pyarrow as pa, pyarrow.compute as pc, pyarrow.parquet as pq
from astropy.coordinates import SkyCoord, search_around_sky
import astropy.units as u
import hats, lsdb
from hats.pixel_math import healpix_shim as hp
from hats.pixel_math.spatial_index import compute_spatial_index
from hats.io.file_io import read_fits_image

sys.path.insert(0, TEMPLATES_DIR)
from _common import load_config, read_properties, data_files, runlog
from catalog_source import load_source, same_type

logging.getLogger().setLevel(logging.WARNING)
warnings.filterwarnings("ignore")
pd.set_option("display.max_rows", 300); pd.set_option("display.width", 200); pd.set_option("display.max_colwidth", 120)

cfg = load_config(CONFIG_PATH)
COLL = Path(COLLECTION)
P_COLL = read_properties(COLL / "collection.properties")
MAIN = COLL / P_COLL["hats_primary_table_url"]
P_MAIN = read_properties(MAIN / "properties") if (MAIN / "properties").exists() else read_properties(MAIN / "hats.properties")
MARGIN_NAME = P_COLL.get("default_margin", "")
P_MARGIN = read_properties(COLL / MARGIN_NAME / "properties") if MARGIN_NAME else {}
RA, DEC = P_MAIN["hats_col_ra"], P_MAIN["hats_col_dec"]
HCOL = P_MAIN.get("hats_col_healpix", "_healpix_29")
HORDER = int(P_MAIN.get("hats_col_healpix_order", 29))
ID_COLS = cfg["columns"].get("id") or []
ID_COLS = ID_COLS if isinstance(ID_COLS, list) else [ID_COLS]
MARGIN_ARCSEC = float(P_MARGIN.get("hats_margin_threshold", cfg["margins"].get("default_arcsec", 10)))
ALL_PAIRS_MAX = int(cfg["verify"].get("all_pairs_max_rows", 20_000_000))

RESULTS = {}   # check -> ("PASS" | "FAIL" | "SKIP", detail)
def record(name, ok, detail=""):
    RESULTS[name] = ("PASS" if ok else "FAIL", str(detail))
    print(f"{'PASS' if ok else 'FAIL'}  {name}: {detail}")
def skip(name, reason):
    """A check this data cannot exercise: reported with its reason, never counted as a pass."""
    RESULTS[name] = ("SKIP", str(reason))
    print(f"SKIP  {name}: {reason}")
CELL_ERRORS = []   # a cell that raises makes the verdict FAIL even though the later cells still run (--allow-errors)
def _track_errors(result):
    if result.error_in_exec is not None:
        CELL_ERRORS.append(f"{type(result.error_in_exec).__name__}: {str(result.error_in_exec)[:200]}")
get_ipython().events.register("post_run_cell", _track_errors)
MIN_BRUTE_PAIRS = 50   # fewer brute-force pairs than this cannot show that the margin is complete
print("lsdb", lsdb.__version__, "| hats", hats.__version__, "| catalog", P_MAIN.get("obs_collection"), "| margin", MARGIN_NAME or None)''')

    md("## 1. Provenance")
    code('''t0 = time.time()
df, schema_expect, meta = load_source(cfg, max_rows=SAMPLE_ROWS or None)
print(f"source: {meta['rows']:,} rows x {df.shape[1]} columns from {len(meta['files'])} file(s) in {time.time()-t0:.0f}s")
print("values changed by stripping blank padding:", {k: v for k, v in meta["strip_changed"].items() if v})
if meta["dropped"] or meta["renamed"] or meta["cast"]:
    print("column rules applied:", {k: meta[k] for k in ("dropped", "renamed", "cast")})
recorded = cfg["source"].get("sha256") or {}
if meta.get("reimport_from"):
    skip("1.source_sha256", "route reimport: the source is the partition files of [run].reimport_from, not the downloaded files")
elif recorded:
    bad = []
    for f in meta["files"]:
        want = recorded.get(Path(f).name)
        if want:
            h = hashlib.sha256()
            with open(f, "rb") as fh:
                for chunk in iter(lambda: fh.read(1 << 24), b""):
                    h.update(chunk)
            if h.hexdigest() != want:
                bad.append(Path(f).name)
    record("1.source_sha256", not bad, f"{len(recorded)} recorded digests; mismatching: {bad}")
else:
    skip("1.source_sha256", "no [source].sha256 recorded at download")
pd.Series({k: P_MAIN.get(k) for k in ["obs_collection", "hats_nrows", "hats_builder", "hats_creation_date", "hats_order",
                                      "hats_max_rows", "moc_sky_fraction", "hats_col_ra", "hats_col_dec", "hats_cols_default",
                                      "hats_cols_survey_id", "obs_regime", "hats_frame"]}).to_frame("value")''')

    md("## 2. Schema vs source\nNames and order must be preserved; the only extra column allowed is the spatial index. Types are compared strictly for FITS and Parquet sources, by family for CSV.")
    code('''schema = pq.read_schema(MAIN / "dataset" / "_common_metadata")
loose = meta["format"] in ("csv", "tsv", "indexed_csv")
exp_names = [n for n, _ in schema_expect]
hats_names = [n for n in schema.names if n != HCOL]
missing = sorted(set(exp_names) - set(schema.names)); extra = sorted(set(schema.names) - set(exp_names))
record("2.columns_present_and_ordered", not missing and extra == [HCOL] and hats_names == exp_names,
       f"missing={missing} extra={extra} order_preserved={hats_names == exp_names}")
bad = [(n, str(schema.field(n).type), str(t)) for n, t in schema_expect if n in schema.names and not same_type(schema.field(n).type, t, loose=loose)]
record("2.dtypes_match_source", not bad, f"{len(bad)} mismatches ({'family' if loose else 'strict'} comparison)" + (f": {bad[:5]}" if bad else ""))
pd.DataFrame([(n, str(t), str(schema.field(n).type) if n in schema.names else "-") for n, t in schema_expect],
             columns=["column", "expected", "hats"]).groupby(["expected", "hats"]).size().to_frame("n_columns")''')

    md("## 3. Row completeness")
    code('''if (MAIN / "dataset" / "_metadata").exists():
    md_main = pq.read_metadata(MAIN / "dataset" / "_metadata")
    rg_sum = sum(md_main.row_group(i).num_rows for i in range(md_main.num_row_groups))
else:   # [writer].create_metadata = false: sum the file footers instead
    from concurrent.futures import ThreadPoolExecutor
    with ThreadPoolExecutor(32) as _ex:
        rg_sum = sum(_ex.map(lambda f: pq.read_metadata(f).num_rows, data_files(MAIN)))
cat_all = lsdb.open_catalog(COLL, columns="all")
if SAMPLE_ROWS:
    # restrict every HATS read to the partitions that hold the sampled source rows (bounded memory)
    _h29 = compute_spatial_index(df[RA].to_numpy(dtype=np.float64), df[DEC].to_numpy(dtype=np.float64), HORDER)
    SAMPLE_PIXELS = [p for p in cat_all.get_healpix_pixels() if np.any((_h29 >> (2 * (HORDER - p.order))) == p.pixel)]
    print(f"sampled run: {len(SAMPLE_PIXELS)} of {len(cat_all.get_healpix_pixels())} partitions hold the {len(df):,} sampled rows")
    cat_all = cat_all.pixel_search(SAMPLE_PIXELS)
else:
    SAMPLE_PIXELS = None
t0 = time.time()
if SAMPLE_ROWS:
    # one partition at a time, keeping only the rows at the sampled positions (the spatial index is the frame
    # index): the kernel holds one partition plus the sample, whatever the id column situation
    _parts = []
    for _pix in SAMPLE_PIXELS:
        _part = cat_all.get_partition(_pix.order, _pix.pixel).compute()
        _parts.append(_part[np.isin(_part.index.to_numpy(dtype=np.int64), _h29)])
        del _part
    hats_df = pd.concat(_parts).reset_index(); del _parts
else:
    hats_df = cat_all.compute().reset_index()
print(f"HATS computed: {len(hats_df):,} rows x {hats_df.shape[1]} cols in {time.time()-t0:.0f}s")
def unpack_nested(frame):
    """[reader].pack_nested: the nested column back into the source's flat list columns (main and margin frames)."""
    if not meta.get("nested") or meta["nested"]["name"] not in frame.columns:
        return frame
    nn = meta["nested"]["name"]
    lists = frame[nn].nest.to_lists()
    return pd.concat([pd.DataFrame(frame.drop(columns=[nn])), lists.set_axis(frame.index)], axis=1)

if meta.get("nested"):
    hats_df = unpack_nested(hats_df)
    print(f"nested column {meta['nested']['name']!r} unpacked into list columns for the comparison")
n_source = meta["rows"]
if SAMPLE_ROWS:
    print(f"sampled run: {n_source:,} source rows checked against the matching HATS rows")
    record("3.row_counts", int(P_MAIN["hats_nrows"]) == rg_sum, f"properties {int(P_MAIN['hats_nrows']):,} | _metadata {rg_sum:,} (sampled run: {len(hats_df):,} rows loaded from {len(SAMPLE_PIXELS)} partitions)")
else:
    record("3.row_counts", n_source == int(P_MAIN["hats_nrows"]) == rg_sum == len(hats_df),
           f"source {n_source:,} | properties {int(P_MAIN['hats_nrows']):,} | _metadata row groups {rg_sum:,} | computed {len(hats_df):,}")

def in_keys(values, keys):
    """Membership test for key arrays (hash based: np.isin on string/object arrays is quadratic)."""
    return pd.Index(np.asarray(values)).isin(np.asarray(keys))

def make_key(frame):
    """One key column for a composite [columns].id (values joined with '|'); single ids are used as they are."""
    parts = [frame[c].astype(str) for c in ID_COLS]
    out = parts[0]
    for part in parts[1:]:
        out = out + "|" + part
    return out.to_numpy(dtype=object)

missing_ids = [c for c in ID_COLS if c not in df.columns]
record("3.id_columns_exist_in_source", not missing_ids, f"[columns].id = {ID_COLS}; not in the source: {missing_ids}")
assert not missing_ids, f"configured id column(s) {missing_ids} do not exist in the source: fix [columns].id (hats_cols_survey_id was written from it)"
COMPOSITE = len(ID_COLS) > 1
if COMPOSITE:
    df["_key"] = make_key(df); hats_df["_key"] = make_key(hats_df)
UNIQUE_ID = bool(ID_COLS) and (df["_key"] if COMPOSITE else df[ID_COLS[0]]).is_unique
if UNIQUE_ID:
    KEY = "_key" if COMPOSITE else ID_COLS[0]
    src_key = df[KEY].to_numpy()
    hats_key = hats_df[KEY].to_numpy(dtype=src_key.dtype)
    if SAMPLE_ROWS:
        hats_df = hats_df[in_keys(hats_key, src_key)].reset_index(drop=True); hats_key = hats_df[KEY].to_numpy(dtype=src_key.dtype)
    record("3.id_sets_identical", np.array_equal(np.sort(src_key), np.sort(hats_key)) and len(np.unique(hats_key)) == len(hats_key),
           f"key {ID_COLS if COMPOSITE else KEY}: {len(np.unique(src_key)):,} source ids, {len(np.unique(hats_key)):,} HATS ids")
else:
    # no unique id: align on the spatial index recomputed from the source positions, ties broken by stable order
    KEY = "_key"
    df[KEY] = compute_spatial_index(df[RA].to_numpy(dtype=float), df[DEC].to_numpy(dtype=float), HORDER)
    hats_df[KEY] = hats_df[HCOL].to_numpy(dtype=np.int64)
    if SAMPLE_ROWS:   # keep only the HATS rows at the sampled positions (exact spatial-index match)
        hats_df = hats_df[np.isin(hats_df[KEY].to_numpy(), df[KEY].to_numpy())].reset_index(drop=True)
    src_counts = pd.Series(df[KEY]).value_counts(); hats_counts = pd.Series(hats_df[KEY]).value_counts()
    record("3.id_sets_identical", src_counts.sort_index().equals(hats_counts.sort_index()),
           f"no unique id column ({ID_COLS}); multiset of spatial-index values identical: {src_counts.sort_index().equals(hats_counts.sort_index())}")

def keys_of(frame):
    """The comparison key of any frame read from HATS, built exactly as for the source frame: the id column, the
    joined composite id (make_key), or, without a unique id, the spatial index."""
    if not UNIQUE_ID:
        return (frame[HCOL] if HCOL in frame.columns else frame.index).to_numpy(dtype=np.int64)
    return make_key(frame) if COMPOSITE else frame[KEY].to_numpy(dtype=src_key.dtype)
KEY_COLS = list(dict.fromkeys((ID_COLS if UNIQUE_ID else []) + [RA, DEC]))   # explicit columns for key lookups''')

    md("## 4. Value equality, all columns\nAligned on the key. Integers and bytes exact; strings exact after stripping blank padding (a HATS null equals a source value that stripped to empty); floats bit-exact with NaN matching NaN.")
    code('''sort_cols = [KEY] + [c for c in ID_COLS if c != KEY and c in df.columns]
if not UNIQUE_ID:
    # rows that share a key (duplicate positions) have no natural order on either side: give those rows only a
    # canonical tie key built from every scalar column, so identical rows line up and only real differences remain
    tie_cols = [c for c in df.columns if c not in sort_cols and c in hats_df.columns and c != "_key"
                and not (len(df) and isinstance(df[c].iloc[0], (np.ndarray, list)))]
    for frame in (df, hats_df):
        dup = frame[KEY].duplicated(keep=False).to_numpy()
        frame["_tie"] = ""
        if dup.any():
            frame.loc[dup, "_tie"] = frame.loc[dup, tie_cols].astype(str).agg("|".join, axis=1).to_numpy()
    print(f"non-unique key: {int(df['_tie'].ne('').sum()):,} source rows share a key with another row; ordered by a canonical tie key")
    sort_cols = sort_cols + ["_tie"]
df = df.sort_values(sort_cols, kind="stable").reset_index(drop=True)
hats_df = hats_df.sort_values(sort_cols, kind="stable").reset_index(drop=True)
if UNIQUE_ID:   # re-capture the keys in the sorted order (they were taken before sorting)
    src_key = df[KEY].to_numpy(); hats_key = hats_df[KEY].to_numpy(dtype=src_key.dtype)
assert len(df) == len(hats_df), f"row count differs after alignment: {len(df)} vs {len(hats_df)}"

def _list_arrow(series):
    """Arrow list array of a pandas column holding lists (Arrow-backed, or object with arrays/lists)."""
    arr = getattr(series, "array", None)
    if arr is not None and hasattr(arr, "_pa_array"):
        return pa.chunked_array(arr._pa_array).combine_chunks()
    return pa.array([None if v is None or (isinstance(v, float) and np.isnan(v)) else list(np.asarray(v).tolist()) for v in series.to_numpy(dtype=object)])


def bit_equal(a, b):
    """Element-wise bit equality of float arrays (b cast to a's dtype), NaN matching NaN whatever its payload:
    unlike ==, -0.0 and 0.0 differ."""
    a = np.ascontiguousarray(a); b = np.ascontiguousarray(b, dtype=a.dtype)
    both_nan = np.isnan(a) & np.isnan(b)
    view = {2: np.uint16, 4: np.uint32, 8: np.uint64}.get(a.dtype.itemsize)
    if view is None:   # float128 and other widths: equal values with the same sign bit
        return ((a == b) & (np.signbit(a) == np.signbit(b))) | both_nan
    return (a.view(view) == b.view(view)) | both_nan


def compare_column(name, a_series, b_series):
    """a = source (numpy-backed), b = HATS (arrow-backed). n_mismatch == 0 means exact."""
    a_type = getattr(a_series.dtype, "pyarrow_dtype", None)
    a_vals = a_series.to_numpy(dtype=object) if a_series.dtype == object else None
    is_arr = np.array([isinstance(v, (np.ndarray, list)) for v in a_vals]) if a_vals is not None else None
    object_arrays = a_vals is not None and bool(is_arr.any())   # rows without an array are nulls (conformed columns)
    fixed_size = object_arrays and len({len(v) for v in a_vals[is_arr]}) == 1
    b_type = getattr(b_series.dtype, "pyarrow_dtype", None)
    b_is_list = b_type is not None and (pa.types.is_list(b_type) or pa.types.is_large_list(b_type) or pa.types.is_fixed_size_list(b_type))
    if b_is_list and not object_arrays and (a_vals is None or pd.isna(a_series).all()):
        # a list column that is null in every sampled source row (a region without that band): HATS must be null too
        return dict(column=name, source_dtype="all null", hats_dtype=str(b_series.dtype), n_mismatch=int(b_series.notna().sum()),
                    max_abs_diff=0.0, nan_source=0, nan_hats=0, null_hats=int(b_series.isna().sum()))
    if (a_type is not None and (pa.types.is_list(a_type) or pa.types.is_large_list(a_type))) or (object_arrays and not fixed_size):   # variable-length lists
        la, lb = _list_arrow(a_series), _list_arrow(b_series)
        len_a, len_b = pc.fill_null(pc.list_value_length(la), -1), pc.fill_null(pc.list_value_length(lb), -1)
        same_len = pc.equal(len_a, len_b).to_numpy(zero_copy_only=False)
        bad_rows = ~same_len
        if same_len.any():
            sa, sb = la.filter(pa.array(same_len)), lb.filter(pa.array(same_len))
            fa, fb = pc.list_flatten(sa), pc.list_flatten(sb)
            if pa.types.is_floating(fa.type) or pa.types.is_floating(fb.type):
                xa = fa.to_numpy(zero_copy_only=False).astype(np.float64); xb = fb.to_numpy(zero_copy_only=False).astype(np.float64)
                eq = bit_equal(xa, xb)
            else:
                both_null = pc.and_(pc.is_null(fa), pc.is_null(fb)).to_numpy(zero_copy_only=False)
                eq = pc.fill_null(pc.equal(fa, fb), False).to_numpy(zero_copy_only=False) | both_null
            if (~eq).any():
                parent = pc.list_parent_indices(sa).to_numpy(zero_copy_only=False)
                bad_rows[np.flatnonzero(same_len)[np.unique(parent[~eq])]] = True
        return dict(column=name, source_dtype=str(a_series.dtype), hats_dtype=str(b_series.dtype), n_mismatch=int(bad_rows.sum()),
                    max_abs_diff=0.0, nan_source=0, nan_hats=0, null_hats=int(b_series.isna().sum()))
    if object_arrays and fixed_size:   # fixed-size array column (FITS TFORM nA); null rows must be null on both sides
        # through Arrow: pandas' to_numpy(object) fails on fixed-size-list columns that contain nulls
        b_vals = _list_arrow(b_series).to_numpy(zero_copy_only=False) if hasattr(getattr(b_series, "array", None), "_pa_array") \
            else b_series.to_numpy(dtype=object)
        b_arr = np.array([isinstance(v, (np.ndarray, list)) for v in b_vals])
        both = is_arr & b_arr
        A = np.stack(list(a_vals[both])); B = np.stack([np.asarray(v, dtype=A.dtype) for v in b_vals[both]])
        if np.issubdtype(A.dtype, np.floating):
            eq = np.all(bit_equal(A, B), axis=1)
        else:
            eq = np.all(A == B, axis=1)
        n_bad = int((~eq).sum()) + int((is_arr != b_arr).sum())
        return dict(column=name, source_dtype=f"array[{A.shape[1]}] {A.dtype}", hats_dtype=str(b_series.dtype), n_mismatch=n_bad,
                    max_abs_diff=float(np.nanmax(np.abs(A - B))) if (~eq).any() and np.issubdtype(A.dtype, np.floating) else 0.0,
                    nan_source=0, nan_hats=0, null_hats=int(b_series.isna().sum()))
    a = a_series.to_numpy()
    nulls_b = int(b_series.isna().sum())
    nan_a = nan_b = 0; maxdiff = 0.0
    if a.dtype == object:
        if hasattr(getattr(b_series, "array", None), "_pa_array"):   # Arrow-backed: nulls -> "" without pandas' typed fill
            b = np.array(["" if v is None else v for v in pa.chunked_array(b_series.array._pa_array).to_pylist()], dtype=object)
        else:
            b = b_series.to_numpy(dtype=object, na_value="")
        a_cmp = np.where(pd.isna(a), "", a)
        mism = int((a_cmp != b).sum())
    elif np.issubdtype(a.dtype, np.floating):
        b = b_series.to_numpy(dtype=a.dtype, na_value=np.nan)
        nan_a, nan_b = int(np.isnan(a).sum()), int(np.isnan(b).sum())
        eq = bit_equal(a, b)
        mism = int((~eq).sum())
        if mism:
            maxdiff = float(np.nanmax(np.abs(a[~eq].astype(np.float64) - b[~eq].astype(np.float64))))
    elif np.issubdtype(a.dtype, np.bool_):
        b = b_series.to_numpy(dtype=bool, na_value=False)
        mism = int((a != b).sum()) + nulls_b
    else:
        if nulls_b:
            mism = len(a)
        else:
            b = b_series.to_numpy(dtype=a.dtype)
            mism = int((a != b).sum())
            if mism:
                maxdiff = float(np.abs(a.astype(np.int64) - b.astype(np.int64)).max())
    return dict(column=name, source_dtype=str(a.dtype), hats_dtype=str(b_series.dtype), n_mismatch=mism,
                max_abs_diff=maxdiff, nan_source=nan_a, nan_hats=nan_b, null_hats=nulls_b)

t0 = time.time()
cols = [c for c in df.columns if c not in ("_key", "_tie") and c in hats_df.columns]
report = pd.DataFrame([compare_column(c, df[c], hats_df[c]) for c in cols])
print(f"compared {len(report)} columns x {len(df):,} rows in {time.time()-t0:.0f}s")
record("4.values_exact_all_columns", (report.n_mismatch == 0).all(),
       f"{int((report.n_mismatch == 0).sum())}/{len(report)} columns exact; {int(report.n_mismatch.sum()):,} mismatching values")
record("4.nan_counts_match", (report.nan_source == report.nan_hats).all(),
       f"{int((report.nan_source > 0).sum())} columns contain NaN; NaN total source {int(report.nan_source.sum()):,} / HATS {int(report.nan_hats.sum()):,}")
report.sort_values(["n_mismatch", "nan_hats"], ascending=False).head(12)''')

    md("## 5. Spatial index and partitioning")
    code('''ra = df[RA].to_numpy(dtype=np.float64); dec = df[DEC].to_numpy(dtype=np.float64)
idx_expected = compute_spatial_index(ra, dec, HORDER)
idx_stored = hats_df[HCOL].to_numpy(dtype=np.int64)
record("5.spatial_index_recomputed_from_source", np.array_equal(idx_expected, idx_stored), f"{int((idx_expected != idx_stored).sum()):,} rows differ")

def tag(part, healpix_pixel):
    out = {"Norder": healpix_pixel.order, "Npix": healpix_pixel.pixel}
    frame = pd.DataFrame(out, index=part.index)
    if UNIQUE_ID:   # an explicit dtype keeps the empty meta frame and the real partitions identical for Dask
        frame[KEY] = pd.Series(make_key(part), index=part.index, dtype="str") if COMPOSITE else part[KEY]   # keep the column's own dtype: meta and partitions agree
    return frame
tagged = cat_all[(ID_COLS if UNIQUE_ID else []) + [RA]].map_partitions(tag, include_pixel=True).compute().reset_index()
tagged = tagged.rename(columns={tagged.columns[0]: HCOL}) if HCOL not in tagged.columns else tagged
if SAMPLE_ROWS:   # keep the tags of the sampled rows only (hats_df was restricted to them above)
    tagged = tagged[in_keys(tagged[KEY].to_numpy(dtype=src_key.dtype), src_key)] if UNIQUE_ID else tagged[np.isin(tagged[HCOL].to_numpy(dtype=np.int64), hats_df[HCOL].to_numpy(dtype=np.int64))]
tagged = tagged.sort_values([c for c in sort_cols if c in tagged.columns] or [HCOL], kind="stable").reset_index(drop=True)
assert np.array_equal(tagged[HCOL].to_numpy(dtype=np.int64), hats_df[HCOL].to_numpy(dtype=np.int64)), "partition tags not aligned with the data frame"
h = tagged[HCOL].to_numpy(dtype=np.int64); norder = tagged["Norder"].to_numpy(dtype=np.int64); npix = tagged["Npix"].to_numpy(dtype=np.int64)
ancestor = h >> (2 * (HORDER - norder))
record("5.every_row_inside_its_partition_pixel", np.array_equal(ancestor, npix), f"{int((ancestor != npix).sum()):,} rows outside their partition")
tagged.groupby("Norder").agg(partitions=("Npix", "nunique"), rows=(HCOL, "size"))''')

    md("## 6. Margin cache\nEach margin row must be an exact copy of a main-catalog row and lie outside its own partition pixel. Completeness is one-way containment, as the HATS note requires: every pair within the threshold found by brute force must be found by the lsdb self-crossmatch (the margin MAY hold extra rows).")
    code('''if (not MARGIN_NAME or cat_all.margin is None) and float(cfg["margins"].get("default_arcsec", 10) or 0) == 0:
    skip("6.margin_present", "no margin by configuration ([margins].default_arcsec = 0, for tests only)")
elif not MARGIN_NAME or cat_all.margin is None:
    record("6.margin_present", False, "collection has no default margin")
else:
    # the margin is read from the unsearched catalog, partition by partition: in lsdb 0.11 a pixel search on a
    # margin (directly or through its main catalog) drops margin rows, so the sampled partitions are picked by hand
    margin_cat = lsdb.open_catalog(COLL, columns="all").margin
    MARGIN_PIXELS = [p for p in margin_cat.get_healpix_pixels() if SAMPLE_PIXELS is None or p in set(SAMPLE_PIXELS)]
    _mparts = [margin_cat.get_partition(p.order, p.pixel).compute() for p in MARGIN_PIXELS]
    if not _mparts:   # no margin rows in the partitions read: an empty frame with the margin's columns
        _mparts = [pd.DataFrame({c: pd.Series(dtype=t) for c, t in margin_cat.dtypes.items()}, index=pd.Index([], dtype=np.int64, name=HCOL))]
    mdf = unpack_nested(pd.concat(_mparts).reset_index()); del _mparts
    if COMPOSITE:
        mdf["_key"] = make_key(mdf)
    MAIN_PIX = [(p.order, p.pixel) for p in hats.read_hats(COLL).main_catalog.get_healpix_pixels()]
    def partition_of(h29):
        \"\"\"(order, pixel) arrays of the main-catalog partition holding each spatial-index value (-1 when none does).\"\"\"
        h29 = np.asarray(h29, dtype=np.int64)
        order = np.full(len(h29), -1, dtype=np.int64); pixel = np.full(len(h29), -1, dtype=np.int64)
        for o in sorted({o for o, _ in MAIN_PIX}):
            anc = h29 >> (2 * (HORDER - o))
            hit = np.isin(anc, np.array([q for oo, q in MAIN_PIX if oo == o], dtype=np.int64))
            order[hit] = o; pixel[hit] = anc[hit]
        return order, pixel
    if SAMPLE_PIXELS is None:
        record("6.margin_row_count_matches_properties", len(mdf) == int(P_MARGIN["hats_nrows"]), f"{len(mdf):,} rows")
        if UNIQUE_ID:
            m_key = keys_of(mdf)
            record("6.margin_ids_exist_in_main", in_keys(m_key, src_key).all(), f"{int((~in_keys(m_key, src_key)).sum())} unknown ids; {len(np.unique(m_key)):,} distinct sources in the margin")
    else:
        # every margin row of the sampled partitions, before any filtering, must be a row of the main catalog: look
        # each one up in the main partition that holds its position, reading only the key columns of those partitions
        print(f"sampled run: {len(mdf):,} margin rows in the sampled partitions")
        m_order, m_pixel = partition_of(mdf[HCOL].to_numpy(dtype=np.int64))
        m_keys = keys_of(mdf)
        found = np.zeros(len(mdf), dtype=bool)
        cat_key_cols = lsdb.open_catalog(COLL, columns=KEY_COLS)
        for o, q in sorted(set(zip(m_order[m_order >= 0].tolist(), m_pixel[m_order >= 0].tolist()))):
            here = (m_order == o) & (m_pixel == q)
            found[here] = in_keys(m_keys[here], keys_of(cat_key_cols.get_partition(o, q).compute().reset_index()))
        if len(mdf):
            record("6.margin_ids_exist_in_main", found.all(),
                   f"{len(mdf):,} margin rows of {len(SAMPLE_PIXELS)} sampled partitions looked up in their main partitions: "
                   f"{int((~found).sum())} not found ({int((m_order < 0).sum())} outside every main partition)")
        else:
            skip("6.margin_ids_exist_in_main", f"the {len(SAMPLE_PIXELS)} sampled partitions have no margin rows")
        # the value comparisons below use the source sample: keep the margin rows of sampled sources only
        mdf = mdf[in_keys(keys_of(mdf), src_key) if UNIQUE_ID else np.isin(keys_of(mdf), df[KEY].to_numpy())].reset_index(drop=True)
    if UNIQUE_ID:
        m_sorted = mdf.sort_values(KEY, kind="stable").reset_index(drop=True)
        ref = df.set_index(KEY).loc[m_sorted[KEY].to_numpy(dtype=src_key.dtype)].reset_index()
        if len(m_sorted):
            mrep = pd.DataFrame([compare_column(c, ref[c], m_sorted[c]) for c in cols])
            record("6.margin_values_equal_source", (mrep.n_mismatch == 0).all(), f"{int(mrep.n_mismatch.sum())} mismatching values across {len(mrep)} columns, {len(m_sorted):,} margin rows")
        else:
            skip("6.margin_values_equal_source", "no margin rows" + (" of sampled sources" if SAMPLE_PIXELS is not None else ""))
    else:
        print("no unique id: margin value equality and completeness are checked on positions only")
    if len(mdf):
        m_idx = compute_spatial_index(mdf[RA].to_numpy(dtype=np.float64), mdf[DEC].to_numpy(dtype=np.float64), HORDER)
        record("6.margin_spatial_index_matches_position", np.array_equal(m_idx, mdf[HCOL].to_numpy(dtype=np.int64)), f"{len(mdf):,} rows")
    else:
        skip("6.margin_spatial_index_matches_position", "no margin rows to check")
    inside = n_margin_rows = 0
    for pix in MARGIN_PIXELS:
        part = margin_cat.get_partition(pix.order, pix.pixel).compute().reset_index()
        h29 = part[HCOL].to_numpy(dtype=np.int64)
        inside += int(((h29 >> (2 * (HORDER - pix.order))) == pix.pixel).sum()); n_margin_rows += len(part)
    if n_margin_rows:
        record("6.margin_rows_outside_own_pixel", inside == 0, f"{inside} of {n_margin_rows:,} margin rows fall inside their own pixel")
    else:
        skip("6.margin_rows_outside_own_pixel", "the margin partitions read hold no rows")

    if UNIQUE_ID:
        n = len(df)
        # brute force on every row up to all_pairs_max_rows, else on a random subset; a subset (or a sampled run) is
        # sparser than the partitions it lies in, so the lsdb side then keeps only those rows on the left
        SUBSAMPLED = bool(SAMPLE_ROWS) or n > ALL_PAIRS_MAX
        sel = np.arange(n) if n <= ALL_PAIRS_MAX else np.sort(np.random.default_rng(0).choice(n, min(1_000_000, ALL_PAIRS_MAX), replace=False))
        coords = SkyCoord(ra[sel] * u.deg, dec[sel] * u.deg)
        t0 = time.time()
        i1, i2, sep, _ = search_around_sky(coords, coords, MARGIN_ARCSEC * u.arcsec)
        keep = i1 != i2
        keys = src_key[sel]
        pairs_brute = set(zip(keys[i1[keep]].tolist(), keys[i2[keep]].tolist()))
        max_nb = int(np.bincount(i1[keep]).max()) if keep.any() else 0
        print(f"brute force on {len(sel):,} rows: {len(pairs_brute):,} ordered pairs within {MARGIN_ARCSEC:g} arcsec, max neighbours {max_nb}, {time.time()-t0:.0f}s")
        def side_keys(frame, suffix):
            \"\"\"Keys of one side of a crossmatch result, built with make_key like every other frame.\"\"\"
            renamed = frame.rename(columns={f"{c}{suffix}": c for c in ID_COLS})
            return make_key(renamed) if COMPOSITE else renamed[KEY].to_numpy(dtype=src_key.dtype)
        sub = cat_all[KEY_COLS]
        # right side from the unsearched catalog: a pixel-searched catalog carries a margin with rows dropped (see
        # above); the crossmatch still reads only the right partitions that overlap the left side
        right = lsdb.open_catalog(COLL, columns=KEY_COLS) if SAMPLE_PIXELS is not None else sub
        if SUBSAMPLED:
            # left side = the brute-force rows only (the brute-force pairs are among them); the right side keeps the
            # whole partitions and their margin, which are denser than the subset, so the neighbour cap measured on
            # the subset is not enough: start generous and recompute with a larger cap whenever it is reached
            _sel_h29 = compute_spatial_index(ra[sel], dec[sel], HORDER)
            left = sub.map_partitions(lambda part, _h=_sel_h29: part[np.isin(part.index.to_numpy(dtype=np.int64), _h)])
            n_nb = max(max_nb + 1, 64)
        else:
            left, n_nb = sub, max_nb + 1
        t0 = time.time()
        while True:
            xm = left.crossmatch(right, radius_arcsec=MARGIN_ARCSEC, n_neighbors=n_nb, suffixes=("_l", "_r")).compute()
            l = side_keys(xm, "_l")
            cap_reached = bool(len(xm)) and int(pd.Series(l).value_counts().max()) >= n_nb
            if not (SUBSAMPLED and cap_reached) or n_nb >= 100_000:
                break
            n_nb *= 8
            print(f"neighbour cap reached; recomputing with n_neighbors={n_nb}")
        r = side_keys(xm, "_r")
        pairs_lsdb = set(zip(l[l != r].tolist(), r[l != r].tolist()))
        missing_pairs = pairs_brute - pairs_lsdb
        detail = (f"brute force {len(pairs_brute):,} pairs among {len(sel):,} rows, lsdb {len(pairs_lsdb):,} (n_neighbors {n_nb}); "
                  f"missing from lsdb {len(missing_pairs)} ({time.time()-t0:.0f}s)")
        if missing_pairs or len(pairs_brute) >= MIN_BRUTE_PAIRS:
            record("6.margin_complete_all_pairs_contained", not missing_pairs, detail)
        else:
            skip("6.margin_complete_all_pairs_contained", f"too few brute-force pairs to test completeness (at least {MIN_BRUTE_PAIRS} needed): {detail}")
        if missing_pairs:
            pos = df.set_index(KEY)[[RA, DEC]]
            for a_, b_ in list(missing_pairs)[:10]:
                s_ = SkyCoord(*pos.loc[a_].to_numpy() * u.deg).separation(SkyCoord(*pos.loc[b_].to_numpy() * u.deg)).arcsec
                print(f"  missing pair ({a_}, {b_}): separation {s_:.6f} arcsec")
        # control: the same crossmatch without the margin; brute-force pairs whose two rows lie in different main
        # partitions can only be found through the margin
        bare = lsdb.open_catalog(MAIN, columns=KEY_COLS)
        xb = left.crossmatch(bare, radius_arcsec=MARGIN_ARCSEC, n_neighbors=n_nb, suffixes=("_l", "_r"), require_right_margin=False).compute()
        lb = side_keys(xb, "_l"); rb = side_keys(xb, "_r")
        pairs_control = set(zip(lb[lb != rb].tolist(), rb[lb != rb].tolist()))
        b_order, b_pixel = partition_of(compute_spatial_index(ra[sel], dec[sel], HORDER))
        home = dict(zip(keys.tolist(), zip(b_order.tolist(), b_pixel.tolist())))
        cross = {pr for pr in pairs_brute if home[pr[0]] != home[pr[1]]}
        via_margin = cross & (pairs_lsdb - pairs_control)
        print(f"control without margin: {len(pairs_control):,} pairs; {len(pairs_lsdb - pairs_control):,} found only through the margin")
        if cross:
            record("6.margin_recovers_boundary_pairs", via_margin == cross,
                   f"{len(cross):,} brute-force pairs cross a partition boundary; {len(via_margin):,} of them found by lsdb only with the margin, "
                   f"{len(cross & pairs_control)} found without it")
        else:
            skip("6.margin_recovers_boundary_pairs", f"no brute-force pair crosses a partition boundary among {len(sel):,} rows")
    else:
        skip("6.margin_complete_all_pairs_contained", "no unique id column: pairs cannot be keyed")''')

    md("## 7. Metadata consistency: partition list, per-partition statistics, skymap, MOC")
    code('''pinfo = pd.read_csv(MAIN / "partition_info.csv")
hc_main = hats.read_hats(COLL).main_catalog
pix_tree = sorted((p.order, p.pixel) for p in hc_main.get_healpix_pixels())   # the whole catalog, also in sample mode
files = sorted((int(re.search(r"Norder=(\\d+)", str(p)).group(1)), int(re.search(r"Npix=(\\d+)", str(p)).group(1))) for p in data_files(MAIN))
pinfo_pairs = sorted(zip(pinfo["Norder"].astype(int), pinfo["Npix"].astype(int)))
record("7.partition_list_consistent", pix_tree == files == pinfo_pairs, f"{len(files)} data files | {len(pinfo_pairs)} partition_info rows | {len(pix_tree)} pixels in the tree")

stat_cols = [c for c in [RA, DEC] + ([KEY] if UNIQUE_ID and pd.api.types.is_numeric_dtype(hats_df[KEY]) else []) if c in hats_df.columns]   # min/max compared as numbers
pps = hc_main.per_pixel_statistics(include_columns=stat_cols, include_stats=["min_value", "max_value", "row_count"])
pps.index = [(p.order, p.pixel) for p in pps.index]
frame = pd.concat([hats_df[stat_cols].reset_index(drop=True), tagged[["Norder", "Npix"]].reset_index(drop=True)], axis=1) if not SAMPLE_ROWS else None
if frame is not None:
    agg = {f"{c}: min_value": (c, "min") for c in stat_cols} | {f"{c}: max_value": (c, "max") for c in stat_cols} | {f"{RA}: row_count": (RA, "size")}
    actual = frame.groupby(["Norder", "Npix"]).agg(**agg); actual.index = list(actual.index)
    common = [c for c in actual.columns if c in pps.columns]
    if not common or not len(actual):
        skip("7.per_partition_statistics_match_data", f"nothing to compare: statistics {list(pps.columns)[:6]}, data columns {stat_cols}, {len(actual)} partitions")
    else:
        diff = int((pps.loc[actual.index, common].astype(np.float64).to_numpy() != actual[common].astype(np.float64).to_numpy()).sum())
        record("7.per_partition_statistics_match_data", diff == 0 and len(pps) == len(actual), f"{len(common)} stats x {len(pps)} partitions; {diff} disagreements")
else:
    skip("7.per_partition_statistics_match_data", "sampled run: the statistics describe whole partitions, the sample does not")

order = int(P_MAIN.get("hats_skymap_order", 8))
skymap = read_fits_image(MAIN / "skymap.fits").astype(np.int64)
if not SAMPLE_ROWS:
    counts = np.bincount(hp.radec2pix(order, ra, dec), minlength=12 * 4**order)
    record("7.skymap_counts_match_source_positions", np.array_equal(counts, skymap), f"order {order}: {int(skymap.sum()):,} sources, {int((skymap > 0).sum()):,} non-empty pixels")
else:
    skip("7.skymap_counts_match_source_positions", "sampled run: the skymap counts every row, the sample does not")
moc = cat_all.hc_structure.moc
if moc is not None:
    inside = moc.contains_lonlat(ra * u.deg, dec * u.deg)
    record("7.all_source_positions_inside_moc", inside.all(), f"sky fraction {moc.sky_fraction:.5f}, max order {moc.max_order}; {int((~inside).sum())} positions outside")
else:
    record("7.all_source_positions_inside_moc", False, "catalog has no MOC")

# cone-search test points: a source inside the densest and inside the sparsest non-empty skymap pixel
pix8 = hp.radec2pix(order, ra, dec)
nonzero = np.flatnonzero(skymap > 0)
CONE_POINTS = []
for which, target in (("densest", nonzero[np.argmax(skymap[nonzero])]), ("sparsest", nonzero[np.argmin(skymap[nonzero])])):
    rows = np.flatnonzero(pix8 == target)
    if len(rows):
        j = rows[0]; CONE_POINTS.append((which, float(ra[j]), float(dec[j])))
cat_def = lsdb.open_catalog(COLL)
cat_key_all = lsdb.open_catalog(COLL, columns=KEY_COLS)   # the id columns explicitly: they need not be default columns
for which, ra0, dec0 in CONE_POINTS:
    cone = cat_key_all.cone_search(ra=ra0, dec=dec0, radius_arcsec=3600.0).compute()
    if SAMPLE_ROWS:   # the brute force below sees the sampled rows only: compare lsdb's cone on the same rows
        cone = cone[in_keys(keys_of(cone), src_key)] if UNIQUE_ID else cone[np.isin(cone.index.to_numpy(dtype=np.int64), _h29)]
    sep = SkyCoord(ra * u.deg, dec * u.deg).separation(SkyCoord(ra0 * u.deg, dec0 * u.deg)).arcsec
    record(f"7.cone_search_{which}", len(cone) == int((sep <= 3600.0).sum()), f"({ra0:.4f}, {dec0:.4f}) 1 degree: lsdb {len(cone)} vs brute force {int((sep <= 3600.0).sum())}")''')

    md("## 8. Default columns")
    code('''defaults = (P_MAIN.get("hats_cols_default") or "").split()
if defaults:
    top = list(dict.fromkeys(d.split(".")[0] for d in defaults))   # nested defaults are frame.subcol
    subcols = {}
    for d in defaults:
        if "." in d:
            subcols.setdefault(d.split(".")[0], []).append(d.split(".", 1)[1])
    opened = list(cat_def.columns)
    added = [c for c in (RA, DEC) if c not in top]   # lsdb always loads the coordinate columns
    nested_ok = {k: list(getattr(cat_def.dtypes[k], "column_dtypes", None) or []) == v for k, v in subcols.items() if k in opened}
    ok = (set(top) <= set(schema.names) and [c for c in opened if c not in added] == top
          and set(nested_ok) == set(subcols) and all(nested_ok.values()))
    record("8.default_columns_declared_and_applied", ok,
           f"{len(defaults)} declared ({len(top)} top-level" + (f"; nested subcolumns {subcols}: as declared {nested_ok}" if subcols else "")
           + f"); opened without a column list -> {opened}" + (f" (coordinates {added} added by lsdb)" if added else ""))
else:
    record("8.default_columns_declared_and_applied", False, "hats_cols_default is not set (set it: wide tables without defaults read everything)")''')

    md("## 9. Copies: staging vs collection vs the public URL")
    code('''PER_COPY_KEYS = (b"hats_status=", b"hats_service_url=", b"hats_primary_table_url=")
def strip_per_copy(data):
    """A property file without the keys that differ between copies by design."""
    return b"\\n".join(l for l in data.splitlines() if not l.startswith(PER_COPY_KEYS))
def sha_tree(root):
    """sha256 of every file under root; property files are hashed without their per-copy keys."""
    out = {}
    for p in sorted(Path(root).rglob("*")):
        if p.is_file() and p.name.endswith("properties"):
            out[str(p.relative_to(root))] = hashlib.sha256(strip_per_copy(p.read_bytes())).hexdigest()
        elif p.is_file():
            h = hashlib.sha256()
            with open(p, "rb") as f:
                for chunk in iter(lambda: f.read(1 << 24), b""):
                    h.update(chunk)
            out[str(p.relative_to(root))] = h.hexdigest()
    return out
if STAGING:
    t0 = time.time(); s1, s2 = sha_tree(STAGING), sha_tree(COLL)
    differ = sorted(k for k in s1.keys() | s2.keys() if s1.get(k) != s2.get(k))
    record("9.copy_identical_to_staging", not differ,
           f"{len(s2)} files hashed on each side (property files without {[k.decode().rstrip('=') for k in PER_COPY_KEYS]}) "
           f"in {time.time()-t0:.0f}s; differing or missing on one side: {differ[:10]}")
if PUBLIC_URL:
    diffs = []
    for rel in ["collection.properties", f"{MAIN.name}/properties", f"{MAIN.name}/partition_info.csv"] + ([f"{MARGIN_NAME}/properties"] if MARGIN_NAME else []):
        try:
            remote = urllib.request.urlopen(f"{PUBLIC_URL}/{rel}", timeout=120).read()
        except Exception as e:
            diffs.append(f"{rel} ({type(e).__name__})"); continue
        local = (COLL / rel).read_bytes()
        if strip_per_copy(remote) != strip_per_copy(local):   # per-copy keys legitimately differ between copies
            diffs.append(rel)
    record("9.public_metadata_files_identical", not diffs, f"fetched from {PUBLIC_URL}; differing (ignoring per-copy keys): {diffs}")
    pub = lsdb.open_catalog(PUBLIC_URL, columns=KEY_COLS)
    which, ra0, dec0 = CONE_POINTS[0]
    cone_pub = pub.cone_search(ra=ra0, dec=dec0, radius_arcsec=3600.0).compute()
    cone_loc = cat_key_all.cone_search(ra=ra0, dec=dec0, radius_arcsec=3600.0).compute()
    same = (pub.hc_structure.catalog_info.total_rows == int(P_MAIN["hats_nrows"]) and len(cone_pub) == len(cone_loc)
            and sorted(keys_of(cone_pub).tolist()) == sorted(keys_of(cone_loc).tolist()))
    record("9.public_catalog_opens_and_matches_local", same,
           f"public rows {pub.hc_structure.catalog_info.total_rows:,}; cone public {len(cone_pub)} vs local {len(cone_loc)}; margin attached: {pub.margin is not None}")
if not STAGING and not PUBLIC_URL:
    skip("9.copies", "no --staging or --public-url given")''')

    md("## 9b. Nested product\nWhen `[nested]` is set: the product written by `attach_nested.py` next to the flat catalog must hold every flat row that has a match in the reference, one nested column of per-object lists whose lengths agree row by row (and with the count column when there is one), and arrays identical to the reference catalog's for a sample of objects.")
    code(r"""NESTED = cfg.get("nested") or {}
if NESTED.get("output") and not (COLL.parent / NESTED["output"]).exists():
    print(f"nested product {NESTED['output']} not built yet (attach_nested.py); section skipped")
elif NESTED.get("output"):
    nested_path = COLL.parent / NESTED["output"]; nested_name = NESTED.get("name") or "nested"
    ncat = lsdb.open_catalog(nested_path, columns="all")
    n_rows = ncat.hc_structure.catalog_info.total_rows
    unmatched = 0
    for _l in open(cfg.work_dir / "runlog.jsonl"):
        _e = json.loads(_l)
        if _e.get("phase") == "nested" and _e.get("kind") == "end":   # run-log entries keep their values under "data"
            unmatched = int((_e.get("data") or {}).get("unmatched", _e.get("unmatched", 0)) or 0)
    record("9b.nested_rows_reconciled", n_rows + unmatched == int(P_MAIN["hats_nrows"]),
           f"nested {n_rows:,} + unmatched {unmatched:,} = flat {int(P_MAIN['hats_nrows']):,}")
    record("9b.nested_column_present", nested_name in ncat.columns and type(ncat.dtypes[nested_name]).__name__ == "NestedDtype",
           f"{nested_name}: {type(ncat.dtypes.get(nested_name)).__name__ if nested_name in ncat.columns else 'absent'}; columns {list(ncat.columns)[:6]}...")
    NID = NESTED["id"]; LISTS = list(NESTED["list_columns"])
    COUNT_COL = next((c for c in NESTED.get("columns", []) if c not in LISTS and c.startswith("n_") and c in ncat.columns), None)
    bad_rows = 0; checked = 0
    for pix in ncat.get_healpix_pixels():
        part = ncat.get_partition(pix.order, pix.pixel).compute()
        if not len(part):
            continue
        lengths = np.asarray(part[nested_name].array.list_lengths, dtype=np.int64)
        checked += len(part)
        if COUNT_COL:
            bad_rows += int((lengths != part[COUNT_COL].to_numpy(dtype=np.int64)).sum())
    if not COUNT_COL:
        skip("9b.nested_lengths_consistent", f"no count column (n_*) among [nested].columns to compare the {checked:,} nested lengths with")
    elif not checked:
        skip("9b.nested_lengths_consistent", "the nested product has no rows")
    else:
        record("9b.nested_lengths_consistent", bad_rows == 0, f"{checked:,} rows; {bad_rows:,} rows whose nested length differs from {COUNT_COL}")
    first = ncat.get_healpix_pixels()[0]
    head = ncat.get_partition(first.order, first.pixel).compute().head(1000)
    ids = head[NID].to_numpy().tolist()
    ref = lsdb.open_catalog(NESTED["catalog"], columns=[NID] + LISTS)
    ref_rows = ref.id_search(values={NID: ids}).compute().set_index(NID)
    mine = head.set_index(NID)[nested_name].nest.to_lists()
    mism = 0; tested = 0
    for sid in ids:
        if sid not in ref_rows.index:
            mism += 1; continue
        for c in LISTS:
            a = [None if (isinstance(x, float) and np.isnan(x)) else x for x in list(mine.loc[sid, c])]
            b = [None if (isinstance(x, float) and np.isnan(x)) else x for x in list(ref_rows.loc[sid, c])]
            tested += 1
            if a != b:
                mism += 1
    if tested or mism:
        record("9b.nested_sample_equals_reference", mism == 0, f"{len(ids)} objects x {len(LISTS)} lists: {mism} mismatches in {tested} arrays")
    else:
        skip("9b.nested_sample_equals_reference", f"no arrays compared ({len(ids)} objects, list columns {LISTS})")
else:
    print("no [nested] product configured")""")
    md("## 10. Summary")
    code('''if CELL_ERRORS:   # a cell that raised did not finish its checks: the run cannot pass
    record("10.no_cell_errors", False, f"{len(CELL_ERRORS)} cell(s) raised: {CELL_ERRORS[:3]}")
summary = pd.DataFrame([(k, st, d) for k, (st, d) in RESULTS.items()], columns=["check", "result", "detail"])
display(summary)
failed = summary[summary.result == "FAIL"]; skipped = summary[summary.result == "SKIP"]
n_pass = int((summary.result == "PASS").sum())
print(f"{n_pass}/{len(summary)} checks passed, {len(skipped)} skipped, {len(failed)} failed")
status = "ok" if failed.empty else "fail"
runlog(cfg, "end", "verify-values", status, status=status, passed=n_pass, failed=int(len(failed)), skipped=int(len(skipped)),
       failed_checks=failed.check.tolist(), skipped_checks=skipped.check.tolist(), cell_errors=CELL_ERRORS[:5])
assert failed.empty, f"{len(failed)} checks FAILED: {failed.check.tolist()}"
print("VERDICT: the HATS collection reproduces the source exactly; spatial index, partitioning, skymap and margin follow from the source positions"
      + (f" (not exercised on this data: {skipped.check.tolist()})" if len(skipped) else "") + ".")''')

    return nbf.v4.new_notebook(cells=cells, metadata={"kernelspec": {"name": "python3", "display_name": "Python 3"},
                                                      "language_info": {"name": "python"}})


def catalog_rows(collection: Path, catalog_name: str) -> int:
    """hats_nrows of the main catalog (collection root or bare catalog), 0 when not yet written."""
    for props in (collection / catalog_name / "hats.properties", collection / "hats.properties"):
        if props.exists():
            m = re.search(r"^hats_nrows=(\d+)", props.read_text(), re.M)
            return int(m.group(1)) if m else 0
    return 0


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--config", default="catalog.toml")
    ap.add_argument("--collection", help="collection to verify (default: the build path from catalog.toml)")
    ap.add_argument("--staging", default="", help="another copy of the collection to hash against")
    ap.add_argument("--public-url", default="", help="served URL of the collection to open and compare")
    ap.add_argument("--sample-rows", type=int, default=None,
                    help="verify only the first N source rows; 0 = every source row. Default: every row when the catalog "
                         "has at most HATS_PUBLISH_VERIFY_FULL_MAX_ROWS rows (20 000 000), otherwise the first "
                         "HATS_PUBLISH_VERIFY_SAMPLE_ROWS rows (500 000), so a manual run cannot load a billion-row "
                         "catalog into one kernel by accident")
    ap.add_argument("--out", help="notebook path (default verify_<catalog>.ipynb in the work dir)")
    a = ap.parse_args(argv)
    cfg = load_config(a.config)
    check_versions(cfg, warn_newer=False)
    collection = str(Path(a.collection).resolve()) if a.collection else str(cfg.collection_path)
    out = Path(a.out) if a.out else cfg.work_dir / f"verify_{cfg['names']['catalog']}.ipynb"
    out.parent.mkdir(parents=True, exist_ok=True)
    sample_rows = a.sample_rows
    if sample_rows is None:
        full_max = int(os.environ.get("HATS_PUBLISH_VERIFY_FULL_MAX_ROWS") or 20_000_000)
        full_max_gb = float(os.environ.get("HATS_PUBLISH_VERIFY_FULL_MAX_GB") or 8)
        default_sample = int(os.environ.get("HATS_PUBLISH_VERIFY_SAMPLE_ROWS") or 500_000)
        nrows = catalog_rows(Path(collection), cfg["names"]["catalog"])
        data_gb = sum(f.stat().st_size for f in (Path(collection) / cfg["names"]["catalog"] / "dataset").rglob("*.parquet")) / 1e9 \
            if (Path(collection) / cfg["names"]["catalog"] / "dataset").exists() else 0.0
        sample_rows = 0 if (nrows <= full_max and data_gb <= full_max_gb) else default_sample
        if sample_rows:
            print(f"catalog has {nrows:,} rows and {data_gb:.1f} GB of data (limits {full_max:,} rows, {full_max_gb:g} GB): sample mode on the first {sample_rows:,} source rows; "
                  f"pass --sample-rows 0 to compare every row")
    nb = build(cfg, collection, a.staging, a.public_url, str(Path(__file__).resolve().parent), sample_rows,
               str(Path(a.config).resolve()))
    nbf.write(nb, out)
    print(f"wrote {out} ({len(nb.cells)} cells)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
