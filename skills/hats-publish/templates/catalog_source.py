"""catalog_source.py — load the source catalog the SAME way for the small import route and for the value-level
verification, so the comparison is against exactly what was imported.

`load_source(cfg, files=None, max_rows=None)` returns:
    df            pandas DataFrame, numpy-backed, with [columns].drop / rename / cast applied and, when
                  [reader].strip_strings is set, leading and trailing blanks stripped from string columns (counts of
                  changed values in meta["strip_changed"])
    schema_expect list of (column, pyarrow.DataType) in order: what the HATS files must store
    meta          {"rows", "files", "format", "strip_changed", "source_columns", "dropped", "renamed", "cast"}

Formats (the same list as import_collection.py): fits (astropy; TFORM/TZERO/TSCAL -> Arrow type map; a fixed-size
array column holds one native-byte-order 1-D numpy array per row), csv / tsv / indexed_csv (pandas; the
[reader].type_map is applied; without it the expected schema is only "what pandas inferred"), ecsv (astropy),
parquet / indexed_parquet (pyarrow; its own schema). The indexed formats list their data files in index files, which
are expanded here as hats-import does.

Route "reimport" with [source].files empty: the source is the partition files of [run].reimport_from, read as the
import reads them (Parquet whatever [source].format says; the import's column list: no Norder/Dir/Npix, no
[columns].drop) and without the spatial index column, which the import writes first and the verification recomputes
from the positions. [reader] options do not apply, as on the import. [source].files set: the files and [source].format
are used as on the other routes (e.g. the original raw files, for a check against the archive rather than the old copy).

Order of the steps, the same as the import route's reader chain: read (FITS keep-list or skip-list, companions,
[reader].derive) -> [columns].drop / rename / cast -> [reader].strip_strings -> [reader].conform_schema ->
[reader].drop_rows_without_coordinates (on the renamed coordinate columns).
"""
from __future__ import annotations

import re
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa

from _common import input_files

TFORM_TO_ARROW = {"D": pa.float64(), "E": pa.float32(), "K": pa.int64(), "J": pa.int32(), "I": pa.int16(),
                  "B": pa.uint8(), "L": pa.bool_()}
UNSIGNED_BY_TZERO = {("I", 2**15): pa.uint16(), ("J", 2**31): pa.uint32(), ("K", 2**63): pa.uint64()}


def expected_type_from_tform(tform: str, tzero=None, tscal=None) -> pa.DataType | None:
    """The Arrow type hats-import's FitsReader (astropy) produces for a binary-table column, or None when this map
    does not know the TFORM (the caller then takes the type of the values read).

    TZERO = 2^(bits-1) on I/J/K gives the unsigned type; any other TZERO or TSCAL gives float64 (astropy applies the
    scaling). P/Q (variable-length arrays) give a variable-length list of the element type."""
    m = re.fullmatch(r"(\d*)([PQ]?)([A-Z])(.*)", tform.strip())
    if not m:
        return None
    rep, var, code, _ = m.groups()
    if code == "A":
        return None if var else pa.string()
    base = TFORM_TO_ARROW.get(code)
    if base is None:
        return None
    scaled = (tscal not in (None, 1, 1.0)) or (tzero not in (None, 0, 0.0))
    if scaled:
        base = UNSIGNED_BY_TZERO.get((code, tzero)) if tscal in (None, 1, 1.0) else None
        base = base or pa.float64()
    if var:
        return pa.list_(base)                # variable-length array column
    if rep not in ("", "1"):
        return pa.list_(base, int(rep))      # fixed-size array column -> fixed_size_list (hats-import flatten_tensors)
    return base


def _numpy_to_arrow(dtype) -> pa.DataType:
    """Arrow type for a numpy, pandas (nullable) or Arrow-backed dtype."""
    if isinstance(dtype, pd.ArrowDtype):
        return dtype.pyarrow_dtype
    if dtype == object or str(dtype) in ("string", "str"):
        return pa.string()
    try:
        return pa.from_numpy_dtype(dtype)
    except (TypeError, pa.ArrowNotImplementedError):
        return pa.array(pd.Series([], dtype=dtype)).type   # pandas extension dtypes (Int32, boolean, ...)


def _select_fits_columns(frame: pd.DataFrame, cfg) -> tuple[pd.DataFrame, list]:
    """[reader].column_names (keep-list, wins) or skip_column_names, as FitsReader / CompanionFitsReader apply them
    before [reader].derive. Returns the frame and the names removed."""
    keep = cfg["reader"].get("column_names") or []
    if keep:
        removed = [c for c in frame.columns if c not in keep]
    else:
        removed = [c for c in cfg["reader"].get("skip_column_names") or [] if c in frame.columns]
    return (frame.drop(columns=removed) if removed else frame), removed


def _apply_column_rules(df: pd.DataFrame, cfg, dropped=None) -> tuple[pd.DataFrame, dict]:
    cols = cfg["columns"]
    info = {"dropped": list(dropped or []), "renamed": {}, "cast": {}}
    for c in cols.get("drop") or []:
        if c in df.columns:
            df = df.drop(columns=c); info["dropped"].append(c)
    renames = {k: v for k, v in (cols.get("rename") or {}).items() if k in df.columns}
    if renames:
        df = df.rename(columns=renames); info["renamed"] = renames
    for c, dt in (cols.get("cast") or {}).items():
        if c in df.columns:
            if c in (cols.get("ra"), cols.get("dec")):
                raise ValueError(f"refusing to cast the coordinate column {c}")
            df[c] = df[c].astype(dt); info["cast"][c] = dt
    return df, info


def _strip_strings(df: pd.DataFrame, strip: bool) -> dict:
    """Decode bytes columns; strip leading and trailing blanks only when the import did the same
    ([reader].strip_strings)."""
    changed = {}
    for c in df.columns:
        if df[c].dtype == object or pd.api.types.is_string_dtype(df[c].dtype):   # object, or pandas str/string dtypes
            s = df[c]
            first = s.dropna()
            first = first.iloc[0] if len(first) else None
            if first is not None and not isinstance(first, (str, bytes, np.bytes_)):
                continue   # array column, not a string
            if isinstance(first, (bytes, np.bytes_)):
                s = s.str.decode("utf-8")
            if strip:
                stripped = s.str.strip()
                changed[c] = int((stripped != s).sum())
                s = stripped
            df[c] = s
    return changed


def _expand_index_files(paths: list[Path]) -> list[Path]:
    """indexed_csv / indexed_parquet inputs are text files listing one data file per line."""
    out: list[Path] = []
    for p in paths:
        out += [Path(l.strip()) for l in Path(p).read_text().splitlines() if l.strip()]
    return out


def _read_fits(p, hdu: int, max_rows: int | None):
    """One FITS table -> (numpy-backed frame, [(column, expected Arrow type)]). Array columns hold one 1-D array per
    row. With max_rows, the first rows are read by fitsio row range (astropy touches the whole file, even memory-mapped)."""
    from astropy.io import fits
    from astropy.table import Table

    try:
        import fitsio
    except ImportError:       # pragma: no cover - optional dependency
        fitsio = None
    with fits.open(p, memmap=True) as hdul:
        cols = hdul[hdu].columns      # header only; .data would read the whole table
        ascii_table = isinstance(hdul[hdu], fits.TableHDU)   # ASCII tables (VizieR): TFORM like E21.18, I1, A4
        schema = None if ascii_table else [(c.name, expected_type_from_tform(c.format, c.bzero, c.bscale)) for c in cols]
        if max_rows and fitsio is not None:
            with fitsio.FITS(str(p)) as ff:
                rec = ff[hdu][0:min(max_rows, ff[hdu].get_nrows())]
            tbl = Table(rec)
            for c in tbl.colnames:    # fitsio gives bytes; astropy gives str (padding kept in both)
                if tbl[c].dtype.kind == "S":
                    tbl[c] = np.char.decode(np.asarray(tbl[c]), "ascii", errors="replace")
        else:
            tbl = Table(hdul[hdu].data)
        if schema is None:   # expected types = what astropy gives for the ASCII formats
            schema = [(c, _numpy_to_arrow(tbl[c].dtype)) for c in tbl.colnames]
        else:                # TFORMs the map does not know: the type of the values read
            schema = [(c, t if t is not None else _numpy_to_arrow(tbl[c].dtype)) for c, t in schema]
        multi = [c for c in tbl.colnames if len(tbl[c].shape) > 1]
        if multi:
            flat = tbl[[c for c in tbl.colnames if c not in multi]].to_pandas()
            for c in multi:
                arr = np.asarray(tbl[c])
                arr = arr.astype(arr.dtype.newbyteorder("="), copy=False).reshape(len(arr), -1)   # native order, flattened
                flat[c] = list(arr)   # one 1-D array per row, same order as the FITS column
            return flat[tbl.colnames], schema
        return tbl.to_pandas(), schema


def _read_parquet(paths, keep, max_rows):
    """Parquet files -> (frames, [(column, Arrow type)]), list columns kept Arrow-backed; stops after max_rows."""
    import pyarrow.parquet as pq

    frames, schema_expect = [], None
    for p in paths:
        t = pq.read_table(p, columns=keep)
        if schema_expect is None:
            schema_expect = [(f.name, f.type) for f in t.schema]
        frame = t.to_pandas()
        for f in t.schema:   # keep list columns Arrow-backed (to_pandas would give object arrays of ndarrays)
            if pa.types.is_list(f.type) or pa.types.is_large_list(f.type):
                frame[f.name] = pd.array(t.column(f.name).combine_chunks(), dtype=pd.ArrowDtype(f.type))
        frames.append(frame)
        if max_rows and sum(len(f) for f in frames) >= max_rows:
            break
    return frames, schema_expect


def reimport_rows(cfg) -> bool:
    """True when the source rows are the partition files of [run].reimport_from (route "reimport", no [source].files)."""
    return cfg["run"].get("route") == "reimport" and not (cfg["source"].get("files") or [])


def _load_reimport(cfg, files, max_rows):
    """The reimport route's rows: the import's column list without the spatial index, no [reader] steps."""
    import pyarrow.parquet as pq
    from hats.io.paths import HIVE_COLUMNS
    from hats.pixel_math.spatial_index import SPATIAL_INDEX_COLUMN

    from _common import reimport_source
    from import_collection import reimport_read_columns

    paths = list(files) if files else input_files(cfg)
    names = reimport_read_columns(cfg, reimport_source(cfg))   # also refuses rename/cast and a drop of needed columns
    if names is None:   # no _common_metadata: the first file's columns
        names = [n for n in pq.read_schema(paths[0]).names if n not in HIVE_COLUMNS]
    keep = [n for n in names if n != SPATIAL_INDEX_COLUMN]
    frames, schema_expect = _read_parquet(paths, keep, max_rows)
    df = pd.concat(frames, ignore_index=True) if len(frames) > 1 else frames[0].reset_index(drop=True)
    if max_rows:
        df = df.iloc[:max_rows].copy()
    src = pq.read_schema(paths[0]).names
    dropped = [c for c in src if c not in keep]   # Norder/Dir/Npix, [columns].drop and the spatial index
    meta = {"rows": len(df), "files": [str(p) for p in paths], "format": "parquet", "strip_changed": {},
            "source_columns": src, "dropped_no_coords": 0, "nested": None, "conformed": [],
            "dropped": dropped, "renamed": {}, "cast": {}, "reimport_from": cfg["run"].get("reimport_from")}
    return df, schema_expect, meta


def load_source(cfg, files=None, max_rows: int | None = None):
    if reimport_rows(cfg):
        return _load_reimport(cfg, files, max_rows)
    fmt = cfg["source"]["format"]
    paths = [Path(f) for f in files] if files else input_files(cfg)
    if fmt in ("indexed_csv", "indexed_parquet"):
        paths = _expand_index_files(paths)
    frames, schema_expect = [], None
    nested = None
    selected_out: list = []
    if fmt == "fits":
        hdu = int(cfg["reader"].get("hdu", 1))
        companions = (cfg["source"].get("companions") or {}).get("files") or []
        keys = (cfg["source"].get("companions") or {}).get("key_columns") or []
        derive = cfg["reader"].get("derive") or ""
        if derive:
            dpath, _, dfunc = derive.rpartition(":")
            derive = f"{cfg.path(dpath)}:{dfunc}"
        for p in paths:
            want = max(max_rows - sum(len(f) for f in frames), 0) if max_rows else None
            frame, file_schema = _read_fits(p, hdu, want)
            if companions:   # row-parallel companion files: same rows, same order; keys and repeated columns must agree
                from readers import companion_paths

                for cp in companion_paths(p, companions):
                    cframe, cschema = _read_fits(cp, hdu, want)
                    if len(cframe) != len(frame):
                        raise ValueError(f"{cp.name}: {len(cframe)} rows, {p.name}: {len(frame)}")
                    for c in cframe.columns:
                        if c in frame.columns:
                            x, y = frame[c].to_numpy(), cframe[c].to_numpy()
                            if x.dtype == object and len(x) and isinstance(x[0], np.ndarray):
                                same = all(np.array_equal(u, v) for u, v in zip(x, y))
                            else:
                                same = np.array_equal(x, y)
                            if not same:
                                raise ValueError(f"column {c} differs between {p.name} and {cp.name}" + (" (a key column)" if c in keys else ""))
                    new = [c for c in cframe.columns if c not in frame.columns]
                    frame = pd.concat([frame, cframe[new]], axis=1)
                    file_schema += [(n, t) for n, t in cschema if n in new]
            frame, removed = _select_fits_columns(frame, cfg)   # keep-list / skip-list, then derive (import order)
            selected_out = removed
            if derive:   # derived columns: the same hook as the import, applied to the source values
                from readers import load_derive_hook

                added = load_derive_hook(derive)(lambda c: frame[c].to_numpy(), str(p))
                for name, values in added.items():
                    arr = pa.array(np.asarray(values))
                    frame[name] = arr.to_numpy(zero_copy_only=False)
                    file_schema.append((name, arr.type))
            if schema_expect is None:
                schema_expect = file_schema
            frames.append(frame)
            if max_rows and sum(len(f) for f in frames) >= max_rows:
                break
        pn = cfg["reader"].get("pack_nested") or {}
        if pn.get("name"):   # the source keeps flat list columns; the HATS table holds them in one nested column
            from readers import nested_columns

            nested = {"name": pn["name"], "columns": nested_columns([n for n, _ in schema_expect], list(pn.get("columns") or []))}
    elif fmt in ("csv", "tsv", "indexed_csv"):
        from readers import csv_na_defaults

        kw = csv_na_defaults(cfg["reader"].get("csv_kwargs") or {})   # the import route's missing-value rule
        if fmt == "tsv":
            kw.setdefault("sep", "\t")
        kw.pop("compression", None)   # pandas infers it from the path here
        if cfg["reader"].get("column_names"):   # hats-import semantics for CSV: the names of a header-less file
            kw.update(names=list(cfg["reader"]["column_names"]), header=None)
        type_map = dict(cfg["reader"].get("type_map") or {})
        ecsv_arrays = {}
        if cfg["reader"].get("ecsv_header") and paths:   # same header-derived types as the import route
            from readers import ecsv_pandas_types, parse_array_strings

            header_types, ecsv_arrays = ecsv_pandas_types(paths[0])
            type_map = header_types | {c: "str" for c in ecsv_arrays} | type_map
            kw.setdefault("comment", "#")
        for p in paths:
            want = max(max_rows - sum(len(f) for f in frames), 0) if max_rows else None
            frame = pd.read_csv(p, dtype=type_map or None, nrows=want, **kw)
            for c, subtype in ecsv_arrays.items():   # array strings -> Arrow-backed list columns
                if c in frame.columns:
                    parsed = parse_array_strings(frame[c], subtype)
                    frame[c] = pd.array(parsed, dtype=pd.ArrowDtype(parsed.type))   # plain pd.array() would fall back to object
            frames.append(frame)
            if max_rows and sum(len(f) for f in frames) >= max_rows:
                break
        df0 = frames[0]
        schema_expect = [(c, _numpy_to_arrow(df0[c].dtype)) for c in df0.columns]
    elif fmt == "ecsv":
        from astropy.table import Table

        for p in paths:
            t = Table.read(p, format="ascii.ecsv")
            if schema_expect is None:
                schema_expect = [(c, _numpy_to_arrow(t[c].dtype)) for c in t.colnames]
            frames.append(t.to_pandas())
            if max_rows and sum(len(f) for f in frames) >= max_rows:
                break
    elif fmt in ("parquet", "indexed_parquet"):
        keep = list(cfg["reader"].get("column_names") or []) or None   # the import's Parquet keep-list
        frames, schema_expect = _read_parquet(paths, keep, max_rows)
    else:
        raise ValueError(f"unsupported [source].format {fmt!r}")

    df = pd.concat(frames, ignore_index=True) if len(frames) > 1 else frames[0]
    if max_rows:
        df = df.iloc[:max_rows].copy()
    source_columns = list(df.columns)
    df, info = _apply_column_rules(df, cfg, dropped=selected_out)   # [columns] rules first: names below are renamed
    strip_changed = _strip_strings(df, bool(cfg["reader"].get("strip_strings")))
    # apply the same rules to the expected schema
    from readers import arrow_type

    exp = dict(schema_expect)
    exp = {info["renamed"].get(k, k): v for k, v in exp.items() if k not in info["dropped"]}
    for c, dt in info["cast"].items():
        exp[c] = arrow_type(dt)
    schema_expect = [(c, exp[c]) for c in df.columns if c in exp]
    if nested:   # the packed columns appear as one struct<list...> column at the position of the first of them
        packed = [c for c in nested["columns"] if c in exp]
        struct = pa.struct([pa.field(c, pa.list_(exp[c].value_type if hasattr(exp[c], "value_type") else exp[c])) for c in packed])
        out = []
        for c, t in schema_expect:
            if c == packed[0]:
                out.append((nested["name"], struct))
            elif c not in packed:
                out.append((c, t))
        schema_expect = out
    conformed = []
    target_path = cfg["reader"].get("conform_schema")
    if target_path:   # same target schema as the import (any format): columns a file lacks are null in HATS
        from readers import read_target_schema

        target = read_target_schema(cfg.path(target_path))
        packed = set(nested["columns"]) | {nested["name"]} if nested else set()
        for f in target:
            if f.name not in df.columns and f.name not in packed:
                is_list = pa.types.is_list(f.type) or pa.types.is_fixed_size_list(f.type) or pa.types.is_large_list(f.type)
                df[f.name] = pd.Series([None] * len(df), dtype=object) if is_list else np.full(len(df), np.nan)
                conformed.append(f.name)
        schema_expect = [(f.name, f.type) for f in target]
    dropped_no_coords = 0
    if cfg["reader"].get("drop_rows_without_coordinates"):   # after the rename, as the import's coordinate filter
        ra_c, dec_c = cfg["columns"]["ra"], cfg["columns"]["dec"]
        ra_v = pd.to_numeric(df[ra_c], errors="coerce").to_numpy(dtype=float)
        dec_v = pd.to_numeric(df[dec_c], errors="coerce").to_numpy(dtype=float)
        ok = np.isfinite(ra_v) & np.isfinite(dec_v) & (ra_v >= 0) & (ra_v < 360) & (dec_v >= -90) & (dec_v <= 90)
        dropped_no_coords = int((~ok).sum())
        df = df[ok].reset_index(drop=True)
    meta = {"rows": len(df), "files": [str(p) for p in paths], "format": fmt, "strip_changed": strip_changed,
            "source_columns": source_columns, "dropped_no_coords": dropped_no_coords, "nested": nested,
            "conformed": conformed} | info
    return df, schema_expect, meta


def same_type(a: pa.DataType, b: pa.DataType, loose: bool = False) -> bool:
    """Arrow type equality that treats string/large_string alike; loose=True compares integer/float families only."""
    if pa.types.is_string(a) or pa.types.is_large_string(a):
        return pa.types.is_string(b) or pa.types.is_large_string(b)
    if (pa.types.is_fixed_size_list(a) or pa.types.is_list(a)) and (pa.types.is_fixed_size_list(b) or pa.types.is_list(b)):
        return same_type(a.value_type, b.value_type, loose)
    if loose:
        return (pa.types.is_integer(a) and pa.types.is_integer(b)) or (pa.types.is_floating(a) and pa.types.is_floating(b)) or a == b
    return a == b
