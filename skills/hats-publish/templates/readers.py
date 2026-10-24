"""readers.py — reader wrappers used by the import templates.

CoordinateFilterReader wraps any hats-import InputReader and drops rows whose coordinates are missing or out of
range (NaN, |dec| > 90, ra outside [0, 360)). hats-import's spatial index cannot be computed for such rows and the
pipeline fails in the mapping stage; a spatial catalog cannot hold them. The number of dropped rows is printed per
chunk and must be reconciled by the verification step (source rows minus dropped rows = catalog rows).
Enable it with `[reader].drop_rows_without_coordinates = true` in catalog.toml.

ColumnRulesReader applies `[columns].drop`, `[columns].rename` and `[columns].cast` to every chunk, so the import
route and the verification loader see the same columns (names in `[columns].ra/dec/id/default` are the renamed ones).

ECSV-headed CSV (`[reader].ecsv_header = true`): `ecsv_datatypes` reads the `datatype:` block of the header,
`ecsv_pandas_types` turns it into the pandas type map for the scalar columns, and `parse_array_strings` turns the
array columns (ECSV `subtype: '<elem>[null]'`, written as JSON-like strings "[v1,v2,...]") into Arrow list arrays with
Arrow kernels only. astropy's ECSV reader must not be used for these arrays: it reads quoted booleans ("False") as True.

CompanionFitsReader (`[source.companions]`): a release that splits one table into row-parallel FITS files (a main file
plus companions with the same rows in the same order, e.g. `x.fits` with `x-extra.fits` and `x-photoz.fits`) is read
as one table.
Row ranges come from fitsio (bounded memory; astropy's memmap costs the whole file in RSS), the key columns must be
identical in every file chunk by chunk, and a column repeated in a companion must equal the main one before it is
dropped. Two optional per-chunk steps run inside the reader: `[reader].derive` ("file.py:function", columns computed
from the chunk and the input path, e.g. a region tag) and `[reader].pack_nested` (list columns packed into one
struct-of-lists column that lsdb opens as a nested column).

CSV missing values (`csv_na_defaults`, used by the import route and by the verification loader): pandas' default
NA strings ("NA", "N/A", "NULL", "null", "None", "n/a", "#N/A", "<NA>", ...) are valid values in astronomical
tables (a flag "NA", a class "None"), so unless `[reader].csv_kwargs` sets `keep_default_na`, `na_values` or
`na_filter`, only an empty field and the float NaN spellings ("nan", "NaN", "NAN", "-nan", "-NaN") are missing.
`check_csv_names` refuses column names that are not non-empty strings (a NaN name: pandas reads a column NAMED "NA"
in a names/types side file as NaN, which leaves that column all-null in the catalog), type_map keys that are not
columns, and a schema file whose columns differ from what the reader produces (the reduce step reads every pixel with
that schema: a schema column the reader does not produce is written all-null, a reader column the schema lacks is
dropped, both silently).

ConformSchemaReader (`[reader].conform_schema`, a Parquet file, any input format) gives every chunk the same schema,
so inputs that lack some columns (a region without a band) get all-null columns instead of a partition with a shorter
schema, which hats-import's finishing step rejects. It runs after the column rules, so the target schema uses the
final (renamed) column names.
"""
from __future__ import annotations

import fnmatch
import gzip
import importlib.util
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc

from hats_import.catalog.file_readers.input_reader import InputReader


def valid_coordinate_mask(ra, dec):
    ra = np.asarray(ra, dtype=float)
    dec = np.asarray(dec, dtype=float)
    return np.isfinite(ra) & np.isfinite(dec) & (ra >= 0) & (ra < 360) & (dec >= -90) & (dec <= 90)


class CoordinateFilterReader(InputReader):
    """Yield the inner reader's chunks without rows lacking valid coordinates."""

    def __init__(self, inner: InputReader, ra_column: str, dec_column: str):
        self.inner = inner
        self.ra_column = ra_column
        self.dec_column = dec_column
        self.dropped = 0

    def read(self, input_file, read_columns=None):
        cols = read_columns
        if cols is not None:
            cols = list(dict.fromkeys(list(cols) + [self.ra_column, self.dec_column]))
        for chunk in self.inner.read(input_file, read_columns=cols):
            if isinstance(chunk, pa.Table):
                mask = valid_coordinate_mask(chunk[self.ra_column].to_numpy(zero_copy_only=False),
                                             chunk[self.dec_column].to_numpy(zero_copy_only=False))
                n_bad = int((~mask).sum())
                if n_bad:
                    chunk = chunk.filter(pa.array(mask))
                if read_columns is not None:
                    chunk = chunk.select([c for c in read_columns if c in chunk.column_names])
            else:
                mask = valid_coordinate_mask(chunk[self.ra_column].to_numpy(dtype=float, na_value=np.nan),
                                             chunk[self.dec_column].to_numpy(dtype=float, na_value=np.nan))
                n_bad = int((~mask).sum())
                if n_bad:
                    chunk = chunk[mask]
                if read_columns is not None:
                    chunk = chunk[[c for c in read_columns if c in chunk.columns]]
            if n_bad:
                self.dropped += n_bad
                print(f"CoordinateFilterReader: dropped {n_bad} rows without valid coordinates in {input_file}")
            yield chunk

    def provenance_info(self) -> dict:
        info = self.inner.provenance_info() if hasattr(self.inner, "provenance_info") else {}
        return info | {"coordinate_filter": True}

    def regular_file_exists(self, input_file, **kwargs):
        return self.inner.regular_file_exists(input_file, **kwargs)


class StringStripReader(InputReader):
    """Yield the inner reader's chunks with leading and trailing whitespace removed from every string column.

    hats-import's FitsReader already drops the trailing blanks of FITS fixed-width strings (astropy decodes them
    that way), so on FITS inputs this changes only values with leading blanks. It also removes any padding a reader
    leaves in place, for example blanks around the fields of fixed-width or space-aligned CSV exports. Enable with
    `[reader].strip_strings = true` when the source pads its values; the verification loader applies the same rule
    so the comparison stays exact.
    """

    def __init__(self, inner: InputReader):
        self.inner = inner

    def read(self, input_file, read_columns=None):
        for chunk in self.inner.read(input_file, read_columns=read_columns):
            if isinstance(chunk, pa.Table):
                cols = []
                for name in chunk.column_names:
                    col = chunk[name]
                    if pa.types.is_string(col.type) or pa.types.is_large_string(col.type):
                        col = pc.utf8_trim_whitespace(col)
                    cols.append(col)
                chunk = pa.table(cols, names=chunk.column_names, metadata=chunk.schema.metadata)
            else:
                import pandas as pd

                for name in chunk.columns:
                    s = chunk[name]
                    if not (s.dtype == object or pd.api.types.is_string_dtype(s.dtype)):
                        continue
                    first = s.dropna()
                    first = first.iloc[0] if len(first) else None
                    if not isinstance(first, (str, bytes, np.bytes_)):
                        continue   # all null, or an object column of arrays: not a string column
                    if isinstance(first, (bytes, np.bytes_)):
                        s = s.str.decode("utf-8")
                    chunk[name] = s.str.strip()
            yield chunk

    def provenance_info(self) -> dict:
        info = self.inner.provenance_info() if hasattr(self.inner, "provenance_info") else {}
        return info | {"strip_strings": True}

    def regular_file_exists(self, input_file, **kwargs):
        return self.inner.regular_file_exists(input_file, **kwargs)


def arrow_type(dtype) -> pa.DataType:
    """Arrow type for a dtype name as written in catalog.toml: numpy names ('float32', 'uint16'), pandas nullable
    names ('Int32', 'UInt8', 'boolean', 'string', 'Float64') or 'str'."""
    import pandas as pd

    name = str(dtype)
    if name in ("str", "string", "object"):
        return pa.string()
    try:
        return pa.from_numpy_dtype(np.dtype(name))
    except TypeError:
        pass
    pd_dtype = pd.api.types.pandas_dtype(name)   # raises TypeError for an unknown name
    if isinstance(pd_dtype, pd.ArrowDtype):
        return pd_dtype.pyarrow_dtype
    return pa.array(pd.Series([], dtype=pd_dtype)).type


class ColumnRulesReader(InputReader):
    """Apply [columns].drop / rename / cast to the inner reader's chunks (pandas or pyarrow)."""

    def __init__(self, inner: InputReader, drop=None, rename=None, cast=None):
        self.inner = inner
        self.drop = list(drop or [])
        self.rename = dict(rename or {})
        self.cast = dict(cast or {})
        self._back = {v: k for k, v in self.rename.items()}   # renamed -> original, for read_columns requests

    def read(self, input_file, read_columns=None):
        cols = None if read_columns is None else [self._back.get(c, c) for c in read_columns]
        for chunk in self.inner.read(input_file, read_columns=cols):
            if isinstance(chunk, pa.Table):
                keep = [c for c in chunk.column_names if c not in self.drop]
                if len(keep) != chunk.num_columns:
                    chunk = chunk.select(keep)
                if self.rename:
                    chunk = chunk.rename_columns([self.rename.get(c, c) for c in chunk.column_names])
                for c, dt in self.cast.items():
                    if c in chunk.column_names:
                        i = chunk.schema.get_field_index(c)
                        chunk = chunk.set_column(i, c, pc.cast(chunk.column(c), arrow_type(dt)))
            else:
                drop = [c for c in self.drop if c in chunk.columns]
                if drop:
                    chunk = chunk.drop(columns=drop)
                if self.rename:
                    chunk = chunk.rename(columns={k: v for k, v in self.rename.items() if k in chunk.columns})
                for c, dt in self.cast.items():
                    if c in chunk.columns:
                        chunk[c] = chunk[c].astype(dt)
            yield chunk

    def provenance_info(self) -> dict:
        info = self.inner.provenance_info() if hasattr(self.inner, "provenance_info") else {}
        return info | {"column_rules": {"drop": self.drop, "rename": self.rename, "cast": self.cast}}

    def regular_file_exists(self, input_file, **kwargs):
        return self.inner.regular_file_exists(input_file, **kwargs)


# ----------------------------------------------------------------------------------------------------------------
# CSV missing values and column names
# ----------------------------------------------------------------------------------------------------------------
CSV_NA_VALUES = ["", "nan", "NaN", "NAN", "-nan", "-NaN"]   # empty field and float NaN spellings only


def csv_na_defaults(csv_kwargs: dict) -> dict:
    """pandas.read_csv arguments with the skill's missing-value default: keep_default_na=False and
    na_values=CSV_NA_VALUES, unless the user set keep_default_na, na_values or na_filter (then pandas' own rules apply
    to what they set). Without the float NaN spellings a float column written as "nan" fails to parse with a type map
    and silently becomes a string column without one."""
    kw = dict(csv_kwargs or {})
    if not {"keep_default_na", "na_values", "na_filter"} & kw.keys():
        kw.update(keep_default_na=False, na_values=list(CSV_NA_VALUES))
    return kw


def check_csv_names(column_names=None, type_map=None, schema_names=None, drop=None, rename=None) -> list[str]:
    """Problems with the CSV reader's names (empty list = fine): names that are not non-empty strings, duplicate
    names, type_map keys that are not in column_names, and, with a schema file, schema columns the reader does not
    produce (written all-null) or produced columns the schema lacks (dropped). Produced = column_names after
    [columns].drop and rename; the spatial index column is ignored."""
    problems = []
    names = list(column_names or [])
    bad = [n for n in names if not isinstance(n, str) or not n.strip()]
    bad += [k for k in (type_map or {}) if not isinstance(k, str) or not k.strip()]
    if bad:
        problems.append(f"column names that are not non-empty strings: {bad!r} (a names/types side file read with pandas "
                        "defaults turns a column named NA, N/A, NULL or None into NaN: read it with keep_default_na=False)")
    dup = sorted({n for n in names if names.count(n) > 1}, key=str)
    if dup:
        problems.append(f"duplicate column names: {dup}")
    if names and type_map:
        unknown = [k for k in type_map if k not in names]
        if unknown:
            problems.append(f"[reader].type_map keys that are not in [reader].column_names: {unknown}")
    if names and schema_names is not None:
        drop, rename = set(drop or []), dict(rename or {})
        produced = [rename.get(n, n) for n in names if n not in drop]
        schema = [n for n in schema_names if n not in ("_healpix_29", "_hipscat_index")]
        null = [n for n in schema if n not in produced]
        lost = [n for n in produced if n not in schema]
        if null:
            problems.append(f"schema file columns the reader does not produce (they would be written all-null): {null}")
        if lost:
            problems.append(f"reader columns missing from the schema file (they would be dropped): {lost}")
    return problems


# ----------------------------------------------------------------------------------------------------------------
# ECSV header and array columns
# ----------------------------------------------------------------------------------------------------------------
# ECSV datatypes (the ECSV standard's list). Arrow and Parquet have no complex or 128-bit float type: those columns
# are kept as their text, which is lossless, and a warning names them (ecsv_pandas_types).
ECSV_TEXT_ONLY = ("float128", "complex64", "complex128", "complex256")
ECSV_ELEMENT_TYPES = {"float64": pa.float64(), "float32": pa.float32(), "float16": pa.float16(),
                      "int64": pa.int64(), "int32": pa.int32(), "int16": pa.int16(), "int8": pa.int8(),
                      "uint64": pa.uint64(), "uint32": pa.uint32(), "uint16": pa.uint16(), "uint8": pa.uint8(),
                      "bool": pa.bool_(), "string": pa.string()} | {t: pa.string() for t in ECSV_TEXT_ONLY}
ECSV_PANDAS_TYPES = {"float64": "float64", "float32": "float32", "float16": "float16",
                     "int64": "Int64", "int32": "Int32", "int16": "Int16", "int8": "Int8",
                     "uint64": "UInt64", "uint32": "UInt32", "uint16": "UInt16", "uint8": "UInt8",
                     "bool": "boolean", "string": "str"} | {t: "str" for t in ECSV_TEXT_ONLY}


def ecsv_datatypes(path) -> list[dict]:
    """The `datatype:` entries of an ECSV header (name, datatype, optional subtype), from a plain or gzipped file.
    Only the datatype block is parsed: the free-text `meta:` block of some exports is not valid YAML."""
    import yaml

    opener = gzip.open if str(path).endswith(".gz") else open
    lines = []
    with opener(path, "rt") as fh:
        for line in fh:
            if not line.startswith("#"):
                break
            lines.append(line[1:].rstrip("\n"))
    lines = [l[1:] if l.startswith(" ") else l for l in lines]
    try:
        start = next(i for i, l in enumerate(lines) if l.startswith("datatype:"))
    except StopIteration:
        raise ValueError(f"{path}: no ECSV datatype block in the header") from None
    end = next((i for i in range(start + 1, len(lines)) if lines[i] and not lines[i].startswith((" ", "-"))), len(lines))
    return yaml.safe_load("\n".join(lines[start:end]))["datatype"]


def ecsv_pandas_types(path) -> tuple[dict, dict]:
    """(pandas type map for the scalar columns, {array column: ECSV subtype}) from an ECSV header."""
    scalars, arrays, text_only = {}, {}, []
    for col in ecsv_datatypes(path):
        if col.get("subtype"):
            arrays[col["name"]] = col["subtype"]
            if col["subtype"].split("[")[0] in ECSV_TEXT_ONLY:
                text_only.append(col["name"])
        else:
            scalars[col["name"]] = ECSV_PANDAS_TYPES.get(col["datatype"], "str")
            if col["datatype"] in ECSV_TEXT_ONLY:
                text_only.append(col["name"])
    if text_only:
        print(f"note: ECSV columns {text_only} have a complex or 128-bit float type, which Arrow cannot store; "
              "they are kept as text")
    return scalars, arrays


def parse_array_strings(values, subtype: str) -> pa.ListArray:
    """JSON-like array strings ("[1.5,null,2.0]", quoted booleans allowed) -> Arrow list<elem>; null cell -> null list."""
    base = subtype.split("[")[0]
    if base not in ECSV_ELEMENT_TYPES:
        raise ValueError(f"unsupported ECSV array subtype {subtype!r}")
    elem = ECSV_ELEMENT_TYPES[base]
    if not isinstance(values, (pa.Array, pa.ChunkedArray)):
        # from_pandas: an empty CSV cell (NaN in pandas) is a null list, as on the import route
        values = pa.array(np.asarray(values, dtype=object), type=pa.string(), from_pandas=True)
    if isinstance(values, pa.ChunkedArray):
        values = values.combine_chunks()
    s = pc.cast(values, pa.string())
    inner = pc.utf8_trim(s, "[]")
    empty = pc.fill_null(pc.equal(inner, ""), True).to_numpy(zero_copy_only=False)
    parts = pc.split_pattern(pc.fill_null(inner, ""), ",")
    plen = pc.list_value_length(parts).to_numpy(zero_copy_only=False).astype(np.int64)
    lengths = np.where(empty, 0, plen)
    flat = pc.utf8_trim(pc.utf8_trim_whitespace(pc.list_flatten(parts)), "\"'")
    flat = flat.filter(pa.array(np.repeat(~empty, plen)))
    flat = pc.if_else(pc.equal(pc.utf8_lower(flat), "null"), pa.scalar(None, pa.string()), flat)
    if elem == pa.bool_():
        low = pc.utf8_lower(flat)
        flat = pc.cast(pc.if_else(pc.equal(low, "true"), 1, pc.if_else(pc.equal(low, "false"), 0, pa.scalar(None, pa.int8()))), pa.bool_())
    elif elem != pa.string():
        flat = pc.cast(flat, elem)
    offsets = pa.array(np.concatenate([[0], np.cumsum(lengths)]).astype(np.int32), pa.int32())
    return pa.ListArray.from_arrays(offsets, flat, mask=pa.array(s.is_null().to_numpy(zero_copy_only=False)))


class EcsvArrayReader(InputReader):
    """Wrap a pandas-chunk reader of an ECSV-headed CSV: chunks come out as Arrow tables with the array columns
    parsed into list columns (the scalar columns keep the header's types through the reader's type map)."""

    def __init__(self, inner: InputReader, arrays: dict):
        self.inner = inner
        self.arrays = dict(arrays)

    def read(self, input_file, read_columns=None):
        for chunk in self.inner.read(input_file, read_columns=read_columns):
            table = chunk if isinstance(chunk, pa.Table) else pa.Table.from_pandas(chunk, preserve_index=False)
            for name, subtype in self.arrays.items():
                if name in table.column_names:
                    i = table.schema.get_field_index(name)
                    table = table.set_column(i, name, parse_array_strings(table.column(name), subtype))
            yield table.replace_schema_metadata()

    def provenance_info(self) -> dict:
        info = self.inner.provenance_info() if hasattr(self.inner, "provenance_info") else {}
        return info | {"ecsv_arrays": sorted(self.arrays)}

    def regular_file_exists(self, input_file, **kwargs):
        return self.inner.regular_file_exists(input_file, **kwargs)


# ----------------------------------------------------------------------------------------------------------------
# Row-parallel companion files, derived columns, nested packing
# ----------------------------------------------------------------------------------------------------------------
def companion_paths(main_path, companions: list[dict]) -> list[Path]:
    """Companion of <dir>/<stem>.fits for each {dir, suffix}: <dir>/<companion dir>/<stem><suffix>.fits, where the
    companion dir is relative to the main file's directory ("" = the same directory)."""
    main_path = Path(main_path)
    return [(main_path.parent / (c.get("dir") or ".") / f"{main_path.stem}{c.get('suffix', '')}{main_path.suffix}").resolve()
            for c in companions]


def numpy_to_arrow_column(array: np.ndarray, flatten_tensors: bool = True) -> pa.Array:
    """One FITS column read by fitsio -> Arrow, typed as hats-import's FitsReader types it (astropy decodes strings
    and drops their trailing blanks; multi-dimensional columns become fixed-size lists)."""
    if array.dtype.kind == "S":
        array = np.char.rstrip(np.char.decode(array, "ascii", errors="replace"))
    try:
        from hats_import.catalog.file_readers.fits import _np_to_pyarrow_array
    except ImportError:  # pragma: no cover - private helper moved: same rules, inline
        _np_to_pyarrow_array = None
    if _np_to_pyarrow_array is not None:
        return _np_to_pyarrow_array(array, flatten_tensors=flatten_tensors)
    array = np.asanyarray(array, dtype=array.dtype.newbyteorder("="))
    values = pa.array(array.reshape(-1))
    if array.ndim == 1:
        return values
    return pa.FixedSizeListArray.from_arrays(values, int(np.prod(array.shape[1:])))


def records_to_arrow(rec: np.ndarray, columns=None, flatten_tensors: bool = True) -> pa.Table:
    names = [n for n in rec.dtype.names if columns is None or n in columns]
    return pa.table({n: numpy_to_arrow_column(rec[n], flatten_tensors) for n in names})


def load_derive_hook(spec: str):
    """'path/to/file.py:function' -> the function. It is called as function(get, input_file) where get(column)
    returns the chunk's column as a numpy array, and returns {new column: numpy array} appended in order."""
    path, _, func = str(spec).rpartition(":")
    if not path or not func:
        raise ValueError(f"[reader].derive must be 'file.py:function', got {spec!r}")
    key = (str(Path(path).resolve()), func)
    if key not in _DERIVE_CACHE:
        module_spec = importlib.util.spec_from_file_location(f"derive_{abs(hash(key[0]))}", key[0])
        module = importlib.util.module_from_spec(module_spec)
        module_spec.loader.exec_module(module)
        _DERIVE_CACHE[key] = getattr(module, func)
    return _DERIVE_CACHE[key]


_DERIVE_CACHE: dict = {}


def apply_derive(table: pa.Table, spec: str, input_file) -> pa.Table:
    if not spec:
        return table
    hook = load_derive_hook(spec)
    new = hook(lambda c: table.column(c).to_numpy(), str(input_file))
    for name, values in new.items():
        if name in table.column_names:
            raise ValueError(f"[reader].derive returned {name!r}, which already exists")
        table = table.append_column(name, pa.array(np.asarray(values)))
    return table


def conform_to_schema(table: pa.Table, schema: pa.Schema) -> pa.Table:
    """Give a chunk exactly the target schema: columns in the target order, missing ones as all-null columns of the
    target type (e.g. bands a survey region does not have), the others cast to the target type. A column that is not
    in the target is an error: the target must be the union of all inputs."""
    extra = [c for c in table.column_names if c not in schema.names]
    if extra:
        raise ValueError(f"[reader].conform_schema: columns not in the target schema: {extra}")
    cols = []
    for f in schema:
        if f.name in table.column_names:
            c = table.column(f.name)
            cols.append(c if c.type.equals(f.type) else c.cast(f.type))
        else:
            cols.append(pa.nulls(table.num_rows, type=f.type))
    return pa.Table.from_arrays(cols, schema=pa.schema(list(schema)))


class ConformSchemaReader(InputReader):
    """Give every full chunk of the inner reader exactly the [reader].conform_schema schema (conform_to_schema).
    Works on any input format: pandas chunks are converted to Arrow first. Chunks read for a column subset (the
    mapping stage asks for the coordinates only) pass through unchanged."""

    def __init__(self, inner: InputReader, schema_path: str):
        self.inner = inner
        self.schema_path = str(schema_path)   # the schema is read in read(): the reader is pickled to the workers

    def read(self, input_file, read_columns=None):
        target = read_target_schema(self.schema_path) if read_columns is None else None
        for chunk in self.inner.read(input_file, read_columns=read_columns):
            if target is not None:
                table = chunk if isinstance(chunk, pa.Table) else pa.Table.from_pandas(chunk, preserve_index=False)
                chunk = conform_to_schema(table, target)
            yield chunk

    def provenance_info(self) -> dict:
        info = self.inner.provenance_info() if hasattr(self.inner, "provenance_info") else {}
        return info | {"conform_schema": self.schema_path}

    def regular_file_exists(self, input_file, **kwargs):
        return self.inner.regular_file_exists(input_file, **kwargs)


def read_target_schema(path) -> pa.Schema:
    """The [reader].conform_schema file (any Parquet file or _common_metadata) without the spatial index column."""
    import pyarrow.parquet as pq

    schema = pq.read_schema(path)
    return pa.schema([f for f in schema if f.name != "_healpix_29"])


def nested_columns(names: list[str], patterns: list[str]) -> list[str]:
    """Column names matching the [reader].pack_nested patterns (fnmatch), in table order."""
    return [n for n in names if any(fnmatch.fnmatchcase(n, p) for p in patterns)]


def pack_nested(table: pa.Table, name: str, patterns: list[str]) -> pa.Table:
    """Pack list columns into one struct<list...> column `name` at the position of the first packed column.
    Fixed-size lists become plain lists (nested-pandas' layout); every row must have equal lengths across the
    packed columns, or the nested frame would be ragged."""
    cols = nested_columns(table.column_names, patterns)
    if not cols:
        raise ValueError(f"[reader].pack_nested: no column matches {patterns}")
    arrays, lengths = [], None
    for c in cols:
        arr = table.column(c).combine_chunks()
        if pa.types.is_fixed_size_list(arr.type):
            arr = arr.cast(pa.list_(arr.type.value_type))
        if not (pa.types.is_list(arr.type) or pa.types.is_large_list(arr.type)):
            raise ValueError(f"[reader].pack_nested: {c} is {arr.type}, not a list column")
        ln = pc.fill_null(pc.list_value_length(arr), 0).to_numpy(zero_copy_only=False)
        if lengths is None:
            lengths = ln
        elif not np.array_equal(lengths, ln):
            raise ValueError(f"[reader].pack_nested: {c} has different per-row lengths from {cols[0]}")
        arrays.append(arr)
    struct = pa.StructArray.from_arrays(arrays, names=cols)
    pos = table.column_names.index(cols[0])
    table = table.drop_columns(cols)
    return table.add_column(pos, name, struct)


class CompanionFitsReader(InputReader):
    """Read a main FITS table and its row-parallel companions as one table (see the module docstring)."""

    def __init__(self, companions: list[dict], key_columns: list[str], chunksize: int = 200_000, hdu: int = 1,
                 flatten_tensors: bool = True, column_names=None, skip_column_names=None, derive: str = "",
                 pack_nested: dict | None = None):
        self.companions = list(companions)
        self.key_columns = list(key_columns)
        self.chunksize = int(chunksize)
        self.hdu = int(hdu)
        self.flatten_tensors = flatten_tensors
        self.column_names = list(column_names) if column_names else None
        self.skip_column_names = set(skip_column_names or [])
        self.derive = derive   # absolute 'file.py:function' (import_collection resolves it against the work dir)
        self.pack_nested = dict(pack_nested or {})

    def read(self, input_file, read_columns=None):
        import fitsio

        input_file = self.regular_file_exists(input_file)
        if read_columns is not None:
            # hats-import's mapping stage asks for the coordinates only: when the main file has every requested
            # column, read just those (no companions, derived or nested columns); otherwise fall through to the full
            # read and select at the end
            with fitsio.FITS(str(input_file)) as fh:
                main_cols = set(fh[self.hdu].get_colnames())
            if set(read_columns) <= main_cols:
                with fitsio.FITS(str(input_file)) as fh:
                    hdu = fh[self.hdu]
                    n = hdu.get_nrows()
                    for start in range(0, n, self.chunksize):
                        rec = hdu.read(columns=list(read_columns), rows=np.arange(start, min(start + self.chunksize, n)))
                        yield records_to_arrow(rec, flatten_tensors=self.flatten_tensors).select(list(read_columns))
                return
        paths = [Path(input_file)] + companion_paths(input_file, self.companions)
        handles = [fitsio.FITS(str(p)) for p in paths]
        try:
            hdus = [h[self.hdu] for h in handles]
            nrows = [h.get_nrows() for h in hdus]
            if len(set(nrows)) != 1:
                raise ValueError(f"companion row counts differ for {input_file}: {dict(zip(map(str, paths), nrows))}")
            for start in range(0, nrows[0], self.chunksize):
                stop = min(start + self.chunksize, nrows[0])
                parts = [records_to_arrow(h[start:stop], flatten_tensors=self.flatten_tensors) for h in hdus]
                table = parts[0]
                for path, part in zip(paths[1:], parts[1:]):
                    for k in self.key_columns:
                        if not table.column(k).equals(part.column(k)):
                            raise ValueError(f"key column {k} differs between {paths[0].name} and {path.name} in rows {start}:{stop}")
                    for c in part.column_names:
                        if c in table.column_names:
                            if c not in self.key_columns and not table.column(c).equals(part.column(c)):
                                raise ValueError(f"column {c} repeated in {path.name} differs from the earlier file in rows {start}:{stop}")
                            continue
                        table = table.append_column(c, part.column(c))
                if self.column_names:
                    table = table.select([c for c in table.column_names if c in self.column_names])
                elif self.skip_column_names:
                    table = table.select([c for c in table.column_names if c not in self.skip_column_names])
                table = apply_derive(table, self.derive, input_file)
                if self.pack_nested.get("name"):
                    table = pack_nested(table, self.pack_nested["name"], list(self.pack_nested.get("columns") or []))
                if read_columns is not None:
                    table = table.select([c for c in read_columns if c in table.column_names])
                yield table
        finally:
            for h in handles:
                h.close()

    def provenance_info(self) -> dict:
        return {"input_reader_type": "CompanionFitsReader", "companions": self.companions, "key_columns": self.key_columns,
                "chunksize": self.chunksize, "derive": self.derive, "pack_nested": self.pack_nested}
