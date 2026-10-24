#!/usr/bin/env python
"""choose_encoding.py — choose dictionary or BYTE_STREAM_SPLIT for each float column, and the row-group size, by
measuring real partition files, and record the choice in [writer].

hats >= 0.11.1 writes every float leaf BYTE_STREAM_SPLIT (BSS). BSS shrinks floats whose values are nearly all
distinct (0.6-0.8x of dictionary) and inflates floats with few distinct values 2-6x (values rounded by an archive's
CSV export, flags and counts stored as floats, constants). Catalogs made of such floats get LARGER with the
default (measured 1.3-2.5x on archive-CSV catalogs); catalogs from binary FITS or Parquet get smaller (0.6-0.8x).
The decision has to be made per column, so this script measures it:

  1. each input file is written twice with the run's [writer] settings: `dict` (dictionary on every leaf except
     _healpix_29) and `hats` (the hats defaults, BSS on every float leaf);
  2. per float leaf the smaller column chunk (summed over row groups, from the footers) wins; non-float leaves stay
     dictionary and _healpix_29 stays DELTA_BINARY_PACKED; the result is the `best` layout, which is never larger
     than either variant by more than page overhead;
  3. `best` is written with 100 000- and 250 000-row row groups. Under BSS the row-group size hardly matters; under
     dictionary every row group repeats its dictionaries (100 000-row groups cost 5-15 %). Recommendation: 100 000
     rows when dictionary-encoded leaves hold less than half of the bytes of `best`, else 250 000;
  4. `best` is read back and compared with the input (floats with NaN == NaN); a difference stops the script.

The per-leaf fraction of distinct values (on a 200 000-value sample) is reported as the explanation: BSS wins near 1.

Usage:
    choose_encoding.py [--config catalog.toml]                        # measure, print, write encoding_choice.json
    choose_encoding.py [--config catalog.toml] --write                # also set [writer].use_dictionary and row_group_size
    choose_encoding.py [--config catalog.toml] --file P [--file Q]    # measure these Parquet files
    options: --tmp DIR (scratch for the variants; default <work_dir>/choose_encoding_tmp), --keep (keep the variant
             files), --no-pypi

Default input: the largest of the first 50 partition files (path order) of the trial build
<build_root>/trial_<names.collection>/trial_<names.catalog> (import_collection.py --trial-file), or, for
[run].route = "reimport", of the catalog in [run].reimport_from. Pass --file to measure chosen partitions instead
(several files are summed column by column before choosing). Use partitions of at least ~100 000 rows: a tiny file
favours BSS because its dictionaries are not yet amortised.

Output: <work_dir>/encoding_choice.json (per leaf: dict and BSS bytes, choice, distinct fraction; totals; dict, hats
and best sizes relative to each input file; the measured row-group sizes and the recommendation) and a one-screen
summary. Without --write the [writer] change is printed as a diff; with --write it is applied to catalog.toml:
`use_dictionary` = sorted list of the leaves that get a dictionary (every non-float leaf plus the floats where
dictionary is smaller; the key is removed when no float prefers dictionary, i.e. the hats default is right) and
`row_group_size` = the recommendation. The rest of the file, comments included, is kept. Exit code 3 when the round
trip fails (nothing is written to catalog.toml then).
"""
from __future__ import annotations

import argparse
import difflib
import json
import re
import shutil
import sys
import tempfile
import time
import tomllib
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _common import check_versions, data_files, load_config, pin_cpus, runlog, writer_kwargs  # noqa: E402

HP = "_healpix_29"
RG_DEFAULT = 100_000          # hats DEFAULT_PARQUET_ROW_GROUP_SIZE; the dict/hats/best comparison uses it
RG_LARGE = 250_000
DICT_SHARE_LIMIT = 0.5        # dictionary share of the best layout above which RG_LARGE is recommended
SAMPLE_VALUES = 200_000
DEFAULT_CANDIDATES = 50
MB = 1e6


# ----------------------------------------------------------------------------------------------------------------
# inputs
# ----------------------------------------------------------------------------------------------------------------
def partition_files(path: Path) -> list[Path]:
    """Parquet files of a partition path: the file itself, or the files in an Npix=N/ directory."""
    return sorted(path.glob("*.parquet")) if path.is_dir() else [path]


def default_files(cfg) -> list[Path]:
    """The largest of the first DEFAULT_CANDIDATES partition files of the trial build, or of [run].reimport_from."""
    if cfg["run"].get("route") == "reimport":
        from hats.io import paths

        from import_collection import reimport_source

        cat = reimport_source(cfg)
        pixels = sorted(cat.get_healpix_pixels(), key=lambda p: (p.order, p.pixel))[:DEFAULT_CANDIDATES]
        where = f"[run].reimport_from = {cfg['run'].get('reimport_from')}"
        candidates = []
        for pixel in pixels:
            p = paths.pixel_catalog_file(cat.catalog_base_dir, pixel, npix_suffix=cat.catalog_info.npix_suffix)
            candidates += partition_files(Path(str(p.path if hasattr(p, "path") else p)))
    else:
        trial = cfg.build_root / f"trial_{cfg['names']['collection']}" / f"trial_{cfg['names']['catalog']}"
        where = f"the trial build {trial}"
        candidates = [f for p in data_files(trial) for f in partition_files(p)]
    existing = [f for f in candidates[:DEFAULT_CANDIDATES] if f.is_file()]
    if not existing:   # a partial copy (e.g. a staged mini-source with one partition): take whatever partition files exist
        root = Path(str(candidates[0])).parents[2] if candidates else None
        if root is not None and root.is_dir():
            existing = sorted(root.rglob("*.parquet"))[:DEFAULT_CANDIDATES]
            if existing:
                print(f"note: none of the first {len(candidates[:DEFAULT_CANDIDATES])} listed partitions exist under {where}; using the {len(existing)} partition file(s) found on disk")
    candidates = existing
    if not candidates:
        raise SystemExit(f"no partition files in {where}: run import_collection.py --trial-file FILE first, or pass --file")
    best = max(candidates, key=lambda f: f.stat().st_size)
    print(f"input: the largest of the first {len(candidates)} partition file(s) of {where}")
    return [best]


# ----------------------------------------------------------------------------------------------------------------
# measurement
# ----------------------------------------------------------------------------------------------------------------
def footer_sizes(path: Path) -> tuple[dict, dict, pq.FileMetaData]:
    """Per-leaf compressed bytes summed over row groups, and per-leaf (physical type, is_float), from a footer."""
    md = pq.read_metadata(path)
    sizes: dict = {}
    for i in range(md.num_row_groups):
        rg = md.row_group(i)
        for j in range(rg.num_columns):
            c = rg.column(j)
            sizes[c.path_in_schema] = sizes.get(c.path_in_schema, 0) + c.total_compressed_size
    kinds = {}
    for j in range(len(md.schema)):
        col = md.schema.column(j)
        is_float = col.physical_type in ("FLOAT", "DOUBLE") or col.logical_type.type == "FLOAT16"
        kinds[col.path] = (col.physical_type.lower(), is_float)
    return sizes, kinds, md


def leaf_values(table: pa.Table) -> dict[str, pa.ChunkedArray]:
    """Every leaf of the table as a flat chunked array of its values, keyed by its Parquet path
    ("a.list.element", "s.x", "m.key_value.key"); used for the distinct-value fractions only."""
    out: dict[str, list] = {}

    def walk(path: str, arr: pa.Array) -> None:
        t = arr.type
        if pa.types.is_dictionary(t):
            arr, t = arr.dictionary_decode(), t.value_type
        if pa.types.is_struct(t):
            for i in range(t.num_fields):
                walk(f"{path}.{t.field(i).name}", arr.field(i))
        elif pa.types.is_list(t) or pa.types.is_large_list(t) or pa.types.is_fixed_size_list(t):
            walk(f"{path}.list.element", arr.flatten())
        elif pa.types.is_map(t):
            walk(f"{path}.key_value.key", arr.keys)
            walk(f"{path}.key_value.value", arr.items)
        else:
            out.setdefault(path, []).append(arr)

    for name in table.column_names:
        for chunk in table.column(name).chunks:
            walk(name, chunk)
    return {k: pa.chunked_array(v) for k, v in out.items()}


def distinct_fraction(values: pa.ChunkedArray, rng: np.random.Generator) -> float | None:
    """Distinct values / values on a random sample of SAMPLE_VALUES non-null (and, for floats, finite) values."""
    try:
        n = len(values)
        if n > SAMPLE_VALUES:
            values = values.take(pa.array(np.sort(rng.choice(n, SAMPLE_VALUES, replace=False))))
        values = pc.drop_null(values)
        x = values.to_numpy()
        if pa.types.is_floating(values.type):
            x = x[np.isfinite(x)]
        if x.size == 0:
            return None
        try:
            uniq = np.unique(x).size
        except TypeError:   # mixed objects (e.g. decimals with None) that numpy cannot sort
            uniq = len(set(x.tolist()))
        return round(uniq / x.size, 4)
    except Exception:  # noqa: BLE001 - an explanation column only; never stop the measurement for it
        return None


def same_values(a, b) -> bool:
    """a == b for arrow arrays, with NaN == NaN in float values (Arrow's equals treats NaN as unequal)."""
    if isinstance(a, pa.ChunkedArray):
        a = a.combine_chunks()
    if isinstance(b, pa.ChunkedArray):
        b = b.combine_chunks()
    if a.type != b.type or len(a) != len(b):
        return False
    if a.equals(b):
        return True
    if not a.is_null().equals(b.is_null()):
        return False
    t = a.type
    if pa.types.is_dictionary(t):
        return same_values(a.dictionary_decode(), b.dictionary_decode())
    if pa.types.is_floating(t):
        return bool(np.array_equal(a.to_numpy(zero_copy_only=False), b.to_numpy(zero_copy_only=False), equal_nan=True))
    if pa.types.is_struct(t):
        return all(same_values(a.field(i), b.field(i)) for i in range(t.num_fields))
    if pa.types.is_list(t) or pa.types.is_large_list(t):
        return (pc.list_value_length(a).equals(pc.list_value_length(b))
                and same_values(a.flatten(), b.flatten()))
    if pa.types.is_fixed_size_list(t):
        return same_values(a.flatten(), b.flatten())
    if pa.types.is_map(t):
        return (pc.list_value_length(a).equals(pc.list_value_length(b))
                and same_values(a.keys, b.keys) and same_values(a.items, b.items))
    return False


def round_trip(table: pa.Table, path: Path) -> list[str]:
    """Columns of `path` that differ from `table` (empty list = identical)."""
    back = pq.ParquetFile(path).read()
    if back.num_rows != table.num_rows or back.column_names != table.column_names:
        return [f"rows {back.num_rows} vs {table.num_rows}, columns {back.column_names[:5]}... vs {table.column_names[:5]}..."]
    return [name for name in table.column_names if not same_values(table.column(name), back.column(name))]


def write_variant(table: pa.Table, path: Path, kwargs: dict) -> tuple[int, dict, pq.FileMetaData]:
    pq.write_table(table, path, **kwargs)
    sizes, _, md = footer_sizes(path)
    return path.stat().st_size, sizes, md


# ----------------------------------------------------------------------------------------------------------------
# catalog.toml [writer] edit
# ----------------------------------------------------------------------------------------------------------------
HEADER = re.compile(r"^\s*\[\[?\s*([^\]]+?)\s*\]\]?\s*(#.*)?$")
KEY = re.compile(r"^\s*([A-Za-z0-9_-]+)\s*=")


def value_end(lines: list[str], i: int) -> int:
    """Index of the last line of the TOML value that starts on line i (multi-line arrays and tables, strings and
    comments inside them are followed)."""
    depth, in_str = 0, None
    for j in range(i, len(lines)):
        s = lines[j]
        k = s.index("=") + 1 if j == i else 0
        while k < len(s):
            if in_str:
                if len(in_str) == 3:
                    if s.startswith(in_str, k):
                        in_str, k = None, k + 3
                        continue
                    k += 2 if (in_str == '"""' and s[k] == "\\") else 1
                    continue
                if in_str == '"' and s[k] == "\\":
                    k += 2
                    continue
                if s[k] == in_str:
                    in_str = None
                k += 1
                continue
            c = s[k]
            if c == "#":
                break
            if s.startswith('"""', k) or s.startswith("'''", k):
                in_str, k = s[k:k + 3], k + 3
                continue
            if c in "\"'":
                in_str = c
            elif c in "[{":
                depth += 1
            elif c in "]}":
                depth -= 1
            k += 1
        if in_str in ('"', "'"):   # single-line strings end with their line
            in_str = None
        if depth <= 0 and not in_str:
            return j
    raise SystemExit(f"cannot find the end of the value starting at line {i + 1} of the config")


def toml_list(key: str, items: list[str], comment: str, width: int = 116) -> list[str]:
    lines, cur = [f"{key} = [   # {comment}"], "    "
    for item in items:
        tok = json.dumps(item, ensure_ascii=False) + ", "   # a JSON string is a valid TOML basic string
        if len(cur) + len(tok) > width and cur.strip():
            lines.append(cur.rstrip())
            cur = "    "
        cur += tok
    if cur.strip():
        lines.append(cur.rstrip())
    lines[-1] = lines[-1].rstrip(",")
    return lines + ["]"]


def edit_writer(text: str, use_dictionary: list[str] | None, row_group_size: int, note: str) -> str:
    """catalog.toml text with [writer].use_dictionary (None = remove the key) and [writer].row_group_size set;
    every other line is kept as it is."""
    lines = text.splitlines()
    start = next((i for i, l in enumerate(lines) if (m := HEADER.match(l)) and m.group(1) == "writer"), None)
    if start is None:
        lines += ["", "[writer]"]
        start = len(lines) - 1
    # walk the section: key spans (multi-line values included) until the next table header
    spans, i, end = {}, start + 1, len(lines)
    while i < len(lines):
        line = lines[i]
        if HEADER.match(line) and not KEY.match(line):
            end = i
            break
        m = KEY.match(line)
        if m:
            j = value_end(lines, i)
            spans[m.group(1)] = (i, j)
            i = j + 1
            continue
        i += 1
    last = end   # insertion point for new keys: after the section's last non-blank line
    while last - 1 > start and not lines[last - 1].strip():
        last -= 1
    new_keys = {}
    if use_dictionary:
        new_keys["use_dictionary"] = toml_list("use_dictionary", use_dictionary, note)
    new_keys["row_group_size"] = [f"row_group_size = {row_group_size}".ljust(36)
                                  + f"# {note.split(':')[0]}: rows per row group; replaces [partitioning].row_groups"]
    edits, appended = [], []   # (first, last, replacement) on the original line numbers, applied bottom-up
    for key in ("use_dictionary", "row_group_size"):
        if key in spans:
            edits.append((spans[key][0], spans[key][1], new_keys.get(key, [])))
        elif key in new_keys:
            appended += new_keys[key]
    if appended:
        edits.append((last, last - 1, appended))
    for first, stop, repl in sorted(edits, key=lambda e: (e[0], e[1]), reverse=True):
        lines[first:stop + 1] = repl
    return "\n".join(lines) + "\n"


def check_edit(old_text: str, new_text: str, use_dictionary: list[str] | None, row_group_size: int) -> None:
    """The edited file must parse, carry the new values and leave everything else unchanged."""
    old, new = tomllib.loads(old_text), tomllib.loads(new_text)
    want = dict(old.get("writer", {}))
    want.pop("use_dictionary", None)
    if use_dictionary:
        want["use_dictionary"] = use_dictionary
    want["row_group_size"] = row_group_size
    if new.get("writer") != want or {k: v for k, v in new.items() if k != "writer"} != {k: v for k, v in old.items() if k != "writer"}:
        raise SystemExit("internal error: the edited catalog.toml does not round-trip; nothing written")


# ----------------------------------------------------------------------------------------------------------------
def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--config", default="catalog.toml")
    ap.add_argument("--file", action="append", default=[], help="Parquet partition file to measure (repeatable)")
    ap.add_argument("--write", action="store_true", help="set [writer].use_dictionary and row_group_size in the config")
    ap.add_argument("--tmp", help="scratch directory for the variant files (default <work_dir>/choose_encoding_tmp)")
    ap.add_argument("--keep", action="store_true", help="keep the variant files")
    ap.add_argument("--no-pypi", action="store_true", help="skip the newer-version lookup")
    a = ap.parse_args(argv)

    cfg = load_config(a.config)
    check_versions(cfg, need=("hats",), warn_newer=not a.no_pypi)
    pin_cpus(cfg)   # one process, pyarrow threads only: pinning is the cap that applies
    from hats.io.file_io.file_io import get_parquet_write_table_kwargs

    files = [Path(f) for f in a.file] or default_files(cfg)
    for f in files:
        if not f.is_file():
            raise SystemExit(f"not a file: {f}")
    base = writer_kwargs(cfg)
    for k in ("use_dictionary", "row_group_size"):   # what is being re-decided
        base.pop(k, None)
    previous = {k: cfg["writer"].get(k) for k in ("use_dictionary", "row_group_size")}
    fixed = set((base.get("column_encoding") or {}).keys())   # leaves the user pinned with [writer].column_encoding
    tmp_root = Path(a.tmp) if a.tmp else cfg.work_dir / "choose_encoding_tmp"
    tmp_root.mkdir(parents=True, exist_ok=True)
    tmp = Path(tempfile.mkdtemp(prefix="run_", dir=tmp_root))
    runlog(cfg, "start", "encoding", f"choose_encoding on {len(files)} file(s)", files=[str(f) for f in files])
    t0 = time.time()
    rng = np.random.default_rng(0)

    def kwargs_for(schema: pa.Schema, dictionary: list[str] | None, rows: int = RG_DEFAULT) -> dict:
        extra = dict(base) | {"row_group_size": rows}
        if dictionary is not None:
            extra["use_dictionary"] = dictionary
        return get_parquet_write_table_kwargs(schema, write_table_kwargs=extra)

    try:
        # pass 1: dict and hats variants of every file
        per_file, leaf_dict, leaf_bss, kinds, distinct, tables = [], {}, {}, {}, {}, {}
        for n, f in enumerate(files):
            t1 = time.time()
            table = pq.ParquetFile(f).read()
            src_md = pq.read_metadata(f)
            values = leaf_values(table)
            leaves = list(values)   # Parquet leaf paths, schema order
            all_dict = [p for p in leaves if p != HP and p not in fixed]
            d_bytes, d_sizes, _ = write_variant(table, tmp / f"{n}_dict.parquet", kwargs_for(table.schema, all_dict))
            h_bytes, h_sizes, _ = write_variant(table, tmp / f"{n}_hats.parquet", kwargs_for(table.schema, None))
            _, k, _ = footer_sizes(tmp / f"{n}_hats.parquet")
            missing = set(k) - set(leaves)
            if missing:
                print(f"warning: footer leaves not found in the arrow schema walk (no distinct fraction): {sorted(missing)[:5]}")
            kinds |= k
            for p in k:
                leaf_dict[p] = leaf_dict.get(p, 0) + d_sizes.get(p, 0)
                leaf_bss[p] = leaf_bss.get(p, 0) + h_sizes.get(p, 0)
                if p in values:
                    distinct.setdefault(p, []).append(distinct_fraction(values[p], rng))
            if not a.keep:
                for v in ("dict", "hats"):
                    (tmp / f"{n}_{v}.parquet").unlink()
            per_file.append({"path": str(f), "bytes": f.stat().st_size, "rows": table.num_rows,
                             "row_groups": src_md.num_row_groups, "created_by": src_md.created_by,
                             "variant_bytes": {"dict": d_bytes, "hats": h_bytes}})
            print(f"measured {f} ({f.stat().st_size / MB:.1f} MB, {table.num_rows:,} rows, {len(k)} leaves) "
                  f"in {time.time() - t1:.0f} s")
            if len(files) == 1:
                tables[n] = table
            del table, values

        # decision
        choice = {}
        for p, (_, is_float) in kinds.items():
            if p == HP:
                choice[p] = "delta"
            elif p in fixed:
                choice[p] = "fixed"
            elif is_float:
                choice[p] = "bss" if leaf_bss[p] < leaf_dict[p] else "dict"
            else:
                choice[p] = "dict"
        dictionary = sorted(p for p, c in choice.items() if c == "dict")
        floats = [p for p, (_, is_float) in kinds.items() if is_float and p not in fixed and p != HP]
        float_dict = [p for p in floats if choice[p] == "dict"]
        use_dictionary = dictionary if float_dict else None   # None: the hats default is already the best layout

        # pass 2: best layout at two row-group sizes, round trip
        best_bytes = {RG_DEFAULT: 0, RG_LARGE: 0}
        best_sizes: dict = {}
        failures = {}
        for n, f in enumerate(files):
            table = tables.pop(n, None) or pq.ParquetFile(f).read()
            info = per_file[n]
            info["variant_bytes"]["best_row_groups"] = {}
            for rows in (RG_DEFAULT, RG_LARGE):
                path = tmp / f"{n}_best_{rows}.parquet"
                b, sizes, md = write_variant(table, path, kwargs_for(table.schema, dictionary, rows))
                best_bytes[rows] += b
                info["variant_bytes"]["best_row_groups"][str(rows)] = {"bytes": b, "row_groups": md.num_row_groups}
                if rows == RG_DEFAULT:
                    info["variant_bytes"]["best"] = b
                    for p, s in sizes.items():
                        best_sizes[p] = best_sizes.get(p, 0) + s
                    bad = round_trip(table, path)
                    info["roundtrip_ok"] = not bad
                    if bad:
                        failures[str(f)] = bad
                if not a.keep:
                    path.unlink()
            info["ratios"] = {v: round(info["variant_bytes"][v] / info["bytes"], 3) for v in ("dict", "hats", "best")}
            info["ratios"][f"best_{RG_LARGE}"] = round(info["variant_bytes"]["best_row_groups"][str(RG_LARGE)]["bytes"] / info["bytes"], 3)
            del table
    finally:
        if not a.keep:
            shutil.rmtree(tmp, ignore_errors=True)
            if not a.tmp:
                try:
                    tmp_root.rmdir()
                except OSError:
                    pass

    if failures:
        msg = f"ROUND TRIP FAILED: the best layout read back differs from the input in {failures}"
        print(msg)
        runlog(cfg, "end", "encoding", msg, status="fail", elapsed_s=round(time.time() - t0, 1))
        return 3

    # totals and recommendation
    total_in = sum(i["bytes"] for i in per_file)
    tot = {v: sum(i["variant_bytes"][v] for i in per_file) for v in ("dict", "hats", "best")}
    chunk_total = sum(best_sizes.values()) or 1
    dict_share = sum(best_sizes.get(p, 0) for p in dictionary) / chunk_total
    recommended = RG_LARGE if dict_share >= DICT_SHARE_LIMIT else RG_DEFAULT
    leaves_out = {}
    for p, (ptype, is_float) in kinds.items():
        fr = [x for x in distinct.get(p, []) if x is not None]
        leaves_out[p] = {"type": ptype, "float": is_float, "dict_bytes": leaf_dict[p],
                         "bss_bytes": leaf_bss[p] if is_float else None, "best_bytes": best_sizes.get(p, 0),
                         "choice": choice[p], "distinct_fraction": round(float(np.mean(fr)), 4) if fr else None}
    result = {
        "created": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "config": str(Path(a.config).resolve()),
        "writer_overrides_kept": base,
        "files": per_file,
        "totals": {
            "input_bytes": total_in, "dict_bytes": tot["dict"], "hats_bytes": tot["hats"], "best_bytes": tot["best"],
            "ratio_dict": round(tot["dict"] / total_in, 3), "ratio_hats": round(tot["hats"] / total_in, 3),
            "ratio_best": round(tot["best"] / total_in, 3), "best_over_hats": round(tot["best"] / tot["hats"], 3),
            "leaves": len(kinds), "float_leaves": len(floats), "floats_bss": len(floats) - len(float_dict),
            "floats_dict": len(float_dict), "dictionary_share_of_best": round(dict_share, 3),
        },
        "row_groups": {"measured_bytes": {str(k): v for k, v in best_bytes.items()},
                       "ratio_large_over_default": round(best_bytes[RG_LARGE] / best_bytes[RG_DEFAULT], 3),
                       "rule": f"{RG_DEFAULT} when dictionary-encoded leaves hold < {DICT_SHARE_LIMIT:.0%} of the best layout, else {RG_LARGE}",
                       "recommended": recommended},
        "use_dictionary": use_dictionary,
        "row_group_size": recommended,
        "previous": previous,
        "leaves": leaves_out,
    }
    out = cfg.work_dir / "encoding_choice.json"
    out.write_text(json.dumps(result, indent=1, default=str) + "\n")

    # one-screen summary
    T = result["totals"]
    print()
    for i in per_file:
        r = i["ratios"]
        print(f"{Path(i['path']).name}: {i['bytes'] / MB:.1f} MB, {i['rows']:,} rows -> dict {r['dict']:.3f}x  "
              f"hats default {r['hats']:.3f}x  best {r['best']:.3f}x  (best at {RG_LARGE:,} rows {r[f'best_{RG_LARGE}']:.3f}x)")
    print(f"\nlargest leaves (bytes summed over {len(files)} file(s); BSS = the hats default for floats):")
    print(f"  {'leaf':<40} {'type':<10} {'dict MB':>8} {'BSS MB':>8} {'BSS/dict':>8} {'distinct':>8}  choice")
    top = sorted(leaves_out.items(), key=lambda kv: -max(kv[1]["dict_bytes"], kv[1]["bss_bytes"] or 0))[:10]
    for p, v in top:
        bss, dct, fr = v["bss_bytes"], v["dict_bytes"], v["distinct_fraction"]
        bss_mb = f"{bss / MB:.2f}" if bss is not None else "-"
        ratio = f"{bss / dct:.2f}" if bss is not None and dct else "-"
        frac = f"{fr:.3f}" if fr is not None else "-"
        print(f"  {p[:40]:<40} {v['type'][:10]:<10} {dct / MB:>8.2f} {bss_mb:>8} {ratio:>8} {frac:>8}  {v['choice']}")
    print(f"\nfloat leaves: BYTE_STREAM_SPLIT smaller on {T['floats_bss']}/{T['float_leaves']}, dictionary on {T['floats_dict']}"
          + (f"; {len(fixed)} leaves pinned by [writer].column_encoding" if fixed else ""))
    print(f"sizes vs input: dict {T['ratio_dict']:.3f}x, hats default {T['ratio_hats']:.3f}x, best {T['ratio_best']:.3f}x "
          f"(best = {T['best_over_hats']:.3f}x the hats default)")
    print(f"row groups of the best layout: {RG_DEFAULT:,} rows {best_bytes[RG_DEFAULT] / MB:.1f} MB, "
          f"{RG_LARGE:,} rows {best_bytes[RG_LARGE] / MB:.1f} MB ({result['row_groups']['ratio_large_over_default']:.3f}x); "
          f"dictionary-encoded leaves hold {dict_share:.0%} of the bytes -> row_group_size = {recommended:,}")
    print("use_dictionary: " + (f"{len(use_dictionary)} leaves ({len(float_dict)} of them floats)" if use_dictionary
                                else "not needed (BYTE_STREAM_SPLIT is smaller on every float leaf: the hats default)"))
    print(f"wrote {out}")

    # catalog.toml
    cfg_path = Path(a.config)
    old_text = cfg_path.read_text()
    note = (f"choose_encoding.py {result['created'][:10]}: best {T['ratio_best']:.3f}x, hats default {T['ratio_hats']:.3f}x "
            f"of {len(files)} input file(s); BSS on {T['floats_bss']}/{T['float_leaves']} floats")
    new_text = edit_writer(old_text, use_dictionary, recommended, note)
    check_edit(old_text, new_text, use_dictionary, recommended)
    diff = list(difflib.unified_diff(old_text.splitlines(), new_text.splitlines(), str(cfg_path), str(cfg_path) + " (new)",
                                     n=0, lineterm=""))
    if tomllib.loads(new_text).get("writer") == tomllib.loads(old_text).get("writer"):
        diff = []   # only the note comment would change
        print("[writer] already holds this choice; catalog.toml unchanged")
    elif a.write:
        cfg_path.write_text(new_text)
        print(f"updated [writer] in {cfg_path}:")
    else:
        print("[writer] change (apply with --write):")
    for line in diff[:60]:
        print("  " + line)
    if len(diff) > 60:
        print(f"  ... {len(diff) - 60} more diff lines")
    elapsed = round(time.time() - t0, 1)
    runlog(cfg, "end", "encoding", "encoding choice" + (" written to [writer]" if a.write and diff else ""), status="ok",
           elapsed_s=elapsed, ratio_dict=T["ratio_dict"], ratio_hats=T["ratio_hats"], ratio_best=T["ratio_best"],
           floats_bss=T["floats_bss"], floats_dict=T["floats_dict"], dictionary_share=T["dictionary_share_of_best"],
           row_group_size=recommended, use_dictionary_n=len(use_dictionary or []), written=bool(a.write and diff),
           previous_use_dictionary_n=len(previous["use_dictionary"] or []), previous_row_group_size=previous["row_group_size"])
    return 0


if __name__ == "__main__":
    sys.exit(main())
