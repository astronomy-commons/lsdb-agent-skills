"""_common.py — shared helpers for the hats-publish templates: configuration, versions, site variables, writer
settings, properties, file discovery and run-log entries. Every template imports this module; nothing here is
specific to a machine, a person or a dataset.
"""
from __future__ import annotations

import glob
import json
import os
import re
import shlex
import sys
import tomllib
from datetime import datetime, timezone
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path

REQUIRED_PACKAGES = ("hats", "lsdb", "hats-import")


# ----------------------------------------------------------------------------------------------------------------
# configuration
# ----------------------------------------------------------------------------------------------------------------
class Config(dict):
    """catalog.toml as nested dicts, with the paths resolved.

    Relative paths in the file are taken from the directory that holds catalog.toml (`config_dir`), not from the
    current directory: `work_dir = "."` is that directory, and relative `raw_dir`, `build_root` and `[source].files`
    globs are taken from the work directory."""

    config_dir: Path = Path(".")

    def derived(self, data: dict) -> "Config":
        """A new Config with other contents and the same config directory (for steps that rewrite sections)."""
        new = Config(data)
        new.config_dir = self.config_dir
        return new

    def path(self, value) -> Path:
        """A path from catalog.toml: absolute as given, relative to the work directory otherwise."""
        p = Path(str(value)).expanduser()
        return p if p.is_absolute() else (self.work_dir / p).resolve()

    @property
    def run(self) -> dict:
        return self["run"]

    @property
    def work_dir(self) -> Path:
        p = Path(self.run.get("work_dir") or ".").expanduser()
        return (p if p.is_absolute() else Path(self.config_dir) / p).resolve()

    @property
    def raw_dir(self) -> Path:
        if self.run.get("raw_dir"):
            return self.path(self.run["raw_dir"])
        root = Path(os.environ.get("HATS_PUBLISH_RAW_ROOT") or Path.cwd() / "raw")
        return root / self.run["survey"] / f"raw_{self.run['release']}"

    @property
    def build_root(self) -> Path:
        if self.run.get("build_root"):
            return self.path(self.run["build_root"])
        return Path(os.environ.get("HATS_PUBLISH_BUILD_ROOT") or Path.cwd() / "build")

    @property
    def collection_path(self) -> Path:
        return self.build_root / self["names"]["collection"]

    @property
    def catalog_path(self) -> Path:
        return self.collection_path / self["names"]["catalog"]


def load_config(path: str | Path = "catalog.toml") -> Config:
    path = Path(path)
    if not path.exists():
        raise SystemExit(f"config not found: {path} (copy templates/catalog.toml into the work directory)")
    site_env()   # make the site file's HATS_PUBLISH_* defaults visible to every template
    cfg = Config(tomllib.loads(path.read_text()))
    cfg.config_dir = path.resolve().parent
    for section in ("run", "source", "reader", "columns", "names", "partitioning", "margins", "index", "properties",
                    "writer", "dask", "ancillary", "verify", "hosting", "site", "registry", "versions"):
        cfg.setdefault(section, {})
    for key in ("survey", "release"):
        if not cfg["run"].get(key):
            raise SystemExit(f"[run].{key} is required in {path}")
    for key in ("collection", "catalog"):
        if not cfg["names"].get(key):
            raise SystemExit(f"[names].{key} is required in {path}")
    for key, value in [("names.collection", cfg["names"]["collection"]), ("names.catalog", cfg["names"]["catalog"]),
                       ("run.survey", cfg["run"]["survey"])]:
        if not re.fullmatch(r"[A-Za-z0-9._-]+", str(value)):
            raise SystemExit(f"[{key}] may only contain letters, digits, '.', '_' and '-': {value!r}")
    # the public link may be nested in a group directory of the web tree (e.g. "<group>/<catalog>")
    for key, value in [("names.public_name", cfg["names"].get("public_name") or cfg["names"]["collection"]),
                       ("hosting.web_link_name", cfg["hosting"].get("web_link_name") or "x")]:
        parts = str(value).split("/")
        if not all(re.fullmatch(r"[A-Za-z0-9._-]+", s) and s not in (".", "..") for s in parts):
            raise SystemExit(f"[{key}] must be '/'-separated names of letters, digits, '.', '_' and '-' "
                             f"(no empty, '.' or '..' parts): {value!r}")
    return cfg


# ----------------------------------------------------------------------------------------------------------------
# versions
# ----------------------------------------------------------------------------------------------------------------
def _parse(v: str) -> tuple:
    return tuple(int(x) for x in re.findall(r"\d+", v)[:3])


FLOOR_VERSION = "0.11.1"


def check_versions(cfg: Config, *, need: tuple[str, ...] = REQUIRED_PACKAGES, warn_newer: bool = True) -> dict:
    """Refuse to run below the minimum versions in [versions]; warn if PyPI has a newer release.

    The PyPI lookup is skipped with warn_newer=False (the scripts' --no-pypi). Otherwise it makes one connection
    attempt with a 2 s timeout; when that fails (a batch node without outbound network) the remaining packages are
    not looked up."""
    # 0.11.1 is a hard floor whatever the config says: older releases write floats with pyarrow's dictionary
    # encoding (about 1.4x larger files) and lack hats.io.file_io.get_parquet_write_table_kwargs
    minimums = {pkg: max(cfg["versions"].get(key, FLOOR_VERSION), FLOOR_VERSION, key=_parse)
                for pkg, key in (("hats", "hats"), ("lsdb", "lsdb"), ("hats-import", "hats_import"))}
    found = {}
    for pkg in need:
        try:
            found[pkg] = version(pkg)
        except PackageNotFoundError:
            raise SystemExit(f"{pkg} is not installed; `pip install -U {pkg}`") from None
        if _parse(found[pkg]) < _parse(minimums[pkg]):
            raise SystemExit(f"{pkg} {found[pkg]} is below the minimum {minimums[pkg]}; `pip install -U {pkg}`")
    if warn_newer:
        for pkg, have in found.items():
            latest = _latest_on_pypi(pkg)
            if latest is None:
                print("note: PyPI not reachable; newer-release check skipped", file=sys.stderr)
                break
            if latest and _parse(latest) > _parse(have):
                print(f"note: {pkg} {have} installed, {latest} is on PyPI", file=sys.stderr)
    print("versions: " + ", ".join(f"{k} {v}" for k, v in found.items()))
    return found


def _latest_on_pypi(pkg: str) -> str | None:
    try:
        import urllib.request

        with urllib.request.urlopen(f"https://pypi.org/pypi/{pkg}/json", timeout=2) as r:
            return json.load(r)["info"]["version"]
    except Exception:
        return None


# ----------------------------------------------------------------------------------------------------------------
# site variables
# ----------------------------------------------------------------------------------------------------------------
def _site_line(line: str) -> tuple[str, str] | None:
    """(name, value) of one `export HATS_PUBLISH_X=value` line of a site file, or None.

    Shell quoting and a trailing `# comment` are handled; a `${HATS_PUBLISH_X:-default}` value yields the default;
    `$VAR` / `${VAR}` references in the value are expanded from the environment. A value that still names an unset
    variable after expansion is skipped (the scripts then use their own default)."""
    try:
        words = shlex.split(line, comments=True)
    except ValueError:
        return None
    if words[:1] == ["export"]:
        words = words[1:]
    if len(words) != 1:
        return None
    m = re.fullmatch(r"(HATS_PUBLISH_[A-Z0-9_]+)=(.*)", words[0], flags=re.S)
    if not m:
        return None
    name, value = m.groups()
    d = re.fullmatch(r"\$\{" + name + r":?-(.*)\}", value, flags=re.S)
    if d:
        value = d.group(1)
    value = os.path.expandvars(value)
    if re.search(r"\$(\{?[A-Za-z_])", value):
        return None
    return name, value


def site_env() -> dict:
    """HATS_PUBLISH_* variables, loading HATS_PUBLISH_SITE_ENV (a file of `export KEY=value` lines) first.

    A variable that is already in the environment is never replaced, even when it is set to the empty string: an
    empty value is how a caller switches a site default off (job.sbatch exports HATS_PUBLISH_MAX_CPUS= and
    HATS_PUBLISH_MAX_MEM_GB= on a dedicated node, meaning no cap)."""
    env_file = os.environ.get("HATS_PUBLISH_SITE_ENV")
    if env_file and Path(env_file).exists():
        for line in Path(env_file).read_text().splitlines():
            parsed = _site_line(line)
            if parsed and parsed[0] not in os.environ:
                os.environ[parsed[0]] = parsed[1]
    return {k: v for k, v in os.environ.items() if k.startswith("HATS_PUBLISH_")}


def require_env(*names: str) -> dict:
    env = site_env()
    missing = [n for n in names if not env.get(n)]
    if missing:
        raise SystemExit("missing site variables: " + ", ".join(missing) + " (see CONFIG.md)")
    return env


# ----------------------------------------------------------------------------------------------------------------
# writer settings and properties
# ----------------------------------------------------------------------------------------------------------------
HIVE_LEAVES = ("Norder", "Dir", "Npix")   # columns hats-import adds to index tables (and old catalogs carry)


def schema_leaves(schema: pa.Schema) -> list[tuple[str, bool]]:
    """(Parquet leaf path, is_float) for every leaf of an Arrow schema, with Parquet's path spelling ("a.list.element",
    "s.x", "m.key_value.key") - the names use_dictionary and column_encoding take."""
    import pyarrow as pa

    out: list[tuple[str, bool]] = []

    def walk(path: str, t) -> None:
        if pa.types.is_dictionary(t):
            t = t.value_type
        if pa.types.is_struct(t):
            for i in range(t.num_fields):
                walk(f"{path}.{t.field(i).name}", t.field(i).type)
        elif pa.types.is_list(t) or pa.types.is_large_list(t) or pa.types.is_fixed_size_list(t):
            walk(f"{path}.list.element", t.value_type)
        elif pa.types.is_map(t):
            walk(f"{path}.key_value.key", t.key_type)
            walk(f"{path}.key_value.value", t.item_type)
        else:
            out.append((path, pa.types.is_floating(t)))

    for field in schema:
        walk(field.name, field.type)
    return out


def writer_kwargs(cfg: Config, schema: pa.Schema | None = None) -> dict:
    """Departures from the hats Parquet defaults listed in [writer]; passed as write_table_kwargs to hats-import
    and lsdb, which merge them over the defaults (empty for a standard run).

    The defaults (hats >= 0.11.1, hats.io.file_io.get_parquet_write_table_kwargs, hats#742) are the team standard:
    ZSTD level 15, page index, statistics, BYTE_STREAM_SPLIT on every float leaf, DELTA_BINARY_PACKED on
    _healpix_29 and on an integer leading sort column, dictionary on the rest, 100 000-row row groups.

    [writer].use_dictionary and [writer].row_group_size are what choose_encoding.py measures and writes.
    row_group_size reaches pq.write_table (the skill's own files, hats-import margins and indexes); hats-import and
    lsdb drop it for catalog partitions, which take their row groups from row_group_kwargs() instead.

    use_dictionary decides the FLOAT leaves only: a non-float leaf missing from the list would otherwise lose its
    dictionary (pyarrow encodes only the listed leaves). So the list is extended with Norder/Dir/Npix (index tables
    and stray hive columns; names absent from a table are ignored by pyarrow) and, when `schema` is given (the table
    about to be written), with every non-float leaf of that schema except _healpix_29 and [writer].column_encoding
    leaves. hats-import receives one list for the catalog, margins and indexes without a schema, so a hand-written
    list must still name the catalog's non-float leaves (choose_encoding.py writes them all)."""
    w = cfg["writer"]
    if w.get("use_byte_stream_split"):
        raise SystemExit("[writer].use_byte_stream_split is obsolete: hats >= 0.11.1 writes every float column "
                         "BYTE_STREAM_SPLIT; remove it (override single columns with [writer].column_encoding)")
    out = {k: w[k] for k in ("compression", "compression_level", "write_page_index", "write_statistics") if k in w}
    if "compression_level" in out:
        out["compression_level"] = int(out["compression_level"])
    # an empty list would mean "no dictionary on any column" to hats, not "use the defaults"
    if w.get("column_encoding"):
        out["column_encoding"] = dict(w["column_encoding"])
    if w.get("use_dictionary"):
        listed = list(w["use_dictionary"])
        pinned = set(out.get("column_encoding") or {}) | {"_healpix_29"}
        extra = [c for c in HIVE_LEAVES if c not in pinned]
        if schema is not None:
            extra += [path for path, is_float in schema_leaves(schema) if not is_float and path not in pinned]
        out["use_dictionary"] = listed + [c for c in dict.fromkeys(extra) if c not in set(listed)]
    if w.get("row_group_size"):
        out["row_group_size"] = writer_row_group_size(cfg)
    return out


def writer_row_group_size(cfg: Config) -> int:
    """[writer].row_group_size as a positive int, 0 when unset."""
    value = cfg["writer"].get("row_group_size") or 0
    try:
        value = int(value)
    except (TypeError, ValueError):
        raise SystemExit(f"[writer].row_group_size must be a whole number of rows: {value!r}") from None
    if value < 0:
        raise SystemExit(f"[writer].row_group_size must be positive: {value}")
    return value


def parquet_write_kwargs(cfg: Config, schema: pa.Schema) -> dict:
    """Full pyarrow.parquet.write_table keyword arguments for a table the skill writes itself: the hats defaults
    for this schema with the [writer] overrides, the same settings hats-import uses for the partitions
    (row_group_size = [writer].row_group_size when set, else the hats default of 100 000 rows)."""
    from hats.io.file_io.file_io import get_parquet_write_table_kwargs

    return get_parquet_write_table_kwargs(schema, write_table_kwargs=writer_kwargs(cfg, schema))


def row_group_kwargs(cfg: Config) -> dict | None:
    """hats-import's row_group_kwargs for the catalog partitions. [writer].row_group_size, when set, takes
    precedence over [partitioning].row_groups and becomes {"num_rows": N}: hats-import splits the partitions
    itself and drops row_group_size from the writer arguments, so this is how that setting reaches them."""
    if writer_row_group_size(cfg):
        return {"num_rows": writer_row_group_size(cfg)}
    rg = cfg["partitioning"].get("row_groups") or {}
    strategy = rg.get("strategy", "subtile_order_delta")
    if strategy == "single":
        return None
    if strategy == "num_rows":
        return {"num_rows": int(rg.get("value", 200_000))}
    return {"subtile_order_delta": int(rg.get("value", 1))}


def properties_dict(cfg: Config) -> dict:
    """addl_hats_properties: every non-empty [properties] key plus default columns and survey id."""
    p = cfg["properties"]
    out: dict = {}
    for key, value in p.items():
        if key == "extra":
            continue
        if key in ("t_min", "t_max"):
            if value:
                out[key] = float(value)
            continue
        if value not in ("", None, [], {}):
            out[key] = value
    out.update({k: v for k, v in (p.get("extra") or {}).items() if v not in ("", None)})
    cols = cfg["columns"]
    if cols.get("default"):
        out["hats_cols_default"] = " ".join(cols["default"])
    if cols.get("id"):
        ids = cols["id"] if isinstance(cols["id"], list) else [cols["id"]]
        out["hats_cols_survey_id"] = " ".join(ids)
    return out


OBS_REGIMES = {"Radio", "Millimeter", "Infrared", "Optical", "UV", "EUV", "X-ray", "Gamma-ray"}
HATS_FRAMES = {"equatorial", "galactic", "ecliptic"}


def validate_properties(cfg: Config) -> list[str]:
    """Problems with [properties] against the HATS note vocabularies (empty list = fine)."""
    p, problems = cfg["properties"], []
    if p.get("obs_regime") and p["obs_regime"] not in OBS_REGIMES:
        problems.append(f"obs_regime {p['obs_regime']!r} not in {sorted(OBS_REGIMES)}")
    if p.get("hats_frame") and p["hats_frame"] not in HATS_FRAMES:
        problems.append(f"hats_frame {p['hats_frame']!r} not in {sorted(HATS_FRAMES)}")
    if p.get("creator_did") and not p["creator_did"].startswith("ivo://"):
        problems.append("creator_did must be an IVOID (ivo://...)")
    if (p.get("t_min") or 0) > (p.get("t_max") or 0) > 0:
        problems.append("t_min is after t_max")
    return problems


# ----------------------------------------------------------------------------------------------------------------
# files
# ----------------------------------------------------------------------------------------------------------------
LEAF_PATTERN = re.compile(r"Norder=(\d+)/Dir=(\d+)/Npix=(\d+)")


def data_files(catalog_path: str | Path) -> list[Path]:
    """Leaf data partitions of a catalog or margin (sorted)."""
    return sorted(p for p in Path(catalog_path).glob("dataset/Norder=*/Dir=*/Npix=*") if p.is_file() or p.is_dir())


def index_files(index_path: str | Path) -> list[Path]:
    return sorted(Path(index_path).glob("dataset/**/*.parquet"))


def read_properties(path: str | Path) -> dict:
    return dict(l.split("=", 1) for l in Path(path).read_text().splitlines() if "=" in l and not l.startswith("#"))


def write_properties(path: str | Path, props: dict) -> None:
    """Rewrite a properties file keeping comments and order, updating or appending keys."""
    path = Path(path)
    lines = path.read_text().splitlines() if path.exists() else ["#HATS catalog"]
    seen = set()
    out = []
    for line in lines:
        if "=" in line and not line.startswith("#"):
            key = line.split("=", 1)[0]
            if key in props:
                out.append(f"{key}={props[key]}")
                seen.add(key)
                continue
        out.append(line)
    out += [f"{k}={v}" for k, v in props.items() if k not in seen]
    path.write_text("\n".join(out) + "\n")


def both_property_files(table_path: str | Path) -> list[Path]:
    """hats writes `hats.properties` and `properties`; edits must go to both."""
    table_path = Path(table_path)
    return [p for p in (table_path / "hats.properties", table_path / "properties") if p.exists()]


# ----------------------------------------------------------------------------------------------------------------
# run log
# ----------------------------------------------------------------------------------------------------------------
INPUT_FORMATS = ("fits", "csv", "tsv", "ecsv", "parquet", "indexed_csv", "indexed_parquet")
NON_DATA_SUFFIXES = (".sha256", ".html", ".log", ".md", ".txt", ".json", ".pid", ".sh")


def input_files(cfg: Config) -> list[Path]:
    """The input files of a run: [source].files (paths or globs) or, when empty, every data file in the raw dir.
    One helper for the estimate, the import and the verification so all three read the same files."""
    files: list[Path] = []
    for pattern in cfg["source"].get("files") or []:
        full = cfg.path(pattern)   # relative entries are taken from the work directory
        if any(ch in str(pattern) for ch in "*?["):
            files += sorted(Path(p) for p in glob.glob(str(full)))
        else:
            files.append(full)
    if not files:
        files = sorted(p for p in cfg.raw_dir.glob("*") if p.is_file() and not p.name.endswith(NON_DATA_SUFFIXES))
    if not files:
        raise SystemExit(f"no input files: set [source].files or download into {cfg.raw_dir}")
    return files


# Readers that hold a whole input file in one worker's memory while mapping and splitting (one Dask task per file).
# FITS: astropy's memmap does not keep the resident size down for binary tables (a 22 GB table costs ~22 GB RSS);
# ECSV: astropy reads the whole table. CSV (pandas chunks) and Parquet (row-group batches) are bounded by chunksize.
WHOLE_FILE_FORMATS = {"fits": 1.5, "ecsv": 3.0}   # format -> memory needed per worker as a multiple of the largest file


def parse_memory(value) -> int:
    """'12GB', '12G', '512MiB', '512M', '1.5 TB' or a number -> bytes, as Dask reads memory_limit strings: GB, G, MB,
    M are decimal (10^9, 10^6), GiB and MiB binary. 'auto' or empty -> 0 (resolved by dask_resources)."""
    if value is None or value == "" or str(value).lower() == "auto":
        return 0
    m = re.fullmatch(r"\s*([0-9.]+)\s*([kmgt]?i?b?)\s*", str(value).lower())
    if not m:
        raise ValueError(f"cannot parse memory size {value!r}")
    num, unit = float(m.group(1)), m.group(2).rstrip("b")
    base = 1024 if unit.endswith("i") else 1000
    power = {"": 0, "k": 1, "m": 2, "g": 3, "t": 4}[unit.rstrip("i")]
    return int(num * base**power)


INDEXED_FORMATS = ("indexed_csv", "indexed_parquet")
# a re-download copy is a known data-file name followed by a numeric suffix (wget's name.csv.gz.1); `part.1` is not
COPY_PATTERN = re.compile(r"(.+\.(?:fits?|fz|csv|tsv|ecsv|txt|dat|parquet|pq|hdf5|h5|vot|xml|gz|bz2|xz|zst|zip))\.\d+$",
                          re.IGNORECASE)


def index_listed_files(index_files: list) -> list[Path]:
    """The data files listed in indexed_csv / indexed_parquet index files (one path per line), in order."""
    out: list[Path] = []
    for f in index_files:
        out += [Path(line.strip()) for line in Path(f).read_text().splitlines() if line.strip()]
    return out


def first_listed_file(index_file) -> Path | None:
    """The first data file named in an index file (reads one line, not the whole list)."""
    with open(index_file) as fh:
        for line in fh:
            if line.strip():
                return Path(line.strip())
    return None


def input_size_report(cfg: Config, files: list, *, check_expected_files: bool = True) -> dict:
    """Sizes of the input files and whether one worker can hold the largest of them (the pass that runs before
    any import starts; printed by --dry-run, --trial-file and the full import, and written to the run log).

    For the indexed formats the data files listed in the index files are measured, checked for re-download copies
    and counted against [source].expected_files; the index files themselves are what the reader receives.
    check_expected_files=False skips the [source].expected_files comparison (re-import and re-partition runs, whose
    inputs are partition files, not the archive's files). Run it after dask_resources() has applied the caps."""
    fmt = cfg["source"]["format"]
    measured = index_listed_files(files) if fmt in INDEXED_FORMATS else list(files)
    sizes = sorted(((Path(f).stat().st_size if Path(f).exists() else 0), str(f)) for f in measured)
    total = sum(b for b, _ in sizes); largest_b, largest_f = sizes[-1] if sizes else (0, "")
    median = sizes[len(sizes) // 2][0] if sizes else 0
    mem = parse_memory(cfg["dask"].get("memory_limit"))
    factor = WHOLE_FILE_FORMATS.get(fmt)
    r = cfg["reader"]
    if fmt == "fits" and ((cfg["source"].get("companions") or {}).get("files") or r.get("derive") or (r.get("pack_nested") or {}).get("name")):
        factor = None   # CompanionFitsReader reads fitsio row ranges: memory is bounded by [reader].chunksize, not the file
    needed = int(largest_b * factor) if factor else 0
    ok = not factor or not mem or needed <= mem
    rep = {"files": len(sizes), "format": fmt, "index_files": len(files) if fmt in INDEXED_FORMATS else None, "total_gb": round(total / 1e9, 2), "largest_gb": round(largest_b / 1e9, 2),
           "largest_file": Path(largest_f).name, "median_gb": round(median / 1e9, 3), "worker_memory_gb": round(mem / 1e9, 1) if mem else None,
           "whole_file_per_worker": bool(factor), "worker_memory_needed_gb": round(needed / 1e9, 1) if factor else None, "ok": ok}
    # re-download copies (wget's name.csv.gz.1, .2): the glob would import the same rows twice
    stems = {}   # per directory: the same name in two directories (e.g. north/ and south/) is not a copy
    for _, f in sizes:
        n = Path(f).name
        m = COPY_PATTERN.fullmatch(n)
        base = str(Path(f).parent / (m.group(1) if m else n))
        stems.setdefault(base, []).append(n)
    dupes = {k: v for k, v in stems.items() if len(v) > 1}
    expected_files = int(cfg["source"].get("expected_files") or 0) if check_expected_files else 0
    if dupes:
        ok = False
        rep["duplicate_names"] = {k: v for k, v in list(dupes.items())[:5]}
        rep["advice"] = (f"{len(dupes)} input name(s) occur more than once up to a numeric suffix (e.g. {next(iter(dupes.values()))}): "
                         "re-download copies would be imported twice; remove them or list the exact files in [source].files")
    if expected_files and len(sizes) != expected_files:
        ok = False
        rep["advice"] = (rep.get("advice", "") + f" the archive publishes {expected_files} files, {len(sizes)} were found").strip()
    rep["ok"] = ok
    if not ok and "advice" not in rep:
        rep["advice"] = (f"the largest input ({rep['largest_file']}, {rep['largest_gb']} GB, {fmt}) is read whole by one worker while "
                         f"mapping and splitting: set [dask].memory_limit to at least {rep['worker_memory_needed_gb']} GB "
                         f"({factor}x the largest file) and lower [dask].n_workers to stay inside the memory budget, "
                         f"or split the file before importing")
    return rep


def runlog(cfg: Config | None, kind: str, phase: str, note: str = "", **data) -> None:
    """Append one entry to runlog.jsonl in the work directory (same format as run_log.py)."""
    log = (cfg.work_dir if cfg is not None else Path.cwd()) / "runlog.jsonl"
    log.parent.mkdir(parents=True, exist_ok=True)
    entry = {"ts": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"), "kind": kind, "phase": phase,
             "note": note}
    for key in ("status", "elapsed_s"):   # top-level fields of the run_log.py format
        if key in data:
            entry[key] = data.pop(key)
    if kind == "end":
        entry.setdefault("status", "ok")
    entry["data"] = data
    with log.open("a") as f:
        f.write(json.dumps(entry, ensure_ascii=False, default=str) + "\n")


# ----------------------------------------------------------------------------------------------------------------
# shared-node caps: CPUs and Dask memory
# ----------------------------------------------------------------------------------------------------------------
def env_cap(name: str, *, memory: bool = False) -> float:
    """A HATS_PUBLISH_* cap from the environment; unset or empty = 0 = no cap. Memory caps are in GB (10^9 bytes);
    a value with a unit ('128GB', '120GiB') is also accepted and converted to GB."""
    raw = (os.environ.get(name) or "").strip()
    if not raw:
        return 0
    try:
        return float(raw)
    except ValueError:
        if memory:
            return parse_memory(raw) / 1e9
        raise SystemExit(f"{name}={raw!r} is not a number") from None


def _physical_core_cpus(allowed: list[int]) -> list[int]:
    """One logical CPU per physical core, in core order, from the CPUs this process may use. The topology comes from
    /sys (thread_siblings_list), else from `lscpu -p`; [] when neither is available."""
    first_of_core: dict = {}
    for cpu in allowed:
        f = Path(f"/sys/devices/system/cpu/cpu{cpu}/topology/thread_siblings_list")
        if not f.exists():
            first_of_core = {}
            break
        siblings = []
        for part in f.read_text().strip().split(","):
            a, _, b = part.partition("-")
            siblings += list(range(int(a), int(b or a) + 1))
        first_of_core.setdefault(tuple(sorted(siblings)), cpu)
    if not first_of_core:
        try:
            import subprocess

            out = subprocess.run(["lscpu", "-p=CPU,CORE,SOCKET"], capture_output=True, text=True, timeout=10).stdout
        except (OSError, subprocess.SubprocessError):
            return []
        for line in out.splitlines():
            if line.startswith("#") or not line.strip():
                continue
            cpu, core, socket = (int(x or 0) for x in line.split(",")[:3])
            if cpu in allowed:
                first_of_core.setdefault((socket, core), cpu)
    return sorted(set(first_of_core.values()))


def pin_cpus(cfg: Config) -> int:
    """On shared nodes, limit this process (and the Dask workers it starts, which inherit the affinity) to N
    physical cores, N = [dask].max_cpus or else HATS_PUBLISH_MAX_CPUS (unset, empty or 0 = no pinning).

    One logical CPU is taken from each of the first N physical cores the process may use (hyper-thread siblings are
    left out), using the core topology from /sys or lscpu; when the topology cannot be read or there are fewer
    physical cores than N, the first N logical CPUs are used instead; when the process may already use N or fewer
    CPUs, nothing is pinned. Pinning is not coordinated between users: two runs with the same N on one node share
    the same cores. Returns the number of CPUs the process may use afterwards (pinned or not), which
    dask_resources() takes as the ceiling for n_workers x threads_per_worker."""
    n = int(cfg["dask"].get("max_cpus") or 0) or int(env_cap("HATS_PUBLISH_MAX_CPUS"))
    if not hasattr(os, "sched_getaffinity"):
        return n or (os.cpu_count() or 1)
    allowed = sorted(os.sched_getaffinity(0))
    if n <= 0:
        return len(allowed)
    cores = _physical_core_cpus(allowed)
    how = "physical cores"
    if len(cores) < n:
        if len(allowed) <= n:
            print(f"cap of {n} CPUs >= the {len(allowed)} CPUs this process may use: not pinned")
            return len(allowed)
        cores, how = allowed, "logical CPUs (fewer physical cores than the cap, or topology unavailable)"
    cores = cores[:n]
    os.sched_setaffinity(0, cores)
    os.environ.setdefault("OMP_NUM_THREADS", str(len(cores)))
    print(f"pinned to {len(cores)} {how}: {cores[:8]}{' ...' if len(cores) > 8 else ''}")
    return len(cores)


def _system_memory() -> int:
    try:
        from dask.system import MEMORY_LIMIT   # cgroup-aware, what Dask's "auto" divides

        return int(MEMORY_LIMIT)
    except Exception:  # noqa: BLE001 - dask missing or changed: fall back to the physical memory
        return int(os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES"))


def dask_resources(cfg: Config, cpus: int | None = None) -> dict:
    """The Dask cluster size after the shared-node caps, as {n_workers, threads_per_worker, memory_limit (bytes),
    notes}. Order of the reductions:

    1. n_workers x threads_per_worker <= cpus (the count pin_cpus returned): fewer workers first, then fewer threads;
    2. n_workers x memory_limit <= HATS_PUBLISH_MAX_MEM_GB: fewer workers first, keeping the configured per-worker
       limit (so the whole-file rule for FITS inputs still holds); only when one worker alone exceeds the cap is its
       limit lowered to the cap;
    3. memory_limit "auto" = the cap (or the node's memory) divided by n_workers, as Dask does.
    Write the result back into cfg["dask"] (apply_dask_resources) before input_size_report and collection_args."""
    d = cfg["dask"]
    n_workers = max(int(d.get("n_workers", 1)), 1)
    threads = max(int(d.get("threads_per_worker", 1)), 1)
    notes = []
    if cpus and n_workers * threads > cpus:
        new_workers = max(cpus // threads, 1)
        new_threads = min(threads, cpus)
        notes.append(f"{n_workers} workers x {threads} threads exceeds the {cpus} CPUs available: "
                     f"{new_workers} x {new_threads}")
        n_workers, threads = new_workers, new_threads
    cap = int(env_cap("HATS_PUBLISH_MAX_MEM_GB", memory=True) * 1e9)
    mem = parse_memory(d.get("memory_limit"))
    if not mem:   # "auto" or unset
        mem = (cap or _system_memory()) // n_workers
    if cap and n_workers * mem > cap:
        fit = cap // mem
        if fit >= 1:
            notes.append(f"{n_workers} workers x {mem / 1e9:.1f} GB exceeds HATS_PUBLISH_MAX_MEM_GB={cap / 1e9:g}: {fit} workers")
            n_workers = int(fit)
        else:
            notes.append(f"one worker of {mem / 1e9:.1f} GB exceeds HATS_PUBLISH_MAX_MEM_GB={cap / 1e9:g}: 1 worker of {cap / 1e9:.1f} GB")
            n_workers, mem = 1, cap
    return {"n_workers": n_workers, "threads_per_worker": threads, "memory_limit": int(mem), "notes": notes}


def apply_dask_resources(cfg: Config, cpus: int | None = None) -> dict:
    """dask_resources() written back into cfg["dask"] (memory_limit in bytes); prints what was reduced."""
    res = dask_resources(cfg, cpus)
    for note in res["notes"]:
        print("dask: " + note)
    cfg["dask"] = dict(cfg["dask"]) | {k: res[k] for k in ("n_workers", "threads_per_worker", "memory_limit")}
    return res
