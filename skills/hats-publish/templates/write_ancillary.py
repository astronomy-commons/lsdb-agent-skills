#!/usr/bin/env python
"""write_ancillary.py — the ancillary files the HATS note recommends, written with the run's writer settings.

For the main catalog of the collection in catalog.toml (or --collection PATH):
  * data_thumbnail.parquet   a representative sample: N random rows from every partition (default 10, capped by
                             [ancillary].thumbnail_max_rows and by the partition threshold), same schema as the
                             partitions including the Parquet schema metadata (VOParquet keys when present),
                             sorted by the spatial index, written with the [writer] codec and page index.
                             (hats-import's own thumbnail takes the first row of each partition.)
  * per_partition_statistics.parquet  if missing (hats computes it from _metadata)
  * skymap.png, partition_info.png    via hats.io.summary_file
  * README.md and index.html          via hats.io.summary_file for the collection and for the catalog, plus a
                                      "Provenance" section in all four files (source files with sizes and
                                      checksums, download URLs, data model, citation, acknowledgment, writer
                                      settings, library versions)
Thread counts for footer reads follow HATS_PUBLISH_MAX_CPUS (or [dask].max_cpus); when both are unset or empty
(a dedicated batch node), the SLURM allocation or the CPUs available to this process, at most 32.
Usage:
    write_ancillary.py [--config catalog.toml] [--collection PATH] [--seed 0] [--skip-thumbnail] [--skip-png] [--skip-docs]
"""
from __future__ import annotations

import argparse
import html
import os
import re
import sys
import time
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _common import (  # noqa: E402
    check_versions, data_files, env_cap, input_files, load_config, read_properties, runlog, parquet_write_kwargs, writer_kwargs,
)

MAX_IO_THREADS = 32


def io_workers(cfg) -> int:
    """Threads for parallel footer and row-group reads. A cap in HATS_PUBLISH_MAX_CPUS or [dask].max_cpus wins;
    an empty or unset value means no cap (a dedicated node): use the SLURM allocation or the CPUs this process
    may run on. Bounded to MAX_IO_THREADS: the reads are I/O bound and more threads only load the file system."""
    n = int(env_cap("HATS_PUBLISH_MAX_CPUS")) or int(cfg["dask"].get("max_cpus") or 0)
    if not n:
        slurm = str(os.environ.get("SLURM_CPUS_PER_TASK") or "").strip()
        n = int(slurm) if slurm.isdigit() else (len(os.sched_getaffinity(0)) if hasattr(os, "sched_getaffinity") else (os.cpu_count() or 1))
    return max(1, min(n, MAX_IO_THREADS))


def write_thumbnail(catalog_path: Path, cfg, seed: int) -> int:
    from hats.io import paths

    rng = np.random.default_rng(seed)
    per = int(cfg["ancillary"].get("thumbnail_rows_per_partition", 10))
    cap = int(cfg["ancillary"].get("thumbnail_max_rows", 10_000))
    props = read_properties(catalog_path / "hats.properties")
    threshold = int(props.get("hats_max_rows") or cap)
    cap = min(cap, threshold)
    files = data_files(catalog_path)
    if not files:
        raise SystemExit(f"no data partitions under {catalog_path}")
    if len(files) > cap:   # more partitions than the row cap: one row from each of `cap` random partitions
        files = [files[i] for i in np.sort(rng.choice(len(files), size=cap, replace=False))]
    per = max(1, min(per, cap // max(1, len(files))))
    schema = pq.read_schema(catalog_path / "dataset" / "_common_metadata")
    from concurrent.futures import ThreadPoolExecutor

    workers = io_workers(cfg)
    def group_sizes(f):
        md = pq.ParquetFile(f).metadata
        return [md.row_group(i).num_rows for i in range(md.num_row_groups)]

    with ThreadPoolExecutor(workers) as ex:   # footers in parallel: one at a time is ~3 files/s on a shared FS
        sizes = list(ex.map(group_sizes, files))
    picks = []   # (file, row indices): drawn in file order so the sample does not depend on thread scheduling
    for f, rg in zip(files, sizes):
        # one random row group (chosen by size) and random rows inside it: a partition file can hold several
        # gigabytes in a few row groups, and reading every group a pick lands in costs most of the file
        g = int(rng.choice(len(rg), p=np.asarray(rg) / sum(rg))) if sum(rg) else 0
        start = int(np.sum(rg[:g]))
        picks.append((f, start + np.sort(rng.choice(rg[g], size=min(per, rg[g]), replace=False)) if rg[g] else np.array([], int)))

    def sample(item):
        """Read only the row groups that hold the chosen rows (a whole partition can be gigabytes)."""
        f, take = item
        pf = pq.ParquetFile(f)
        bounds = np.cumsum([0] + [pf.metadata.row_group(i).num_rows for i in range(pf.metadata.num_row_groups)])
        parts = []
        for g in np.unique(np.searchsorted(bounds, take, side="right") - 1):
            rows = take[(take >= bounds[g]) & (take < bounds[g + 1])] - bounds[g]
            parts.append(pf.read_row_group(int(g)).take(pa.array(rows)))
        return pa.concat_tables(parts)

    with ThreadPoolExecutor(workers) as ex:
        pieces = [t for t in ex.map(sample, [pk for pk in picks if len(pk[1])]) if t is not None]
    thumb = pa.concat_tables(pieces)
    healpix = props.get("hats_col_healpix")
    if healpix and healpix in thumb.column_names:
        thumb = thumb.sort_by(healpix)
    thumb = thumb.cast(schema) if thumb.schema != schema else thumb
    thumb = thumb.replace_schema_metadata(schema.metadata)   # keep VOParquet / pandas metadata like the partitions
    out = paths.get_data_thumbnail_pointer(catalog_path)
    pq.write_table(thumb, out, **parquet_write_kwargs(cfg, thumb.schema))
    print(f"thumbnail: {len(thumb):,} rows from {len(files)} partitions -> {out.name}")
    return len(thumb)


def write_stats(catalog_path: Path, cfg) -> None:
    """Ensure per_partition_statistics.parquet exists and is encoded with the run's writer settings
    (hats-import writes it with pyarrow defaults, i.e. SNAPPY)."""
    from hats.io.parquet_metadata import write_per_partition_statistics_from_metadata

    target = catalog_path / "per_partition_statistics.parquet"
    if not target.exists():
        write_per_partition_statistics_from_metadata(catalog_path)
    table = pq.read_table(target)
    pq.write_table(table, target, **parquet_write_kwargs(cfg, table.schema))
    print(f"per_partition_statistics.parquet: {table.num_rows} rows, re-encoded with the writer settings")


def write_pngs(catalog_path: Path, cfg) -> None:
    import matplotlib

    matplotlib.use("Agg")
    from hats.io import summary_file

    # hats' plotting helpers raise on degenerate maps (a trial build with one populated pixel gives LogNorm an
    # empty range: "Invalid vmin or vmax"); a missing preview must not fail the ancillary step
    for key, writer, name in (("skymap_png", summary_file.write_skymap_png, "skymap.png"),
                              ("partition_info_png", summary_file.write_partition_info_png, "partition_info.png")):
        if not cfg["ancillary"].get(key, True):
            continue
        try:
            writer(catalog_path)
            print(f"{name} written")
        except Exception as exc:  # noqa: BLE001 - previews are optional; the README notes their absence
            print(f"WARNING: {name} not written ({type(exc).__name__}: {exc}); a one-pixel or empty map cannot be drawn")


def _human_bytes(n: int) -> str:
    for unit, size in (("GB", 1e9), ("MB", 1e6), ("kB", 1e3)):
        if n >= size:
            return f"{n / size:.1f} {unit}"
    return f"{n} bytes"


def source_file_lines(cfg) -> list[str]:
    """One line per input file: the name relative to the common input directory (never a local absolute path),
    its size and, when recorded at download, its sha256. Download URLs are listed after the files."""
    src = cfg["source"]
    digests = dict(src.get("sha256") or {})
    lines = []
    files = []
    try:   # [source].files (globs expanded) or, when empty, the downloaded files in the raw directory (also on route reimport)
        files = [Path(f) for f in input_files(cfg, reimport_partitions=False) if Path(f).is_file()]
    except SystemExit:   # inputs not on this machine (e.g. documenting a copy): the recorded digests stand in
        files = []
    if files:
        base = Path(os.path.commonpath([str(f.resolve().parent) for f in files]))
        names = [f.name for f in files]
        for f in files:
            rel = f.resolve().relative_to(base).as_posix()
            size = f.stat().st_size if f.exists() else None
            # digests are recorded by file name; a name present in two directories needs its relative path as key
            digest = digests.pop(rel, None) or (digests.pop(f.name, None) if names.count(f.name) == 1 else None)
            lines.append(f"source file `{rel}`" + (f", {_human_bytes(size)}" if size is not None else "")
                         + (f", sha256 `{digest}`" if digest else ""))
    for name, digest in digests.items():   # recorded digests of files that are not (or no longer) listed locally
        lines.append(f"source file `{name}`, sha256 `{digest}`")
    for u in src.get("urls") or []:
        lines.append(f"downloaded from {u}")
    return lines


def provenance_items(cfg, versions: dict) -> list[str]:
    src, p = cfg["source"], cfg["properties"]
    items = source_file_lines(cfg)
    if src.get("data_model_url"):
        items.append(f"column descriptions: {src['data_model_url']}")
    if p.get("bib_reference"):
        items.append(f"reference: {p['bib_reference']}" + (f" ({p['bib_reference_url']})" if p.get("bib_reference_url") else ""))
    if p.get("obs_ack"):
        items.append(f"acknowledgment: {p['obs_ack']}")
    if p.get("obs_copyright"):
        items.append(f"original data terms: {p['obs_copyright']}" + (f" ({p['obs_copyright_url']})" if p.get("obs_copyright_url") else ""))
    w = parquet_write_kwargs(cfg, pa.schema([]))
    items.append(f"parquet writer: {w['compression']} level {w['compression_level']}, page index {'on' if w['write_page_index'] else 'off'}, "
                 "BYTE_STREAM_SPLIT float columns" + (f", overrides {writer_kwargs(cfg)}" if writer_kwargs(cfg) else ""))
    items.append("built with " + ", ".join(f"{k} {v}" for k, v in versions.items()))
    pin = Path("templates.pinned/PIN")   # pin_templates.sh, relative to the work directory
    if pin.exists():
        kv = dict(line.split("=", 1) for line in pin.read_text().splitlines() if "=" in line)
        items.append(f"hats-publish templates: lsdb-agent-skills commit {kv.get('commit', 'unknown')[:12]}"
                     + (" with local changes" if kv.get("dirty") == "yes" else ""))
    if cfg["columns"].get("default"):
        items.append(f"default columns (`hats_cols_default`): {' '.join(cfg['columns']['default'])}")
    return items


def provenance_section(cfg, versions: dict) -> str:
    """The Provenance section in Markdown."""
    return "\n".join(["", "## Provenance", ""] + [f"- {item}" for item in provenance_items(cfg, versions)]) + "\n"


def _html_item(item: str) -> str:
    """One provenance line as HTML: escaped, `code` spans and https links rendered."""
    out = html.escape(item, quote=False)
    out = re.sub(r"`([^`]+)`", r"<code>\1</code>", out)
    return re.sub(r"(https://[^\s()<>]+)", r'<a href="\1">\1</a>', out)


def provenance_html(cfg, versions: dict) -> str:
    items = "\n".join(f"  <li>{_html_item(i)}</li>" for i in provenance_items(cfg, versions))
    return f'<h2 id="provenance">Provenance</h2>\n<ul>\n{items}\n</ul>\n'


def add_provenance(target: Path, cfg, versions: dict) -> None:
    """Add (or replace) the Provenance section in target/README.md and target/index.html."""
    readme = target / "README.md"
    if readme.exists():
        text = readme.read_text()
        text = re.sub(r"\n## Provenance\n.*?(?=\n## |\Z)", "", text, flags=re.S)   # a re-run replaces the section
        readme.write_text(text.rstrip("\n") + "\n" + provenance_section(cfg, versions))
    page = target / "index.html"
    if page.exists():
        text = re.sub(r'<h2 id="provenance">.*?</ul>\n', "", page.read_text(), flags=re.S)
        section = provenance_html(cfg, versions)
        text = text.replace("</body>", section + "</body>", 1) if "</body>" in text else text + section
        page.write_text(text)


def write_docs(collection_path: Path, catalog_path: Path, cfg, versions: dict) -> None:
    from hats.io import summary_file

    p = cfg["properties"]
    name = p.get("obs_title") or cfg["names"]["collection"]
    description = p.get("obs_description") or None
    env_url = os.environ.get("HATS_PUBLISH_WEB_URL")
    uri = f"{env_url}/{cfg['hosting'].get('web_link_name') or cfg['names'].get('public_name') or cfg['names']['collection']}" if env_url else None
    failed = []
    for target in (collection_path, catalog_path):
        for key, fmt in (("readme", "markdown"), ("index_html", "html")):
            if not cfg["ancillary"].get(key, True):
                continue
            try:
                summary_file.write_catalog_summary_file(target, fmt=fmt, name=name, description=description, uri=uri)
            except Exception as exc:  # noqa: BLE001 - hats' summary writer converts the schema to pandas; tz-aware
                # timestamps (e.g. tz=utc) need pytz in the environment, and the failure must not end the run
                failed.append(f"{target.name}/{fmt}: {type(exc).__name__}: {exc}")
        add_provenance(target, cfg, versions)
    if failed:
        print("WARNING: summary files not written: " + "; ".join(failed)[:400] + " (install pytz for tz-aware timestamp columns)")
    else:
        print("README.md / index.html with a Provenance section written for the collection and the catalog")


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--config", default="catalog.toml")
    ap.add_argument("--collection", help="collection directory (default: from catalog.toml)")
    ap.add_argument("--seed", type=int, default=0)
    ap.add_argument("--skip-thumbnail", action="store_true")
    ap.add_argument("--skip-png", action="store_true")
    ap.add_argument("--skip-docs", action="store_true")
    ap.add_argument("--no-pypi", action="store_true")
    a = ap.parse_args(argv)

    cfg = load_config(a.config)
    versions = check_versions(cfg, warn_newer=not a.no_pypi)
    collection = Path(a.collection) if a.collection else cfg.collection_path
    catalog = collection / cfg["names"]["catalog"]
    if (collection / "collection.properties").exists():   # any collection, e.g. the nested product: follow its own pointer
        main = read_properties(collection / "collection.properties").get("hats_primary_table_url")
        if main:
            catalog = collection / main
    if not (catalog / "hats.properties").exists():
        raise SystemExit(f"no catalog at {catalog}")
    t0 = time.time()
    runlog(cfg, "start", "ancillary", str(collection))
    n_thumb = None if a.skip_thumbnail else write_thumbnail(catalog, cfg, a.seed)
    write_stats(catalog, cfg)
    if not a.skip_png:
        write_pngs(catalog, cfg)
    if not a.skip_docs:
        write_docs(collection, catalog, cfg, versions)
    runlog(cfg, "end", "ancillary", "done", status="ok", elapsed_s=round(time.time() - t0, 1), thumbnail_rows=n_thumb)
    return 0


if __name__ == "__main__":
    sys.exit(main())
