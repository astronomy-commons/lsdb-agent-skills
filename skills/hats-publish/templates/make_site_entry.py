#!/usr/bin/env python
"""make_site_entry.py — stage the data.lsdb.io page for a published collection (no commit, no push).

Usage:
    make_site_entry.py --config catalog.toml [--collection PATH] [--repo DIR] [--branch NAME] [--no-branch]

In a clone of astronomy-commons/data.lsdb.io (HATS_PUBLISH_SITE_REPO or --repo) this script:
  1. refuses to run if tracked files have uncommitted changes;
  2. creates or switches to a branch named after the catalog id, from the current HEAD (unless --no-branch);
  3. writes data/<Group>/<Name>/catalog.json from [site] in catalog.toml, with the public URLs from the site
     variables and the metadata block computed from the LOCAL collection (not fetched from the URL). An existing
     entry is deep-merged: the keys this script produces are updated, every other key (extra URLs, mean maps,
     connectivity flags, notes, anything added by hand) is kept; empty values in catalog.toml (no notes, no
     other_urls, no description) never erase existing ones. A summary of the changed keys is printed;
  4. renders data/<Group>/<Name>/point_density.webp from the local collection with the same call the repository's
     own script uses;
  5. runs scripts/update_index.py (data/catalogs.json and data/rubinCatalogs.json are generated, never
     hand-edited);
  6. stages the entry, the density map and whichever index files the index script changed, and prints
     `git status --short`. The commit message is the user's.
Rewriting an existing entry keeps its `id` (ids are permanent).
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _common import check_versions, load_config, read_properties, runlog, site_env  # noqa: E402


def slug(label: str) -> str:
    """Directory name of a site label (copy of the repository's rule)."""
    return label.replace(" ", "_").replace("≥", "gte")


def human_size(kib: int) -> str:
    """sizeOnDisk exactly as the site's own metadata script writes it: human_readable.file_size(bytes, binary=True)."""
    nbytes = int(kib) * 1024
    try:
        import human_readable

        return human_readable.file_size(nbytes, binary=True)
    except ImportError:   # the same rule, for environments without the package
        if nbytes == 1:
            return "1 Byte"
        if nbytes < 1024:
            return f"{nbytes} Bytes"
        for i, suffix in enumerate((" KiB", " MiB", " GiB", " TiB", " PiB", " EiB", " ZiB", " YiB")):
            unit = 1024 ** (i + 2)
            if nbytes < unit:
                break
        return f"{1024 * float(nbytes) / unit:.1f}{suffix}"


def _empty(value) -> bool:
    return value in ("", None, [], {})


def deep_merge(existing: dict, produced: dict) -> dict:
    """existing updated with produced: nested dicts merged key by key, other values replaced, and empty produced
    values written only where the key is new, so that an unset field in catalog.toml never erases what the page
    already has."""
    out = dict(existing)
    for key, value in produced.items():
        if isinstance(value, dict) and isinstance(out.get(key), dict):
            out[key] = deep_merge(out[key], value)
        elif not _empty(value) or key not in out:
            out[key] = value
    return out


def _flatten(d, prefix=""):
    for key, value in d.items():
        path = f"{prefix}.{key}" if prefix else str(key)
        if isinstance(value, dict) and value:
            yield from _flatten(value, path)
        else:
            yield path, value


def change_summary(old: dict, new: dict) -> list[str]:
    a, b = dict(_flatten(old)), dict(_flatten(new))
    lines = [f"  + {k}" for k in sorted(b.keys() - a.keys())]
    lines += [f"  ~ {k}: {a[k]!r} -> {b[k]!r}" if not isinstance(b[k], list) else f"  ~ {k} (list changed)"
              for k in sorted(a.keys() & b.keys()) if a[k] != b[k]]
    lines += [f"  - {k}" for k in sorted(a.keys() - b.keys())]
    kept = sorted(k for k in a.keys() & b.keys() if a[k] == b[k])
    return lines + [f"  = {len(kept)} keys unchanged"]


def git(repo: Path, *args: str, check: bool = True) -> str:
    return subprocess.run(["git", "-C", str(repo), *args], capture_output=True, text=True, check=check).stdout.strip()


def local_metadata(collection: Path, catalog_name: str) -> dict:
    import pyarrow.parquet as pq

    main = collection / catalog_name
    props = read_properties(main / "properties")
    schema = pq.read_schema(main / "dataset" / "_common_metadata")
    healpix = props.get("hats_col_healpix", "_healpix_29")
    partitions = sum(1 for line in (main / "partition_info.csv").read_text().splitlines()[1:] if line.strip())
    return {
        "numRows": int(props["hats_nrows"]),
        "numColumns": len([n for n in schema.names if n != healpix]),
        "numPartitions": partitions,
        "sizeOnDisk": human_size(int(props.get("hats_estsize", 0))),
        "hatsBuilder": props.get("hats_builder", ""),
    }


def render_density_map(collection: Path, out: Path) -> None:
    import matplotlib

    matplotlib.use("Agg")
    from matplotlib.colors import LogNorm

    import hats

    cat = hats.read_hats(collection)
    if hasattr(cat, "main_catalog"):
        cat = cat.main_catalog
    fig, _ = hats.inspection.plot_density(cat, edgecolors="face", norm=LogNorm())
    fig.savefig(out, format="webp", bbox_inches="tight")


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--config", default="catalog.toml")
    ap.add_argument("--collection", help="local collection path (default: the build path from catalog.toml)")
    ap.add_argument("--repo", help="data.lsdb.io clone (default: $HATS_PUBLISH_SITE_REPO or ./data.lsdb.io)")
    ap.add_argument("--branch", help="branch name (default: the catalog id)")
    ap.add_argument("--no-branch", action="store_true", help="stay on the current branch")
    ap.add_argument("--no-density-map", action="store_true")
    a = ap.parse_args(argv)

    cfg = load_config(a.config)
    check_versions(cfg, warn_newer=False)
    env = site_env()
    site = cfg["site"]
    for key in ("group", "name", "id"):
        if not site.get(key):
            raise SystemExit(f"[site].{key} is required")
    web_url = env.get("HATS_PUBLISH_WEB_URL")
    if not web_url:
        raise SystemExit("HATS_PUBLISH_WEB_URL is required to build the collection URL (see CONFIG.md)")

    repo = Path(a.repo or env.get("HATS_PUBLISH_SITE_REPO") or Path.cwd() / "data.lsdb.io").expanduser().resolve()
    if not (repo / "data" / "README.md").exists() or not (repo / "scripts" / "update_index.py").exists():
        raise SystemExit(f"{repo} does not look like a data.lsdb.io clone")
    if git(repo, "status", "--porcelain", "--untracked-files=no"):
        raise SystemExit(f"{repo} has uncommitted changes to tracked files; commit or stash them first")
    branch = a.branch or site["id"]
    if not a.no_branch:
        current = git(repo, "branch", "--show-current")
        if current != branch:
            if git(repo, "branch", "--list", branch):
                git(repo, "checkout", "-q", branch)
            else:
                git(repo, "checkout", "-q", "-b", branch)
        print(f"on branch {branch} (from {current})")

    collection = Path(a.collection).resolve() if a.collection else cfg.collection_path
    if not (collection / "collection.properties").exists():
        raise SystemExit(f"not a collection: {collection}")
    catalog_name = cfg["names"]["catalog"]
    link = cfg["hosting"].get("web_link_name") or cfg["names"].get("public_name") or cfg["names"]["collection"]
    targets = list(cfg["hosting"].get("targets") or [])

    label = f"{site['group']}/{site['name']}"
    entry_dir = repo / "data" / slug(site["group"]) / slug(site["name"])
    entry_dir.mkdir(parents=True, exist_ok=True)
    entry_file = entry_dir / "catalog.json"
    existing = json.loads(entry_file.read_text()) if entry_file.exists() else {}
    rel_dir = entry_dir.relative_to(repo).as_posix()

    urls = {"collection": f"{web_url.rstrip('/')}/{link}"}
    mirror_url = env.get("HATS_PUBLISH_MIRROR_URL")
    if mirror_url and "mirror" in targets:
        urls[os.environ.get("HATS_PUBLISH_MIRROR_URL_KEY", "mirror_catalog")] = f"{mirror_url.rstrip('/')}/{cfg['run']['survey']}/{cfg['names']['collection']}"
    urls["point_density_map"] = f"{rel_dir}/point_density.webp"

    produced = {
        "id": existing.get("id") or site["id"],
        "label": label,
        "name": site.get("display_name") or site["name"],
        "description": site.get("description", ""),
        "urls": urls,
        "metadata": local_metadata(collection, catalog_name),
        "other_urls": [u for u in site.get("other_urls", []) if u.get("url")],
        "badges": [{"title": b} for b in site.get("badges", [])],
    }
    notes = [n for n in (site.get("notes") or []) if n.get("title") or n.get("content")]
    if notes:   # notes in catalog.toml replace the page's notes; no notes there keeps the page's own
        produced["notes"] = notes
    if existing.get("id") and existing["id"] != site["id"]:
        print(f"note: keeping the existing permanent id {existing['id']!r} (catalog.toml says {site['id']!r})")
    entry = deep_merge(existing, produced)
    entry_file.write_text(json.dumps(entry, indent=2, ensure_ascii=False) + "\n")
    print(f"wrote {entry_file.relative_to(repo)}" + (" (merged into the existing entry)" if existing else " (new entry)"))
    if existing:
        print("changes to the existing entry:")
        print("\n".join(change_summary(existing, entry)))

    staged = [entry_file]
    if not a.no_density_map:
        webp = entry_dir / "point_density.webp"
        render_density_map(collection, webp)
        print(f"wrote {webp.relative_to(repo)}")
        staged.append(webp)

    subprocess.run([sys.executable, "scripts/update_index.py"], cwd=repo, check=True)
    for index in ("catalogs.json", "rubinCatalogs.json"):   # both are rewritten by the index script
        rel = f"data/{index}"
        if (repo / rel).exists() and git(repo, "status", "--porcelain", "--", rel):
            staged.append(repo / rel)
    git(repo, "add", *[str(p.relative_to(repo)) for p in staged])
    print(git(repo, "status", "--short"))
    print("\nstaged, not committed: the commit message is yours. Deployment (npm run build + rsync + post-deploy) "
          "is a separate step for a maintainer with web-host access.")
    runlog(cfg, "event", "register", "site entry staged", label=label, branch=branch, collection_url=urls["collection"])
    return 0


if __name__ == "__main__":
    sys.exit(main())
