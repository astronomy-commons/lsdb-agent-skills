#!/usr/bin/env python
"""registry_entry.py — generate the VO registry resource for a published catalog (no commit, no push).

Usage:
    registry_entry.py --config catalog.toml [--site-repo DIR] [--registry-repo DIR] [--check] [--allow-dirty]

The registry (astronomy-commons/registry.lsdb.io) does not take hand-written XML: its scripts/from_catalogs_json.py
renders one resource per catalog from the data.lsdb.io `data/` tree, and it only accepts catalogs whose
`urls.collection` (or `urls.catalog`) hostname is in its LOCATIONS table. The resource identifier is
<authority>/<short location>/<id> as the generator defines it, so [properties].creator_did should be set to that
value before the import ([registry].authority in catalog.toml holds the expected prefix).

This script, run after the site entry is merged and deployed and the catalog is reachable at its public URL:
  1. checks the site clone: no uncommitted changes, nothing untracked under data/, and HEAD equal to origin/main
     after `git fetch` (the generator writes "deleted" records for every registered catalog missing from the data
     tree, so a stale or edited clone would de-register catalogs). --allow-dirty overrides this, loudly;
  2. reads the catalog.json written by make_site_entry.py and checks its URL hostname against LOCATIONS;
  3. in the registry clone (HATS_PUBLISH_REGISTRY_REPO or --registry-repo), which must have no uncommitted changes,
     creates the branch named after the catalog id from origin/main (or switches to it), runs the generator on a
     scratch copy of the resource directory with this catalog in --rerender_list, and copies back ONLY
     <id>.xml (plus, when the identifier changed, the deleted record of the old identifier, named explicitly);
     every other file the generator would have changed is listed, not staged;
  4. fails (exit 1) if <id>.xml was not written or the generator reported an error for this catalog;
  5. stages the copied files and shows the staged diff, the identifier, and a warning if creator_did differs.
--check reports what would happen (including the site clone check) and runs nothing in the registry clone.
"""
from __future__ import annotations

import argparse
import ast
import json
import re
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from urllib.parse import urlparse

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _common import load_config, runlog, site_env  # noqa: E402

DEFAULT_AUTHORITY = "ivo://data.lsdb/uw"   # the registry's current authority; [registry].authority overrides it


def slug(label: str) -> str:
    return label.replace(" ", "_").replace("≥", "gte")


def git(repo: Path, *args: str, check: bool = True) -> str:
    return subprocess.run(["git", "-C", str(repo), *args], capture_output=True, text=True, check=check).stdout.strip()


def locations_table(registry_repo: Path) -> dict:
    """LOCATIONS = {hostname: (location, short location)} parsed from the registry's scripts/common.py."""
    text = (registry_repo / "scripts" / "common.py").read_text()
    m = re.search(r"^LOCATIONS\s*=\s*(\{.*?\})\s*$", text, flags=re.M | re.S)
    if not m:
        raise SystemExit("could not find LOCATIONS in the registry's scripts/common.py")
    return ast.literal_eval(m.group(1))


def identifier_prefix(registry_repo: Path) -> str | None:
    """The identifier prefix the generator uses (the text before /{short location}/{id} in scripts/common.py)."""
    text = (registry_repo / "scripts" / "common.py").read_text()
    m = re.search(r'f"(ivo://[^"{]+)/\{self\.short_location\}/\{self\.id\}"', text)
    return m.group(1) if m else None


def resource_subdir(registry_repo: Path) -> str:
    """The generator's default --out_dir below resources/ (read from scripts/from_catalogs_json.py)."""
    text = (registry_repo / "scripts" / "from_catalogs_json.py").read_text()
    m = re.search(r'ROOT_DIR\s*/\s*"resources"\s*/\s*"([^"]+)"', text)
    if not m:
        raise SystemExit("could not find the default --out_dir in the registry's scripts/from_catalogs_json.py")
    return m.group(1)


def clone_state(repo: Path, untracked_under: str | None = None, fetch: bool = True) -> list[str]:
    """Problems that make a clone unsafe to generate from: local changes, or HEAD not equal to origin/main."""
    problems = []
    if fetch:
        r = subprocess.run(["git", "-C", str(repo), "fetch", "-q", "origin"], capture_output=True, text=True)
        if r.returncode:
            problems.append(f"git fetch origin failed: {r.stderr.strip()}")
    if git(repo, "status", "--porcelain", "--untracked-files=no"):
        problems.append("uncommitted changes to tracked files")
    if untracked_under and git(repo, "status", "--porcelain", "--untracked-files=all", "--", untracked_under):
        problems.append(f"untracked or modified files under {untracked_under}/ (the generator reads every catalog.json there)")
    head = git(repo, "rev-parse", "HEAD", check=False)
    upstream = git(repo, "rev-parse", "--verify", "-q", "origin/main", check=False)
    if not upstream:
        problems.append("no origin/main to compare with")
    elif head != upstream:
        behind = git(repo, "rev-list", "--count", "HEAD..origin/main", check=False)
        ahead = git(repo, "rev-list", "--count", "origin/main..HEAD", check=False)
        problems.append(f"HEAD is not origin/main ({behind} commits behind, {ahead} ahead; branch {git(repo, 'branch', '--show-current') or 'detached'})")
    return problems


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--config", default="catalog.toml")
    ap.add_argument("--site-repo", help="data.lsdb.io clone (default: $HATS_PUBLISH_SITE_REPO or ./data.lsdb.io)")
    ap.add_argument("--registry-repo", help="registry.lsdb.io clone (default: $HATS_PUBLISH_REGISTRY_REPO or ./registry.lsdb.io)")
    ap.add_argument("--check", action="store_true", help="report only; run nothing in the registry clone")
    ap.add_argument("--allow-dirty", action="store_true",
                    help="run even when the site clone has local changes or is not at origin/main (the generated "
                         "registry state may then de-register or misdescribe catalogs; only the catalog's own file is staged)")
    a = ap.parse_args(argv)

    cfg = load_config(a.config)
    env = site_env()
    site = cfg["site"]
    site_repo = Path(a.site_repo or env.get("HATS_PUBLISH_SITE_REPO") or Path.cwd() / "data.lsdb.io").expanduser().resolve()
    registry_repo = Path(a.registry_repo or env.get("HATS_PUBLISH_REGISTRY_REPO") or Path.cwd() / "registry.lsdb.io").expanduser().resolve()
    for repo, marker in ((site_repo, "data/README.md"), (registry_repo, "scripts/from_catalogs_json.py")):
        if not (repo / marker).exists():
            raise SystemExit(f"{repo} does not contain {marker}; clone the repository or set the path (see CONFIG.md)")

    problems = clone_state(site_repo, untracked_under="data")
    if problems:
        msg = f"site clone {site_repo} is not a clean copy of origin/main: " + "; ".join(problems)
        if not a.allow_dirty:
            raise SystemExit(msg + "\n(update it with `git checkout main && git pull`, or pass --allow-dirty to override)")
        print("!" * 100 + f"\nWARNING --allow-dirty: {msg}\nThe generator sees this state; review the staged file "
              "carefully before opening a pull request.\n" + "!" * 100, file=sys.stderr)
    else:
        print(f"site clone: clean, at origin/main ({git(site_repo, 'rev-parse', '--short', 'HEAD')})")

    label = f"{site['group']}/{site['name']}"
    entry_file = site_repo / "data" / slug(site["group"]) / slug(site["name"]) / "catalog.json"
    if not entry_file.exists():
        raise SystemExit(f"site entry not found: {entry_file} (run make_site_entry.py and merge it first)")
    entry = json.loads(entry_file.read_text())
    url = entry.get("urls", {}).get("collection") or entry.get("urls", {}).get("catalog")
    if not url:
        raise SystemExit("the site entry has no collection or catalog URL")
    host = urlparse(url).hostname
    locations = locations_table(registry_repo)
    if host not in locations:
        raise SystemExit(f"{host} is not in the registry's LOCATIONS table {sorted(locations)}; the registry only "
                         "lists catalogs served from those hosts")
    location, short = locations[host]
    catalog_id = entry["id"]
    prefix = identifier_prefix(registry_repo)
    authority = str(cfg.get("registry", {}).get("authority") or DEFAULT_AUTHORITY).rstrip("/")
    identifier = f"{prefix}/{short}/{catalog_id}" if prefix else f"{authority}/{catalog_id}"
    if identifier != f"{authority}/{catalog_id}":
        print(f"warning: the generator assigns {identifier!r}; [registry].authority gives {authority}/{catalog_id!r}")
    creator_did = cfg["properties"].get("creator_did", "")
    label_dir = slug(site["group"]) + "/" + slug(site["name"])
    subdir = resource_subdir(registry_repo)
    resource_dir = registry_repo / "resources" / subdir
    resource_rel = f"resources/{subdir}/{catalog_id}.xml"

    print(f"catalog: {label}  id={catalog_id}  url={url}")
    print(f"registry location: {location} ({short}); identifier: {identifier}")
    if creator_did and creator_did != identifier:
        print(f"warning: [properties].creator_did is {creator_did!r}; the registry identifier is {identifier!r}")
    elif not creator_did:
        print("note: [properties].creator_did is empty; set it to the identifier above on the next (re)import")

    if a.check:
        print(f"would run in {registry_repo}: scripts/from_catalogs_json.py --data_dir {site_repo / 'data'} "
              f"--out_dir <scratch copy of resources/{subdir}> --rerender_list {label_dir}")
        print(f"would stage only {resource_rel} on branch {catalog_id}")
        return 0

    reg_problems = clone_state(registry_repo)
    if any("uncommitted" in p for p in reg_problems):
        raise SystemExit(f"{registry_repo} has uncommitted changes to tracked files; commit or stash them first")
    current = git(registry_repo, "branch", "--show-current")
    if current != catalog_id:
        if git(registry_repo, "branch", "--list", catalog_id):
            git(registry_repo, "checkout", "-q", catalog_id)
        else:
            git(registry_repo, "checkout", "-q", "-b", catalog_id, "origin/main")
        print(f"on branch {catalog_id} (from {current})")
    if subprocess.run(["git", "-C", str(registry_repo), "merge-base", "--is-ancestor", "origin/main", "HEAD"]).returncode:
        print(f"warning: branch {catalog_id} does not contain origin/main; rebase it before opening the pull request")

    original = {p.name: p.read_bytes() for p in resource_dir.glob("*.xml")} if resource_dir.exists() else {}
    old_identifier = None
    if f"{catalog_id}.xml" in original:
        m = re.search(rb"<identifier>([^<]+)</identifier>", original[f"{catalog_id}.xml"])
        old_identifier = m.group(1).decode() if m else None
    with tempfile.TemporaryDirectory(prefix="registry_") as tmp:
        scratch = Path(tmp) / subdir
        if resource_dir.exists():
            shutil.copytree(resource_dir, scratch)
        else:
            scratch.mkdir(parents=True)
        cmd = [sys.executable, "scripts/from_catalogs_json.py", "--data_dir", str(site_repo / "data"),
               "--out_dir", str(scratch), "--rerender_list", label_dir]
        print("running:", " ".join(cmd[1:]))
        t0 = time.time()
        r = subprocess.run(cmd, cwd=registry_repo, capture_output=True, text=True)
        print(r.stdout[-4000:], end="")
        if r.returncode:
            print(r.stderr[-4000:], file=sys.stderr)
            raise SystemExit(f"the generator failed (exit {r.returncode}); nothing was staged")
        mine = [line for line in r.stdout.splitlines() if line.split()[:1] == [label_dir]]
        target = scratch / f"{catalog_id}.xml"
        if not target.exists() or target.stat().st_mtime < t0 or not any("SUCCESS" in line for line in mine):
            raise SystemExit(f"{catalog_id}.xml was not rendered by the generator; its report for {label_dir}: "
                             f"{mine or 'no line'}; nothing was staged")
        to_copy = [target.name]
        if old_identifier and old_identifier != identifier:
            retired = old_identifier.rsplit("/", 1)[-1] + ".xml"   # the generator's name for the old identifier's deleted record
            if (scratch / retired).exists():
                to_copy.append(retired)
        side_effects = sorted(p.name for p in scratch.glob("*.xml")
                              if p.name not in to_copy and p.read_bytes() != original.get(p.name))
        resource_dir.mkdir(parents=True, exist_ok=True)
        for name in to_copy:
            shutil.copyfile(scratch / name, resource_dir / name)
    if side_effects:
        print(f"not staged: the generator would also have written {len(side_effects)} other file(s) "
              f"(new or de-registered catalogs outside this pull request): {side_effects[:20]}")
    staged = [f"resources/{subdir}/{n}" for n in to_copy]
    git(registry_repo, "add", "--", *staged)
    print(git(registry_repo, "status", "--short"))
    print(git(registry_repo, "diff", "--cached", "--stat"))
    diff = git(registry_repo, "diff", "--cached")
    print("\n".join(diff.splitlines()[:200]) + ("\n... (diff truncated)" if diff.count("\n") > 200 else ""))
    print("\nstaged in the registry clone, not committed: open a pull request to astronomy-commons/registry.lsdb.io; "
          "a merge to main deploys the registry automatically.")
    runlog(cfg, "event", "register", "registry resource generated", identifier=identifier, label=label, staged=staged)
    return 0


if __name__ == "__main__":
    sys.exit(main())
