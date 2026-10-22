#!/usr/bin/env bash
# publish_mirror.sh — copy a built HATS collection into a locally mounted mirror tree.
#
# Usage:  publish_mirror.sh [--config catalog.toml] [--check | --yes [--replace]] [-h]
#
# Reads catalog.toml ([run], [names], [hosting]) and the HATS_PUBLISH_MIRROR_* site variables (CONFIG.md).
# Steps: rsync into <MIRROR_ROOT>/<survey>/<collection>/, set the unix group, verify a sha256 manifest of every
# file, stamp per-copy properties (hats_status; hats_service_url = <MIRROR_URL>/<survey>/<collection>/<table> for
# every table when MIRROR_URL is set; relative hats_primary_table_url) on the copy only, then re-run
# verify_structure.py on the copy when it is available (results in verify_results_mirror.csv, so the build's
# verify_results.csv is kept).
# --check performs only read-only probes and reports what would change, including the file count of an existing
# copy and the files in it that the local collection does not have; --yes is required to write anything.
# When the destination already holds files that the local collection does not have, --yes stops before writing
# unless --replace is given, which deletes them (rsync --delete).
set -euo pipefail

CONFIG="catalog.toml"
CHECK=0
YES=0
REPLACE=0
for arg in "$@"; do
  case "$arg" in
    --check) CHECK=1 ;;
    --yes) YES=1 ;;
    --replace) REPLACE=1 ;;
    --config=*) CONFIG="${arg#--config=}" ;;
    -h|--help) awk 'NR > 1 && /^#/ {sub(/^# ?/, ""); print; next} NR > 1 {exit}' "$0"; exit 0 ;;
    --config) ;;
    *) [ "${prev:-}" = "--config" ] && CONFIG="$arg" ;;
  esac
  prev="$arg"
done

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1090
source "${HATS_PUBLISH_SITE_ENV:-/dev/null}"
[ -n "${HATS_PUBLISH_MIRROR_ROOT:-}" ] || { echo "missing site variable HATS_PUBLISH_MIRROR_ROOT (see CONFIG.md)" >&2; exit 2; }
MIRROR_URL="${HATS_PUBLISH_MIRROR_URL:-}"
MIRROR_GROUP="${HATS_PUBLISH_MIRROR_GROUP:-}"

eval "$(python - "$CONFIG" "$HERE" <<'PY'
import shlex, sys
sys.path.insert(0, sys.argv[2])
from _common import load_config
cfg = load_config(sys.argv[1])
targets = list(cfg["hosting"].get("targets") or ["mirror", "web"])
status = "main" if targets and targets[0] == "mirror" else "mirror"
print(f"SURVEY={shlex.quote(cfg['run']['survey'])}")
print(f"COLLECTION={shlex.quote(cfg['names']['collection'])}")
print(f"CATALOG={shlex.quote(cfg['names']['catalog'])}")
print(f"STATUS={status}")
print(f"SRC={shlex.quote(str(cfg.collection_path))}")
print(f"WORK_DIR={shlex.quote(str(cfg.work_dir))}")
PY
)"
export HATS_PUBLISH_RUNLOG="$WORK_DIR/runlog.jsonl"
log() { python "$HERE/run_log.py" event publish-mirror --note "$1" >/dev/null 2>&1 || true; }
step() { printf '\n== %s\n' "$*"; }

DEST="$HATS_PUBLISH_MIRROR_ROOT/$SURVEY/$COLLECTION"
SERVICE_URL=""
[ -n "$MIRROR_URL" ] && SERVICE_URL="$MIRROR_URL/$SURVEY/$COLLECTION"
[ -d "$SRC" ] || { echo "local collection not found: $SRC" >&2; exit 2; }

read -r -d '' STAMP_PY <<'PY' || true
import sys
from pathlib import Path
root, catalog, status, base = Path(sys.argv[1]), sys.argv[2], sys.argv[3], sys.argv[4]
def rewrite(path, props):
    lines = path.read_text().splitlines(); seen = set(); out = []
    for line in lines:
        if "=" in line and not line.startswith("#"):
            key = line.split("=", 1)[0]
            if key in props:
                out.append(f"{key}={props[key]}"); seen.add(key); continue
        out.append(line)
    out += [f"{k}={v}" for k, v in props.items() if k not in seen]
    path.write_text("\n".join(out) + "\n")
tables = [p for p in sorted(root.iterdir()) if p.is_dir() and ((p / "properties").exists() or (p / "hats.properties").exists())]
changed = 0
for table in tables:
    props = {"hats_status": f"public {status} clonableOnce"}
    if base:
        props["hats_service_url"] = f"{base}/{table.name}"   # every table: its own directory URL
    for f in (table / "hats.properties", table / "properties"):
        if not f.exists():
            continue
        p = dict(props)
        if table.name != catalog and "hats_primary_table_url=" in f.read_text():
            p["hats_primary_table_url"] = f"../{catalog}"
        rewrite(f, p); changed += 1
print(f"stamped {changed} property files in {len(tables)} tables (hats_status=public {status} clonableOnce)")
PY

if [ "$CHECK" -eq 0 ] && [ "$YES" -eq 0 ]; then
  echo "This writes to shared storage. Run with --check to preview, then with --yes after the user has approved." >&2
  exit 2
fi

RSYNC=(rsync -a --chmod=D2775,F664)
# the files of the existing copy that the local collection does not have (rsync without --delete keeps them)
LOCAL_LIST="$(mktemp)"; DEST_LIST="$(mktemp)"; MANIFEST="$(mktemp)"
trap 'rm -f "$LOCAL_LIST" "$DEST_LIST" "$MANIFEST"' EXIT
(cd "$SRC" && find . -type f | sed 's|^\./||' | sort) > "$LOCAL_LIST"
if [ -d "$DEST" ]; then (cd "$DEST" && find . -type f | sed 's|^\./||' | sort) > "$DEST_LIST"; fi
EXTRA_N=$(comm -13 "$LOCAL_LIST" "$DEST_LIST" | wc -l)

if [ "$CHECK" -eq 1 ]; then
  step "check: rsync dry run $SRC -> $DEST"
  if [ -d "$DEST" ]; then
    rc=0; out=$("${RSYNC[@]}" --dry-run --itemize-changes "$SRC/" "$DEST/" 2>&1) || rc=$?
    [ "$rc" -eq 0 ] || { echo "rsync dry run failed (exit $rc):" >&2; echo "$out" >&2; exit 1; }
    n=$(grep -c '^[<>]f' <<< "$out" || true)
  else
    n=$(wc -l < "$LOCAL_LIST")   # no copy yet: every file
  fi
  echo "files that would be transferred: $n of $(wc -l < "$LOCAL_LIST")"
  if [ -d "$DEST" ]; then
    echo "copy exists: $(wc -l < "$DEST_LIST") files; $EXTRA_N of them are not in the local collection"
    [ "$EXTRA_N" -gt 0 ] && comm -13 "$LOCAL_LIST" "$DEST_LIST" | head -10 | sed 's/^/  extra: /' \
      && echo "  --yes stops on these; --yes --replace deletes them"
    grep -h "^hats_status\|^hats_service_url" "$DEST/$CATALOG/properties" 2>/dev/null | sed 's/^/  /' || true
  else
    echo "copy absent"
    [ -w "$(dirname "$DEST")" ] || [ -w "$HATS_PUBLISH_MIRROR_ROOT" ] || echo "warning: no write access under $HATS_PUBLISH_MIRROR_ROOT" >&2
  fi
  [ "$n" = "0" ] && [ "$EXTRA_N" = "0" ] && [ -d "$DEST" ] && echo "nothing to do"
  exit 0
fi

DELETE=()
if [ "$EXTRA_N" -gt 0 ]; then
  if [ "$REPLACE" -eq 0 ]; then
    echo "$DEST holds $EXTRA_N files that the local collection does not have, e.g.:" >&2
    comm -13 "$LOCAL_LIST" "$DEST_LIST" | head -10 | sed 's/^/  /' >&2
    echo "nothing was written. Re-run with --yes --replace to delete them, or publish under a new collection name." >&2
    exit 1
  fi
  DELETE=(--delete); echo "--replace: $EXTRA_N files on the copy that the local collection does not have will be deleted"
fi

step "a. rsync $SRC -> $DEST"
# a new survey directory gets the same mode (and group) as the copies, not the login umask
if [ ! -d "$HATS_PUBLISH_MIRROR_ROOT/$SURVEY" ]; then
  mkdir -m 2775 "$HATS_PUBLISH_MIRROR_ROOT/$SURVEY"
  [ -n "$MIRROR_GROUP" ] && chgrp "$MIRROR_GROUP" "$HATS_PUBLISH_MIRROR_ROOT/$SURVEY"
fi
mkdir -p "$DEST"
"${RSYNC[@]}" ${DELETE[@]+"${DELETE[@]}"} --info=stats1 "$SRC/" "$DEST/"
log "rsync to mirror done"

if [ -n "$MIRROR_GROUP" ]; then
  step "b. chgrp -R $MIRROR_GROUP"
  chgrp -R "$MIRROR_GROUP" "$DEST"
fi

step "c. integrity: sha256 manifest"
(cd "$SRC" && find . -type f | sort | sed 's|^\./||' | xargs -d '\n' sha256sum) > "$MANIFEST"
(cd "$DEST" && sha256sum -c --quiet "$MANIFEST")
local_n=$(wc -l < "$MANIFEST"); dest_n=$(find "$DEST" -type f | wc -l)
echo "source files $local_n, copy files $dest_n, all checksums match"
[ "$local_n" = "$dest_n" ] || { echo "file count differs" >&2; exit 1; }
log "integrity verified: $local_n files"

step "d. per-copy properties (hats_status=$STATUS${SERVICE_URL:+, hats_service_url=$SERVICE_URL/<table>})"
python3 - "$DEST" "$CATALOG" "$STATUS" "$SERVICE_URL" <<< "$STAMP_PY"

step "e. verification on the copy"
if [ -f "$HERE/verify_structure.py" ] && python -c "import hats_import" 2>/dev/null; then
  # a separate results file: the build's verify_results.csv (a deliverable, with --spot-check) is kept
  python "$HERE/verify_structure.py" --config "$CONFIG" --collection "$DEST" --no-pypi --results-file "$WORK_DIR/verify_results_mirror.csv" \
    || { echo "verification of the copy failed; see verify_results_mirror.csv in the work directory" >&2; exit 1; }
else
  echo "verify_structure.py or hats_import not available here; the sha256 manifest check above stands in for it"
fi
log "mirror copy at $DEST${SERVICE_URL:+ ($SERVICE_URL)}"
echo
echo "mirror copy: $DEST${SERVICE_URL:+  served at $SERVICE_URL when the mirror service is up}"
