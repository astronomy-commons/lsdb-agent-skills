#!/usr/bin/env bash
# publish_mirror.sh — copy a built HATS collection into a locally mounted mirror tree.
#
# Usage:  publish_mirror.sh [--config catalog.toml] [--check | --yes [--replace]] [-h]
#
# Reads catalog.toml ([run], [names], [hosting]) and the HATS_PUBLISH_MIRROR_* site variables (CONFIG.md).
# Steps: rsync into the destination, set the unix group, verify a sha256 manifest of every file, stamp per-copy
# properties (hats_status; hats_service_url = <MIRROR_URL>/<survey>/<collection>/<table> for every table when
# MIRROR_URL is set; relative hats_primary_table_url) on the copy only, then re-run verify_structure.py on the copy
# when it is available (results in verify_results_mirror.csv, so the build's verify_results.csv is kept).
# Layout:
#   [hosting].version set (e.g. "2026-10"): the destination is <MIRROR_ROOT>/<survey>/<collection>_<version>/ and
#     the stable path <MIRROR_ROOT>/<survey>/<collection> is a relative symlink to it, created or re-pointed
#     atomically only after the copy, the checksums, the properties and the verification succeeded. The previous
#     target stays on disk and is named as the copy to retire later. A real directory at the stable path (a legacy
#     unversioned copy) stops the script with the rename to run first: the script never renames or deletes a
#     served copy.
#   [hosting].version empty: the legacy layout, the destination is the stable path itself.
# Both layouts refuse to write when [run].reimport_from or the local collection is the destination or inside it.
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
import re, shlex, sys
sys.path.insert(0, sys.argv[2])
from _common import load_config
cfg = load_config(sys.argv[1])
targets = list(cfg["hosting"].get("targets") or ["mirror", "web"])
status = "main" if targets and targets[0] == "mirror" else "mirror"
version = str(cfg["hosting"].get("version") or "")
if version and not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]*", version):
    raise SystemExit(f"[hosting].version may only contain letters, digits, '.', '_' and '-': {version!r}")
reimport = cfg["run"].get("reimport_from") or ""
print(f"SURVEY={shlex.quote(cfg['run']['survey'])}")
print(f"COLLECTION={shlex.quote(cfg['names']['collection'])}")
print(f"CATALOG={shlex.quote(cfg['names']['catalog'])}")
print(f"STATUS={status}")
print(f"VERSION={shlex.quote(version)}")
print(f"REIMPORT_FROM={shlex.quote('' if not reimport or '://' in reimport else str(cfg.path(reimport)))}")
print(f"SRC={shlex.quote(str(cfg.collection_path))}")
print(f"WORK_DIR={shlex.quote(str(cfg.work_dir))}")
PY
)"
export HATS_PUBLISH_RUNLOG="$WORK_DIR/runlog.jsonl"
log() { python "$HERE/run_log.py" event publish-mirror --note "$1" >/dev/null 2>&1 || true; }
step() { printf '\n== %s\n' "$*"; }

SURVEY_DIR="$HATS_PUBLISH_MIRROR_ROOT/$SURVEY"
STABLE="$SURVEY_DIR/$COLLECTION"                               # the path users and hats_service_url address
if [ -n "$VERSION" ]; then DEST="${STABLE}_$VERSION"; else DEST="$STABLE"; fi
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

# ---- versioned layout: state of the stable path (same code as publish_web.sh, which runs it on the data host) --
read -r -d '' STABLE_PROBE <<'SH' || true
# args: stable path, main catalog name, destination. Prints the state of the stable path, the resolved destination
# and "end" (output without "end" means the probe did not complete).
s=$1; cat=$2; dest=$3
if [ -L "$s" ]; then
  printf 'symlink\t%s\t%s\n' "$(readlink "$s")" "$(readlink -m "$s")"
elif [ -d "$s" ]; then
  label=""; from="the directory's modification time"
  for f in "$s/collection.properties" "$s/$cat/hats.properties" "$s/$cat/properties"; do
    [ -r "$f" ] || continue
    label=$(sed -n 's/^hats_creation_date *= *\([0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]\).*/\1/p' "$f" | head -1)
    if [ -n "$label" ]; then from="hats_creation_date in ${f#"$s"/}"; break; fi
  done
  [ -n "$label" ] || label=$(date -r "$s" +%Y-%m-%d 2>/dev/null || echo old)
  taken=0; if [ -e "${s}_$label" ] || [ -L "${s}_$label" ]; then taken=1; fi
  printf 'dir\t%s\t%s\t%s\n' "$label" "$taken" "$from"
elif [ -e "$s" ]; then
  printf 'other\n'
else
  printf 'absent\n'
fi
printf 'dest\t%s\n' "$(readlink -m "$dest")"
echo end
SH

# args: survey directory, stable name, new target (a name in the survey directory; empty = remove the symlink),
# group or "". Points the stable symlink at the target atomically (a new symlink renamed over the old one).
read -r -d '' SWITCH_SH <<'SH' || true
set -eu
cd "$1"
if [ -e "$2" ] && [ ! -L "$2" ]; then echo "refusing: $1/$2 is no longer a symlink" >&2; exit 1; fi
if [ -z "$3" ]; then rm -f "$2"; echo "removed symlink $1/$2"; exit 0; fi
[ -d "$3" ] || { echo "refusing: $1/$3 is not a directory" >&2; exit 1; }
tmp=".$2.switch.$$"
ln -sfn "$3" "$tmp"
if [ -n "$4" ]; then chgrp -h "$4" "$tmp" 2>/dev/null || true; fi
mv -T "$tmp" "$2"
echo "$1/$2 -> $(readlink "$2")"
SH

STABLE_STATE=""; STABLE_LINK=""; STABLE_TARGET=""; STABLE_LABEL=""; STABLE_TAKEN=0; STABLE_LABEL_FROM=""; BLOCKED=0
DEST_REAL="$(realpath -m "$DEST")"
parse_probe() {   # $1 = output of STABLE_PROBE; sets STABLE_* and DEST_REAL; fails on incomplete output
  [ "$(tail -n 1 <<< "$1")" = "end" ] || return 1
  local line; line="$(head -n 1 <<< "$1")"
  STABLE_STATE="${line%%$'\t'*}"
  case "$STABLE_STATE" in
    symlink) IFS=$'\t' read -r _ STABLE_LINK STABLE_TARGET <<< "$line" ;;
    dir) IFS=$'\t' read -r _ STABLE_LABEL STABLE_TAKEN STABLE_LABEL_FROM <<< "$line" ;;
    absent|other) ;;
    *) return 1 ;;
  esac
  DEST_REAL="$(sed -n 's/^dest\t//p' <<< "$1")"
  [ -n "$DEST_REAL" ]
}
report_stable() {   # prints the stable path state; sets BLOCKED for a state the script does not change
  local ver="${COLLECTION}_$VERSION" new="${COLLECTION}_$STABLE_LABEL"
  case "$STABLE_STATE" in
    absent) echo "stable path: $STABLE (absent): created as a symlink -> $ver after the copy" ;;
    symlink)
      if [ "$STABLE_TARGET" = "$DEST_REAL" ]; then
        echo "stable path: $STABLE -> $STABLE_LINK: already this version (a push writes into the served copy;" \
             "a new build takes a new [hosting].version)"
      else
        echo "stable path: $STABLE -> $STABLE_LINK: re-pointed to $ver at the end; the previous target" \
             "$STABLE_TARGET stays on disk as the copy to retire later"
      fi ;;
    dir)
      BLOCKED=1
      echo "STOP: stable path $STABLE is a real directory (a legacy unversioned copy); this script never renames"
      echo "  or deletes a served copy. Rename it to a versioned name, leave a symlink in its place (it keeps"
      echo "  serving), then re-run:"
      echo "    cd '$SURVEY_DIR' && mv -T '$COLLECTION' '$new' && ln -s '$new' '$COLLECTION'"
      echo "  (label $STABLE_LABEL from $STABLE_LABEL_FROM)"
      if [ "$STABLE_TAKEN" = 1 ]; then echo "  $new exists already: choose another label"; fi
      if [ "$STABLE_LABEL" = "$VERSION" ]; then echo "  that label equals [hosting].version: choose another label for the old copy"; fi ;;
    other) BLOCKED=1; echo "STOP: $STABLE exists and is neither a directory nor a symlink; move it away and re-run" ;;
  esac
  return 0
}
under() { [ "$1" = "$2" ] || [ "${1#"$2"/}" != "$1" ]; }   # $1 is $2 or inside it

if [ "$CHECK" -eq 0 ] && [ "$YES" -eq 0 ]; then
  echo "This writes to shared storage. Run with --check to preview, then with --yes after the user has approved." >&2
  exit 2
fi

# ---- up front, before anything is written: destination, stable path, input guards ----------------------------
rc=0; out="$(bash -c "$STABLE_PROBE" probe "$STABLE" "$CATALOG" "$DEST" 2>&1)" || rc=$?
{ [ "$rc" -eq 0 ] && parse_probe "$out"; } || { echo "cannot read the state of $STABLE (exit $rc):" >&2; echo "$out" >&2; exit 1; }
if [ -n "$VERSION" ]; then
  echo "versioned destination: $DEST"
  report_stable
else
  echo "destination: $DEST (legacy layout: [hosting].version is empty)"
  if [ "$STABLE_STATE" = "symlink" ]; then   # a versioned catalog: the legacy layout would write into the served version
    echo "refusing: $STABLE is a symlink to $STABLE_LINK (versioned layout); set [hosting].version to a new label." \
         "Nothing was written." >&2
    exit 1
  fi
fi
SRC_REAL="$(realpath -m "$SRC")"
if under "$SRC_REAL" "$DEST_REAL" || under "$DEST_REAL" "$SRC_REAL"; then
  echo "refusing: the local collection $SRC_REAL and the destination $DEST_REAL overlap; nothing was written" >&2
  exit 1
fi
if [ -n "$REIMPORT_FROM" ]; then
  REIMPORT_REAL="$(realpath -m "$REIMPORT_FROM")"
  if under "$REIMPORT_REAL" "$DEST_REAL" || under "$DEST_REAL" "$REIMPORT_REAL"; then
    echo "refusing: [run].reimport_from ($REIMPORT_REAL) is the destination $DEST_REAL or overlaps it; the copy" \
         "would overwrite the input of this re-import. Set [hosting].version to a new label. Nothing was written." >&2
    exit 1
  fi
  if [ "$STABLE_STATE" = "symlink" ] && under "$REIMPORT_REAL" "$STABLE_TARGET"; then
    echo "note: [run].reimport_from is the copy the stable path serves now ($STABLE_TARGET); it is not written and" \
         "stays on disk as the previous version"
  fi
fi
[ "$BLOCKED" -eq 0 ] || { echo "nothing was written" >&2; exit 1; }

RSYNC=(rsync -a --chmod=D2775,F664)
# the files of the existing copy that the local collection does not have (rsync without --delete keeps them)
LOCAL_LIST="$(mktemp)"; DEST_LIST="$(mktemp)"; MANIFEST="$(mktemp)"
trap 'rm -f "$LOCAL_LIST" "$DEST_LIST" "$MANIFEST"' EXIT
(cd "$SRC" && find . -type f | sed 's|^\./||' | LC_ALL=C sort) > "$LOCAL_LIST"
if [ -d "$DEST" ]; then (cd "$DEST" && find . -type f | sed 's|^\./||' | LC_ALL=C sort) > "$DEST_LIST"; fi
EXTRA_N=$(LC_ALL=C comm -13 "$LOCAL_LIST" "$DEST_LIST" | wc -l)

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
    [ "$EXTRA_N" -gt 0 ] && LC_ALL=C comm -13 "$LOCAL_LIST" "$DEST_LIST" | head -10 | sed 's/^/  extra: /' \
      && echo "  --yes stops on these; --yes --replace deletes them"
    grep -h "^hats_status\|^hats_service_url" "$DEST/$CATALOG/properties" 2>/dev/null | sed 's/^/  /' || true
  else
    echo "copy absent"
    [ -w "$(dirname "$DEST")" ] || [ -w "$HATS_PUBLISH_MIRROR_ROOT" ] || echo "warning: no write access under $HATS_PUBLISH_MIRROR_ROOT" >&2
  fi
  linked=1; [ -z "$VERSION" ] || { [ "$STABLE_STATE" = "symlink" ] && [ "$STABLE_TARGET" = "$DEST_REAL" ]; } || linked=0
  [ "$n" = "0" ] && [ "$EXTRA_N" = "0" ] && [ -d "$DEST" ] && [ "$linked" = 1 ] && echo "nothing to do"
  exit 0
fi

DELETE=()
if [ "$EXTRA_N" -gt 0 ]; then
  if [ "$REPLACE" -eq 0 ]; then
    echo "$DEST holds $EXTRA_N files that the local collection does not have, e.g.:" >&2
    LC_ALL=C comm -13 "$LOCAL_LIST" "$DEST_LIST" | head -10 | sed 's/^/  /' >&2
    echo "nothing was written. Re-run with --yes --replace to delete them, or publish under a new collection name." >&2
    exit 1
  fi
  DELETE=(--delete); echo "--replace: $EXTRA_N files on the copy that the local collection does not have will be deleted"
fi

step "a. rsync $SRC -> $DEST"
# a new survey directory gets the same mode (and group) as the copies, not the login umask
if [ ! -d "$SURVEY_DIR" ]; then
  mkdir -m 2775 "$SURVEY_DIR"
  [ -n "$MIRROR_GROUP" ] && chgrp "$MIRROR_GROUP" "$SURVEY_DIR"
fi
mkdir -p "$DEST"
"${RSYNC[@]}" ${DELETE[@]+"${DELETE[@]}"} --info=stats1 "$SRC/" "$DEST/"
log "rsync to mirror done"

if [ -n "$MIRROR_GROUP" ]; then
  step "b. chgrp -R $MIRROR_GROUP"
  chgrp -R "$MIRROR_GROUP" "$DEST"
fi

step "c. integrity: sha256 manifest"
(cd "$SRC" && find . -type f | LC_ALL=C sort | sed 's|^\./||' | xargs -d '\n' sha256sum) > "$MANIFEST"
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

RETIRE=""
if [ -n "$VERSION" ]; then
  step "f. stable path $STABLE -> ${COLLECTION}_$VERSION"
  # the state is read again: the copy may have taken hours, and a changed stable path is not overwritten
  rc=0; out="$(bash -c "$STABLE_PROBE" probe "$STABLE" "$CATALOG" "$DEST" 2>&1)" || rc=$?
  { [ "$rc" -eq 0 ] && parse_probe "$out"; } || { echo "cannot read the state of $STABLE (exit $rc):" >&2; echo "$out" >&2; exit 1; }
  case "$STABLE_STATE" in
    absent|symlink) ;;
    *) echo "$STABLE changed during the copy and is no longer a symlink or absent; the copy is at $DEST and" \
            "nothing served changed" >&2; exit 1 ;;
  esac
  if [ "$STABLE_STATE" = "symlink" ] && [ "$STABLE_TARGET" = "$DEST_REAL" ]; then
    echo "$STABLE already points at this version"
  else
    bash -c "$SWITCH_SH" switch "$SURVEY_DIR" "$COLLECTION" "${COLLECTION}_$VERSION" "$MIRROR_GROUP"
    if [ "$STABLE_STATE" = "symlink" ]; then RETIRE="$STABLE_TARGET"; fi
  fi
fi
log "mirror copy at $DEST${SERVICE_URL:+ ($SERVICE_URL)}"
echo
echo "mirror copy: $DEST${SERVICE_URL:+  served at $SERVICE_URL when the mirror service is up}"
if [ -n "$VERSION" ]; then
  echo "stable path: $STABLE -> ${COLLECTION}_$VERSION"
  if [ -n "$RETIRE" ]; then
    echo "previous version kept on disk: $RETIRE (retire it later; reference/publishing.md \"Retiring a previous version\")"
  fi
fi
