#!/usr/bin/env bash
# publish_web.sh — copy a built HATS collection to the public web host and expose it at <WEB_URL>/<link>.
#
# Usage:  publish_web.sh [--config catalog.toml] [--check | --yes [--replace]] [-h]
#
# Reads catalog.toml ([run], [names], [hosting], [properties]) and the HATS_PUBLISH_WEB_* site variables
# (CONFIG.md; `export HATS_PUBLISH_SITE_ENV=<site file>` to load the team defaults). Steps:
#   a. rsync the local collection to the destination on <WEB_DATA_HOST>: <WEB_DATA_ROOT>/<survey>/<collection>_<version>/
#      with [hosting].version set, <WEB_DATA_ROOT>/<survey>/<collection>/ without (the legacy layout)
#   b. set the unix group on the copy (HATS_PUBLISH_WEB_GROUP)
#   c. integrity: sha256 manifest of every local file verified on the data host, plus file counts
#   d. per-copy properties on the COPY only: hats_status, hats_service_url (<WEB_URL>/<link>/<table> for every
#      table), relative hats_primary_table_url, written to both `hats.properties` and `properties` of the main
#      catalog and every margin/index table
#   e. symlink in the web tree: <WEB_ROOT>/<link> -> the destination as seen from the web host
#      (<WEB_DATA_ROOT_ON_WEB>/<survey>/<collection>[_<version>]); with a version, the stable path
#      <WEB_DATA_ROOT>/<survey>/<collection> on the data host is then pointed (atomically) at <collection>_<version>
#   f. public check: collection.properties over HTTP, then an lsdb open test; on failure the web symlink and the
#      stable symlink are pointed back to their previous targets (or removed when there were none) and the script stops
#   g. autoindex description in <WEB_ROOT>/.htaccess: an existing entry for the link is replaced in place, a new
#      one is inserted before any existing prefix-matching entry
# --check performs only read-only probes and reports what would change, including the file count of an existing
# copy and the files in it that the local collection does not have; --yes is required to write anything.
# When the destination already holds files that the local collection does not have, --yes stops before writing
# unless --replace is given, which deletes them (rsync --delete).
# Versioned layout ([hosting].version, e.g. "2026-10"): the stable path is a relative symlink to the versioned
# directory, switched only after the copy, the checksums, the properties and the public open test succeeded; the
# previous target stays on disk and is named as the copy to retire later. A real directory at the stable path (a
# legacy unversioned copy) stops the script with the rename to run first: the script never renames or deletes a
# served copy. Both layouts refuse to write when [run].reimport_from or the local collection is the destination or
# inside it (a comparison of paths, meaningful when the data store is mounted on this machine too).
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
    --config) ;;  # value follows
    *) [ "${prev:-}" = "--config" ] && CONFIG="$arg" ;;
  esac
  prev="$arg"
done

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1090
source "${HATS_PUBLISH_SITE_ENV:-/dev/null}"
for v in HATS_PUBLISH_WEB_URL HATS_PUBLISH_WEB_DATA_HOST HATS_PUBLISH_WEB_DATA_ROOT HATS_PUBLISH_WEB_HOST \
         HATS_PUBLISH_WEB_DATA_ROOT_ON_WEB HATS_PUBLISH_WEB_ROOT; do
  [ -n "${!v:-}" ] || { echo "missing site variable $v (see CONFIG.md)" >&2; exit 2; }
done
WEB_USER="${HATS_PUBLISH_WEB_USER:-$USER}"
WEB_GROUP="${HATS_PUBLISH_WEB_GROUP:-}"
DATA_HOST="$WEB_USER@$HATS_PUBLISH_WEB_DATA_HOST"
WEB_HOST="$WEB_USER@$HATS_PUBLISH_WEB_HOST"
SSH="ssh -o BatchMode=yes -o ConnectTimeout=20"
RSYNC=(rsync -a -e "$SSH" --chmod=D2775,F664)

# ---- configuration -> shell variables (via the shared loader) -------------------------------------------------
eval "$(python - "$CONFIG" "$HERE" <<'PY'
import re, shlex, sys
sys.path.insert(0, sys.argv[2])
from _common import load_config
cfg = load_config(sys.argv[1])
targets = list(cfg["hosting"].get("targets") or ["mirror", "web"])
version = str(cfg["hosting"].get("version") or "")
if version and not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]*", version):
    raise SystemExit(f"[hosting].version may only contain letters, digits, '.', '_' and '-': {version!r}")
reimport = cfg["run"].get("reimport_from") or ""
link = cfg["hosting"].get("web_link_name") or cfg["names"].get("public_name") or cfg["names"]["collection"]
desc = cfg["hosting"].get("autoindex_description") or cfg["properties"].get("obs_title") or cfg["names"]["collection"]
status = "main" if targets and targets[0] == "web" else "mirror"
print(f"SURVEY={shlex.quote(cfg['run']['survey'])}")
print(f"COLLECTION={shlex.quote(cfg['names']['collection'])}")
print(f"CATALOG={shlex.quote(cfg['names']['catalog'])}")
print(f"LINK={shlex.quote(link)}")
print(f"DESC={shlex.quote(desc)}")
print(f"STATUS={status}")
print(f"VERSION={shlex.quote(version)}")
print(f"REIMPORT_FROM={shlex.quote('' if not reimport or '://' in reimport else str(cfg.path(reimport)))}")
print(f"SRC={shlex.quote(str(cfg.collection_path))}")
print(f"WORK_DIR={shlex.quote(str(cfg.work_dir))}")
import base64
print(f"LINK_B64={base64.b64encode(link.encode()).decode()}")
print(f"DESC_B64={base64.b64encode(desc.encode()).decode()}")
PY
)"
export HATS_PUBLISH_RUNLOG="$WORK_DIR/runlog.jsonl"
log() { python "$HERE/run_log.py" event publish-web --note "$1" >/dev/null 2>&1 || true; }
step() { printf '\n== %s\n' "$*"; }

SURVEY_DIR="$HATS_PUBLISH_WEB_DATA_ROOT/$SURVEY"
STABLE="$SURVEY_DIR/$COLLECTION"                     # the stable path in the data store (a symlink when versioned)
SUFFIX=""; [ -z "$VERSION" ] || SUFFIX="_$VERSION"
DEST="$STABLE$SUFFIX"                                # where the copy goes (DEST_VER when versioned)
DEST_ON_WEB="$HATS_PUBLISH_WEB_DATA_ROOT_ON_WEB/$SURVEY/$COLLECTION$SUFFIX"   # the web link targets the versioned directory
PUBLIC="$HATS_PUBLISH_WEB_URL/$LINK"
[ -d "$SRC" ] || { echo "local collection not found: $SRC" >&2; exit 2; }

# ---- stdlib-only python run on the data host to stamp per-copy properties -----------------------------------
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
        text = f.read_text()
        p = dict(props)
        if table.name != catalog and "hats_primary_table_url=" in text:
            p["hats_primary_table_url"] = f"../{catalog}"
        rewrite(f, p); changed += 1
print(f"stamped {changed} property files in {len(tables)} tables (hats_status=public {status} clonableOnce)")
PY

# ---- stdlib-only python run on the web host to insert the autoindex description ------------------------------
read -r -d '' HTACCESS_PY <<'PY' || true
import re, sys
from pathlib import Path
import base64
path, check = Path(sys.argv[1]), sys.argv[4] == "1"
link = base64.b64decode(sys.argv[2]).decode(); desc = base64.b64decode(sys.argv[3]).decode()   # passed base64-encoded: no shell interpolation on the remote side
# Apache reads the text as one double-quoted word: control characters (a newline would start a new directive)
# become spaces, a double quote is escaped, and a backslash is refused (before the closing quote it would escape it)
desc = " ".join(re.sub(r"[\x00-\x1f\x7f]", " ", desc).split())
if not desc or "\\" in desc:
    sys.exit(f"refusing autoindex description {desc!r}: empty or contains a backslash")
desc = desc.replace('"', '\\"')
new = f'AddDescription "{desc}" {link} {link}/'
raw = path.read_text() if path.exists() else ""
old = raw.splitlines()
def entry_name(line):
    # both forms seen in hand-written files: `AddDescription "text" name` and `AddDescription "text" name name/`
    parts = line.split()
    if len(parts) < 3 or parts[0] != "AddDescription":
        return None
    return parts[-2] if parts[-1] == parts[-2] + "/" else parts[-1].rstrip("/")
same = [i for i, line in enumerate(old) if entry_name(line) == link]
if same and old[same[0]] == new and len(same) == 1:
    print("autoindex description already present"); sys.exit(0)
if same:
    # replace the first entry for this link in place (Apache uses the first match) and drop later duplicates
    at = same[0]
    lines = [new if i == at else line for i, line in enumerate(old) if i == at or i not in same]
    action = f"replace line {at + 1} ({old[at]})" + (f" and remove {len(same) - 1} later duplicate(s)" if len(same) > 1 else "")
else:
    at = len(old)
    for i, line in enumerate(old):
        name = entry_name(line)
        if name and link.startswith(name) and name != link:
            at = i; break
    lines = old[:at] + [new] + old[at:]
    action = f"insert at line {at + 1}"
if check:
    print(f"would {action}: {new}"); sys.exit(0)
try:
    path.with_name(path.name + ".bak").write_text(raw)   # best-effort backup
except OSError:
    pass
text = "\n".join(lines) + "\n"
path.write_text(text)          # truncate and rewrite in place: the directory is sticky, renames fail
if path.read_text() != text:
    path.write_text(raw)       # put the previous content back
    sys.exit("unexpected .htaccess content after writing: previous content restored")
print(f"{action}: {new}")
PY

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

read -r -d '' LINK_PROBE <<'SH' || true
# arg: the web link. Prints its state and "end".
l=$1
if [ -L "$l" ]; then printf 'symlink\t%s\n' "$(readlink "$l")"; elif [ -e "$l" ]; then echo other; else echo absent; fi
echo end
SH

STABLE_STATE=""; STABLE_LINK=""; STABLE_TARGET=""; STABLE_LABEL=""; STABLE_TAKEN=0; STABLE_LABEL_FROM=""; BLOCKED=0
DEST_REAL="$DEST"   # the destination resolved on the data host (set by probe_stable)
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
probe_stable() {   # reads the stable path's state on the data host; a failed ssh is an error, not "absent"
  local out rc=0
  out="$($SSH "$DATA_HOST" "bash -s -- '$STABLE' '$CATALOG' '$DEST'" <<< "$STABLE_PROBE" 2> "$ERR")" || rc=$?
  { [ "$rc" -eq 0 ] && parse_probe "$out"; } \
    || { echo "cannot read the state of $DATA_HOST:$STABLE (ssh exit $rc):" >&2; echo "$out" >&2; cat "$ERR" >&2; exit 1; }
}
report_stable() {   # prints the stable path state; sets BLOCKED for a state the script does not change
  local ver="${COLLECTION}_$VERSION" new="${COLLECTION}_$STABLE_LABEL"
  case "$STABLE_STATE" in
    absent) echo "stable path: $DATA_HOST:$STABLE (absent): created as a symlink -> $ver after the copy" ;;
    symlink)
      if [ "$STABLE_TARGET" = "$DEST_REAL" ]; then
        echo "stable path: $DATA_HOST:$STABLE -> $STABLE_LINK: already this version (a push writes into the served" \
             "copy; a new build takes a new [hosting].version)"
      else
        echo "stable path: $DATA_HOST:$STABLE -> $STABLE_LINK: re-pointed to $ver with the web link; the previous" \
             "target $STABLE_TARGET stays on disk as the copy to retire later"
      fi ;;
    dir)
      BLOCKED=1
      echo "STOP: stable path $DATA_HOST:$STABLE is a real directory (a legacy unversioned copy); this script never"
      echo "  renames or deletes a served copy. Rename it to a versioned name, leave a symlink in its place (the web"
      echo "  link keeps serving through it), then re-run:"
      echo "    ssh $DATA_HOST \"cd '$SURVEY_DIR' && mv -T '$COLLECTION' '$new' && ln -s '$new' '$COLLECTION'\""
      echo "  (label $STABLE_LABEL from $STABLE_LABEL_FROM)"
      if [ "$STABLE_TAKEN" = 1 ]; then echo "  $new exists already: choose another label"; fi
      if [ "$STABLE_LABEL" = "$VERSION" ]; then echo "  that label equals [hosting].version: choose another label for the old copy"; fi ;;
    other) BLOCKED=1; echo "STOP: $DATA_HOST:$STABLE exists and is neither a directory nor a symlink; move it away and re-run" ;;
  esac
  return 0
}
under() { [ "$1" = "$2" ] || [ "${1#"$2"/}" != "$1" ]; }   # $1 is $2 or inside it
switch_stable() {   # $1 = target name in the survey directory, empty = remove the stable symlink
  $SSH "$DATA_HOST" "bash -s -- '$SURVEY_DIR' '$COLLECTION' '$1' '$WEB_GROUP'" <<< "$SWITCH_SH"
}

if [ "$CHECK" -eq 0 ] && [ "$YES" -eq 0 ]; then
  echo "This writes to shared storage. Run with --check to preview, then with --yes after the user has approved." >&2
  exit 2
fi

LOCAL_LIST="$(mktemp)"; REMOTE_LIST="$(mktemp)"; MANIFEST="$(mktemp)"; ERR="$(mktemp)"
trap 'rm -f "$LOCAL_LIST" "$REMOTE_LIST" "$MANIFEST" "$ERR"' EXIT

# ---- up front, before anything is written: destination, stable path, web link, input guards ------------------
probe_stable
if [ -n "$VERSION" ]; then
  echo "versioned destination: $DATA_HOST:$DEST (web host: $DEST_ON_WEB)"
  report_stable
else
  echo "destination: $DATA_HOST:$DEST (legacy layout: [hosting].version is empty)"
  if [ "$STABLE_STATE" = "symlink" ]; then   # a versioned catalog: the legacy layout would write into the served version
    echo "refusing: $DATA_HOST:$STABLE is a symlink to $STABLE_LINK (versioned layout); set [hosting].version to a" \
         "new label. Nothing was written." >&2
    exit 1
  fi
fi
rc=0; out="$($SSH "$WEB_HOST" "bash -s -- '$HATS_PUBLISH_WEB_ROOT/$LINK'" <<< "$LINK_PROBE" 2> "$ERR")" || rc=$?
[ "$rc" -eq 0 ] && [ "$(tail -n 1 <<< "$out")" = "end" ] \
  || { echo "cannot read the web link $WEB_HOST:$HATS_PUBLISH_WEB_ROOT/$LINK (ssh exit $rc):" >&2; echo "$out" >&2; cat "$ERR" >&2; exit 1; }
LINK_STATE="$(head -n 1 <<< "$out")"; LINK_NOW=""
case "$LINK_STATE" in
  symlink*) LINK_NOW="${LINK_STATE#symlink$'\t'}"
            if [ "$LINK_NOW" = "$DEST_ON_WEB" ]; then echo "web link: $HATS_PUBLISH_WEB_ROOT/$LINK -> $LINK_NOW (already this copy)"
            else echo "web link: $HATS_PUBLISH_WEB_ROOT/$LINK -> $LINK_NOW: re-pointed to $DEST_ON_WEB after the copy"; fi ;;
  absent) echo "web link: $HATS_PUBLISH_WEB_ROOT/$LINK (absent): created -> $DEST_ON_WEB after the copy" ;;
  *) BLOCKED=1; echo "STOP: web link $HATS_PUBLISH_WEB_ROOT/$LINK exists and is not a symlink; move it away and re-run" ;;
esac
for dest in "$DEST" "$DEST_REAL"; do
  if under "$(realpath -m "$SRC")" "$dest" || under "$dest" "$(realpath -m "$SRC")"; then
    echo "refusing: the local collection $SRC and the destination $dest overlap; nothing was written" >&2; exit 1
  fi
  if [ -n "$REIMPORT_FROM" ] && { under "$(realpath -m "$REIMPORT_FROM")" "$dest" || under "$dest" "$(realpath -m "$REIMPORT_FROM")"; }; then
    echo "refusing: [run].reimport_from ($(realpath -m "$REIMPORT_FROM")) is the destination $dest or overlaps it; the" \
         "copy would overwrite the input of this re-import. Set [hosting].version to a new label. Nothing was written." >&2
    exit 1
  fi
done
if [ -n "$REIMPORT_FROM" ] && [ "$STABLE_STATE" = "symlink" ] && under "$(realpath -m "$REIMPORT_FROM")" "$STABLE_TARGET"; then
  echo "note: [run].reimport_from is the copy the stable path serves now ($STABLE_TARGET); it is not written and" \
       "stays on disk as the previous version"
fi
[ "$BLOCKED" -eq 0 ] || { echo "nothing was written" >&2; exit 1; }

# the files of the existing copy that the local collection does not have (rsync without --delete keeps them)
(cd "$SRC" && find . -type f | sed 's|^\./||' | LC_ALL=C sort) > "$LOCAL_LIST"
rc=0
remote=$($SSH "$DATA_HOST" "if [ -d '$DEST' ]; then echo present; cd '$DEST' && find . -type f | sed 's|^\./||' | LC_ALL=C sort; else echo absent; fi" 2> "$ERR") || rc=$?
[ "$rc" -eq 0 ] || { echo "cannot list $DATA_HOST:$DEST (ssh exit $rc):" >&2; cat "$ERR" >&2; exit 1; }
DEST_EXISTS=0; [ "$(head -1 <<< "$remote")" = "present" ] && DEST_EXISTS=1
tail -n +2 <<< "$remote" | grep -v '^$' > "$REMOTE_LIST" || true
EXTRA_N=$(LC_ALL=C comm -13 "$LOCAL_LIST" "$REMOTE_LIST" | wc -l)

if [ "$CHECK" -eq 1 ]; then
  step "check: rsync dry run $SRC -> $DATA_HOST:$DEST"
  if [ "$DEST_EXISTS" -eq 1 ]; then
    rc=0; out=$("${RSYNC[@]}" --dry-run --itemize-changes "$SRC/" "$DATA_HOST:$DEST/" 2>&1) || rc=$?
    [ "$rc" -eq 0 ] || { echo "rsync dry run failed (exit $rc):" >&2; echo "$out" >&2; exit 1; }
    n=$(grep -c '^[<>]f' <<< "$out" || true)
  else
    n=$(wc -l < "$LOCAL_LIST")   # no copy yet (rsync cannot dry-run into a missing parent directory): every file
  fi
  echo "files that would be transferred: $n of $(wc -l < "$LOCAL_LIST")"
  if [ "$DEST_EXISTS" -eq 1 ]; then
    echo "copy exists: $(wc -l < "$REMOTE_LIST") files; $EXTRA_N of them are not in the local collection"
    [ "$EXTRA_N" -gt 0 ] && LC_ALL=C comm -13 "$LOCAL_LIST" "$REMOTE_LIST" | head -10 | sed 's/^/  extra: /' \
      && echo "  --yes stops on these; --yes --replace deletes them"
  else
    echo "copy absent"
  fi
  step "check: autoindex description"
  $SSH "$WEB_HOST" "python3 - '$HATS_PUBLISH_WEB_ROOT/.htaccess' '$LINK_B64' '$DESC_B64' 1" <<< "$HTACCESS_PY"
  step "check: public URL"
  code=$(curl -sL -o /dev/null -w '%{http_code}' --max-time 30 "$PUBLIC/collection.properties" || echo 000)
  echo "$PUBLIC/collection.properties -> HTTP $code"
  linked=1; [ "$LINK_NOW" = "$DEST_ON_WEB" ] || linked=0
  if [ -n "$VERSION" ] && { [ "$STABLE_STATE" != "symlink" ] || [ "$STABLE_TARGET" != "$DEST_REAL" ]; }; then linked=0; fi
  if [ "$n" = "0" ] && [ "$EXTRA_N" = "0" ] && [ "$code" = "200" ] && [ "$linked" = 1 ]; then echo "nothing to do"; fi
  exit 0
fi

DELETE=()
if [ "$EXTRA_N" -gt 0 ]; then
  if [ "$REPLACE" -eq 0 ]; then
    echo "$DATA_HOST:$DEST holds $EXTRA_N files that the local collection does not have, e.g.:" >&2
    LC_ALL=C comm -13 "$LOCAL_LIST" "$REMOTE_LIST" | head -10 | sed 's/^/  /' >&2
    echo "nothing was written. Re-run with --yes --replace to delete them, or publish under a new collection name." >&2
    exit 1
  fi
  DELETE=(--delete); echo "--replace: $EXTRA_N files on the copy that the local collection does not have will be deleted"
fi

step "a. rsync $SRC -> $DATA_HOST:$DEST"
# a new survey directory gets the same mode (and group) as the copies, not the login umask
$SSH "$DATA_HOST" "[ -d '$HATS_PUBLISH_WEB_DATA_ROOT/$SURVEY' ] || { mkdir -m 2775 '$HATS_PUBLISH_WEB_DATA_ROOT/$SURVEY'${WEB_GROUP:+ && chgrp '$WEB_GROUP' '$HATS_PUBLISH_WEB_DATA_ROOT/$SURVEY'}; }; mkdir -p '$DEST'"
"${RSYNC[@]}" ${DELETE[@]+"${DELETE[@]}"} --info=stats1 "$SRC/" "$DATA_HOST:$DEST/"
log "rsync to web data host done"

if [ -n "$WEB_GROUP" ]; then
  step "b. chgrp -R $WEB_GROUP"
  $SSH "$DATA_HOST" "chgrp -R '$WEB_GROUP' '$DEST'"
fi

step "c. integrity: sha256 manifest"
(cd "$SRC" && find . -type f | LC_ALL=C sort | sed 's|^\./||' | xargs -d '\n' sha256sum) > "$MANIFEST"
local_n=$(wc -l < "$MANIFEST")
$SSH "$DATA_HOST" "cd '$DEST' && sha256sum -c --quiet -" < "$MANIFEST"
remote_n=$($SSH "$DATA_HOST" "cd '$DEST' && find . -type f | wc -l")
echo "local files $local_n, remote files $remote_n, all checksums match"
[ "$local_n" = "$remote_n" ] || { echo "file count differs" >&2; exit 1; }
log "integrity verified: $local_n files"

step "d. per-copy properties (hats_status=$STATUS, hats_service_url=$PUBLIC/<table>)"
$SSH "$DATA_HOST" "python3 - '$DEST' '$CATALOG' '$STATUS' '$PUBLIC'" <<< "$STAMP_PY"

step "e. symlink $HATS_PUBLISH_WEB_ROOT/$LINK -> $DEST_ON_WEB${VERSION:+ and $STABLE -> ${COLLECTION}_$VERSION}"
if [ -n "$VERSION" ]; then
  # the state is read again: the copy may have taken hours, and a changed stable path is not overwritten
  probe_stable
  case "$STABLE_STATE" in
    absent|symlink) ;;
    *) echo "$DATA_HOST:$STABLE changed during the copy and is no longer a symlink or absent; the copy is at $DEST" \
            "and nothing served changed" >&2; exit 1 ;;
  esac
fi
rc=0; PREV=$($SSH "$WEB_HOST" "if [ -L '$HATS_PUBLISH_WEB_ROOT/$LINK' ]; then readlink '$HATS_PUBLISH_WEB_ROOT/$LINK'; fi") || rc=$?
[ "$rc" -eq 0 ] || { echo "cannot read the web link (ssh exit $rc); nothing served changed" >&2; exit 1; }
STABLE_SWITCHED=0; RETIRE=""
if [ "${LINK%/*}" != "$LINK" ]; then   # a nested link (group/name): its group directory must exist in the web tree
  PARENT="$HATS_PUBLISH_WEB_ROOT/${LINK%/*}"
  $SSH "$WEB_HOST" "[ -d '$PARENT' ] || { mkdir -p -m 2775 '$PARENT'${WEB_GROUP:+ && chgrp '$WEB_GROUP' '$PARENT'}; }"
fi
$SSH "$WEB_HOST" "test -d '$DEST_ON_WEB' && ln -sfn '$DEST_ON_WEB' '$HATS_PUBLISH_WEB_ROOT/$LINK' && ls -l '$HATS_PUBLISH_WEB_ROOT/$LINK'"
revert() {   # point the public name (and the stable path) back to where they pointed before this run
  if [ "$STABLE_SWITCHED" -eq 1 ]; then
    if [ "$STABLE_STATE" = "symlink" ]; then switch_stable "$STABLE_LINK" >&2 && echo "stable path pointed back to $STABLE_LINK" >&2
    else switch_stable "" >&2 && echo "stable path removed (there was none before)" >&2; fi
  fi
  if [ -n "$PREV" ] && [ "$PREV" != "$DEST_ON_WEB" ]; then
    $SSH "$WEB_HOST" "ln -sfn '$PREV' '$HATS_PUBLISH_WEB_ROOT/$LINK'" && echo "symlink pointed back to $PREV" >&2
  elif [ -z "$PREV" ]; then
    $SSH "$WEB_HOST" "rm -f '$HATS_PUBLISH_WEB_ROOT/$LINK'" && echo "symlink removed (there was none before)" >&2
  else
    echo "the symlink already pointed at this copy before the run: left as is" >&2
  fi
  log "public check failed after the symlink step"
}
if [ -n "$VERSION" ]; then
  if [ "$STABLE_STATE" = "symlink" ] && [ "$STABLE_TARGET" = "$DEST_REAL" ]; then
    echo "$DATA_HOST:$STABLE already points at this version"
  else
    STABLE_SWITCHED=1
    switch_stable "${COLLECTION}_$VERSION" || { echo "switching the stable path failed" >&2; revert; exit 1; }
    if [ "$STABLE_STATE" = "symlink" ]; then RETIRE="$STABLE_TARGET"; fi
  fi
fi

step "f. public check"
code=$(curl -sL -o /dev/null -w '%{http_code}' --max-time 60 "$PUBLIC/collection.properties" || echo 000)
echo "$PUBLIC/collection.properties -> HTTP $code"
[ "$code" = "200" ] || { echo "public URL not serving" >&2; revert; exit 1; }
rc=0
python - "$PUBLIC" <<'PY' || rc=$?
import sys
try:
    import lsdb
except ImportError:
    sys.exit(3)   # not importable here: the open test is skipped, not failed
cat = lsdb.open_catalog(sys.argv[1])
info = cat.hc_structure.catalog_info
margin = cat.margin.hc_structure.catalog_info.catalog_name if cat.margin is not None else None
print(f"lsdb.open_catalog OK: {info.catalog_name}, {info.total_rows:,} rows, margin {margin}")
PY
if [ "$rc" -eq 3 ]; then
  echo "lsdb open test skipped (lsdb not importable here); run it from an environment with lsdb before announcing"
elif [ "$rc" -ne 0 ]; then
  echo "lsdb.open_catalog($PUBLIC) failed (exit $rc)" >&2; revert; exit 1
fi

step "g. autoindex description"
$SSH "$WEB_HOST" "python3 - '$HATS_PUBLISH_WEB_ROOT/.htaccess' '$LINK_B64' '$DESC_B64' 0" <<< "$HTACCESS_PY"
log "published at $PUBLIC"
echo
echo "published: $PUBLIC"
if [ -n "$VERSION" ]; then
  echo "stable path: $DATA_HOST:$STABLE -> ${COLLECTION}_$VERSION"
  if [ -n "$RETIRE" ]; then
    echo "previous version kept on disk: $DATA_HOST:$RETIRE (retire it later; reference/publishing.md \"Retiring a previous version\")"
  fi
fi
if [ -n "$PREV" ] && [ "$PREV" != "$DEST_ON_WEB" ]; then echo "the web link pointed at $PREV before this run"; fi
