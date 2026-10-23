#!/usr/bin/env bash
# pin_templates.sh — freeze the skill's templates for one run.
#
# Usage (from the work directory that holds catalog.toml):
#   T=$(bash <skill>/templates/pin_templates.sh)            # pin once; every later step runs $T/...
#   bash <skill>/templates/pin_templates.sh --repin         # replace the pin with the current templates
#
# Copies the templates next to this script into ./templates.pinned (read-only) and prints that directory's absolute
# path. Every step of the run, job.sbatch and resubmit_loop.sh included, then runs this copy, so a git pull or an
# edit of the skill while the run is going does not change the code it executes, and the run log names the commit
# that built the catalog. templates.pinned/PIN records the source directory, the git commit, whether the source had
# uncommitted changes (their diff is in PIN.diff) and the pin time; SHA256SUMS lists every copied file.
# An existing pin is kept and its path printed (resubmits and reruns keep the code the run started with). --repin
# moves the old pin to templates.pinned.<UTC time> and copies the templates again; the run log records both.
set -euo pipefail
SRC=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
W=$PWD
P=$W/templates.pinned
repin=""
[ "${1:-}" = "--repin" ] && repin=1

if [ "$SRC" = "$P" ]; then
  [ -z "$repin" ] || { echo "--repin needs the skill's templates directory, not the pinned copy" >&2; exit 2; }
  echo "$P"; exit 0
fi
if [ -f "$P/PIN" ] && [ -z "$repin" ]; then
  echo "$P"; exit 0
fi
[ -f "$SRC/import_collection.py" ] || { echo "not a templates directory: $SRC" >&2; exit 2; }

# copy into a private directory, then rename: two processes pinning at once (job and resubmit loop) cannot mix files
tmp=$(mktemp -d "$W/.templates.pinned.XXXXXX")
trap 'chmod -R u+w "$tmp" 2>/dev/null; rm -rf "$tmp"' EXIT
# the template files only: when the templates sit next to catalog.toml, the work directory's logs are not copied
shopt -s nullglob
for f in "$SRC"/*.py "$SRC"/*.sh "$SRC"/*.sbatch "$SRC"/*.template.md; do
  cp -p "$f" "$tmp/"
done
commit=$(git -C "$SRC" rev-parse HEAD 2>/dev/null || echo none)
dirty=no
if [ "$commit" != none ] && [ -n "$(git -C "$SRC" status --porcelain -- . 2>/dev/null)" ]; then
  dirty=yes
  { git -C "$SRC" diff HEAD -- .; git -C "$SRC" ls-files --others --exclude-standard -- . | sed 's/^/untracked: /'; } > "$tmp/PIN.diff"
fi
{
  echo "source=$SRC"
  echo "commit=$commit"
  echo "dirty=$dirty"
  echo "pinned_at=$(date -u +%FT%TZ)"
} > "$tmp/PIN"
(cd "$tmp" && sha256sum -- * > SHA256SUMS)
chmod -R a-w "$tmp"
chmod u+w "$tmp"   # the directory itself stays writable for the owner so that --repin can move it

old=""
if [ -n "$repin" ] && [ -e "$P" ]; then
  old=$P.$(date -u +%Y%m%dT%H%M%SZ)
  mv -T "$P" "$old"
fi
if ! mv -T "$tmp" "$P" 2>/dev/null; then
  # another process pinned first: use its copy
  [ -f "$P/PIN" ] || { echo "could not create $P" >&2; exit 2; }
  echo "$P"; exit 0
fi
trap - EXIT
note="templates $([ -n "$old" ] && echo re-pinned || echo pinned) at commit ${commit:0:12}"
[ "$dirty" = yes ] && note="$note with uncommitted changes (templates.pinned/PIN.diff)"
[ -n "$old" ] && note="$note; previous pin kept as $(basename "$old")"
python "$P/run_log.py" event pin --note "$note" --kv commit="$commit" dirty="$dirty" >/dev/null 2>&1 || true
echo "$note" >&2
echo "$P"
