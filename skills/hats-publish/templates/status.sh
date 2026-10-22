#!/usr/bin/env bash
# status.sh — one-screen progress summary of an import run, for periodic updates to the user.
#
# Usage: status.sh [work dir]   (default: the current directory, the one holding catalog.toml)
# Prints only what exists for the run:
#   download   when download.pid exists: whether that pid is alive, and the file count and size of the raw directory
#   job        the SLURM job in current_job.txt (state, elapsed, node) or the user's jobs named hats-*
#   loop       a resubmit loop's heartbeat age (loop.heartbeat), when one is running
#   stage      the latest hats-import progress line (Catalog / Margin / Index / Collection) from import.log
#   batch      the last phase line of the newest SLURM output file (== import, == ancillary, ...)
#   runlog     the latest runlog.jsonl entry (kind, phase, status, note)
# Intended for scheduled updates (every few minutes during a long import): report what changed since the last one.
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# Site defaults: source $HATS_PUBLISH_SITE_ENV when set. A HATS_PUBLISH_* variable already present in the
# environment keeps its value, even when empty (the same rule as _common.site_env() in the Python templates).
load_site_env() {
  [ -n "${HATS_PUBLISH_SITE_ENV:-}" ] && [ -f "$HATS_PUBLISH_SITE_ENV" ] || return 0
  local names=(${!HATS_PUBLISH_@}) saved=""
  [ ${#names[@]} -gt 0 ] && saved=$(declare -p "${names[@]}" | sed 's/^declare /declare -g /')
  # shellcheck disable=SC1090
  source "$HATS_PUBLISH_SITE_ENV"
  eval "$saved"
}
load_site_env
W="${1:-$PWD}"
cd "$W" || exit 1
now=$(date +%s)

if [ -f download.pid ]; then
    # a download started in the background records its pid there, e.g. `nohup <download command> & echo $! > download.pid`;
    # the pid is checked on this host only
    dpid=$(cat download.pid 2>/dev/null)
    raw=$(python - "$HERE" <<'PY' 2>/dev/null
import sys
sys.path.insert(0, sys.argv[1])
from _common import load_config
print(load_config("catalog.toml").raw_dir)
PY
)
    size="?"; nraw="?"
    if [ -n "$raw" ] && [ -d "$raw" ]; then
        size=$(du -sb "$raw" 2>/dev/null | awk '{printf "%.2f GB", $1/1e9}')
        nraw=$(find "$raw" -type f 2>/dev/null | wc -l)
    fi
    if [ -n "$dpid" ] && kill -0 "$dpid" 2>/dev/null; then state="running (pid $dpid)"; else state="not running (pid ${dpid:-?})"; fi
    echo "download: $state | ${raw:-raw directory unknown}: $nraw files, $size"
fi

job=$(cat current_job.txt 2>/dev/null)
if [ -n "$job" ]; then
    line=$(squeue -j "$job" -h -o '%i %j %T %M %N %R' 2>/dev/null)
    [ -z "$line" ] && line="$job $(sacct -j "$job" -X -n -o State%20,Elapsed 2>/dev/null | head -1 | tr -s ' ')"
    echo "job: $line"
else
    q=$(squeue -u "$USER" -h -o '%i %j %T %M %N' 2>/dev/null | grep ' hats' | tr '\n' ';')
    [ -n "$q" ] && echo "jobs: $q"
fi

if [ -f loop.heartbeat ]; then
    echo "loop: $(cat loop.heartbeat), heartbeat $(( now - $(stat -c %Y loop.heartbeat) )) s ago"
fi

if [ -f import.log ]; then
    stage=$(tr '\r' '\n' < import.log | grep -E '^(Catalog|Margin|Index|Collection)' | tail -1 | sed -E 's/\|[^|]*\|/|/')
    [ -n "$stage" ] && echo "stage: $stage"
    err=$(grep -a -m1 -E 'Traceback|Error' import.log 2>/dev/null)
    [ -n "$err" ] && echo "import.log has an error: $(grep -a -E '^[A-Za-z]*(Error|Exception)' import.log | tail -1 | cut -c1-160)"
fi

out=$(ls -t hats-publish-*.out 2>/dev/null | head -1)
[ -n "$out" ] && echo "batch: $(grep -E '^==|exit|FAILED|done' "$out" | tail -2 | tr '\n' ' ')"

if [ -f runlog.jsonl ]; then
    tail -1 runlog.jsonl | python3 -c 'import json, sys
e = json.loads(sys.stdin.read())
print("runlog:", e.get("ts"), e.get("kind"), e.get("phase"), e.get("status") or "", (e.get("note") or "")[:120])'
fi
exit 0
