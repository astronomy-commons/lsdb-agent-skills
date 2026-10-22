#!/usr/bin/env bash
# memwatch.sh — kill a step when the Python processes in its process tree together exceed a memory budget. For long
# foreground steps on a shared node (verification notebooks, comparisons): Dask-free Python has no memory limit of
# its own.
# Usage:  memwatch.sh <limit GB> <pidfile of the step> [command pattern, default ipykernel_launcher|python]
#         run it detached next to the step; it watches the descendants of the pid in the pid file only (two runs on
#         one node never kill each other's steps), stops when that pid exits and logs kills to memwatch.log.
#         The limit may be fractional (e.g. 7.5). The total is the sum of the resident memory (RSS) of the
#         matching processes; above it, the largest one is killed (SIGKILL) and the step's pid and the rest of
#         its process tree are terminated (SIGTERM).
LIMIT_GB=${1:?limit in GB}; PIDFILE=${2:?pid file}; PATTERN=${3:-ipykernel_launcher|python}
LIMIT_MB=$(awk -v g="$LIMIT_GB" 'BEGIN {if (g + 0 > 0) printf "%d", g * 1024; else print ""}')
[ -n "$LIMIT_MB" ] || { echo "memwatch.sh: limit must be a positive number of GB, got '$LIMIT_GB'" >&2; exit 2; }
descendants() { local c; for c in $(pgrep -P "$1" 2>/dev/null); do echo "$c"; descendants "$c"; done; }
while kill -0 "$(cat "$PIDFILE" 2>/dev/null)" 2>/dev/null; do
  root=$(cat "$PIDFILE" 2>/dev/null)
  total_kb=0; big_kb=0; big=""
  for p in $root $(descendants "$root"); do
    ps -o cmd= -p "$p" 2>/dev/null | grep -Eq "$PATTERN" || continue
    rss_kb=$(ps -o rss= -p "$p" 2>/dev/null | tr -d ' '); [[ "$rss_kb" =~ ^[0-9]+$ ]] || continue
    total_kb=$((total_kb + rss_kb))
    [ "$rss_kb" -gt "$big_kb" ] && { big_kb=$rss_kb; big=$p; }
  done
  if [ -n "$big" ] && [ $((total_kb / 1024)) -gt "$LIMIT_MB" ]; then
    echo "$(date -u +%FT%TZ) step $root at $((total_kb / 1024)) MB total > $LIMIT_GB GB: killed pid $big ($((big_kb / 1024)) MB)" >> memwatch.log
    tree=$(descendants "$root")   # listed before the kill: orphans would leave the tree
    kill -9 "$big" 2>/dev/null; kill "$root" $tree 2>/dev/null
  fi
  sleep 20
done
echo "$(date -u +%FT%TZ) watch ended" >> memwatch.log
