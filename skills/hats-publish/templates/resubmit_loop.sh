#!/usr/bin/env bash
# resubmit_loop.sh — keep a batch import alive on a preemptable queue.
#
# Usage (from the work directory, with the import environment active so `python` is the import's python):
#   setsid nohup ./resubmit_loop.sh <job id> [sbatch options for resubmits ...] > /dev/null 2>&1 < /dev/null &
# The job script resubmitted is $HATS_PUBLISH_JOB_SCRIPT, else ./templates.pinned/job.sbatch.
# The site file named by $HATS_PUBLISH_SITE_ENV is sourced when set (for $HATS_PUBLISH_SBATCH_OPTS).
# A resubmit uses, in this order (later options win): $HATS_PUBLISH_SBATCH_OPTS, the original job's resources
# (CPUs per task, memory, time limit, partition, QOS, account; read once with scontrol, or sacct when the job has
# already left the controller, and kept in resubmit_opts.txt), the extra options given here, and
# --export=ALL,HATS_PUBLISH_TEMPLATES=<the pinned templates>.
#
# Behaviour:
#   - waits for the job to leave the queue, then reads its final state from sacct;
#   - PREEMPTED, NODE_FAIL and "CANCELLED by 0" (a system or admin cancel) are resubmitted; hats-import resumes
#     from its done-markers in tmp_dir, so finished stages and partitions are not redone. When the import had
#     already finished (import.done, or the run log's last "end import" entry has status ok), the resubmit runs
#     with HATS_PUBLISH_SKIP_IMPORT=1 so only the later phases run;
#   - RUNNING/PENDING/REQUEUED (SLURM requeued the job itself) after an empty squeue keeps watching the same job,
#     for at most 60 checks in a row (about 90 minutes), then stops with STALE_<state>;
#   - an empty sacct state (accounting lag) is retried 10 times, one minute apart, then the loop stops with UNKNOWN;
#   - anything else (COMPLETED, FAILED, TIMEOUT, OUT_OF_MEMORY, a cancel by the user) ends the loop and writes
#     loop_final_state.txt;
#   - at most $HATS_PUBLISH_MAX_RESUBMITS resubmits (default 10).
# Files in the work directory: resubmit_loop.log (also holds sbatch's error output), current_job.txt (the job being
# watched), resubmit_opts.txt, loop.heartbeat (host:pid, rewritten every minute). A second loop refuses to start
# while the heartbeat is younger than 3 minutes, on any node, unless the heartbeat names this host and a pid that is
# no longer alive (kill -0): the previous loop has exited, so a restart right after it does not wait out the
# 3 minutes. Never edit this script while a loop runs (bash reads it as it executes): stop it, edit, restart.
# The loop runs from ./templates.pinned, the run's frozen copy of the templates (pin_templates.sh, created here when
# the first job has not created it yet), and resubmits that copy's job.sbatch.
set -uo pipefail
SELF=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
if [ "$SELF" != "$PWD/templates.pinned" ]; then
  P=$(bash "$SELF/pin_templates.sh") || { echo "pinning the templates failed" >&2; exit 2; }
  exec bash "$P/resubmit_loop.sh" "$@"
fi
job=${1:?usage: resubmit_loop.sh <job id> [sbatch options ...]}
shift
EXTRA=("$@")
W=$PWD
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
export HATS_PUBLISH_TEMPLATES=$SELF   # the pinned copy, also for resubmits (--export below)
if [ -n "${HATS_PUBLISH_JOB_SCRIPT:-}" ]; then SCRIPT=$HATS_PUBLISH_JOB_SCRIPT
elif [ -n "${HATS_PUBLISH_TEMPLATES:-}" ]; then SCRIPT=$HATS_PUBLISH_TEMPLATES/job.sbatch
else SCRIPT=$W/job.sbatch; fi
[ -f "$SCRIPT" ] || { echo "job script not found: $SCRIPT (set HATS_PUBLISH_JOB_SCRIPT or HATS_PUBLISH_TEMPLATES)" >&2; exit 2; }
SCRIPT=$(cd "$(dirname "$SCRIPT")" && pwd)/$(basename "$SCRIPT")
T="${HATS_PUBLISH_TEMPLATES:-$(dirname "$SCRIPT")}"
MAX=${HATS_PUBLISH_MAX_RESUBMITS:-10}
HB=$W/loop.heartbeat
OPTS=$W/resubmit_opts.txt
n=0

log() { echo "$(date -u +%FT%TZ) $*" >> "$W/resubmit_loop.log"; }
beat() { echo "$(hostname):$$" > "$HB"; }

if [ -f "$HB" ] && [ $(( $(date +%s) - $(stat -c %Y "$HB") )) -lt 180 ]; then
    hb=$(cat "$HB" 2>/dev/null); hb_host=${hb%%:*}; hb_pid=${hb##*:}
    # a recent heartbeat of an exited loop on this host does not block; another host's is judged by its age only
    if [ "$hb_host" = "$(hostname)" ] && [[ "$hb_pid" =~ ^[0-9]+$ ]] && ! kill -0 "$hb_pid" 2>/dev/null; then
        log "heartbeat $hb is recent but that loop has exited (no pid $hb_pid on this host); starting"
    else
        log "another loop is alive ($hb); not starting"; exit 2
    fi
fi
beat; sleep 5
# two loops started at the same moment both pass the age check: the one whose heartbeat was overwritten stops
if [ "$(cat "$HB" 2>/dev/null)" != "$(hostname):$$" ]; then
    log "another loop started at the same time ($(cat "$HB")); not starting"; exit 2
fi
echo "$job" > "$W/current_job.txt"
rm -f "$W/loop_final_state.txt"
log "watching job $job; job script $SCRIPT"

# the original job's resources, recorded once so that every resubmit asks for the same allocation
record_opts() {
    local info cpus mem time part qos acct
    info=$(scontrol show job "$1" 2>/dev/null | tr ' ' '\n')
    if [ -n "$info" ]; then
        cpus=$(grep -m1 '^CPUs/Task=' <<< "$info" | cut -d= -f2)
        [ -n "$cpus" ] && [ "$cpus" != "N/A" ] || cpus=$(grep -m1 '^NumCPUs=' <<< "$info" | cut -d= -f2)
        mem=$(grep -m1 '^MinMemoryNode=' <<< "$info" | cut -d= -f2)
        [ -n "$mem" ] || mem=$(grep -m1 '^TRES=' <<< "$info" | grep -o 'mem=[^,]*' | cut -d= -f2)
        time=$(grep -m1 '^TimeLimit=' <<< "$info" | cut -d= -f2)
        part=$(grep -m1 '^Partition=' <<< "$info" | cut -d= -f2)
        qos=$(grep -m1 '^QOS=' <<< "$info" | cut -d= -f2)
        acct=$(grep -m1 '^Account=' <<< "$info" | cut -d= -f2)
    else
        IFS='|' read -r cpus mem time part qos acct < <(sacct -j "$1" -X -n -P -o AllocCPUS,ReqMem,Timelimit,Partition,QOS,Account 2>/dev/null | head -1)
        mem=${mem%n}; mem=${mem%c}   # older sacct appends n (per node) or c (per CPU)
    fi
    {
        [ -n "${cpus:-}" ] && [ "$cpus" != "0" ] && echo "--cpus-per-task=$cpus"
        [ -n "${mem:-}" ] && [ "$mem" != "0" ] && echo "--mem=$mem"
        [ -n "${time:-}" ] && [ "$time" != "UNLIMITED" ] && [ "$time" != "Partition_Limit" ] && echo "--time=$time"
        [ -n "${part:-}" ] && echo "--partition=$part"
        [ -n "${qos:-}" ] && echo "--qos=$qos"
        [ -n "${acct:-}" ] && echo "--account=$acct"
    } > "$OPTS.tmp"
    if [ -s "$OPTS.tmp" ]; then mv "$OPTS.tmp" "$OPTS"; log "recorded the original resources: $(tr '\n' ' ' < "$OPTS")"
    else rm -f "$OPTS.tmp"; log "could not read the resources of job $1: resubmits use only the site and extra options"; fi
}
[ -s "$OPTS" ] || record_opts "$job"

# true when the import of this run finished (marker from job.sbatch, or the run log's last "end import" entry)
import_finished() {
    [ -f "$W/import.done" ] && return 0
    [ -f "$W/runlog.jsonl" ] || return 1
    python - "$W/runlog.jsonl" <<'PY' 2>/dev/null
import json, sys
last = None
for line in open(sys.argv[1]):
    try:
        e = json.loads(line)
    except ValueError:
        continue
    if e.get("kind") == "end" and e.get("phase") == "import":
        last = e
sys.exit(0 if last is not None and last.get("status") == "ok" else 1)
PY
}

stale=0; unknown=0
while true; do
    while [ -n "$(squeue -j "$job" -h 2>/dev/null)" ]; do beat; stale=0; sleep 60; done
    beat; sleep 30
    full=$(sacct -j "$job" -X -n -o State%40 2>/dev/null | head -1 | sed 's/^ *//; s/ *$//')
    state=$(echo "$full" | awk '{print $1}')
    [ "$full" = "CANCELLED by 0" ] && state=CANCELLED_BY_ROOT
    if [ -z "$state" ]; then
        unknown=$((unknown + 1))
        if [ "$unknown" -ge 10 ]; then
            log "sacct returned no state for job $job 10 times; stopping"; echo "UNKNOWN" > "$W/loop_final_state.txt"; exit 1
        fi
        log "sacct returned no state for job $job (try $unknown of 10); waiting"; beat; sleep 60; continue
    fi
    unknown=0
    case "$state" in
        RUNNING|PENDING|CONFIGURING|COMPLETING|SUSPENDED|REQUEUED|REQUEUE_HOLD|REQUEUE_FED|RESIZING)
            stale=$((stale + 1))
            if [ "$stale" -ge 60 ]; then
                log "job $job is $state in sacct but absent from squeue for 60 checks; stopping"
                echo "STALE_$state" > "$W/loop_final_state.txt"; exit 1
            fi
            beat; sleep 60; continue ;;
    esac
    stale=0
    log "job $job ended: $state"
    case "$state" in
        PREEMPTED|NODE_FAIL|CANCELLED_BY_ROOT)
            if [ "$n" -ge "$MAX" ]; then log "resubmit limit $MAX reached; stopping"; echo "LIMIT" > "$W/loop_final_state.txt"; exit 1; fi
            [ -f import.log ] && cp import.log "import.$job.$state.log"
            EXPORT="ALL,HATS_PUBLISH_TEMPLATES=$T"
            if import_finished; then
                EXPORT="$EXPORT,HATS_PUBLISH_SKIP_IMPORT=1"; what="import finished before; resubmitted with HATS_PUBLISH_SKIP_IMPORT=1"
            else
                what="resubmitted (resume)"
            fi
            python "$T/run_log.py" event import --note "job $job ended $state; $what" --kv slurm_job="$job" >/dev/null 2>&1
            SAVED=(); [ -s "$OPTS" ] && mapfile -t SAVED < "$OPTS"
            # shellcheck disable=SC2086
            new=$(sbatch --parsable ${HATS_PUBLISH_SBATCH_OPTS:-} ${SAVED[@]+"${SAVED[@]}"} ${EXTRA[@]+"${EXTRA[@]}"} \
                  --export="$EXPORT" "$SCRIPT" 2>> "$W/resubmit_loop.log")
            new=${new%%;*}   # --parsable prints "id" or "id;cluster"
            if [ -z "$new" ]; then log "sbatch failed (see the lines above); stopping"; echo "SUBMIT_FAILED" > "$W/loop_final_state.txt"; exit 1; fi
            job=$new; n=$((n + 1)); echo "$job" > "$W/current_job.txt"
            log "$what as $job (resubmit $n)"
            ;;
        *)
            log "final state $state; loop done"; echo "$state" > "$W/loop_final_state.txt"; exit 0
            ;;
    esac
done
