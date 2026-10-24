#!/usr/bin/env bash
#SBATCH --job-name=hats-publish
#SBATCH --nodes=1
#SBATCH --cpus-per-task=32
#SBATCH --mem=128G
#SBATCH --time=1-00:00:00
#SBATCH --requeue
#SBATCH --output=hats-publish-%j.out
#
# job.sbatch — the skill's batch route: the import and verification phases of the procedure as one SLURM job on a
# dedicated node, driven by the catalog.toml in the submission directory.
#
# Usage (from the work directory, in a shell where the import environment is active; SLURM propagates it):
#   sbatch $HATS_PUBLISH_SBATCH_OPTS [--cpus-per-task=N] [--mem=...] [--time=...] job.sbatch
# Capped run on a shared node instead of a batch job (same phases): export HATS_PUBLISH_LOCAL_CAPS=1 with the
#   HATS_PUBLISH_MAX_CPUS / HATS_PUBLISH_MAX_MEM_GB caps and `[dask]` sized to them, then
#   nice -n 10 setsid nohup bash job.sbatch > local_run.out 2>&1 &   (and memwatch.sh for the notebook step)
# HATS_PUBLISH_SBATCH_OPTS (site file) carries the account, partition and QOS; the #SBATCH lines above are generic
# defaults that command-line options override. Set HATS_PUBLISH_TEMPLATES to the templates directory when the
# templates were not copied next to catalog.toml; export it (rather than setting it inline) so that resubmit_loop.sh
# passes it on. On a preemptable QOS the job may be cancelled rather than requeued: run resubmit_loop.sh next to it;
# a resubmitted job resumes from hats-import's resume logs.
# Every phase runs from ./templates.pinned, the run's frozen copy of the templates (pin_templates.sh): the first job
# of a run creates it from $T, later jobs and resubmits reuse it, so a git pull or an edit of the skill during the run
# does not change the code the run executes. Change the code of a running import only with pin_templates.sh --repin.
#
# [run].route = "small" runs import_small.py (lsdb.from_dataframe) instead of import_collection.py (hats-import).
# HATS_PUBLISH_SKIP_IMPORT=1 runs only the phases after the import (collection already built). A successful import
# writes import.done in the work directory (resubmit_loop.sh uses it to skip the import on a resubmit).
# Phases (each appends to the run log): import -> ancillary -> verify_structure -> value-level notebook (full below
# HATS_PUBLISH_VERIFY_FULL_MAX_ROWS rows, default 20 000 000, AND below HATS_PUBLISH_VERIFY_FULL_MAX_GB GB of data
# files, default 8; else a HATS_PUBLISH_VERIFY_SAMPLE_ROWS sample, default 500 000; a sample also when the size cannot
# be read) -> compare_versions when [run].reimport_of is set -> RUNLOG.md and NOTES.md. A verify_structure FAIL skips
# the notebook and the comparison; the final run-log status is "fail" when any phase failed. Hosting and registration
# are interactive phases and are not part of the job.
set -uo pipefail
cd "${SLURM_SUBMIT_DIR:-.}"
T="${HATS_PUBLISH_TEMPLATES:-${SLURM_SUBMIT_DIR:-.}}"
if [ -f "$PWD/templates.pinned/PIN" ]; then
  T=$PWD/templates.pinned
else
  [ -f "$T/import_collection.py" ] || { echo "templates not found under $T: set HATS_PUBLISH_TEMPLATES" >&2; exit 2; }
  T=$(bash "$T/pin_templates.sh") || { echo "pinning the templates failed" >&2; exit 2; }
fi
echo "templates: $T ($(grep -m1 '^commit=' "$T/PIN" | cut -d= -f2 | cut -c1-12), dirty=$(grep -m1 '^dirty=' "$T/PIN" | cut -d= -f2))"
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
export OMP_NUM_THREADS=1 PYTHONUNBUFFERED=1
if [ -z "${HATS_PUBLISH_LOCAL_CAPS:-}" ]; then
  # a dedicated batch node: the shared-node caps do not apply. Exported empty, not unset: the templates fill only
  # variables that are absent from the environment from the site file, and treat an empty cap as no cap.
  export HATS_PUBLISH_MAX_CPUS= HATS_PUBLISH_MAX_MEM_GB=
else
  echo "capped local run: HATS_PUBLISH_MAX_CPUS=${HATS_PUBLISH_MAX_CPUS:-unset} HATS_PUBLISH_MAX_MEM_GB=${HATS_PUBLISH_MAX_MEM_GB:-unset}"
fi
RL="python $T/run_log.py"

cfg() { python - "$1" <<'PY'
import sys, tomllib
cfg = tomllib.load(open("catalog.toml", "rb"))
node = cfg
for key in sys.argv[1].split("."):
    node = node.get(key, "") if isinstance(node, dict) else ""
print(node if node is not None else "")
PY
}
dask_tmp=$(cfg dask.dask_tmp); [ -n "$dask_tmp" ] && mkdir -p "$dask_tmp"

$RL start batch --note "SLURM job ${SLURM_JOB_ID:-none}: ${SLURM_CPUS_PER_TASK:-?} CPUs, ${SLURM_MEM_PER_NODE:-?} MB"
echo "$(date -u +%FT%TZ) running on $(hostname) pid $$" >> run_host.txt   # where to look for this run; stays local, not in RUNLOG
echo "start $(date -u +%FT%TZ)"

echo "== import"
route=$(cfg run.route); [ "$route" = "small" ] && IMPORT=import_small.py || IMPORT=import_collection.py   # [run].route
echo "route: ${route:-hats-import} ($IMPORT)"
if [ -n "${HATS_PUBLISH_SKIP_IMPORT:-}" ]; then
  # the collection is already built (e.g. the job was stopped after the import): run the later phases only
  echo "HATS_PUBLISH_SKIP_IMPORT set: skipping the import"
else
  rm -f import.done
  if ! python "$T/$IMPORT" --no-pypi > import.log 2>&1; then
    $RL end batch --status fail --note "import failed; see import.log (resubmitted jobs resume)"
    echo "IMPORT FAILED"; exit 1
  fi
  date -u +%FT%TZ > import.done   # the import finished: a resubmit after this point skips it
fi
failed=()   # phases that failed; any entry makes the final status "fail"
echo "== ancillary"
python "$T/write_ancillary.py" --no-pypi > ancillary.log 2>&1; an=$?
[ $an -eq 0 ] || { echo "ancillary step failed (exit $an); see ancillary.log"; failed+=("ancillary=$an"); }
echo "== verify_structure"
python "$T/verify_structure.py" --no-pypi --spot-check > verify_structure.log 2>&1; vs=$?; echo "verify_structure exit $vs"
[ $vs -eq 0 ] || failed+=("verify_structure=$vs")

# the same paths the Python templates use (build_root from catalog.toml, else HATS_PUBLISH_BUILD_ROOT, else ./build)
mapfile -t paths < <(python - "$T" <<'PY'
import sys
sys.path.insert(0, sys.argv[1])
from _common import load_config
cfg = load_config("catalog.toml")
print(cfg.collection_path)
print(cfg.catalog_path)
PY
)
collection="${paths[0]:-}"; catalog="${paths[1]:-}"
[ -n "$collection" ] || echo "could not resolve the collection path from catalog.toml"

if [ $vs -ne 0 ]; then
  echo "verify_structure failed: the value-level notebook and the comparison are skipped (see verify_structure.log)"
else
  echo "== value-level notebook"
  nrows=""; data_gb=""
  if [ -n "$catalog" ] && [ -f "$catalog/hats.properties" ]; then
    nrows=$(grep -m1 '^hats_nrows=' "$catalog/hats.properties" | cut -d= -f2 | tr -d ' ')
  fi
  if [ -n "$catalog" ] && [ -d "$catalog/dataset" ]; then
    data_gb=$(find "$catalog/dataset" -name '*.parquet' -printf '%s\n' 2>/dev/null | awk '{s+=$1; n++} END {if (n) printf "%.3f", s/1e9}')
  fi
  full_max="${HATS_PUBLISH_VERIFY_FULL_MAX_ROWS:-20000000}"; full_max_gb="${HATS_PUBLISH_VERIFY_FULL_MAX_GB:-8}"; sample="${HATS_PUBLISH_VERIFY_SAMPLE_ROWS:-500000}"
  # run_verify.sh builds the notebook itself from these arguments (a notebook built separately would be rebuilt).
  # Full mode loads every row into one kernel: it needs both a row limit and a byte limit (list columns weigh
  # kilobytes per row). When either size is unknown the notebook runs on a sample, never in full mode.
  if [[ "$nrows" =~ ^[0-9]+$ ]] && [ -n "$data_gb" ] && [ "$nrows" -le "$full_max" ] \
     && awk -v g="$data_gb" -v m="$full_max_gb" 'BEGIN {exit !(g + 0 <= m + 0)}'; then
    NBARGS=(--sample-rows 0)
  else
    echo "catalog has ${nrows:-unknown} rows and ${data_gb:-unknown} GB of data files: the notebook runs on the first $sample source rows"
    NBARGS=(--sample-rows "$sample")
  fi
  if [ -n "${HATS_PUBLISH_LOCAL_CAPS:-}" ] && [ -n "${HATS_PUBLISH_MAX_MEM_GB:-}" ]; then
  # shared node: the notebook kernel is a single Python process with no memory limit of its own
    bash "$T/run_verify.sh" "${NBARGS[@]}" > run_verify.log 2>&1 & nbpid=$!; echo $nbpid > run_verify.pid
    bash "$T/memwatch.sh" "$HATS_PUBLISH_MAX_MEM_GB" run_verify.pid > /dev/null 2>&1 &
    wait $nbpid; nb=$?
  else
    bash "$T/run_verify.sh" "${NBARGS[@]}" > run_verify.log 2>&1; nb=$?
  fi
  echo "value-level notebook exit $nb"
  [ $nb -eq 0 ] || failed+=("notebook=$nb")

  old=$(cfg run.reimport_of)
  if [ -n "$old" ] && [ -z "$collection" ]; then
    failed+=("compare=no-collection-path")
  elif [ -n "$old" ]; then
    echo "== compare with $old"
    CMP=(python "$T/compare_versions.py" --new "$collection" --old "$old" --key "$(cfg columns.id | tr -d "[]' ")"
         --sample-pixels "${HATS_PUBLISH_COMPARE_PIXELS:-40}" --max-rows "${HATS_PUBLISH_COMPARE_MAX_ROWS:-5000000}" --out compare_versions.csv)
    [ "$(cfg run.expect_differences)" = "True" ] && CMP+=(--expect-value-changes)   # a re-import that fixes the old copy
    [ "$(cfg run.expect_removed_rows)" = "True" ] && CMP+=(--expect-removed-rows)   # a re-import that removes rows on purpose
    if [ -n "${HATS_PUBLISH_LOCAL_CAPS:-}" ] && [ -n "${HATS_PUBLISH_MAX_MEM_GB:-}" ]; then
      "${CMP[@]}" > compare.log 2>&1 & cmppid=$!; echo $cmppid > compare.pid
      bash "$T/memwatch.sh" "$HATS_PUBLISH_MAX_MEM_GB" compare.pid > /dev/null 2>&1 &
      wait $cmppid; cmp=$?
    else
      "${CMP[@]}" > compare.log 2>&1; cmp=$?
    fi
    echo "compare exit $cmp"
    # 0 = identical, 3 = identical up to numeric narrowing, 4 = only the differences [run].expect_differences
    # announces (same rows and ids); 3 and 4 are reported, not failures; anything else = different or error
    case $cmp in
      0) ;;
      3) echo "compare: identical up to type narrowing (see compare_versions.csv)" ;;
      4) echo "compare: expected differences only (review compare.log and compare_versions.csv: value changes, or rows removed on purpose)" ;;
      *) failed+=("compare=$cmp") ;;
    esac
  fi
fi

$RL end batch --status "$([ ${#failed[@]} -eq 0 ] && echo ok || echo fail)" \
  --note "$([ ${#failed[@]} -eq 0 ] && echo "all phases ok" || echo "failed: ${failed[*]}")"
python "$T/run_log.py" render --scrub
[ -f NOTES.md ] || cp "$T/NOTES.template.md" NOTES.md
echo "done $(date -u +%FT%TZ); next: review verify_structure.log, run_verify.log, compare.log, RUNLOG.md; then hosting (interactive)"
