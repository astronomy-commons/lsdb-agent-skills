#!/usr/bin/env bash
# run_verify.sh — build and execute the value-level verification notebook for one import run.
#
# Usage: run_verify.sh [--config catalog.toml] [--collection PATH] [--staging PATH] [--public-url URL] [--sample-rows N]
# Runs from the work directory (where catalog.toml lives). Steps:
#   1. build_verify_notebook.py  -> verify_<catalog>.ipynb (in the work dir)
#   2. jupyter nbconvert --execute --inplace (pinned to HATS_PUBLISH_MAX_CPUS physical cores when set and not
#      empty; an empty value, as job.sbatch exports on a dedicated node, means no cap)
#   3. strip ipywidget residue so the stored outputs render everywhere
#   4. print the PASS/FAIL/SKIP lines and exit non-zero on any FAIL or execution error (SKIP is not a failure)
# The notebook itself appends the "verify-values" entry to runlog.jsonl.
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
CONFIG="catalog.toml"
ARGS=()
while [ $# -gt 0 ]; do
  case "$1" in
    --config) CONFIG="$2"; ARGS+=("--config" "$2"); shift 2 ;;
    *) ARGS+=("$1"); shift ;;
  esac
done

NB=$(python "$HERE/build_verify_notebook.py" "${ARGS[@]}" | sed -n 's/^wrote \(.*\) (.*/\1/p')
[ -n "$NB" ] || { echo "notebook build failed"; exit 2; }

PIN=()
if [[ "${HATS_PUBLISH_MAX_CPUS:-}" =~ ^[1-9][0-9]*$ ]] && command -v taskset >/dev/null 2>&1; then
  CORES=$(lscpu -p=CPU,CORE 2>/dev/null | grep -v '^#' | sort -t, -k2,2n -u | head -n "$HATS_PUBLISH_MAX_CPUS" | cut -d, -f1 | paste -sd,)
  [ -n "$CORES" ] && PIN=(taskset -c "$CORES")
fi

if python - "$CONFIG" <<'PY'
import sys, tomllib
cfg = tomllib.load(open(sys.argv[1], "rb"))
sys.exit(0 if cfg.get("verify", {}).get("value_level", True) else 1)
PY
then :; else echo "[verify].value_level is false: skipping the value-level notebook"; exit 0; fi

echo "executing $NB ..."
# --allow-errors keeps the outputs of a failing notebook (the PASS/FAIL table is the diagnostic); the exit code
# comes from parsing those outputs below.
${PIN[@]+"${PIN[@]}"} jupyter nbconvert --to notebook --execute --allow-errors --inplace --ExecutePreprocessor.timeout=0 "$NB" > "${NB%.ipynb}.nbconvert.log" 2>&1
STATUS=$?

python - "$NB" <<'PY'
import sys, nbformat
p = sys.argv[1]
nb = nbformat.read(p, as_version=4)
nb.metadata.pop("widgets", None)
dropped = 0; fails = 0; errors = 0
for c in nb.cells:
    if c.cell_type != "code":
        continue
    keep = []
    for o in c.get("outputs", []):
        if "application/vnd.jupyter.widget-view+json" in o.get("data", {}):
            dropped += 1; continue
        keep.append(o)
        if o.output_type == "error":
            errors += 1
        if o.output_type == "stream":
            for line in o.text.splitlines():
                if line.startswith(("PASS", "FAIL", "SKIP")) or "checks passed" in line or line.startswith("VERDICT"):
                    print(line)
                    fails += line.startswith("FAIL")
    c["outputs"] = keep
nbformat.write(nb, p)
print(f"(stripped {dropped} widget outputs; {fails} FAIL lines; {errors} error outputs)")
sys.exit(1 if (fails or errors) else 0)
PY
PARSE=$?

if [ $STATUS -ne 0 ]; then
  echo "nbconvert exited with $STATUS; see ${NB%.ipynb}.nbconvert.log"
  python "$HERE/run_log.py" end verify-values --status fail --note "nbconvert exited $STATUS" 2>/dev/null || true
  exit $STATUS
fi
exit $PARSE
