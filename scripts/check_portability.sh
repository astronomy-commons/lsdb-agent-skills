#!/usr/bin/env bash
# check_portability.sh — fail if any file carries machine-, person-, site- or dataset-specific content.
#
# Runs as a pre-commit hook and in CI. Usage:
#   scripts/check_portability.sh            # scan skills/, runlogs/, scripts/, .github/ and README.md
#   scripts/check_portability.sh --links    # also verify that every https:// URL answers 200
#   scripts/check_portability.sh <path>...  # scan given files or directories
#
# Rules (one grep each; the pattern lists ARE the policy — extend them when a review finds a new leak):
#   1. local paths: home directories, scratch and site mounts, any absolute path of two or more components
#      outside a few system paths and the /path/to/ placeholder
#   2. people: e-mail addresses, first-person sentences
#   3. environments: conda/mamba/venv activation with a literal environment name
#   4. private sources: chat, meeting-note, memory, private-document or private-export references
#   5. non-public URLs: anything not https://, and local or internal hostnames
#   6. dataset names in templates/ and scripts (reference docs may cite public catalogs with a public issue)
#   7. process residue: drafts, rejections, "as of <date>", internal meetings, agent mechanics
#   8. institution hostnames (*.edu, *.gov, *.ac.<country>)
#   9. batch-system literals: --account=, --partition=, --qos= with a value that is not a <placeholder> or $variable
#  10. this machine: the current user name, the real name in the account's GECOS field, the host name and its
#      domain, the top directory of $HOME and the active conda environment, all read at run time (nothing is
#      written into this script); skipped in CI, where they describe the CI runner
# Every file must pass every rule; there are no per-site exemptions. This script is exempt from rules 1-9 only
# because it contains their patterns. A line that must quote a synthetic example (a self-test of a scrubber, say)
# can carry the marker "portability: allow" to be skipped by all rules.

set -uo pipefail
cd "$(dirname "$0")/.."

CHECK_LINKS=0
TARGETS=()
for a in "$@"; do
  case "$a" in
    --links) CHECK_LINKS=1 ;;
    *) TARGETS+=("$a") ;;
  esac
done
if [ ${#TARGETS[@]} -eq 0 ]; then
  for t in skills runlogs scripts .github README.md; do [ -e "$t" ] && TARGETS+=("$t"); done
fi

SELF='(^|/)scripts/check_portability\.sh:'
status=0
report() { # $1 rule, $2 grep pattern, $3 extra grep args (string), $4 path/line filter (grep -v -E) or ""
  local rule="$1" pattern="$2" extra="$3" exclude="$4" hits
  local base="-rnIE"; case " $extra " in *" -P "*) base="-rnI"; ;; esac
  # shellcheck disable=SC2086
  hits=$(grep $base $extra -- "$pattern" "${TARGETS[@]}" 2>/dev/null \
    | grep -v -E '(^|/)(\.git|__pycache__)/' | grep -v 'portability: allow' || true)
  if [ -n "$exclude" ]; then hits=$(printf '%s\n' "$hits" | grep -v -E "$exclude" || true); fi
  if [ -n "$hits" ]; then
    echo "FAIL [$rule]"; printf '%s\n' "$hits" | sed 's/^/    /'; status=1
  else
    echo "ok   [$rule]"
  fi
}

# 1. local paths ($HATS_PUBLISH_* variables, /path/to/ and .../home/<user> placeholders are fine)
report "local paths" \
  '((^|[^A-Za-z0-9_])(/home/|/Users/|~/[a-z]|\$HOME/[a-z]|/scratch/|/lscratch/|/gpfs/|/lustre/|/pscratch/|/global/|/var/www/|/nfs/|/mnt/|/data[0-9]*/)(?!<)|(^|[\]\[\s"'"'"'(=:,`-])/(?!(?:dev|usr|bin|tmp|proc|etc|sys|path/to)/)[A-Za-z0-9_.~-]+/[A-Za-z0-9_.~-])' \
  '-P' "$SELF"

# 2. people
report "people" \
  '([A-Za-z0-9._-]+@[A-Za-z0-9.-]+\.(edu|org|com|io|gov|net)|(^|[^A-Za-z])I (am|was|did|think|want|have)|\bmy (machine|laptop|account|home)\b)' \
  '' "$SELF|[A-Za-z0-9._-]+@example\.(org|com)"

# 3. environments (activation with a <placeholder> or $variable is fine)
report "environment names" \
  '((conda|mamba|micromamba) activate [A-Za-z][A-Za-z0-9._-]*|source [^ ]*/bin/activate)' \
  '' "$SELF|activate (<|\"?\\$)"

# 4. private sources
report "private sources" \
  '(\b(slack|notion) (thread|channel|message|link|dm|workspace)s?\b|\b(on|in|from) (slack|notion)\b|\bgranola\b|docs\.google\.com|drive\.google\.com|notion\.so|slack\.com|\bmemory file\b|\bmemories\b|LSDB_Info|knowledge base export|internal (chat|channel)|meeting (notes|transcript))' \
  '-i' "$SELF"

# 5. non-public URLs and internal hosts
report "non-https or internal URLs" \
  '(http://(?!localhost)|https?://[^ )"]*\.(local|internal|lan)\b|localhost:[0-9]+)' \
  '-P' "$SELF|startswith\("

# 6. dataset names in code and templates (reference docs may cite public catalogs when citing a public issue);
#    names with a release number, with or without a space, underscore or hyphen
report "dataset names in templates/scripts" \
  '\b(gaia[ _-]?e?dr[0-9]|erass[0-9:]|erosita|ztf[ _-]?dr|legacy[ _-]?surveys?|dp[0-9]([ _]|\b)|pan-?starrs|tic[ _-]?v[0-9]|hipparcos|allwise|neowise|catwise|unwise|2mass|sdss[ _-]?dr|des[ _-]?dr|euclid[ _-]?q)' \
  '-i' "$SELF|(^|/)(reference/|runlogs/|README\.md|EXAMPLE\.md)|mysurvey"

# 7. process residue
report "process residue" \
  '\b(earlier draft|rejected (draft|version)|as of 20[0-9]{2}-|the (fork|explore) (agent|pass)|subagent|this session|(sync|standup|co-?working|planning) meeting)\b' \
  '-i' "$SELF"

# 8. institution hostnames
report "institution hostnames" \
  '\b([a-z0-9-]+\.)+(edu|gov|ac\.[a-z]{2})\b' \
  '-i' "$SELF"

# 9. batch-system literals (site accounts, partitions and QOS names belong in an uncommitted site file)
report "batch account/partition/qos" \
  '--(account|partition|qos)=[^<$ "'"'"']' \
  '' "$SELF"

# 10. this machine: names read at run time, never written into the repository
if [ -n "${CI:-}${GITHUB_ACTIONS:-}" ]; then
  echo "skip [this machine] (CI runner)"
else
  words=(); names=()
  for u in "$(id -un 2>/dev/null)" "${USER:-}"; do   # user names: also inside handles (e.g. <prefix><user>)
    if [ ${#u} -ge 5 ]; then words+=("$u"); elif [ ${#u} -eq 4 ]; then words+=("\\b$u\\b"); fi
  done
  gecos=$(getent passwd "$(id -un 2>/dev/null)" 2>/dev/null | cut -d: -f5 | cut -d, -f1)
  for w in $gecos; do [[ "$w" =~ ^[A-Z][a-z]{2,}$ ]] && names+=("\\b$w\\b"); done   # real name: case-sensitive
  host=$(hostname -s 2>/dev/null || hostname 2>/dev/null)
  if [[ "$host" =~ ^([A-Za-z]{3,})-?[0-9]+$ ]]; then words+=("\\b${BASH_REMATCH[1]}-?[0-9]+\\b")   # node naming scheme
  elif [ ${#host} -ge 6 ]; then words+=("\\b$host\\b"); fi
  domain=$(hostname -d 2>/dev/null || true)
  [[ "$domain" == *.* ]] && words+=("${domain//./\\.}")
  top=$(cut -d/ -f2 <<< "${HOME:-}")
  case "$top" in ""|home|Users|root) ;; *) words+=("(^|[^A-Za-z0-9_])/$top/") ;; esac
  env_name="${CONDA_DEFAULT_ENV:-}"
  case "$env_name" in ""|base) ;; *) [ ${#env_name} -ge 4 ] && words+=("${env_name//./\\.}") ;; esac
  join() { printf '%s\n' "$@" | awk 'NF && !seen[$0]++' | paste -sd'|'; }
  w=$(join ${words[@]+"${words[@]}"}); n=$(join ${names[@]+"${names[@]}"})
  if [ -n "$w" ]; then report "this machine (user, host, domain, home, environment)" "($w)" '-i' ''; fi
  if [ -n "$n" ]; then report "this machine (real name)" "($n)" '' ''; fi
  [ -z "$w$n" ] && echo "ok   [this machine] (nothing to look for)"
fi

if [ "$CHECK_LINKS" -eq 1 ]; then
  echo "checking links ..."
  # placeholders (example.org/.com, angle-bracket templates) and this repository's own URL are not checked;
  # an f-string or template URL is cut at its first { (the part after it is filled at run time)
  urls=$(grep -rhoIE 'https://[^ )>"`'"'"']+' "${TARGETS[@]}" | sed 's/[{}].*$//; s/[.,;:]*$//' \
    | grep -vE 'example\.(org|com)|<[^>]*>|<|github\.com/astronomy-commons/lsdb-agent-skills' | grep -E '^https://[^/]+\.[^/]+' | sort -u)
  bad=0
  while IFS= read -r u; do
    [ -z "$u" ] && continue
    code=000
    for try in 1 2 3; do
      code=$(curl -sL -o /dev/null -w '%{http_code}' --max-time 30 -A "lsdb-agent-skills-link-check (curl)" "$u" || echo 000)
      [ "$code" = "200" ] && break
      [ "$try" -lt 3 ] && sleep 5
    done
    if [ "$code" != "200" ]; then echo "    $code $u"; bad=1; fi
  done <<< "$urls"
  if [ "$bad" -eq 1 ]; then echo "FAIL [links]"; status=1; else echo "ok   [links] $(printf '%s\n' "$urls" | grep -c .) URLs"; fi
fi

[ "$status" -eq 0 ] && echo "portability check passed" || echo "portability check FAILED"
exit $status
