# Example run: `mysurvey_dr1`

A placeholder walk-through of one import, from an empty directory to a staged page. Replace the names; every
value shown comes from `catalog.toml` or the site variables.

```bash
# 0. environment and site
pip install -U hats lsdb hats-import nested-pandas pyarrow astropy jupyter
SKILL=<path to your clone>/lsdb-agent-skills/skills/hats-publish
export HATS_PUBLISH_SITE_ENV=$SKILL/sites/<site>.local.env   # your copy of sites/example.env
export HATS_PUBLISH_RAW_ROOT=/path/to/large/disk/raw HATS_PUBLISH_BUILD_ROOT=/path/to/large/disk/build

# 1. work directory and configuration
mkdir mysurvey-dr1 && cd mysurvey-dr1
cp $SKILL/templates/catalog.toml .          # work_dir = "." (this directory)
T=$(bash $SKILL/templates/pin_templates.sh)   # read-only copy in ./templates.pinned; every step below runs it
#    ... decision interview: fill [run], [source], [columns], [names], [partitioning], [margins], [index],
#        [properties], [hosting], [site]; record each answer:
python $T/run_log.py decision partitioning --kv question="target file size" answer="200-800 MB" default="200-800 MB" rationale="docs rule"

# 2. acquire
mkdir -p "$HATS_PUBLISH_RAW_ROOT/mysurvey/raw_dr1" && cd "$_"
curl -LO https://archive.example.org/mysurvey/dr1/catalog.fits.gz && gzip -t catalog.fits.gz
sha256sum catalog.fits.gz        # -> [source].sha256 in catalog.toml
curl -L -o data_model.html https://archive.example.org/mysurvey/dr1/datamodel.html
cd -

# 3. partition plan
python $T/estimate_threshold.py --write

# 4. trial on one file, inspect, adjust [reader]/[columns]
python $T/import_collection.py --trial-file "$HATS_PUBLISH_RAW_ROOT/mysurvey/raw_dr1/catalog.fits.gz"

# 5. full import: detached on this machine ...
nohup python $T/import_collection.py > import.log 2>&1 &
#    ... wait for "done"
python $T/write_ancillary.py
#    ... or as a batch job that also runs steps 5-6 (needs HATS_PUBLISH_SBATCH_OPTS from the site file):
#    source "$HATS_PUBLISH_SITE_ENV"      # puts HATS_PUBLISH_SBATCH_OPTS in this shell
#    export HATS_PUBLISH_TEMPLATES=$T     # the pinned copy; job.sbatch and resubmit_loop.sh also find ./templates.pinned themselves
#    sbatch $HATS_PUBLISH_SBATCH_OPTS --cpus-per-task=64 --mem=400G --time=2-00:00:00 $T/job.sbatch
#    setsid nohup bash $T/resubmit_loop.sh <job id> > /dev/null 2>&1 < /dev/null &   # on a preemptable queue

# 6. verify (stop on any FAIL)
python $T/verify_structure.py --spot-check
bash $T/run_verify.sh

# 7. host: preview, then (after the user approves the preview) write
bash $T/publish_web.sh --check
bash $T/publish_mirror.sh --check
#    ... user approves ...
bash $T/publish_web.sh --yes
bash $T/publish_mirror.sh --yes

# 8. page and registry (staged on branches; the commit messages and pull requests are yours)
python $T/make_site_entry.py --repo <clone of data.lsdb.io>
python $T/registry_entry.py --site-repo <clone of data.lsdb.io> --registry-repo <clone of registry.lsdb.io>   # after the page is deployed

# 9. close out
cp $T/NOTES.template.md NOTES.md    # fill in
python $T/run_log.py feedback --text "the trial step should also report the number of empty strings per column"
python $T/run_log.py render --scrub # -> RUNLOG.md, contribute to runlogs/ in this repository
```

What the directory holds afterwards: `catalog.toml`, `import.log`, `verify_results.csv`,
`verify_mysurvey_dr1.ipynb` (executed), `runlog.jsonl`, `RUNLOG.md`, `NOTES.md`.
