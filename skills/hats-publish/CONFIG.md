# Configuration

The skill never hard-codes a machine, user or site. Everything site-specific comes from environment variables,
and a *site file* sets team defaults for them. Scripts read the variables; nothing else in the skill does.

## Site file

Export the variables once per shell, or point `HATS_PUBLISH_SITE_ENV` at a file that exports them:

```bash
cp <clone>/skills/hats-publish/sites/example.env <clone>/skills/hats-publish/sites/<site>.local.env   # fill it in
export HATS_PUBLISH_SITE_ENV=<clone>/skills/hats-publish/sites/<site>.local.env
```

`sites/example.env` lists every variable with a placeholder. A filled-in copy names hosts, paths, groups and batch
accounts, so it stays out of this repository: `*.local.env` files are ignored by git and the portability check
refuses hosts and site paths in every committed file. Every template and script loads the file named by
`HATS_PUBLISH_SITE_ENV`, so exporting that one variable is enough. A value already exported in the shell wins over
the file.

## Variables

| Variable | Meaning | Default when unset |
|---|---|---|
| `HATS_PUBLISH_RAW_ROOT` | where downloads go: `<root>/<survey>/raw_<release>` | `$PWD/raw` |
| `HATS_PUBLISH_BUILD_ROOT` | where collections are built before publishing | `$PWD/build` |
| `HATS_PUBLISH_SITE_REPO` | a clone of `astronomy-commons/data.lsdb.io` for the page step | `$PWD/data.lsdb.io` |
| `HATS_PUBLISH_REGISTRY_REPO` | a clone of `astronomy-commons/registry.lsdb.io` for the registry entry (the registry's IVOID authority is `[registry].authority` in `catalog.toml`) | `$PWD/registry.lsdb.io` |
| **Web host** (target key `web`) | | |
| `HATS_PUBLISH_WEB_URL` | public base URL of the hosted catalogs | required for `web` |
| `HATS_PUBLISH_WEB_DATA_HOST` | ssh host that writes the data store | required |
| `HATS_PUBLISH_WEB_DATA_ROOT` | catalog store as seen from that host | required |
| `HATS_PUBLISH_WEB_HOST` | ssh host that serves the web tree | required |
| `HATS_PUBLISH_WEB_DATA_ROOT_ON_WEB` | the same store as seen from the web host | required |
| `HATS_PUBLISH_WEB_ROOT` | web directory holding the per-catalog symlinks and `.htaccess` | required |
| `HATS_PUBLISH_WEB_USER` | ssh user on both hosts | `$USER` |
| `HATS_PUBLISH_WEB_GROUP` | unix group for the copied files | unset = leave as created |
| **Mirror** (target key `mirror`) | | |
| `HATS_PUBLISH_MIRROR_ROOT` | local filesystem root of the mirror store | required for `mirror` |
| `HATS_PUBLISH_MIRROR_URL` | base URL the mirror serves, if any | optional (empty = not served) |
| `HATS_PUBLISH_MIRROR_GROUP` | unix group for the copied files | unset = leave as created |
| **Free space** (`publish_web.sh`, `publish_mirror.sh`) | | |
| `HATS_PUBLISH_MIN_FREE_GB` | reserve that must stay free on the destination filesystem after the copy: a publish needs 1.2 × the build size plus this many GB (10^9 bytes) free; `0` drops the reserve, not the 1.2 × the build | `5000` |
| `HATS_PUBLISH_WAIT_FOR_SPACE_MIN` | with `--yes`, when the build does not fit, poll the free space every 10 minutes for up to this many minutes before stopping; `0` stops at once (`--check` never waits) | `0` |
| **Batch jobs** | | |
| `HATS_PUBLISH_SBATCH_OPTS` | account, partition and QOS options for `job.sbatch` | required to use the batch route |
| `HATS_PUBLISH_TEMPLATES` | templates directory that `job.sbatch` pins from when `./templates.pinned` does not exist yet (`pin_templates.sh`); once it exists, every job runs the pinned copy | the submission directory |
| **Shared-node etiquette** | | |
| `HATS_PUBLISH_MAX_CPUS` | cap for Dask and `taskset` on shared interactive nodes: the process is pinned to this many physical cores and Dask workers × threads stay within it | unset or empty = no cap |
| `HATS_PUBLISH_MAX_MEM_GB` | cap for the total Dask worker memory on shared nodes (the import first reduces the worker count, then the per-worker limit, to fit); also the `memwatch.sh` limit for the notebook and comparison steps of a capped `job.sbatch` run; may be fractional | unset or empty = no cap |
| `HATS_PUBLISH_LOCAL_CAPS` | set to `1` to run `job.sbatch` as a capped local run on a shared node; unset, the job script clears the two caps because a batch node is dedicated | unset |
| `HATS_PUBLISH_MIRROR_URL_KEY` | the `catalog.json` key under which the mirror URL is listed on the site page | `mirror_catalog` |
| **Batch job phases** (`job.sbatch`, `resubmit_loop.sh`) | | |
| `HATS_PUBLISH_SKIP_IMPORT` | set to `1` to run only the phases after the import (collection already built); `resubmit_loop.sh` sets it when a job was preempted after the import finished | unset |
| `HATS_PUBLISH_VERIFY_FULL_MAX_ROWS` | the value-level notebook compares every row only when the catalog has at most this many rows | `20000000` |
| `HATS_PUBLISH_VERIFY_FULL_MAX_GB` | and at most this many GB of data files | `8` |
| `HATS_PUBLISH_VERIFY_SAMPLE_ROWS` | otherwise the notebook samples this many source rows | `500000` |
| `HATS_PUBLISH_COMPARE_PIXELS` | partitions of the new build compared with the previous version (`compare_versions.py --sample-pixels`) | `40` |
| `HATS_PUBLISH_COMPARE_MAX_ROWS` | row budget of that comparison (`--max-rows`) | `5000000` |
| `HATS_PUBLISH_JOB_SCRIPT` | the job script `resubmit_loop.sh` resubmits | `./templates.pinned/job.sbatch` |
| `HATS_PUBLISH_MAX_RESUBMITS` | how many times `resubmit_loop.sh` resubmits before giving up | `10` |
| `HATS_PUBLISH_RUNLOG` | the run-log file the templates append to; the scripts set it to `<work_dir>/runlog.jsonl` themselves | `./runlog.jsonl` |
| `HATS_PUBLISH_SCRUB_PREFIXES` | colon-separated mount prefixes that `run_log.py render --scrub` replaces with `<site-path>` (home directories, the user name, e-mail addresses and the host name are scrubbed without configuration; `run_log.py selftest` checks it) | none |

The batch and status scripts load the site file but keep any variable already present in the environment, even an
empty one; that is how `job.sbatch` clears the two caps on a dedicated node.

Target keys other than `web` and `mirror` (for example a partner object-storage bucket) are added by writing a
`publish_<key>.sh` next to the existing ones and documenting its variables here.

## Access you need

- Web host: an account on both `HATS_PUBLISH_WEB_DATA_HOST` and `HATS_PUBLISH_WEB_HOST`, passwordless ssh from the
  machine running the skill, membership of the group that owns the catalog store and of the group that owns the web
  tree. Test: `ssh $HATS_PUBLISH_WEB_HOST "touch $HATS_PUBLISH_WEB_ROOT/.probe && rm $HATS_PUBLISH_WEB_ROOT/.probe"`.
- Mirror: write access to `HATS_PUBLISH_MIRROR_ROOT` from the machine running the skill.
- Site page: push access to a fork or branch of `astronomy-commons/data.lsdb.io`; deployment of the site is a
  separate step done by a maintainer with web-host access (see `reference/publishing.md`).
- Registry: a pull request to `astronomy-commons/registry.lsdb.io`.

## Software

| Package | Minimum | Why |
|---|---|---|
| `hats-import` | 0.11.1 | collection builder, verification pipeline; writes with the hats Parquet defaults |
| `hats` | 0.11.1 | properties, summary files, pixel math; Parquet writer defaults (BYTE_STREAM_SPLIT floats) |
| `lsdb` | 0.11.1 | small route, value-level verification, open tests; writes with the hats Parquet defaults |
| `pyarrow` | any supported by hats | page index and ZSTD writer options |
| `astropy` | any current | FITS reading, sky separations |
| `fitsio` | any current | row-parallel companion FITS files, derived and packed columns, bounded row-range reads in sampled verification |
| `jupyter`, `nbconvert`, `nbformat` | any current | executing the verification notebook |

Install the newest releases into a fresh environment (`pip install -U hats lsdb hats-import nested-pandas pyarrow pytz tzdata
astropy fitsio jupyter`). Every template checks these minimums and warns when a newer release exists on PyPI.
