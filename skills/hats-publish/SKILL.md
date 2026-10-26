---
name: hats-publish
description: Import an external astronomical catalog into HATS, verify it against the source, document it, host it and register it on data.lsdb.io, following the LSDB team's conventions. Use when the user says "create a HATS collection from X", "import X into HATS", "re-import X", "put X on data.lsdb.io", "make X available like <an existing catalog>", or "verify the HATS copy of X against the source". NOT for Rubin Butler products (they have their own pipeline) and NOT for catalogs already in HATS that only need a page or a mirror (use reference/publishing.md directly).
---

# /hats-publish

Turns a survey's catalog release into a HATS collection (catalog + margin + index tables) that is verified
against the source files, documented, hosted and registered. The procedure encodes the hats-import and lsdb
documentation, the Rubin import pipeline (https://github.com/lincc-frameworks/rubin-dash) and the team's
conventions recorded in public issues and archived import notebooks; every
site-specific value comes from the site file described in `CONFIG.md`, every reference is a public URL, and nothing
here names a machine, a person or a dataset.

One run = one `catalog.toml` (copied from `templates/catalog.toml` into a work directory). Every template reads
it, relative paths in it resolve against the directory holding it, and a run is reproducible from that file alone. Keep the work directory: it holds the config, the logs, the
executed verification notebook and the run log.

Pin the templates at the start of every run: `T=$(bash <skill>/templates/pin_templates.sh)` in the work directory
copies them read-only to `templates.pinned/` with the git commit, and every later step (`job.sbatch` and
`resubmit_loop.sh` included) runs `$T/...`. A git pull or an edit of the skill then cannot change a running import, and
the README names the commit that built the catalog. Change the code of a run only with `pin_templates.sh --repin`.

## When to use

A new survey release to publish; a re-import of an existing HATS catalog with better partitioning, writer
settings or metadata; a request to verify that a HATS copy matches its source.

## Inputs (ask only if unset)

Source files or URLs and their format; survey, release, data-model page, paper, acknowledgment and licence;
whether a HATS version already exists (look in the data.lsdb.io `data/` tree and `imports/README.md`); the
coordinate and id columns. Everything else is decided in the interview below.

## Procedure

Phases in order. **Bold** steps stop and wait for the user. Each phase starts and ends with a run-log entry
(`run_log.py start|end <phase>`); the templates do this themselves where they run.

0. **Environment.** Create or update an environment with the newest released `hats`, `lsdb`, `hats-import`
   (`pip install -U hats lsdb hats-import nested-pandas pyarrow astropy fitsio jupyter pytz tzdata`; hats' summary
   writer needs `pytz` for time-zone-aware timestamp columns), print the versions, skim the release notes for format
   changes. Point `HATS_PUBLISH_SITE_ENV` at your site file (`CONFIG.md`). The templates that use the
   libraries refuse to run below the minimum versions and warn when a newer release exists.
1. **Decision interview** (`reference/decisions.md`): the nine groups, a few questions at a time, each with its
   recommended default and consequences. Write the answers into `catalog.toml` and the run log
   (`run_log.py decision`). Then show the plan: route, reader, resources, partitioning, writer settings, names,
   targets.
2. Acquire. Download into the raw directory, verify checksums and archive integrity (`gzip -t`), record the sha256
   of every file in `[source].sha256`, and save the data-model page next to the data. Count the files against the
   archive's listing (`[source].expected_files`) and list exact file names, never a bare glob over a directory that
   may hold re-download copies. For thousands of small files, write index files for the indexed readers. A
   download left running in the background writes its pid to `download.pid` in the work directory so that
   `status.sh` can report it.
2a. Tables without coordinates (`[source.positions_from]`): `attach_positions.py` looks the positions up by id in a
   reference HATS collection (its index table) and writes the source as Parquet with `ra`/`dec`; the import then reads
   those files. ECSV-headed CSV with array columns (`[reader].ecsv_header = true`) is parsed into list columns on the way.
2b. One table split into row-parallel FITS files (`[source.companions]`, e.g. a main file plus extra/photo-z/light-curve
   files with the same rows): the import reads them side by side with fitsio row ranges, checks the key columns chunk
   by chunk, and can add computed columns (`[reader].derive`, a function in the work directory) and pack list columns
   into one nested column (`[reader].pack_nested`). The verification loader reads the same files independently.
3. Plan partitioning. `estimate_threshold.py --write` samples the input with the run's writer settings and
   converts the 200–800 MB file-size rule into `pixel_threshold`, and predicts the partition count and the size of
   `dataset/_metadata` (warns near the 4 GiB footer limit); check `highest_order` against the densest region.
4. Trial. `import_collection.py --trial-file <one file>` into `<build_root>/trial_<collection>/trial_<catalog>` (the
   script prints the path); inspect dtypes, nulls versus sentinels, column names, array columns, rows without
   coordinates (`[reader].drop_rows_without_coordinates` drops and counts them). Verify the trial with
   `verify_structure.py --trial --collection <trial collection>` and, for a re-import, compare it with the old version
   with `compare_versions.py --trial`; the gaps a trial is expected to have (thumbnail, per-partition statistics,
   margins, indexes) are reported as WARN, the data checks keep their full strength. Adjust `[reader]`, `[columns]`
   and `[properties]`; **show the user what changed**.
   Then `choose_encoding.py` on a trial partition (or `--file` on one or two real partitions of at least ~100 000
   rows; re-imports default to `[run].reimport_from`). It writes the partition with dictionary and with
   BYTE_STREAM_SPLIT, keeps per float column the smaller encoding, checks the round trip, and recommends 100 000-row
   row groups (BYTE_STREAM_SPLIT-dominated) or 250 000 (dictionary-dominated). Show the user the summary (largest
   columns, sizes against the hats default), then `--write` sets `[writer].use_dictionary` and `row_group_size`.
   Never ship the raw default unchecked: BYTE_STREAM_SPLIT inflates low-cardinality floats 2–6×, and catalogs from
   archive CSV exports get 1.3–2.5× larger with it.
5. Import. `import_collection.py` detached (`nohup`/`setsid`), or `job.sbatch` on a batch node (it also runs
   phases 5–6 and the comparison), resumable; it builds the catalog, the
   margin(s) and the index tables with the `[properties]` and `[writer]` settings. Small tables (under about 1 M rows
   and 1 GB in memory; above about 100 k rows on wide tables hats-import is faster) use `import_small.py` instead. Then `write_ancillary.py`: data thumbnail (random per-partition sample),
   per-partition statistics, PNG previews, README and `index.html` with a provenance section.
   Progress: `status.sh` (from the work directory) prints the download, the SLURM job, the current hats-import stage
   with its progress bar, and the latest run-log entry. Downloads and imports run for hours: as soon as one runs in
   the background (phase 2 or 5), start updates every 5 minutes without asking. Each update runs `status.sh` and
   reports only what changed. Say that they are running and that the user can change the interval or stop them.
   Stop them when the job finishes or fails, and check the job's real state when it leaves the queue. On a preemptable queue start `resubmit_loop.sh <job id>` next
   to the job: it records the job's resources in `resubmit_opts.txt`, resubmits preempted or system-cancelled jobs
   with the same resources (hats-import resumes; a job preempted after the import finished, marked by `import.done`,
   is resubmitted with the import skipped) and stops on anything else.
5a. Nested time-series product (`[nested]`, optional): `attach_nested.py` joins per-object list columns from a reference
   HATS by id (spatial inner join), packs them into one nested column with `nest_lists` and writes a second collection
   next to the flat one; the notebook's section 9b reconciles rows, list lengths and a sample of arrays.
6. Verify, any failure stops the run (`reference/verification.md`):
   `verify_structure.py --spot-check` (hats-import's checks, HATS-note compliance, encoding sweep) →
   `run_verify.sh` (the value-level notebook against the source; checks end PASS, FAIL or SKIP with a reason) →
   the review checklist with the user.
7. Document and archive. Add `catalog.toml`, the scripts used and the executed notebook to the data.lsdb.io
   `imports/` directory with a row at the top of its README (part of the site pull request).
8. **Host** (`reference/publishing.md`). Run `publish_web.sh --check` and `publish_mirror.sh --check`, show the
   previews (what would be copied, and what the destination already holds), and only after the user's approval run
   them with `--yes` (without one of the two flags they stop; a destination that holds files the build lacks stops
   them before writing unless `--replace` is given, which deletes the stale files): copy, group, checksum manifest,
   per-table `hats_status` and `hats_service_url` (with `[hosting].version` the copy goes to
   `<collection>_<version>` and the stable `<collection>` path becomes a symlink to it, switched only after the
   checks), symlink on the web host, public open test with
   `lsdb.open_catalog` on the collection root (a failure reverts the symlink), then the autoindex description; the
   mirror copy is re-verified into `verify_results_mirror.csv`.
9. **Register.** `make_site_entry.py` writes the page entry and the density map on a branch in a data.lsdb.io clone
   (merging into an existing entry) and stages it; **ask for the commit message**; the pull request is the user's.
   After it is merged and deployed, `registry_entry.py` runs the registry generator on a branch in a registry clone
   for the user's pull request; it requires a clean site clone at `origin/main`.
10. Close out. `NOTES.md` from the template; open items (mirrors, registry, deployment, retirement of a previous
    version); `run_log.py feedback` with the user for what was unclear, manual or wrong in the defaults;
    `run_log.py render --scrub` and offer a pull request adding the log to `runlogs/` in this repository.

Re-imports: `route = "reimport"` (`ImportArguments.reimport_from_hats` semantics) for new thresholds, writer
settings or dropped columns, rebuild margins and indexes, write to a new directory, verify, run
`compare_versions.py --new <new> --old <previous> --key <id>` (schema, rows and values of the shared region; type
narrowing is reported separately; `[run].expect_differences` when the new version fixes the old one), refresh every
copy, repoint the symlinks, keep the old copy until the page and registry point to the new one. What the route
carries and rewrites, and how to trial a catalog you hold only remotely or in part: `reference/import.md`,
"Re-importing an existing HATS catalog".

## Templates

| File | Purpose |
|---|---|
| `catalog.toml` | the run configuration (copy and fill) |
| `pin_templates.sh` | freeze the templates for one run in `templates.pinned/` (commit, local diff, checksums); `--repin` to change them |
| `_common.py`, `catalog_source.py`, `readers.py` | shared helpers: config, versions, site variables, writer settings, properties, source loader, coordinate filter |
| `estimate_threshold.py` | file-size rule → `pixel_threshold` |
| `choose_encoding.py` | per-column dictionary vs BYTE_STREAM_SPLIT and row-group size from a trial partition → `[writer]` |
| `import_collection.py` | hats-import collection build (`--dry-run`, `--trial-file`) |
| `import_small.py` | in-memory route for small tables |
| `attach_positions.py` | positions by id from a reference HATS for tables without coordinates; ECSV arrays parsed into list columns |
| `attach_nested.py` | nested time-series product: join list columns by id from a reference HATS, `nest_lists`, write a collection |
| `job.sbatch` | batch route: import, ancillary files, verification and comparison as one SLURM job on a dedicated node |
| `write_ancillary.py` | thumbnail, statistics, previews, README, `index.html` |
| `verify_structure.py` | structural, compliance and encoding checks |
| `build_verify_notebook.py`, `run_verify.sh` | value-level verification notebook |
| `compare_versions.py` | compare a new build with an existing HATS version (re-imports, validation against a published copy) |
| `publish_mirror.sh`, `publish_web.sh` | hosting targets |
| `make_site_entry.py`, `registry_entry.py` | data.lsdb.io page and registry |
| `memwatch.sh` | memory watchdog for long single-process steps on shared nodes |
| `status.sh` | one-screen progress summary (download, job, import stage, run log) for scheduled updates |
| `resubmit_loop.sh` | resubmit a batch job after preemption or a system cancel; one loop per run (heartbeat guard) |
| `run_log.py`, `NOTES.template.md` | run log and notes |

## Gotchas

One line each; details and more in `reference/pitfalls.md`.

- One Dask task per input file: split huge files, use indexed readers for thousands of small ones.
- Rows without coordinates stop the mapping stage: trial-import first, filter and count them.
- Gzipped CSV inputs need `compression="gzip"` (the reader gets an open handle); the import adds it for `.gz` files.
- `expected_total_rows` must match or the pipeline stops after binning; it is a check, not a hint.
- Start from the hats Parquet defaults (hats, hats-import, lsdb >= 0.11.1): ZSTD 15, page index, BYTE_STREAM_SPLIT
  on float columns. BYTE_STREAM_SPLIT inflates low-cardinality floats 2–6×: `choose_encoding.py` sets
  `[writer].use_dictionary` and `row_group_size` from a trial partition. Leave the other `[writer]` keys empty unless
  the user decides otherwise; write any file the skill writes itself with `parquet_write_kwargs`.
- CSV: only an empty field and the NaN spellings are missing values. A names/types side file read with pandas
  defaults turns a column named `NA` into NaN: read it with `keep_default_na=False`.
- `use_schema_file` decides the written columns: a schema column the reader does not produce is written all-null.
- Default columns must exist in the schema; nested columns use `frame.subcol`.
- hats writes `hats.properties` and `properties`; edit both or neither.
- `hats_release_date` is the HATS note's date, not the data's; put data dates in `t_min`/`t_max`.
- A margin may hold extra rows, never fewer: verify by containment.
- Margins and indexes carry the build path in `hats_primary_table_url`; the publish scripts make it relative.
- Open collections at the root; the page must point at the collection root.
- The web `.htaccess` is first-match and prefix-match; the publish script inserts before generic entries and
  writes by copy, not rename (sticky directory).
- Inputs whose columns differ by region lose columns silently in reducing: conform to one schema
  (`[reader].conform_schema`, any format), trial each region.
- `_metadata` cannot exceed 4 GiB: `estimate_threshold.py` predicts it; `create_metadata = false` for huge catalogs.
- Null fixed-size lists break pyarrow < 26 readers: fill with NaN or use variable-length lists.
- Progress bars in headless notebooks become dead widgets; the notebook builder uses text bars.
- Parallel shells share one working directory: absolute paths, pid files instead of `pgrep -f`.

## Output

A verified HATS collection on the hosting targets, a staged data.lsdb.io page, a registry branch, `NOTES.md`,
`verify_results.csv`, the executed verification notebook, and `RUNLOG.md`.
