# Pitfalls

Symptom, cause, rule. Entries cite a public source where one exists; the others are general engineering risks of
this kind of pipeline. When one of them bites, open an issue on astronomy-commons/data.lsdb.io so it gets a
citeable home.

## Inputs and import

| Symptom | Cause | Rule |
|---|---|---|
| Import runs with one worker busy and the rest idle | one Dask task per input file (hats-import docs) | split large inputs or use `chunksize`; for thousands of small files use the indexed readers with many more index files than workers |
| Binning stage error about needing to split further | `highest_order` too low for the densest region | raise `[partitioning].highest_order`, or lower the threshold |
| Column order or types differ between partitions | input files with uneven schemas | `[reader].schema_file` / `use_schema_file` (hats-import `notebooks/unequal_schema`) |
| Some partitions silently lack columns that others have; `_metadata` then fails to combine the footers | hats-import's reducing stage reads a pixel's shards with the first shard's schema, so when input chunks differ in columns (a band, or a companion file, present over only part of the sky) the extra columns of later shards are dropped without an error | conform every chunk to one target schema (`[reader].conform_schema`, missing columns become null); trial-import one file from each region and compare the schemas; check afterwards that every partition has the full schema |
| Readers fail with "Expected all lists to be of size=N but index i had size=0" | pyarrow before 26 cannot read null entries of fixed-size list columns | do not write null fixed-size lists: fill them with NaN arrays or cast to variable-length lists; open the thumbnail with the oldest pyarrow your users run |
| Re-import fails on `sort_columns`, or the result lacks `hats_col_healpix`, `hats_col_healpix_order` and `hats_cols_sort` | `reimport_from_hats` reuses `_healpix_29`, which forbids new sort columns, and does not write those three properties | the reimport route leaves out sort columns and carries the three properties over from the source |
| The batch output file shows nothing for hours | Python buffers stdout when it is not a terminal | `PYTHONUNBUFFERED=1` in the job script, or follow `import.log` (`status.sh`) |
| The catalog has more rows than the release, with identical duplicate rows | the raw directory held re-download copies (`name.csv.gz.1`, `.2`) and the file glob imported them too (seen on a published all-sky catalog: one file read three times, about a million extra rows) | the input-size pass refuses names that repeat up to a numeric suffix and a file count that differs from `[source].expected_files`; set `expected_total_rows` from the release and check the id column is unique |
| Pipeline stops after binning with a row-count message | `expected_total_rows` does not match | investigate the input (truncated downloads, duplicated files) before changing the number |
| Corrupted partitions found only at verification | silent write corruption on a shared filesystem under heavy parallel writes | verify each stage before the next; keep checksums; lower parallelism on network filesystems |
| Restart rebuilds everything | `resume = false` deletes the output | leave `resume = true`; delete the output yourself only on purpose |
| Mapping stage fails within seconds: "Worker exceeded 95% memory budget. Restarting" | a single large FITS (or ECSV) input is held whole by one worker; memmap does not help for FITS binary tables | the input-size pass stops the import before it starts; set `[dask].memory_limit` ≥ 1.5× the largest FITS file with fewer workers, or split the file |
| Reducing stage fails with "Unsupported cast from large_string to null" | the schema file (or the catalog it was taken from) has a `null`-typed column, written from an all-empty column by an earlier import; new chunks carry a real type | give the column a real type in the schema file (`string` for an empty text column); the comparison with the old version then reports the deliberate type difference |
| Array columns fail on the small route | the in-memory loader rejects multi-dimensional FITS columns | use hats-import with `flatten_tensors`, or drop the column deliberately |
| The small route takes 20 minutes to write a 100 k-row table | lsdb 0.11 `write_catalog` counts rows per pixel with `groupby(...).apply(len)`, one frame copy per pixel: cost grows with rows × columns | prefer hats-import above about 100 k rows or 100 columns until the writer is fixed; the small route stays correct, only slow |
| Flag arrays read from ESA ECSV files are all `True` | astropy's ECSV reader builds boolean arrays from the quoted strings `"True"`/`"False"`, and every non-empty string is truthy | parse the array columns with the skill's Arrow parser (`[reader].ecsv_header = true`); never astropy for these files |
| The second pass over CSV inputs yields only the id column | hats-import's `CsvReader` stores a `read_columns` request in its own kwargs and reuses it for later reads | build a fresh reader per pass (the templates do) |
| `ArrowInvalid: Data type list<...> is not supported in join` | Arrow hash joins reject list-typed non-key columns | look positions up with `index_in` + `take` (the positions template does); keep joins for scalar tables |
| A table has no `ra`/`dec` | id-keyed product (variability tables, detections) | `[source.positions_from]` + `attach_positions.py`; the table cannot be a spatial catalog on its own |
| String values differ between the HATS copy and the source read with another tool | FITS fixed-width strings are blank-padded; astropy and hats-import strip trailing blanks on read, fitsio keeps them, and leading blanks stay in every reader | `[reader].strip_strings = true` strips both sides the same way; the verification loader applies the same rule |

## Partitioning and writer

| Symptom | Cause | Rule |
|---|---|---|
| Files far outside 200–800 MB | threshold set by guess | `estimate_threshold.py`; `byte_pixel_threshold` when rows vary a lot in width (file sizes can still be uneven) |
| Snappy-compressed or larger-than-expected files after a rewrite | a post-processing script rewrote leaves without the writer settings | write with `parquet_write_kwargs(cfg, table.schema)` (the hats defaults plus `[writer]`); the encoding sweep catches it |
| Catalog about 1.4× larger than another build of the same data; float columns encoded `PLAIN/RLE_DICTIONARY` | built with hats or hats-import 0.11.0 or older, which use pyarrow's default dictionary encoding on floats | use hats, hats-import and lsdb 0.11.1 or newer (the templates refuse older versions); `encoding-columns` in the sweep fails such files |
| No page index on files | the writer defaults differ by tool and version (hats-import#719) | use hats >= 0.11.1 (page index on by default); the sweep checks every file |
| Compression level unknown | Parquet metadata records the codec only | the README and the run log record it (hats default 15 unless `[writer]` overrides); use `--spot-check` |
| Row group too large for some engines | single row group per file | `subtile_order_delta` or `num_rows` row groups (see import.md) |
| Finishing stage fails writing `dataset/_metadata` after the whole import (thrift size limit) | `_metadata` is one Parquet footer that lists every row group of every file; a footer cannot exceed 4 GiB; wide schemas × tens of thousands of partitions × several row groups get there | `estimate_threshold.py` predicts the size; above ~60 % of the limit set `[writer].create_metadata = false` (verification then reads the file footers) or use fewer row groups |
| About 2.4× more partitions than total rows / threshold | adaptive binning leaves partitions between a quarter of the threshold and the threshold | plan file counts, footers and `_metadata` with the factor (`estimate_threshold.py` uses 2.4) |
| `point_map.fits` and `skymap.fits` take gigabytes | they are written at `highest_order` (order 12: 201 M pixels, 1.6 GB each) | keep `highest_order` no higher than the densest region needs; the site uses the `skymap_alt_orders` maps |
| Reducing takes most of the import time | ZSTD level 15 is about 96 % of reducing (measured: level 3 is 2.8× faster for 0.7 % more bytes); the splitting stage spends most of its time writing compressed shards | keep the published level (15) and plan the time for it; a lower level is a decision for the user, recorded in the README |

## Margins and indexes

| Symptom | Cause | Rule |
|---|---|---|
| Cross-match drops matches near partition edges | catalog opened without its margin, or margin smaller than the match radius | open the collection root; choose the margin threshold above the radii users will try (10 arcsec, 30 for X-ray/radio) |
| Margin mapping tasks fail at once with `FileNotFoundError` from `write_marker_file` | hats-import 0.11.0 writes the margin's per-task resume markers under `tmp_dir`, but the margin plan creates the marker directories under `resume_tmp` when the two differ | leave `[dask].resume_tmp` empty (one temporary root) until this is fixed upstream; the catalog stage is not affected |
| Margin stage stalls at a few percent with workers at 75–95 % memory and "Pausing worker" messages | margin mapping holds whole partitions in memory and Dask pauses workers near their limit; paused workers never finish, so the pipeline hangs instead of failing; workers restarted at 95 % lose their resume-marker writes (file-not-found errors) | give each worker at least about five times the on-disk size of a partition (for 2 M-row, 150-column partitions, 12 GB); fewer, larger workers beat many small ones; the finished catalog is reused on restart and the margin resumes |
| Nested product comes out as thousands of tiny files | lsdb's `join` returns the finer of the two pixel trees | `attach_nested.py` re-partitions the joined catalog with hats-import at the configured threshold (byte-budgeted for list columns) |
| Light-curve catalog imported at a row threshold copied from a flat catalog gives 50–130 MB files, or multi-GB ones | rows with list columns weigh kilobytes, not bytes | run `estimate_threshold.py` on the materialised files; it samples files spread over the input |
| Nested product has fewer rows than the flat catalog | lsdb `join` is inner and spatial: ids missing from the reference, or placed in another sky partition, drop out silently | `attach_nested.py` reports the count; the notebook (9b) reconciles flat = nested + unmatched; positions of both catalogs must come from the same source |
| `id_search` with a long id list is slow or fails | each call reads the whole index table; a Python list of millions of ids is not what the API expects | chunk the ids (50 k per call) below about a million; scan the partitions with a column-pruned `isin` filter above that (the positions template switches automatically) |
| Margin stale after a rewrite | the primary was rewritten and the margin was not | rebuild margins and indexes after any change to the primary |
| A pixel or order search on a margin returns fewer rows than the margin holds, so sampled margin checks run on incomplete data | lsdb 0.11 drops margin rows when `pixel_search`/`order_search`/`partitions[]` is applied to a disk-backed margin, directly or through the searched main catalog | read the margin from the unsearched catalog, one partition at a time, for the partitions you need (the verification notebook does); never trust a searched margin's row count |
| Verification fails on `hats_primary_table_url` after copying | hats-import writes the absolute build path | the publish scripts rewrite it to the relative path inside the collection |
| Margin "missing" rows in a strict comparison | the note allows extra rows in a margin, never fewer | test one-way containment, not set equality |

## Properties and names

| Symptom | Cause | Rule |
|---|---|---|
| Opening the collection fails with "unexpected extra property" (`hats_frame`, `hats_cols_default`, ...) | hats before 0.11 accepts only collection keys in `collection.properties` | keep catalog keys in the catalog's properties, or state the minimum hats version in the README |
| Users read the whole wide table by default | `hats_cols_default` not set (hats-import#574, hats#691) | set it; hats-import validates that every name exists, and the small route checks it itself |
| Property edit not visible | hats writes both `hats.properties` and `properties` | edit both; the structural check compares them |
| `hats_cols_survey_id` names a column that does not exist | `[columns].id` was mistyped (hats-import validates `hats_cols_default` only) | the trial import, the structural check (`survey-id-columns-exist`) and the notebook all fail on it; fix `[columns].id` before the full run (after the fact: both property files of the catalog and of every margin, which carry the same properties) |
| Mirror reports itself as the primary | `hats_status` defaults to `public main clonableOnce` | the publish scripts set `main` or `mirror` per copy |
| Confusion about `hats_release_date` | it is written by hats as the HATS note's date, not the data's release date | leave it; put data dates in `t_min`/`t_max` and the description |
| Page points at the bare catalog | wrong `urls.collection` (astronomy-commons/data.lsdb.io#9) | point at the collection root |

## Notebooks and tooling

| Symptom | Cause | Rule |
|---|---|---|
| Executed notebook shows "widget could not be displayed" instead of output | progress bars rendered as widgets under a headless run | the notebook builder patches `tqdm.auto` to the text bar before importing lsdb |
| A background wait loop never ends | `pgrep -f "<pattern>"` matches its own shell | wait on a pid file (`kill -0`) |
| Scripts run in the wrong directory when several run at once | parallel shells share one working directory | absolute paths, or `(cd DIR && ...)` subshells |
| `.htaccess` edit fails with "operation not permitted" | the web directory is sticky; rename is refused | write with `cat tmp > .htaccess` (the publish script does) |
| Autoindex shows the wrong description | Apache uses the first `AddDescription` whose name is a prefix | insert the specific entry before the generic one (the publish script does) |
| A step seems to have vanished, or a finished run still holds memory elsewhere | processes are per login node; the run was started on another node | `run_host.txt` records the node; check and kill there |
| A long Python step quietly grows past the node's memory | no limit applies to a plain Python process (unlike Dask workers) | bounded reads (sample mode) plus `memwatch.sh` on shared nodes |
| The value-level notebook compares every source row although sample mode was requested, and the kernel grows without bound | `run_verify.sh` builds the notebook from its own arguments, so a notebook built beforehand with `--sample-rows` is rebuilt without it | pass `--sample-rows` to `run_verify.sh` (the job script does); the builder defaults to sample mode above 20 M catalog rows unless `--sample-rows 0` is given |
| A memory watchdog kills a step that belongs to another run on the same node | the watchdog matched processes by command name across the whole user | `memwatch.sh` watches only the descendants of the step's pid (pid file) |
| Size comparison between copies differs | `du` counts directory entries differently per filesystem | compare file bytes or checksums |
| A preempted batch job never comes back | on some sites a preemptable QOS cancels the job instead of requeueing it, whatever `--requeue` says; a system or admin cancel shows as `CANCELLED by 0` | run `resubmit_loop.sh` next to the job; hats-import resumes from its done-markers |
| A long run fails or changes behaviour after a `git pull` of the skill | the run executed the live clone: each template is read when its step starts, and bash reads a running script as it goes | pin the templates per run (`pin_templates.sh`); `job.sbatch` and `resubmit_loop.sh` run `./templates.pinned`; change a running import only with `--repin` |
| A watcher decides a running job has ended | `squeue` returned nothing for a moment while `sacct` still said RUNNING | treat RUNNING/PENDING from `sacct` as still running (`resubmit_loop.sh` does) |
| Two resubmit loops run for one import | a loop was started again from another login node or session | `resubmit_loop.sh` refuses to start while `loop.heartbeat` is under 3 minutes old |
| Editing a running shell loop breaks it | bash reads a script from disk while executing it | stop the loop, edit, restart |
| The finishing stage looks stalled for minutes | it reads the footer of every data file (about 10 minutes for 35,000 files on a parallel filesystem) | wait; the import log moves to the margin stage when it is done |
