# Importing with hats-import

A distilled guide to the choices that matter, with the documentation to read for each. All of it is driven by
`catalog.toml`; `import_collection.py --dry-run` shows the exact hats-import arguments that result.

Primary documentation: https://hats-import.readthedocs.io/en/stable/ (catalog arguments, collections, temp files,
verification) and the public import recipes under `catalogs/public/`.

## Which route

| Table | Route | Why |
|---|---|---|
| under about 1 M rows and 1 GB in memory | `import_small.py` (`lsdb.from_dataframe`) | documented limit of the in-memory loader (https://docs.lsdb.io/en/stable/tutorials/import_catalogs.html); one margin at most, no index tables, `num_rows` row groups only |
| | | note: with lsdb 0.11 the write step is slow for wide tables (minutes per 100 k rows at 300 columns, see pitfalls); hats-import is faster beyond a few tens of thousands of rows × hundreds of columns |
| anything larger, or many files | `import_collection.py` (hats-import) | parallel, resumable, produces margins and index tables in one collection |

## Inputs and readers

`[source].format` selects a hats-import reader (`reference/file_readers.html`):

| format | reader | notes |
|---|---|---|
| `fits` | `FitsReader` | `hdu`, `column_names` (keep-list) or `skip_column_names`, `flatten_tensors` for array columns |
| `csv` | `CsvReader` (pandas) | pass `type_map` (dtypes) or `schema_file` (a Parquet file with the wanted schema); `csv_kwargs` for `sep` etc. |
| `ecsv` | `AstropyEcsvReader` | |
| `parquet` | `ParquetPyarrowReader` | `column_names` keep-list |
| `indexed_csv`, `indexed_parquet` | one *index file* per task listing many small input files | use when there are thousands of small files; make many more index files than Dask workers |

Rules of thumb from the hats-import docs:

- One Dask task per input file. A single huge file is read by one worker; split it or use `chunksize`.
- Run on **one input file first** (`--trial-file`) and inspect dtypes, nulls versus sentinels, column names and
  array columns before the full run.
- `expected_total_rows` makes the pipeline stop after the binning stage if the count differs: it is the number of
  rows that will be in the catalog. Set it from the survey's documentation; when
  `[reader].drop_rows_without_coordinates` is on, the trial run prints the dropped count for its file (once per
  hats-import pass, so twice), and the verification loader reports the count for the whole source.
- Rows without valid coordinates (NaN or out of range) make the mapping stage fail;
  `drop_rows_without_coordinates = true` filters and counts them, and the verification notebook reconciles
  source rows minus dropped rows against the catalog.
- `use_schema_file` keeps column order and types stable across input files with uneven schemas
  (`notebooks/unequal_schema`); `[reader].conform_schema` does the same inside the skill's reader chain for any
  input format (missing columns become null, extra columns are an error) and the verification loader applies it too.

## Tables without coordinates

Some products carry only an object id (Gaia's variability and astrophysical-parameter tables, detection tables keyed
by object). Set `[source.positions_from]` (reference collection with an index on the id, the id column, the reference
`ra`/`dec` names) and run `attach_positions.py` before the import: it collects the ids, fetches the positions once
(chunked `id_search` up to about a million ids, a column-pruned partition scan above that) and writes the source as
Parquet with the positions merged by id. The import and the verification loader then read those files.

## Array columns in ECSV-headed CSV

ESA's bulk Gaia exports are CSV with an ECSV header; array columns are strings like `"[1.0,null,2.0]"` with the element
type in the header (`subtype: 'float64[null]'`). With `[reader].ecsv_header = true` the scalar types come from the header
and the arrays are parsed into Arrow list columns with Arrow kernels only. Do not read these files with astropy's ECSV
reader: it turns the quoted booleans of the flag arrays (`"False"`) into `True`.

## Partitioning

- Keep data files between about 200 and 800 MB and never more than a couple of GB
  (`notebooks/estimate_pixel_threshold`). `estimate_threshold.py` samples the input, writes it with the run's
  writer settings and converts the target size into `pixel_threshold` (rows per partition);
  `byte_pixel_threshold` partitions by bytes instead.
- `highest_order` caps the splitting; if the densest region cannot get under the threshold the Binning stage
  raises and the order must be increased. `lowest_order` and `constant_order` serve sparse catalogs that will be
  joined to dense ones (`catalogs/arguments.html`, "sparse datasets").
- Row groups: `subtile_order_delta = 1` gives up to four groups per file split by sub-pixel (the Rubin pipeline's
  setting, https://github.com/lincc-frameworks/rubin-dash `src/rubin_dash/config.py`); `num_rows` groups together with
  `sort = "<id>"` make lookups by id cheaper.
- `skymap_alt_orders = [2, 4, 6]` writes down-sampled skymaps used by the site and by quick plots.
- Expect about 2.4× more partitions than total rows / threshold: adaptive binning leaves partitions between a quarter
  of the threshold and the threshold. `dataset/_metadata` holds one footer entry per row group of every file and
  cannot exceed 4 GiB; `estimate_threshold.py` predicts its size and says when to set `[writer].create_metadata = false`.
- `point_map.fits` and `skymap.fits` are written at `highest_order` (order 12: 1.6 GB each).

## Resources and temporary files

- Dask: `n_workers` × `threads_per_worker` ≤ the cores you may use; `memory_limit` per worker (`GB`/`G`/`MB`/`M`
  decimal, `GiB`/`MiB` binary, or `auto`) with headroom for one partition plus its shards. On shared nodes set
  `[dask].max_cpus` or `HATS_PUBLISH_MAX_CPUS` (the scripts pin themselves to that many physical cores and keep
  workers × threads within it) and `HATS_PUBLISH_MAX_MEM_GB` (the worker count is reduced first, then the per-worker
  limit; `auto` under a cap means the cap divided by the workers).
- Input-size pass: `import_collection.py` (dry-run, trial and full run) lists the inputs (count, total, largest,
  median) and checks the largest file against `memory_limit`. One Dask task reads one whole file; for FITS the
  reader holds the entire file in the worker (astropy's memmap does not bound the resident size of a binary
  table), so a worker needs ≥ 1.5× the largest FITS input, ECSV ≥ 3×; CSV and Parquet are read in chunks. The
  import refuses to start when the rule is violated: raise `memory_limit`, lower `n_workers` to stay inside the
  budget (fewer, larger workers), or split the file. `--allow-oversized-inputs` overrides the stop.
- Temporary directories (`catalogs/temp_files.html`): shards (`tmp_dir`) on a large shared disk, they reach
  1–25 % of the output size at peak; worker spill (`dask_tmp`) on local disk. hats-import also offers a separate
  `resume_tmp` for the resume logs; in 0.11.0 the margin pipeline writes its markers under `tmp_dir` regardless, so
  the templates keep `resume_tmp` empty. Everything the pipeline needs to restart lives in the resume logs; the
  templates always run with `resume=True` (restart where it stopped; delete the output directory yourself to start over).
- Run the full import detached (`nohup`, `setsid`) or with `job.sbatch` on a batch node (`HATS_PUBLISH_SBATCH_OPTS`
  from the site file carries the account, partition and QOS; on a preemptable QOS that cancels instead of requeueing,
  run `resubmit_loop.sh` next to the job and hats-import resumes from its resume logs);
  use a `main` guard in any custom script.
  hats-import can also send a completion e-mail (`completion_email_address`); the templates do not use it.
- Gzipped CSV: the reader receives an open handle, so pandas cannot infer the compression; the import adds
  `compression="gzip"` for `.gz` inputs (or set it in `[reader].csv_kwargs`).

## Collections, margins, indexes

`CollectionArguments(...).catalog(...).add_margin(margin_threshold=..., is_default=True).add_index(indexing_column=...)`
builds everything into one directory with `collection.properties` (`catalogs/collections.html`). Margins are named by
hats-import from the threshold: `_<N>arcs` below 60 arcsec, `_<N>arcmin` from 60 arcsec, `_<N>deg` from 3600 arcsec,
`_<N>msec` below 1 arcsec (the number is truncated to an integer); exactly one margin is the default.
Index tables are named `<catalog>_<column>`; they are flat Parquet tables, so only the codec and page-index writer
settings apply to them.

## Properties written by hats-import

From the arguments: `obs_collection` (the directory name), `dataproduct_type`, `hats_col_ra`, `hats_col_dec`,
`hats_cols_sort`, `hats_max_rows`, `hats_order`, `moc_sky_fraction`, `hats_nrows`, `hats_col_healpix`,
`hats_col_healpix_order`, `hats_skymap_order`, `hats_skymap_alt_orders`, plus the provenance block (`hats_builder`,
`hats_creation_date`, `hats_estsize`, `hats_release_date`, `hats_version`). Everything in `[properties]` goes in as
`addl_hats_properties` (`catalogs/properties.html`); see `reference/properties.md` for what to set.

## Writer settings

Use the hats defaults. From hats 0.11.1, hats-import and lsdb write every table with
`hats.io.file_io.get_parquet_write_table_kwargs` (hats#742): ZSTD level 15, page index, statistics,
BYTE_STREAM_SPLIT on every float leaf, DELTA_BINARY_PACKED on `_healpix_29` and on an integer leading sort column,
dictionary on the other columns, 128 KiB data pages, 100 000-row row groups. The skill's own writers (thumbnail,
statistics, sample files, materialised inputs) call the same function, so every file of a run has the same encoding.
`[writer]` holds only departures from these defaults (`compression`, `compression_level`, `use_dictionary`,
`column_encoding`) and is empty for a standard run. The compression level is not stored in Parquet metadata, which is
why it is recorded in the README and the run log.

The float encoding matters more than the compression level. hats 0.11.0 and older wrote floats with pyarrow's default
dictionary encoding. Measured on a Legacy Surveys DR11 partition (94 k rows, 233 columns, mostly float): dictionary
and ZSTD 15 gave 155 MB; BYTE_STREAM_SPLIT and ZSTD 15 gave 111 MB, 29 % smaller. Changing the ZSTD level from 15 to
3 or 19 changed either result by less than 4 %. The whole DR11 catalog would be about 3 TiB smaller. The encoding sweep
fails files whose float columns are not BYTE_STREAM_SPLIT.

Cost: on a 4-billion-row catalog ZSTD level 15 was about 96 % of the reducing stage; level 3 was 2.8× faster for
0.7 % more bytes. The published level stays 15 unless the user decides otherwise; record the choice.

## Nested time-series products

`attach_nested.py` joins per-object list columns from a reference HATS (an epoch-photometry or detection catalog
with one row per object) onto the flat catalog by id with lsdb's spatial `join`, packs them into one nested column with
`nest_lists`, and writes a second collection with the run's writer settings. The join is inner and spatial: rows whose
id is absent from the reference, or sits in another partition, are dropped and the count is written to the run log.
The published-catalog pattern is one nested column (`lightcurve`) holding the transit times, fluxes, magnitudes and
flags, with the object's scalars beside it.
The join aligns the two pixel trees and comes out at the finer partitioning of the two (thousands of small files for a
few hundred thousand objects), so the template writes the joined catalog to a temporary `<output>_fine` collection
and re-partitions it with hats-import at `[partitioning].pixel_threshold`, with the margin and index tables, into
`[nested].output`. Choose that threshold by bytes: with light curves a row weighs kilobytes (Gaia DR3 epoch
photometry: about 5.6 kB per source on disk), so `estimate_threshold.py` on the materialised files, not the row count
of a flat catalog, sets the partition size.

## Re-importing

`ImportArguments.reimport_from_hats(existing, output_dir, ...)` rebuilds a catalog from an existing HATS copy with
new thresholds, sorting or row groups (`guide/reimporting.html`). Margins and index tables must be rebuilt
afterwards. `reimport_from_hats` reuses the source's `_healpix_29`, so it takes no `sort_columns`,
and it does not write `hats_col_healpix`, `hats_col_healpix_order` or `hats_cols_sort`; the skill's reimport route
(`route = "reimport"`, `reimport_from`) leaves out sort columns and carries those properties over from the source. Write the new version to a new directory, verify it, repoint the hosted symlinks, and keep the old copy
until the page and registry point to the new one.
