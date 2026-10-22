# Verification

Three layers, all of which must pass before anything is hosted. Any failure stops the run; the fix is to
understand the cause, never to loosen the check.

## 1. Structural (`verify_structure.py`)

Runs hats-import's own verification pipeline
(https://hats-import.readthedocs.io/en/stable/guide/verification.html) on the collection:

- the catalog is valid (`hats.io.validation.is_valid_catalog(strict=True)`, and the collection as a whole);
- the set of files listed in `_metadata` equals the files on disk;
- row counts agree between the file footers, `_metadata`, `hats_nrows` and the truth count
  (`[source].expected_total_rows`);
- every file's schema equals the truth schema (`_common_metadata`, or a schema file you pass).

Then a HATS-note compliance check (required files and required properties per table type, and, as this skill's
policy beyond the note's recommendations, a thumbnail, `partition_info.csv` and `skymap.fits`; both property files
identical, `hats_col_healpix` present and first, thumbnail present with the partition schema, VOParquet metadata
consistent across partitions, margin and index back-pointers resolving, default columns existing, down-sampled
skymaps present) and the Parquet encoding sweep: every column chunk of every data file uses the configured codec,
the offset and column indexes are present when `write_page_index` is on, encodings and row-group counts are
recorded, and the hats version in `hats_builder` matches the installed one. The compression level cannot be read from Parquet metadata, so it is
taken from `[writer]` and recorded as provenance; `--spot-check` re-encodes one row group at the configured level
and compares the size.

Run it again on every hosted copy (hats-import recommends verification after copying); the publish scripts do so
where Python is available and fall back to a checksum manifest otherwise.

## 2. Value-level (`build_verify_notebook.py` + `run_verify.sh`)

A notebook that compares the HATS collection with the source files themselves, using the same loader as the
small import route so the comparison is against exactly what was imported:

| Section | Check |
|---|---|
| provenance | source checksums match the ones recorded at download |
| schema | column names and order equal the source; types equal the expected Arrow types (FITS `TFORM` map, Parquet schema, or the CSV dtype map); the only extra column is the spatial index |
| rows | source rows = `hats_nrows` = `_metadata` = computed; id sets identical (multiset on spatial index when ids are not unique) |
| values | every column exact after aligning on id: integers and strings equal, floats bit-equal with NaN matching NaN; strings compared after stripping FITS padding |
| spatial index | recomputed from the position columns named in the properties and equal to the stored column; every row inside its partition pixel |
| margin | row count, membership (every margin row exists in the main partition that owns it), values equal to the main rows, rows outside their own pixel, completeness: every pair within the threshold found by a brute-force all-pairs search is also found by the lsdb self-crossmatch (the margin may contain more rows, never fewer), and pairs that cross a partition boundary are found only with the margin |
| metadata | partition list consistent (files, `partition_info.csv`, pixel tree); per-partition statistics equal the data; order-8 skymap counts equal the histogram of source positions; the MOC contains every source |
| defaults | opening without a column list yields `hats_cols_default` |
| copies | data and metadata files checksum-identical between the build and the hosted copy; property files compared apart from the per-copy keys (`hats_status`, `hats_service_url`, `hats_primary_table_url`); the public URL serves the same properties and the same cone-search result |
| cell errors | any notebook cell that raised is recorded as a failed check, so a crash cannot end in a passing verdict |

Every check ends PASS, FAIL or SKIP. SKIP carries a reason (nothing to compare: fewer than 50 brute-force pairs,
no recorded checksums, no matching statistics) and never counts as a pass; a FAIL stops the run.

Above `[verify].all_pairs_max_rows` the all-pairs margin test runs on a random sample and reports it. In sample mode the
brute-force pairs are sample × sample and the lsdb side crossmatches the sampled rows against the whole sampled
partitions with their margin, with a neighbour cap of at least 64 (rechecked when reached): a cap taken from the
sparse sample would truncate pairs in the denser full partitions and report false misses.
Regions built from different inputs (north/south halves, survey fields with different bands or companion files)
are where an import goes wrong, and a random sample over the whole sky can miss a few hundred bad partitions: run
the sampled notebook once per region (a copy of `catalog.toml` whose `[source].files` lists that region's
files first: the sampled loader reads the first files), and for any rule the data must obey everywhere (a column
present wherever its band exists, a flag set by a fixed rule) scan every partition and the margin with a short
script rather than a sample.
The executed notebook is kept with the run; its progress bars are stored as text so it renders anywhere.

## Comparing with an existing version (`compare_versions.py`)

For a re-import, or to validate the skill's output against a published copy of the same data: schema
differences (columns only on one side, type changes, with numeric narrowing flagged separately), partitioning
and writer settings on both sides, row and id sets in the sky region covered by the new catalog, and per-column
value equality on the shared rows (exact for integers, strings and booleans; for floats also the largest absolute
and relative difference, so a float64 → float32 narrowing shows as a bounded relative difference). The old side can
be far larger than the new one: only the overlapping partitions are read, with rows selected by position inside
the compared pixels on both sides, so a coarser previous version compares cleanly. The verdict is "identical",
"identical up to type narrowing" (only narrowings), or "DIFFERENT" with the reasons (one-sided columns, widened or
changed types, row or value differences).

## 3. Review checklist (a person)

- Missing values: one convention per column (null or NaN, not a mix of sentinels).
- Identifier naming consistent with the survey and with the team's other catalogs.
- Skymaps present and plausible; the density map on the page shows the expected footprint.
- Default columns sufficient for the common use (for time series, the light-curve column is included).
- README and page text name the release, the citation and the acknowledgment.
- `hats_cols_default`, `obs_regime`, `hats_frame` and the epoch are right.
- For a re-import: the new version has at least the rows and columns of the old one, or the difference is explained.

## Running it safely

- A smoke run must use the production configuration: every non-default option of the full run (temporary-directory
  layout, readers, writer settings, margins) is exercised on the small catalog first.
- Steps that hold data in one Python process (the notebook, the comparison) need a bounded memory plan: sample mode
  reads only the partitions that hold the sampled rows; on a shared node run `memwatch.sh <GB> <pidfile>` next to the
  step so a runaway process is killed before it harms other users.
  Sample mode is requested with `run_verify.sh --sample-rows N` (the job script does this above 20 M catalog rows or 8 GB of data files;
  the builder applies the same default on its own, and `--sample-rows 0` forces the full comparison). `run_verify.sh`
  always rebuilds the notebook from its arguments, so flags given to a separate `build_verify_notebook.py` call are lost.
  For FITS sources the sampled loader reads row ranges with `fitsio` when it is installed (`pip install fitsio`);
  astropy reads a whole binary table into memory even when memory-mapped, so without fitsio a sampled run of a
  20 GB FITS file still needs 20 GB in the kernel.
  fitsio keeps the blank padding of fixed-width strings that astropy and hats-import strip, so set
  `[reader].strip_strings = true` (recommended for FITS anyway) to keep the source and HATS sides comparable.
- `compare_versions.py` holds both sides in memory with every common column: it takes partitions in order
  until `--max-rows` (default 5 000 000) is reached; the job script also runs it under the watchdog on a shared node.
- Section 9b (nested products): nested rows + unmatched rows = flat rows; list lengths agree with the count column; a
  sample of objects has arrays identical to the reference catalog's (looked up with `id_search`).
- Check a running step on the node that started it (`run_host.txt` in the work directory); process listings are
  per node.

## Reporting

`verify_structure.py` writes `verify_results.csv` (`--results-file` for a second copy, used for the mirror re-check)
and `hats_import_verifier_results.csv`; the notebook prints a PASS/FAIL/SKIP table and asserts at the end; both
append to the run log with `status = fail` on any failure. `compare_versions.py` exits 0 when the versions are
identical, 3 when they differ only by numeric narrowing, and 1 when columns, types, rows or values differ. A failure is reported with the failing check, the offending files or columns, and the
counts, never summarised as "mostly fine".
