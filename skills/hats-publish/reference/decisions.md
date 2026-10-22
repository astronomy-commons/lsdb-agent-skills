# The decision interview

Every import choice is made by the user, not by a default that happens to be set. Ask these questions before
anything is built, a few at a time and in this order, interactively. Each
question names the recommended default and what the alternatives cost. Record every answer in `catalog.toml`
and in the run log (`run_log.py decision <group> --kv question=... answer=... default=... rationale=...`).

Skip a question only when the answer is already in `catalog.toml`; then show the value and continue.

## 1. Scope

| Question | Recommended default | Consequences |
|---|---|---|
| New import, or a re-import of an existing HATS version? Check the data.lsdb.io `data/` tree and `imports/README.md` first. | re-import if a HATS version exists | A re-import keeps the page `id` and replaces the hosted copy after verification; a new import gets a new `id`, page and registry entry. |
| Which release or version of the survey? Which files exactly (all, a subset, a sky region)? | the complete latest release | Partial imports must say so in `obs_description` and in the page notes. |
| Does the table carry its own coordinates? If not, which reference catalog and id column give them? | the table's own `ra`/`dec`; otherwise `[source.positions_from]` with the survey's main object catalog and its id index | A HATS row needs a position. Positions copied from a reference catalog inherit its epoch and errors; ids without a match are dropped (counted) or stop the run. The reference must be a local collection with an index table on the id. |
| Where are the files coming from (URL list, archive bulk download, files already on disk)? | download into the raw directory, keep checksums | Files already on disk are used as given; record where they came from in the run log. |

## 2. Route and resources

| Question | Recommended default | Consequences |
|---|---|---|
| hats-import pipeline or the small in-memory route? | hats-import unless the table is under about a million rows and a gigabyte in memory | The small route is single-process, builds one margin and no index tables, and lsdb documents it for small tables only. With lsdb 0.11 the small route's write step costs minutes per 100 k rows on wide tables (one frame copy per pixel in the histogram), so hats-import is the faster choice well below the documented row limit. |
| Where does the import run (shared interactive node, batch job, laptop)? | a batch allocation for anything over a few GB of input | Batch queues can mean hours of waiting; a capped run on a shared node (`HATS_PUBLISH_LOCAL_CAPS=1` with `HATS_PUBLISH_MAX_CPUS` and `HATS_PUBLISH_MAX_MEM_GB`, or `[dask].max_cpus`, low priority) starts at once but runs slower and must never exceed its caps. |
| Dask size: workers, threads per worker, memory per worker? | workers = min(input files, available cores / 2), one thread each, memory per worker at least about five times a partition's on-disk size (12 GB for 2 M-row, 150-column partitions) and at least 1.5× the largest FITS input (one worker holds a whole file; the import's input-size pass prints the sizes and refuses to start otherwise) | One Dask task is created per input file: a single large file is read by one worker. Many small files need the indexed readers. Too little memory per worker does not fail fast: Dask pauses workers near the limit and the margin stage hangs. |
| Temporary directories: shards (`tmp_dir`), worker spill (`dask_tmp`), resume logs (`resume_tmp`)? | shards on a large shared disk, spill on local disk, resume logs on durable disk | Shards reach 1–25 % of the output size at peak; spill on a network disk is slow; resume logs on scratch may vanish. |

## 3. Partitioning

| Question | Recommended default | Consequences |
|---|---|---|
| Target file size, hence `pixel_threshold`? | 200–800 MB per file; take `estimate_threshold.py`'s suggestion | Smaller files mean more Dask tasks and metadata; larger files mean more memory per task and slower random access. |
| Highest HEALPix order allowed? | 10 for dense catalogs, lower for sparse | Too low: the Binning stage fails for the densest region. Too high: tiny partitions in sparse areas (drop_empty_siblings limits this). |
| Constant order instead of adaptive? | adaptive | Constant order suits sparse catalogs that will be joined to dense ones; it ignores the threshold. |
| Row-group strategy? | `subtile_order_delta = 1` (up to four groups per file by sub-pixel) | `num_rows` groups help id lookups when sorted by id; a single group is simplest for Dask. |
| Sort column within a partition? | the survey id when lookups by id matter, else spatial order only | Sorting by id makes per-object access cheaper with small row groups. |

## 4. Schema

| Question | Recommended default | Consequences |
|---|---|---|
| Columns to keep, drop or rename? | keep all, rename nothing | Dropped columns are gone for every user; renames break the survey's documentation unless noted on the page. |
| Type casts? | none, except float64 → float32 for measurements when the source over-specifies; never for coordinates | Casts lose precision silently; record them in the run log and the README. |
| Nested time series: which reference catalog, which list columns, what name? | none (flat catalog); when the product needs light curves, `[nested]` with the per-object list columns of the epoch table and `name = "lightcurve"` | The join is spatial and inner: only objects whose id exists in the reference inside the same partition get a nested row; the rest are dropped and reported. List columns come from ECSV arrays or a nested reference; scalars stay beside the nested column. |
| Multi-dimensional columns (arrays)? | flatten to fixed-length lists | Tensors are harder to query; dropping them loses data. |
| Null versus sentinel values? | keep the source's convention and document it | Mixed conventions (NaN in one column, −999 in another) confuse users and cross-matches. |
| Strip the blank padding of fixed-width FITS strings? | yes for FITS sources (`[reader].strip_strings`) | Readers differ: astropy and hats-import drop trailing blanks, fitsio keeps them, leading blanks survive everywhere. Stripping makes the HATS copy independent of the reader; the verification applies the same rule to both sides. |
| Drop rows without valid coordinates? | yes (`[reader].drop_rows_without_coordinates`), with the count recorded | A spatial catalog cannot hold them and the import fails on them; dropping them silently would hide data loss, so the count is reported and reconciled. |
| Time series: one row per detection, or one row per object with nested detections? | nested when the science use is light curves and an object catalog exists | Nested layouts need the object catalog imported first and a join step; flat layouts are simpler and larger on disk. |

## 5. Catalog metadata

| Question | Recommended default | Consequences |
|---|---|---|
| Default columns (`hats_cols_default`)? | ids, positions, the primary measurements, quality flags; about 10–40 columns | Users who open the catalog without a column list get these; every name must exist in the schema. |
| Survey id column(s)? | the survey's primary key; composite keys allowed | Used for `hats_cols_survey_id` and for the index table. |
| `obs_regime` and `hats_frame`? | from the survey (vocabularies: Radio, Millimeter, Infrared, Optical, UV, EUV, X-ray, Gamma-ray; equatorial, galactic, ecliptic) | Wrong values mislead registry searches. |
| `hats_coordinate_epoch`? | the survey's reference epoch for positions | Needed to apply proper motions correctly. |
| Title, description, citation (`bib_reference`, `bib_reference_url`), acknowledgment, original-data terms (`obs_copyright`), HATS-product terms (`hats_copyright`), progenitor? | from the survey's release page and paper | These appear in the README, the page and the registry; missing citations are the commonest complaint from data providers. |
| `creator_did` (IVOID), `publisher_id`, `hats_creator`? | `<[registry].authority>/<id>` as the registry will assign it | The registry identifier must not change once published. |
| `t_min`, `t_max` (MJD)? | from the survey's observing period | Used by registry time-coverage searches. |

## 6. Spatial products

| Question | Recommended default | Consequences |
|---|---|---|
| Margin threshold? | 10 arcsec for optical and infrared, 30 arcsec for X-ray and radio | A margin smaller than the cross-match radii users will try silently drops matches at partition edges. |
| An extra wide margin? | only for astrometric reference catalogs (for example 300 arcsec) | A wide margin can hold a large fraction of the catalog (300 arcsec on Gaia DR3: 52 % of the rows); state the expected size before building it. |
| Index tables? | one on the survey id | Enables single-object lookups without a full scan. |
| Thumbnail sample size? | 10 random rows per partition, at most 10 000 rows | The thumbnail is the user's first look at the data; it must represent its diversity. |

## 7. Names

| Question | Recommended default | Consequences |
|---|---|---|
| Collection, catalog, margin and index directory names; public URL name? | `<survey>_<release>` for the collection and catalog; margins and indexes named by hats-import (`<catalog>_<N>arcs`, `<catalog>_<column>`); public name = collection name | Names become `obs_collection`, URLs and page links; renaming later breaks users' code. |

## 8. Hosting and registration

| Question | Recommended default | Consequences |
|---|---|---|
| Hosting targets? | the site defaults (`[hosting].targets`) | The first target is the primary (`hats_status` main); the others are mirrors. |
| Page group, name, description, badges, notes? | group = survey family, name = release, badges = host region and protocol | The group and name form the page URL; `id` is permanent. |
| Registry entry? | yes, generated from the page after it is merged | Without it the catalog is invisible to VO discovery. |

## 9. Replacement policy (re-imports)

| Question | Recommended default | Consequences |
|---|---|---|
| Keep the previous version visible until the new one is verified and linked? | yes | Swapping before verification can expose a broken catalog; keeping both doubles disk use until retirement. |
| When is the old copy removed? | after the page and registry point to the new copy and one check period has passed | Removing it earlier breaks users with cached paths. |
