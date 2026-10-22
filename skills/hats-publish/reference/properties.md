# Catalog properties and names

The HATS note (https://www.ivoa.net/documents/Notes/HATS/) defines the `hats.properties` keys; hats-import
writes the structural ones and takes the descriptive ones from `[properties]` in `catalog.toml`
(https://hats-import.readthedocs.io/en/stable/catalogs/properties.html). hats writes two files with the same
content, `hats.properties` and `properties`; any edit after the import must go to both.

## Set these from the survey's release page and paper

| Key | What | Source |
|---|---|---|
| `obs_title` | one-line data set title | release page |
| `obs_description` | a paragraph on content and selection | release page |
| `obs_regime` | one of Radio, Millimeter, Infrared, Optical, UV, EUV, X-ray, Gamma-ray | survey |
| `hats_frame` | equatorial, galactic or ecliptic | the coordinate columns used |
| `hats_coordinate_epoch` | epoch of the positions, e.g. J2016.0 | data model |
| `bib_reference`, `bib_reference_url` | the paper to cite | release page |
| `obs_ack` | acknowledgment text the survey asks for | release page |
| `obs_copyright`, `obs_copyright_url` | terms of the original data | release page |
| `hats_copyright` | terms of this HATS product | the publishing team |
| `prov_progenitor` | where the input files came from | the download |
| `creator_did` | IVOID of this HATS, the same identifier the registry assigns | `<[registry].authority>/<id>` |
| `publisher_id`, `hats_creator` | who publishes and who built it | the publishing team |
| `t_min`, `t_max` | observing period in MJD | survey |
| `data_ucd` | UCD of the data if one applies | survey |

## Set these from the import decisions

| Key | What | Rule |
|---|---|---|
| `hats_cols_default` | space-delimited default columns | every name must exist in the schema (hats-import#574, hats#691); nested columns as `frame.subcol` |
| `hats_cols_survey_id` | the survey's primary key, composite keys allowed | from `[columns].id` |

## Written by the tools, never edited

`hats_builder`, `hats_creation_date`, `hats_estsize`, `hats_nrows`, `hats_order`, `hats_max_rows`,
`moc_sky_fraction`, `hats_col_healpix`, `hats_col_healpix_order`, `hats_skymap_order`, `hats_skymap_alt_orders`,
and for margins and indexes `hats_margin_threshold`, `hats_index_column`, `hats_primary_table_url`.

`hats_version` is the HATS note version and `hats_release_date` is written by hats as the note's publication date
(hats v0.11.0, `src/hats/catalog/dataset/table_properties.py`). The note's own wording, "Last HATS update date",
is ambiguous; the skill leaves both keys as written and records data-release information in the `obs_*`,
`bib_reference` and `t_min`/`t_max` keys.

## Per-copy keys set at publish time

`hats_status` (`public main clonableOnce` for the primary copy, `public mirror clonableOnce` for mirrors) and
`hats_service_url` (the URL of that table's own directory on that copy) are written into both property files of
every table on each hosted copy by the publish scripts. Margin and index tables carry `hats_primary_table_url`; hats-import writes the build path
there, so the publish scripts rewrite it to the relative path inside the collection.

## Names

| Thing | Convention |
|---|---|
| collection directory and `obs_collection` | `<survey>_<release>`; lowercase letters, digits, `_` (the tools also accept `.` and `-`) |
| main catalog directory | usually the same as the collection |
| margins | `<catalog>_<N>arcs` below 60 arcsec, `_<N>arcmin` from 60, `_<N>deg` from 3600, `_<N>msec` below 1 (integer-truncated), assigned by hats-import |
| index tables | `<catalog>_<column>`, assigned by hats-import |
| public URL name | the collection name |
| site page `id` | lowercase letters, digits, underscores; permanent once published |

The data.lsdb.io page must point at the collection root, not at the bare catalog (astronomy-commons/data.lsdb.io#9),
so that users get the margin and the index automatically.
