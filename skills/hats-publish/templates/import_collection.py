#!/usr/bin/env python
"""import_collection.py — build a HATS collection (catalog + margin(s) + index tables) with hats-import, driven
entirely by catalog.toml.

Usage:
    import_collection.py [--config catalog.toml] --dry-run           # print the resolved hats-import arguments
    import_collection.py [--config catalog.toml] --trial-file FILE   # import ONE input file into <build>/trial_<catalog>
                                                                     # (no margins/indexes; [source].expected_files and expected_total_rows are not checked)
    import_collection.py [--config catalog.toml]                      # full import, resumable
    import_collection.py [--config catalog.toml] --client ADDRESS     # use an existing Dask scheduler

Shared-node caps (HATS_PUBLISH_MAX_CPUS, HATS_PUBLISH_MAX_MEM_GB, [dask].max_cpus) pin this process and reduce the
local Dask cluster before the input-size pass (see _common.dask_resources); they cannot resize a cluster given with
--client. Route "small" is refused here: use import_small.py.

Run the full import detached (tmux, nohup or setsid) with the environment's python; the pipeline resumes from its
own log files if interrupted (see reference/import.md). Everything the pipeline needs is in catalog.toml: readers,
partitioning, properties, writer settings, Dask size and temporary directories.
"""
from __future__ import annotations

import argparse
import json
import shutil
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _common import (  # noqa: E402
    apply_dask_resources, check_versions, first_listed_file, input_files, input_size_report,
    load_config, pin_cpus, properties_dict, read_properties, reimport_files, reimport_source, row_group_kwargs, runlog,
    validate_properties, writer_kwargs,
)


def derive_spec(cfg) -> str:
    """[reader].derive as an absolute 'file.py:function' (a relative file is taken from the work directory)."""
    spec = cfg["reader"].get("derive") or ""
    if not spec:
        return ""
    path, _, func = spec.rpartition(":")
    return f"{cfg.path(path)}:{func}"


def work_path(cfg, value) -> str:
    """A path from catalog.toml, relative to the work directory unless absolute; "" stays ""."""
    if not value:
        return ""
    return str(cfg.path(value))


def file_reader(cfg, files=None, coordinate_filter=True, chunksize=None):
    """A hats-import reader built from [reader] for [source].format (coordinate_filter=False before positions exist).

    Wrapper order (the verification loader applies the same steps in the same order): base reader (keep-list,
    companions, derive, pack_nested) -> ECSV arrays -> [columns] rules -> strip_strings -> conform_schema ->
    coordinate filter. For the indexed formats `files` are the index files; only the first data file they list is
    looked at (gzip and ECSV-header detection)."""
    from hats_import.catalog.file_readers import IndexedParquetReader, ParquetPyarrowReader, get_file_reader

    r, fmt = cfg["reader"], cfg["source"]["format"]
    kwargs = {"chunksize": int(chunksize or r.get("chunksize", 500_000))}
    companions = (cfg["source"].get("companions") or {}).get("files") or []
    if (companions or r.get("derive") or (r.get("pack_nested") or {}).get("name")) and fmt != "fits":
        raise SystemExit("[source.companions], [reader].derive and [reader].pack_nested are implemented for FITS sources only")
    reader = None
    ecsv_arrays = {}
    if fmt == "fits" and (companions or r.get("derive") or (r.get("pack_nested") or {}).get("name")):
        from readers import CompanionFitsReader

        reader = CompanionFitsReader(companions, (cfg["source"].get("companions") or {}).get("key_columns") or [],
                                     chunksize=kwargs["chunksize"], hdu=r.get("hdu", 1),
                                     flatten_tensors=bool(r.get("flatten_tensors", True)),
                                     column_names=r.get("column_names") or None, skip_column_names=r.get("skip_column_names") or None,
                                     derive=derive_spec(cfg), pack_nested=r.get("pack_nested") or None)
    elif fmt == "fits":
        kwargs.update(hdu=r.get("hdu", 1), flatten_tensors=bool(r.get("flatten_tensors", True)),
                      column_names=r.get("column_names") or None, skip_column_names=r.get("skip_column_names") or None)
    elif fmt in ("csv", "tsv", "indexed_csv"):
        from readers import check_csv_names, csv_na_defaults

        csv_kwargs = csv_na_defaults(r.get("csv_kwargs") or {})   # empty field and NaN spellings only; "NA" is a value
        if fmt == "tsv":
            csv_kwargs.setdefault("sep", "\t"); fmt = "csv"
        data_files = list(files or [])
        if fmt == "indexed_csv" and data_files:   # the index files list the real inputs: the first one stands for all
            first = first_listed_file(data_files[0])
            data_files = [first] if first else []
        if "compression" not in csv_kwargs and data_files and all(str(f).endswith(".gz") for f in data_files):
            csv_kwargs["compression"] = "gzip"   # the reader gets an open handle, so pandas cannot infer it from the name
        type_map = dict(r.get("type_map") or {})
        if r.get("ecsv_header") and data_files:   # scalar types and array columns from the ECSV header of the first file
            from readers import ecsv_pandas_types

            header_types, ecsv_arrays = ecsv_pandas_types(data_files[0])
            type_map = header_types | {c: "str" for c in ecsv_arrays} | type_map
            csv_kwargs.setdefault("comment", "#")
        schema_file = work_path(cfg, r.get("schema_file")) or None
        if schema_file:
            import pyarrow.parquet as pq

            schema_names = pq.read_schema(schema_file).names
        else:
            schema_names = None
        problems = check_csv_names(r.get("column_names"), r.get("type_map"), schema_names,
                                   drop=cfg["columns"].get("drop"), rename=cfg["columns"].get("rename"))
        if problems:
            raise SystemExit("[reader] CSV columns: " + "; ".join(problems))
        kwargs.update(schema_file=schema_file, column_names=r.get("column_names") or None,
                      type_map=type_map or None, **csv_kwargs)
    elif fmt in ("parquet", "indexed_parquet"):
        # built directly: get_file_reader takes column_names as its own parameter and does not pass it on
        cls = ParquetPyarrowReader if fmt == "parquet" else IndexedParquetReader
        reader = cls(chunksize=kwargs["chunksize"], column_names=r.get("column_names") or None)
    elif fmt == "ecsv":
        pass
    else:
        raise SystemExit(f"unsupported [source].format {fmt!r}")
    if reader is None:
        reader = get_file_reader(fmt, **kwargs)
    if fmt in ("csv", "tsv", "indexed_csv") and ecsv_arrays:
        from readers import EcsvArrayReader

        reader = EcsvArrayReader(reader, ecsv_arrays)
    c = cfg["columns"]
    if c.get("drop") or c.get("rename") or c.get("cast"):   # same rules as the verification loader
        from readers import ColumnRulesReader

        reader = ColumnRulesReader(reader, drop=c.get("drop"), rename=c.get("rename"), cast=c.get("cast"))
    if r.get("strip_strings"):
        from readers import StringStripReader

        reader = StringStripReader(reader)
    if r.get("conform_schema"):   # every chunk gets the target schema (any format), after the column rules
        from readers import ConformSchemaReader

        reader = ConformSchemaReader(reader, work_path(cfg, r["conform_schema"]))
    if r.get("drop_rows_without_coordinates") and coordinate_filter:
        from readers import CoordinateFilterReader

        reader = CoordinateFilterReader(reader, cfg["columns"]["ra"], cfg["columns"]["dec"])
    return reader


def sort_columns(cfg) -> list[str]:
    """[columns].sort as a list: a comma-separated string ("a, b") or a TOML list."""
    sort = cfg["columns"].get("sort") or []
    return [c.strip() for c in sort.split(",") if c.strip()] if isinstance(sort, str) else [str(c) for c in sort]


def catalog_kwargs(cfg, files: list[str]) -> dict:
    p, c = cfg["partitioning"], cfg["columns"]
    kw = {
        "input_file_list": files,
        "file_reader": file_reader(cfg, files),
        "ra_column": c["ra"],
        "dec_column": c["dec"],
        "sort_columns": ",".join(sort_columns(cfg)) or None,
        "highest_healpix_order": int(p.get("highest_order", 10)),
        "lowest_healpix_order": int(p.get("lowest_order", 0)),
        "constant_healpix_order": int(p.get("constant_order", -1)),
        "drop_empty_siblings": bool(p.get("drop_empty_siblings", True)),
        "expected_total_rows": int(cfg["source"].get("expected_total_rows") or 0),
        "use_schema_file": work_path(cfg, cfg["reader"].get("schema_file")) or None,
    }
    if int(p.get("byte_pixel_threshold") or 0) > 0:
        kw["byte_pixel_threshold"] = int(p["byte_pixel_threshold"])
    else:
        threshold = int(p.get("pixel_threshold") or 0)
        if threshold <= 0:
            raise SystemExit("[partitioning].pixel_threshold is 0: run estimate_threshold.py --write first")
        kw["pixel_threshold"] = threshold
    return kw


def reimport_columns(cfg, cat, source_schema) -> list[str] | None:
    """The columns the reimport route reads and writes: the source schema without the hipscat-era partition columns
    (Norder, Dir, Npix) and without [columns].drop, the spatial index (_healpix_29) first and the rest in source
    order (the HATS note recommends the spatial index as the first column; lsdb-written sources have it elsewhere).
    A dropped column that a configured or carried column list needs is an error."""
    from hats.io.paths import HIVE_COLUMNS
    from hats.pixel_math.spatial_index import SPATIAL_INDEX_COLUMN

    c = cfg["columns"]
    if c.get("rename") or c.get("cast"):
        raise SystemExit("[columns].rename and [columns].cast are not applied on route \"reimport\" (rows are copied from the "
                         "source Parquet files with their types): empty them, or re-import from the raw files")
    drop = [str(d) for d in (c.get("drop") or [])]
    if source_schema is None:
        if drop:
            raise SystemExit("[columns].drop on route \"reimport\": the source has no dataset/_common_metadata to filter")
        return None
    ids = c.get("id") or []
    needed = {"[columns].id": [ids] if isinstance(ids, str) else list(ids),
              "[columns].default": [d.split(".")[0] for d in (c.get("default") or [])],
              "[columns].sort": sort_columns(cfg), "[index].columns": list(cfg["index"].get("columns") or []),
              "spatial index, ra, dec": [SPATIAL_INDEX_COLUMN, cat.catalog_info.ra_column, cat.catalog_info.dec_column]}
    clash = {k: [x for x in v if x in drop] for k, v in needed.items()}
    clash = {k: v for k, v in clash.items() if v}
    if clash:
        raise SystemExit(f"[columns].drop removes columns that are still needed: {clash}")
    src = source_schema.names
    unknown = [d for d in drop if d not in src]
    if unknown:
        print(f"note: [columns].drop names columns the source does not have: {unknown}")
    names = [n for n in src if n not in HIVE_COLUMNS and n not in drop]
    # the same names verify_structure.py fails a build for: a pandas index leaked into the files, or the pre-HATS
    # spatial index. They carry no catalog data, but they are dropped only on an explicit decision ([columns].drop).
    stray = [n for n in names if n == "_hipscat_index" or n.startswith("__index_level")]
    if stray:
        raise SystemExit(f"the source carries stray columns that verify_structure.py would fail: {stray}; check what they "
                         "hold (a leaked pandas index is usually a row number) and add them to [columns].drop")
    if SPATIAL_INDEX_COLUMN in names:
        names = [SPATIAL_INDEX_COLUMN] + [n for n in names if n != SPATIAL_INDEX_COLUMN]
    return names


def reimport_source_schema(cat):
    """(path of the source's dataset/_common_metadata, its Arrow schema); without that file, hats' in-memory schema."""
    import hats
    from hats.io import file_io

    schema_path = str(hats.io.paths.get_common_metadata_pointer(cat.catalog_base_dir))
    try:
        source_schema = file_io.read_parquet_metadata(schema_path).schema.to_arrow_schema()
    except FileNotFoundError:
        source_schema = cat.schema
    return schema_path, source_schema


def reimport_read_columns(cfg, cat) -> list[str] | None:
    """The column list the reimport route's reader reads (reimport_columns on the source schema); None = all."""
    return reimport_columns(cfg, cat, reimport_source_schema(cat)[1])


def reimport_schema_file(cfg, source_path: str, source_schema, names) -> str:
    """The use_schema_file for the reimport route. hats-import's reduce step reads each pixel with this schema
    (pyarrow read_table(schema=...)): a column in the schema but not in the rows comes back all-null, and the
    schema's column order is the written order. So when the source _common_metadata has columns the reader no longer
    reads (Norder/Dir/Npix, [columns].drop), a different order, or pandas metadata (stale: it names the source's
    columns and index, e.g. __index_level_0__), a filtered copy is written to <work_dir>/reimport_schema.parquet
    (an empty table with that schema) and used instead of the source's file."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    if source_schema is None or names is None:
        return source_path
    metadata = {k: v for k, v in (source_schema.metadata or {}).items() if k != b"pandas"}
    target = pa.schema([source_schema.field(n) for n in names], metadata=metadata or None)
    if target.names == source_schema.names and (target.metadata or {}) == (source_schema.metadata or {}):
        return source_path
    out = cfg.work_dir / "reimport_schema.parquet"
    pq.write_table(target.empty_table(), out)
    removed = [n for n in source_schema.names if n not in names]
    kept_order = [n for n in source_schema.names if n in names]
    print(f"reimport schema: {out} ({len(names)} columns; removed {removed}"
          + ("; _healpix_29 moved first" if kept_order != names else "")
          + ("; pandas metadata dropped" if b"pandas" in (source_schema.metadata or {}) else "") + ")")
    return str(out)


def reimport_kwargs(cfg, cat, files: list[str]) -> dict:
    """The arguments ImportArguments.reimport_from_hats builds (hats-import guide/reimporting), as keywords for
    CollectionArguments.catalog(), with [partitioning], [columns].drop, the writer and the run's properties applied.
    Rows are read back from the existing Parquet files; the existing _healpix_29 is reused, not recomputed.

    [columns] on this route: ra / dec are informational (the source's hats_col_ra / hats_col_dec are used);
    `drop` applies (removed from the reader's column list and from the schema file, together with the hipscat-era
    Norder/Dir/Npix); `id` and `default` are written to the properties and must survive `drop`; `sort` is not applied
    (hats-import refuses sort columns together with use_healpix_29) but must survive `drop`; `rename` and `cast` are
    refused (the rows keep the source's names and types). Column order: _healpix_29 first, the rest in source order."""
    from hats_import.catalog.file_readers.parquet import ParquetPyarrowReader

    p = cfg["partitioning"]
    schema_path, source_schema = reimport_source_schema(cat)
    names = reimport_columns(cfg, cat, source_schema)
    kw = {
        "catalog_type": cat.catalog_info.catalog_type,
        "ra_column": cat.catalog_info.ra_column,
        "dec_column": cat.catalog_info.dec_column,
        "input_file_list": files,
        "file_reader": ParquetPyarrowReader(column_names=names),
        "use_healpix_29": True,
        "add_healpix_29": False,
        "use_schema_file": reimport_schema_file(cfg, schema_path, source_schema, names),
        "expected_total_rows": int(cfg["source"].get("expected_total_rows") or cat.catalog_info.total_rows),
        # no sort_columns: hats-import refuses them with use_healpix_29; rows keep the source's order within each
        # _healpix_29 value (the source was written sorted by its own sort columns)
        "highest_healpix_order": int(p.get("highest_order", 10)),
        "lowest_healpix_order": int(p.get("lowest_order", 0)),
        "constant_healpix_order": int(p.get("constant_order", -1)),
        "drop_empty_siblings": bool(p.get("drop_empty_siblings", True)),
    }
    if int(p.get("byte_pixel_threshold") or 0) > 0:
        kw["byte_pixel_threshold"] = int(p["byte_pixel_threshold"])
    else:
        kw["pixel_threshold"] = int(p.get("pixel_threshold") or cat.catalog_info.hats_max_rows or 0) or 1_000_000
    props = source_properties(cat) | properties_dict(cfg)
    if names is not None:   # carried column lists must not name a column this route no longer writes
        gone = {k: [c for c in str(props.get(k) or "").split() if c.split(".")[0] not in names]
                for k in ("hats_cols_default", "hats_cols_sort", "hats_cols_survey_id")}
        gone = {k: v for k, v in gone.items() if v}
        if gone:
            raise SystemExit(f"properties name columns the re-imported catalog will not have: {gone}; set them in "
                             "[columns].default / [columns].id or [properties] (or keep the columns)")
    kw["addl_hats_properties"] = props
    return kw


# keys hats-import writes itself for the new catalog (structure, counts, provenance) or that describe one hosted copy
# (the publish scripts set them per copy); every other key of the source's properties is carried over
REWRITTEN_KEYS = {
    "obs_collection", "dataproduct_type", "hats_nrows", "hats_col_ra", "hats_col_dec", "hats_primary_table_url",
    "hats_margin_threshold", "hats_col_assn_primary", "hats_col_assn_primary_assn", "hats_assn_join_table_url",
    "hats_col_assn_join", "hats_col_assn_join_assn", "hats_assn_max_separation", "hats_assn_leaf_files",
    "hats_index_column", "hats_index_extra_column", "hats_skymap_order", "hats_skymap_alt_orders", "hats_max_rows",
    "hats_max_bytes", "hats_estsize", "moc_sky_fraction", "hats_order", "hats_builder", "hats_creation_date",
    "hats_release_date", "hats_version", "hats_status", "hats_service_url",
}
# hats-import does not declare these when it reuses _healpix_29 (reimport_from_hats carries the last two itself)
CARRIED_KEYS = ("hats_col_healpix", "hats_col_healpix_order", "hats_cols_sort", "hats_cols_default", "hats_npix_suffix")


def source_properties(cat) -> dict:
    """The source catalog's properties for the re-imported catalog: both property files (hats.properties wins),
    without the keys hats-import rewrites. [properties] keys that are non-empty in catalog.toml override them, so
    a value not restated in the TOML (obs_*, bib_*, copyright, default columns) keeps the source's value."""
    base = Path(str(cat.catalog_base_dir))
    src: dict = {}
    for name in ("properties", "hats.properties"):
        if (base / name).exists():
            src |= {k.strip(): v.strip() for k, v in read_properties(base / name).items()}
    out = {k: v for k, v in src.items() if k not in REWRITTEN_KEYS and v != ""}
    missing = [k for k in CARRIED_KEYS if k not in out]
    if missing:   # keys hats keeps in the catalog object rather than the files
        info = cat.catalog_info
        extra = {"hats_cols_default": " ".join(info.default_columns or []), "hats_npix_suffix": info.npix_suffix,
                 # pre-0.6 builds do not declare the spatial index; use_healpix_29 guarantees this column
                 "hats_col_healpix": "_healpix_29", "hats_col_healpix_order": "29"}
        out |= {k: extra[k] for k in missing if extra.get(k)}
    return out


def collection_args(cfg, files: list[str], output_path: Path, name: str, catalog_name: str, trial: bool = False):
    from hats_import.collection.arguments import CollectionArguments

    d = cfg["dask"]
    tmp = d.get("tmp_dir") or str(cfg.build_root / "tmp")
    args = CollectionArguments(
        output_path=str(output_path),
        output_artifact_name=name,
        addl_hats_properties=properties_dict(cfg),
        write_table_kwargs=writer_kwargs(cfg),
        row_group_kwargs=row_group_kwargs(cfg),
        skymap_alt_orders=list(cfg["partitioning"].get("skymap_alt_orders") or []) or None,
        create_thumbnail=False,          # write_ancillary.py writes a representative thumbnail with the writer settings
        create_per_partition_stats=True,
        create_metadata=bool(cfg["writer"].get("create_metadata", True)),   # false when the combined footer would pass 4 GiB
        tmp_dir=tmp,
        dask_tmp=d.get("dask_tmp") or None,
        resume_tmp=d.get("resume_tmp") or None,
        resume=True,
        dask_n_workers=int(d.get("n_workers", 1)),   # the capped values when apply_dask_resources ran
        dask_threads_per_worker=int(d.get("threads_per_worker", 1)),
        simple_progress_bar=True,
    )
    kwargs = (reimport_kwargs(cfg, reimport_source(cfg), files) if cfg["run"].get("route") == "reimport"
              else catalog_kwargs(cfg, files))
    if trial:   # one trial file never holds the archive's row total; hats-import would stop after Binning
        kwargs["expected_total_rows"] = 0
    args = args.catalog(output_artifact_name=catalog_name, **kwargs)
    if not trial:
        margins = cfg["margins"]
        thresholds = [margins.get("default_arcsec", 10)] + list(margins.get("extra_arcsec") or [])
        thresholds = [int(t) if float(t).is_integer() else float(t) for t in thresholds if float(t) > 0]
        for i, threshold in enumerate(thresholds):   # default_arcsec = 0: no default margin (first extra, if any)
            args = args.add_margin(margin_threshold=threshold, is_default=(i == 0))
        for column in cfg["index"].get("columns") or []:
            args = args.add_index(indexing_column=column)
    return args


def describe(args) -> dict:
    cat = args.get_catalog_args() if hasattr(args, "get_catalog_args") else args.catalog_args
    out = {"output": str(args.catalog_path), "catalog": {}}
    if cat is not None:
        for k in ("ra_column", "dec_column", "sort_columns", "pixel_threshold", "byte_pixel_threshold", "highest_healpix_order",
                  "lowest_healpix_order", "constant_healpix_order", "drop_empty_siblings", "expected_total_rows", "use_schema_file"):
            out["catalog"][k] = getattr(cat, k, None)
        out["catalog"]["file_reader"] = type(cat.file_reader).__name__
        reader_columns = getattr(cat.file_reader, "column_names", None)
        if reader_columns:
            out["catalog"]["reader_columns"] = list(reader_columns)
        out["catalog"]["n_input_files"] = len(cat.input_paths)
    out["margins"] = [m.get("output_artifact_name") or m for m in args.margin_kwargs]
    out["margin_paths"] = list(args.margin_paths)
    out["default_margin"] = args.default_margin_name
    out["indexes"] = dict(args.index_paths)
    out["properties"] = args.addl_hats_properties
    if cat is not None and getattr(cat, "addl_hats_properties", None):
        out["catalog_properties"] = cat.addl_hats_properties   # the main catalog's own (reimport: the carried source keys)
    out["write_table_kwargs"] = args.write_table_kwargs
    out["row_group_kwargs"] = args.row_group_kwargs
    out["dask"] = {"n_workers": args.dask_n_workers, "threads_per_worker": args.dask_threads_per_worker,
                   "tmp_dir": str(args.tmp_dir), "dask_tmp": str(args.dask_tmp)}
    return out


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--config", default="catalog.toml")
    ap.add_argument("--dry-run", action="store_true", help="resolve and print the arguments, do not import")
    ap.add_argument("--trial-file", help="import this single input file into <build>/trial_<catalog> (no margins/indexes; the expected file count and row total are not checked)")
    ap.add_argument("--client", help="address of an existing Dask scheduler (otherwise a local client is created)")
    ap.add_argument("--no-pypi", action="store_true", help="skip the newer-version lookup")
    ap.add_argument("--allow-oversized-inputs", action="store_true",
                    help="start even when the input-size pass says one worker cannot hold the largest input file")
    a = ap.parse_args(argv)

    cfg = load_config(a.config)
    if cfg["run"].get("route") == "small":
        raise SystemExit("[run].route = \"small\": use import_small.py (lsdb.from_dataframe); import_collection.py "
                         "runs the \"hats-import\" and \"reimport\" routes")
    versions = check_versions(cfg, warn_newer=not a.no_pypi)
    problems = validate_properties(cfg)
    if problems:
        raise SystemExit("fix [properties] first: " + "; ".join(problems))
    cpus = pin_cpus(cfg)
    if a.client:
        print("note: --client: the Dask cluster at that address is used as it is; the shared-node caps do not resize it")
    else:
        apply_dask_resources(cfg, cpus)   # caps first: the input-size pass below checks the capped memory_limit

    if a.trial_file:
        files = [a.trial_file]
        name, catalog_name = f"trial_{cfg['names']['collection']}", f"trial_{cfg['names']['catalog']}"
        output_path = cfg.build_root
        if (output_path / name).exists():
            shutil.rmtree(output_path / name)
    elif cfg["run"].get("route") == "reimport":   # repartition an existing HATS catalog: its partition files are the input
        files = reimport_files(reimport_source(cfg))
        name, catalog_name, output_path = cfg["names"]["collection"], cfg["names"]["catalog"], cfg.build_root
    else:
        files = [str(p) for p in input_files(cfg)]
        name, catalog_name, output_path = cfg["names"]["collection"], cfg["names"]["catalog"], cfg.build_root
    output_path.mkdir(parents=True, exist_ok=True)

    # the input-size pass: what one worker must hold, before anything starts; a re-import's inputs are partition
    # files and a trial's input is one file, not the archive's files, so [source].expected_files does not apply
    inputs = input_size_report(cfg, files, check_expected_files=not a.trial_file and cfg["run"].get("route") != "reimport")
    args = collection_args(cfg, files, output_path, name, catalog_name, trial=bool(a.trial_file))
    summary = describe(args)
    summary["inputs"] = inputs
    print(json.dumps(summary, indent=2, default=str))
    if a.dry_run:
        return 0
    if not inputs["ok"]:
        if a.allow_oversized_inputs:
            print("WARNING (--allow-oversized-inputs): " + inputs["advice"])
        else:
            raise SystemExit("input-size pass failed: " + inputs["advice"] + " (or pass --allow-oversized-inputs)")

    from hats_import.pipeline import pipeline_with_client

    phase = "trial-import" if a.trial_file else "import"
    runlog(cfg, "start", phase, f"{len(files)} input file(s)", inputs=inputs,
           **{k: v for k, v in summary["catalog"].items() if k not in ("file_reader", "reader_columns")})
    t0 = time.time()
    try:
        from dask.distributed import Client

        if a.client:
            with Client(a.client) as client:
                pipeline_with_client(args, client)
        else:
            d = cfg["dask"]   # already capped by apply_dask_resources
            with Client(n_workers=int(d["n_workers"]), threads_per_worker=int(d["threads_per_worker"]),
                        memory_limit=d["memory_limit"], local_directory=d.get("dask_tmp") or None) as client:
                pipeline_with_client(args, client)
    except Exception as exc:  # noqa: BLE001 - the failure is recorded in the run log before re-raising
        runlog(cfg, "end", phase, f"FAILED: {type(exc).__name__}: {exc}", status="fail", elapsed_s=round(time.time() - t0, 1))
        raise
    elapsed = round(time.time() - t0, 1)
    runlog(cfg, "end", phase, "pipeline finished", status="ok", elapsed_s=elapsed, output=str(args.catalog_path),
           versions=versions)
    print(f"done in {elapsed} s -> {args.catalog_path}")
    if not a.trial_file:
        print("next: write_ancillary.py (thumbnail, previews, README) then verify_structure.py")
        return 0
    # trial run: every configured column name must exist in what the reader produced (hats-import validates only
    # hats_cols_default; a wrong [columns].id would otherwise end up in hats_cols_survey_id)
    import pyarrow.parquet as pq
    names = pq.read_schema(Path(args.catalog_path) / catalog_name / "dataset" / "_common_metadata").names
    cols = cfg["columns"]
    wanted = {"[columns].id": cols.get("id") or [], "[columns].sort": sort_columns(cfg),
              "[columns].default": [d.split(".")[0] for d in (cols.get("default") or [])], "[index].columns": cfg["index"].get("columns") or []}
    bad = {k: [c for c in ([v] if isinstance(v, str) else v) if c not in names] for k, v in wanted.items()}
    bad = {k: v for k, v in bad.items() if v}
    if bad:
        print(f"TRIAL CHECK FAILED: configured columns not in the imported schema: {bad}")
        return 3
    print(f"trial check: id, sort, default and index columns all exist in the {len(names)}-column schema; "
          "inspect dtypes, nulls versus sentinels and array columns before the full run")
    return 0


if __name__ == "__main__":
    sys.exit(main())
