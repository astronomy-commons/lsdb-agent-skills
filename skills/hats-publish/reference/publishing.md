# Hosting, the site page and the registry

## Hosting targets

Targets are keys in `[hosting].targets`; the first one is the primary copy (`hats_status` main), the others are
mirrors. Each target has a publish script and its variables are defined in `CONFIG.md`.

| Target | Script | What it does |
|---|---|---|
| `web` (public web host) | `publish_web.sh` | rsync to the data store, set the group, checksum manifest check, per-table properties, symlink into the web tree, public open test (the symlink is reverted on failure), autoindex description |
| `mirror` (institution mirror) | `publish_mirror.sh` | rsync into the mirror tree, set the group, checksum manifest check, per-table properties, re-run `verify_structure.py` on the copy (results in `verify_results_mirror.csv`) |
| partner object storage | hand-off | see "Partner buckets" |

Both scripts take `--check` to report what would change without writing (including what the destination already
holds), and require `--yes` to write; the procedure runs the check, shows it to the user, and runs with `--yes` only
after approval. A destination that holds files the build does not have stops the script before anything is written;
`--replace` deletes those files instead (a rebuilt collection replacing an older copy). A transport failure during
`--check` is an error, not an empty preview. Data files are byte-identical across copies; only the property files
differ, in `hats_status`, `hats_service_url` (each table's own directory URL on that copy) and the relative
`hats_primary_table_url`.

Before the first write, and in `--check`, both scripts compare the free space of the destination filesystem (the
deepest existing directory above the destination; on the data host for `web`) with 1.2 × the size of the build
plus the reserve `HATS_PUBLISH_MIN_FREE_GB` (default 5000 GB). When it does not fit the script stops with nothing
written, or, with `HATS_PUBLISH_WAIT_FOR_SPACE_MIN` set, polls every 10 minutes for up to that many minutes. A
failed free-space probe is an error, never "enough space".

### Versioned layout

With `[hosting].version` set (a short label, for example a date `2026-10`), each script copies into the versioned
directory `<root>/<survey>/<collection>_<version>` and keeps the stable path `<root>/<survey>/<collection>` as a
relative symlink to it. The symlink is created or re-pointed only after the copy, the checksums, the property
stamping and (web) the public open test succeeded, so the stable path never serves a partial copy; it is replaced
atomically (a new symlink renamed over the old one). Users, mirrors and `hats_service_url` address the stable path
(the mirror) or the public URL (the web host), never a versioned directory. On the web host the public link
`<HATS_PUBLISH_WEB_ROOT>/<name>` points at the versioned directory itself, so a failed open test reverts it, and the
stable symlink with it, to the previous version. An empty `version` keeps the legacy layout: the copy is written
at the stable path itself, which on a re-import overwrites the served copy file by file.

`--check` and `--yes` read the stable path (on the data host for `web`, locally for `mirror`) before anything is
written:

| Stable path | What the script does |
|---|---|
| absent | copies, then creates the symlink |
| a symlink | prints its target; copies, then re-points it; the previous target stays on disk and is printed as the copy to retire later |
| a real directory (a legacy unversioned copy) | stops with exit 1 and prints the command that renames it to `<collection>_<label>` (label from its `hats_creation_date`, else its modification date) and leaves a symlink in its place, which keeps it served; run it, then run the script again. The scripts never rename or delete a served copy |
| the versioned directory exists already | a resumed or repeated push of the same version: the existing-destination rules above apply (extras listed, `--replace` deletes them) |

Both scripts also refuse to write when `[run].reimport_from` or the local collection resolves to the destination or
lies inside it, and the legacy layout refuses a stable path that is already a symlink (it would write into the
served version).

### Web host details

The public URL `<HATS_PUBLISH_WEB_URL>/<name>` is a symlink in `<HATS_PUBLISH_WEB_ROOT>` pointing at the
collection in the data store (as the data.lsdb.io README describes); in the versioned layout, at the versioned
directory. The name may be nested in a group directory
(`wise/allwise`): the script creates the group directory in the web tree if it is missing, and the data copy stays at
`<survey>/<collection>`. The directory listing uses Apache's
`AddDescription "<text>" <name> <name>/` lines in `.htaccess`; Apache takes the **first** matching line and matches
names as prefixes, so a specific entry must come before any shorter entry that is a prefix of it, and the script
inserts it there; an entry that already exists for the same name is replaced in place. The description is escaped
for Apache (quotes, control characters). The web directory is sticky, so the file is rewritten in place rather than
renamed, and restored from its backup if the read-back differs.

### Partner buckets

For catalogs hosted by a partner (object storage at a data centre), hand over: the collection directory with its
checksum manifest, the README, the intended public name, the `hats_service_url` the partner will serve, and the
page entry text. Ask the partner for the final URL and record it in `[registry].mirrors` (informational) and on the page.

### Fan-out after a re-import

Every copy of a catalog must be refreshed after a re-import: the web store, the mirror, and any further internal or
partner copies listed in `[registry].mirrors`. A copy left on the old version serves other rows, columns or encodings
under the same name. Write one checklist line per target in `NOTES.md` (target, path or URL, `hats_status` word,
date refreshed, verification result) and close the run only when every line is ticked. `hats_status` is `main` on
the primary copy and `mirror` on every other copy, and `hats_service_url` is each copy's own URL; the publish scripts
set both, and a copy made by hand or by a partner needs them set by hand. In the versioned layout the stable symlink
`<survey>/<collection>` is what mirrors, partners and users address; refreshing a copy means publishing the new
version beside the old one and switching that symlink, never editing the served directory.

## The data.lsdb.io page

Repository: https://github.com/astronomy-commons/data.lsdb.io (`data/README.md` has the `catalog.json` schema,
`README.md` the expose-on-host steps, `scripts/README.md` the maintenance scripts).

`make_site_entry.py` writes `data/<Group>/<Name>/catalog.json` from `[site]`, renders `point_density.webp` from
the local collection with the repository's own plotting call, fills `metadata` from the local properties, runs
`scripts/update_index.py` (never edit `data/catalogs.json` by hand), and stages the files on a branch named after
the page `id` (a new branch starts from the fetched remote default branch, e.g. `origin/main`, not from the clone's
current branch; without a remote, from HEAD). An existing entry is merged, not replaced: keys the script does not produce (column mean maps,
extra URLs, notes, connectivity flags) are kept, and the changed keys are listed. The commit message is the user's; the pull request is the user's; nothing is pushed by the skill.

Conventions seen across the existing entries:

- `label` is `Group/Name`; the group is the survey family, the name the release and, when several hosts serve the
  same catalog, the host in parentheses, for example `Gaia/Gaia DR3 (US-West, HTTP)`.
- `urls.collection` points at the **collection root** (astronomy-commons/data.lsdb.io#9), not the bare catalog.
- `badges`: region (`US-West`, `US-East`, `Europe`, `South America`) and protocol (`HTTP`, `S3`); others exist
  for special cases.
- `other_urls` labels in use: Official release, Column descriptions, Research paper, How to cite, Community forum
  post, Jira ticket, pipeline links.
- `notes`: `info` or `warning` banners with a title and HTML content, for example an outage or an acknowledgment.
- `id` is permanent: lowercase letters, digits and underscores.

After the pull request is merged, a maintainer with web-host access deploys the site (`npm run build`, rsync of
`dist/` to the web host, `scripts/post-deploy.sh`), as described in the repository README.

## The import archive

The data.lsdb.io repository keeps the import notebooks and scripts of every catalog under `imports/`, with a row
added **at the top** of `imports/README.md` for each import. Archive `catalog.toml`, the scripts used and the
executed verification notebook there as part of the same pull request.

## The registry

https://registry.lsdb.io is a VO registry; its resource files live in
https://github.com/astronomy-commons/registry.lsdb.io and are **generated from the data.lsdb.io `data/` tree** by
that repository's `scripts/from_catalogs_json.py` (identifier `<authority>/<id>`, where the authority is
`[registry].authority` in `catalog.toml` and must match what the generator assigns; one XML per page `id`; URLs must
be on a host the generator knows). `registry_entry.py` refuses to run unless the site clone is clean and at
`origin/main` (a stale clone would make the generator retire catalogs; `--allow-dirty` overrides with a warning),
runs the generator for this catalog on a scratch copy, stages only this catalog's XML on a branch in the registry
clone, fails when the XML was not rendered, and shows the staged diff; the pull request is the user's. Set
`[properties].creator_did` to the same identifier so the catalog and its registry record agree.

## Retiring a previous version

Keep the old copy until the page and the registry point at the new one and users have had a notice period. Then
remove the previous versioned directory, the one the publish script printed as "previous version kept on disk"
(on each target), and update the page notes. The stable symlink `<survey>/<collection>` and the web link are never
removed: they already point at the new version. Check that nothing else points at the old directory (the web link,
a mirror, `[run].reimport_from` of a run still in progress) before deleting it. In the legacy unversioned layout
there is no old copy left to remove: the push overwrote it.

## Retiring a catalog

When a catalog is withdrawn with no replacement at the same name:

1. Add a `warning` banner to the page `notes` with the retirement date and the replacement, if any; deploy it and
   keep it up for at least two weeks.
2. Remove the web symlink and its `AddDescription` line in `.htaccess`.
3. Delete the copies on every target: the web data store, the mirror, and every copy in `[registry].mirrors`
   (partners delete their own; record their confirmation).
4. Remove the page entry from the data.lsdb.io `data/` tree (`scripts/update_index.py`) and regenerate the registry;
   the generator marks the missing resource deleted. Its identifier cannot be reused.
5. Keep a tombstone note in the import archive (`imports/README.md`): the catalog, the date, the reason, the
   replacement, and where the import notebooks stay archived.
