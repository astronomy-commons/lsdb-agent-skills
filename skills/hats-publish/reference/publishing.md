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

### Web host details

The public URL `<HATS_PUBLISH_WEB_URL>/<name>` is a symlink in `<HATS_PUBLISH_WEB_ROOT>` pointing at the
collection in the data store (as the data.lsdb.io README describes). The directory listing uses Apache's
`AddDescription "<text>" <name> <name>/` lines in `.htaccess`; Apache takes the **first** matching line and matches
names as prefixes, so a specific entry must come before any shorter entry that is a prefix of it, and the script
inserts it there; an entry that already exists for the same name is replaced in place. The description is escaped
for Apache (quotes, control characters). The web directory is sticky, so the file is rewritten in place rather than
renamed, and restored from its backup if the read-back differs.

### Partner buckets

For catalogs hosted by a partner (object storage at a data centre), hand over: the collection directory with its
checksum manifest, the README, the intended public name, the `hats_service_url` the partner will serve, and the
page entry text. Ask the partner for the final URL and record it in `[registry].mirrors` (informational) and on the page.

## The data.lsdb.io page

Repository: https://github.com/astronomy-commons/data.lsdb.io (`data/README.md` has the `catalog.json` schema,
`README.md` the expose-on-host steps, `scripts/README.md` the maintenance scripts).

`make_site_entry.py` writes `data/<Group>/<Name>/catalog.json` from `[site]`, renders `point_density.webp` from
the local collection with the repository's own plotting call, fills `metadata` from the local properties, runs
`scripts/update_index.py` (never edit `data/catalogs.json` by hand), and stages the files on a branch named after
the page `id`. An existing entry is merged, not replaced: keys the script does not produce (column mean maps,
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
remove the symlink first, the data second, and update the page notes.
