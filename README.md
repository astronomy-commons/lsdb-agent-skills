# lsdb-agent-skills

Skills for AI coding agents (Claude Code and others that read the `SKILL.md` layout) used by the LSDB team.
Each skill is a directory under `skills/` with a `SKILL.md` playbook, reference pages and the scripts it needs.
Nothing in a skill is tied to a machine, a person or a dataset; site-specific values are configuration.

| Skill | Purpose |
|---|---|
| [`hats-publish`](skills/hats-publish/SKILL.md) | import an external catalog into HATS, verify it against the source, document, host and register it on data.lsdb.io |

## Install

```bash
git clone https://github.com/astronomy-commons/lsdb-agent-skills
cd lsdb-agent-skills
mkdir -p ~/.claude/skills && ln -sfn "$(pwd)/skills/hats-publish" ~/.claude/skills/hats-publish   # Claude Code; other agents: see their docs
```

Restart the agent session; the skill is then available as `/hats-publish`. Then write a site file: copy
`skills/hats-publish/sites/example.env` to `sites/<site>.local.env` (ignored by git), fill in your hosts, paths and
batch options, and export `HATS_PUBLISH_SITE_ENV` pointing at it, as described in
[`skills/hats-publish/CONFIG.md`](skills/hats-publish/CONFIG.md). Filled-in site files are never committed.

## Contributing

- Run `scripts/check_portability.sh` before committing (the pre-commit hook runs it; CI also runs `--links` on pull
  requests): no local paths, hosts, user names, groups, batch accounts, environment names, dataset names in code,
  private sources or dead links. Site-specific values belong in an uncommitted `sites/*.local.env`. A line that
  must carry a synthetic example path can end with `# portability: allow`.
- After an import, contribute the scrubbed run log (`run_log.py render --scrub`) to `runlogs/` in a pull request;
  the logs are reviewed periodically to improve defaults, pitfalls and the decision interview.
- Conventions that have no public write-up yet get an issue on
  [astronomy-commons/data.lsdb.io](https://github.com/astronomy-commons/data.lsdb.io) so they can be cited.

## License

BSD 3-Clause, see `LICENSE`.
