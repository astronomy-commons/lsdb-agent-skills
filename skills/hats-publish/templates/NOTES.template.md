# <survey> <release> — HATS import notes

## Result
- Collection: `<build path>` → published at `<public URL>` (and `<mirror path>` if applicable)
- Rows / columns / partitions / size: <from RUNLOG.md>
- Verification: <N/N PASS> (`verify_structure.py`, value-level notebook, review checklist)

## Decisions (from the interview; rationale in RUNLOG.md)
- Route, partitioning, schema changes, default columns, margins, indexes, names, hosting, site entry.

## Open items
- [ ] site page PR merged and deployed
- [ ] registry entry PR
- [ ] mirrors pending
- [ ] previous version retired (re-imports only)

## Files
`catalog.toml`, `runlog.jsonl` / `RUNLOG.md`, `verify_results.csv`, the executed verification notebook, logs.
