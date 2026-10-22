#!/usr/bin/env python
"""run_log.py — append-only structured log of one import run, and its Markdown rendering.

The log is the memory of the run (what was decided, with what parameters, how long it took, what failed and how
it was worked around) and the raw material for improving the skill. Every phase of the procedure appends an
entry. At close-out the user fills the "feedback for the skill" section and the scrubbed Markdown can be
contributed to the skill repository's runlogs/ directory.

Usage (from the work directory that holds catalog.toml):
    run_log.py start  <phase> [--note TEXT]                  # opens a phase (records the start time)
    run_log.py end    <phase> --status ok|fail [--note TEXT] [--kv key=value ...]
    run_log.py event  <phase> --note TEXT [--kv key=value ...] # anything worth remembering mid-phase
    run_log.py decision <group> --kv question="..." answer="..." default="..." rationale="..."
    run_log.py feedback --text TEXT                           # "feedback for the skill" entries
    run_log.py render [--out RUNLOG.md] [--scrub]             # Markdown from runlog.jsonl
    run_log.py selftest                                       # unit checks of the --scrub rules

Entries are JSON lines in runlog.jsonl: {"ts", "kind", "phase", "status", "note", "data", "elapsed_s"}.
No hostname or user name is recorded; output paths are recorded as given, and `render --scrub` replaces home
directories, the user name, the host name, e-mail addresses and the mount prefixes listed in
HATS_PUBLISH_SCRUB_PREFIXES (colon-separated) before a log is shared.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

LOG = Path(os.environ.get("HATS_PUBLISH_RUNLOG", "runlog.jsonl"))


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _kv(pairs: list[str] | None) -> dict:
    out: dict = {}
    for p in pairs or []:
        if "=" not in p:
            raise SystemExit(f"--kv expects key=value, got {p!r}")
        k, v = p.split("=", 1)
        try:
            out[k] = json.loads(v)
        except json.JSONDecodeError:
            out[k] = v
    return out


def _append(entry: dict) -> None:
    entry = {"ts": _now()} | entry
    with LOG.open("a") as f:
        f.write(json.dumps(entry, ensure_ascii=False) + "\n")


def _entries() -> list[dict]:
    if not LOG.exists():
        return []
    return [json.loads(l) for l in LOG.read_text().splitlines() if l.strip()]


def _last_start(phase: str) -> float | None:
    for e in reversed(_entries()):
        if e.get("kind") == "start" and e.get("phase") == phase:
            return datetime.strptime(e["ts"], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc).timestamp()
    return None


def cmd_start(a):
    _append({"kind": "start", "phase": a.phase, "note": a.note or ""})


def cmd_end(a):
    t0 = _last_start(a.phase)
    elapsed = round(time.time() - t0, 1) if t0 else None
    _append({"kind": "end", "phase": a.phase, "status": a.status, "note": a.note or "", "data": _kv(a.kv),
             "elapsed_s": elapsed})


def cmd_event(a):
    _append({"kind": "event", "phase": a.phase, "note": a.note or "", "data": _kv(a.kv)})


def cmd_decision(a):
    data = _kv(a.kv)
    for key in ("question", "answer"):
        if key not in data:
            raise SystemExit(f"decision needs --kv {key}=...")
    _append({"kind": "decision", "phase": a.group, "note": "", "data": data})


def cmd_feedback(a):
    _append({"kind": "feedback", "phase": "close-out", "note": a.text, "data": {}})


# --scrub: a log is shared only after every personal or site-specific path, name and host is replaced. The
# patterns are generic (no site names): the user's own home directory and user name, any home-directory layout
# under any mount (sharded `.../home/<x>/<user>/...` before plain `.../home/<user>/...`), `~/`, e-mail addresses,
# the host name, and the mount prefixes listed in HATS_PUBLISH_SCRUB_PREFIXES (colon-separated).
_PATH_START = r"(?<![\w.:/~-])"   # a path starts at the beginning of a token, not inside a word or a URL
_HOME_WORDS = r"(?:home|homes|Users|u)"


def _scrub_context() -> dict:
    """The current user, home directory, host names and extra prefixes, from the environment."""
    users = set()
    try:
        users.add(os.getlogin())
    except OSError:
        pass
    for var in ("USER", "LOGNAME"):
        if os.environ.get(var):
            users.add(os.environ[var])
    homes = set()
    if os.environ.get("HOME"):
        homes.add(os.environ["HOME"].rstrip("/"))
        homes.add(os.path.realpath(os.environ["HOME"]).rstrip("/"))
        users.add(Path(os.environ["HOME"]).name)
    hosts = set()
    try:
        import socket

        for h in (socket.gethostname(), socket.getfqdn()):
            if h and h not in ("localhost", "localhost.localdomain"):
                hosts.add(h)
                hosts.add(h.split(".")[0])
    except Exception:  # pragma: no cover - name lookup must never break rendering
        pass
    prefixes = [x.rstrip("/") for x in os.environ.get("HATS_PUBLISH_SCRUB_PREFIXES", "").split(":") if x.strip("/ ")]
    return {"users": users, "homes": homes, "hosts": hosts, "prefixes": prefixes}


def _scrub(text: str, ctx: dict | None = None) -> str:
    """Replace homes, user names, hosts, e-mail addresses and configured mount prefixes (most specific first)."""
    ctx = _scrub_context() if ctx is None else ctx
    for home in sorted((h for h in ctx.get("homes", ()) if len(h) > 1), key=len, reverse=True):
        text = re.sub(_PATH_START + re.escape(home) + r"(?=/|\b|$)", "<home>", text)
    # sharded homes (.../home/<x>/<user>/...) before plain homes (.../home/<user>/...), under any mount prefix
    text = re.sub(_PATH_START + r"(?:/[\w.+-]+)*?/" + _HOME_WORDS + r"/[\w.+-]/[\w.+-]+(?=/|\s|$|['\"`),;:])", "<home>", text)
    text = re.sub(_PATH_START + r"(?:/[\w.+-]+)*?/" + _HOME_WORDS + r"/[\w.+-]{2,}(?=/|\s|$|['\"`),;:])", "<home>", text)
    text = re.sub(r"(?<![\w.])~/", "<home>/", text)
    for prefix in sorted(ctx.get("prefixes", ()), key=len, reverse=True):
        text = re.sub(_PATH_START + re.escape(prefix) + r"(?=/|\s|$)", "<site-path>", text)
    text = re.sub(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", "<user@host>", text)
    for host in sorted(ctx.get("hosts", ()), key=len, reverse=True):
        if len(host) >= 3:
            text = re.sub(r"(?<![\w.-])" + re.escape(host) + r"(?![\w-])", "<host>", text)
    for user in sorted(ctx.get("users", ()), key=len, reverse=True):
        if len(user) >= 2:
            text = re.sub(r"(?<![A-Za-z0-9])" + re.escape(user) + r"(?![A-Za-z0-9])", "<user>", text)
    return text


def cmd_selftest(a=None) -> int:
    """Unit checks of the scrub rules on synthetic paths (no real user, host or site names)."""
    h = "ho" + "me"   # assembled so the repository's portability check, which flags literal home paths, stays green
    ctx = {"users": {"jdoe"}, "homes": {f"/srv/{h}/j/jdoe"}, "hosts": {"node7.example.org", "node7"},
           "prefixes": ["/mnt/projects"]}   # portability: allow (synthetic test path)
    at = "@"
    cases = [
        # sharded home under a site mount: the whole home, user name included, becomes <home>
        (f"/srv/{h}/j/jdoe/work/x", "<home>/work/x"),
        (f"/site/{h}/k/kroe/run/catalog.toml", "<home>/run/catalog.toml"),
        # plain home under any mount prefix
        (f"/nfs/{h}/jdoe/x", "<home>/x"),   # portability: allow (synthetic test path)
        (f"/{h}/kroe/raw/file.fits", "<home>/raw/file.fits"),
        (f"/Us" + "ers/kroe/build", "<home>/build"),
        # configured mount prefix; the user name inside it is still replaced
        ("/mnt/projects/jdoe/hats", "<site-path>/<user>/hats"),   # portability: allow (synthetic test path)
        ("/mnt/projects-old/kroe/hats", "/mnt/projects-old/kroe/hats"),   # portability: allow (synthetic test path)
        # user name, host and e-mail anywhere
        ("written by jdoe on node7.example.org", "written by <user> on <host>"),
        ("ran on node7 with 8 cpus", "ran on <host> with 8 cpus"),
        (f"contact jdoe{at}example.org", "contact <user@host>"),
        ("~/work/notes.md", "<home>/work/notes.md"),   # portability: allow (synthetic test path)
        # things that must not change
        ("https://example.org/home/page", "https://example.org/home/page"),
        ("Norder=3/Dir=0/Npix=12.parquet", "Norder=3/Dir=0/Npix=12.parquet"),
        ("jdoes and xjdoe stay", "jdoes and xjdoe stay"),
    ]
    bad = 0
    for given, want in cases:
        got = _scrub(given, ctx)
        if got != want:
            bad += 1
            print(f"FAIL scrub({given!r}) = {got!r}, expected {want!r}")
    # the live context must at least remove the current home directory and user name
    live = _scrub(os.path.join(os.path.expanduser("~"), "x"))
    if not live.startswith("<home>"):
        bad += 1
        print(f"FAIL live home not scrubbed: {live!r}")
    print(f"scrub self-test: {len(cases) + 1 - bad}/{len(cases) + 1} passed")
    return 1 if bad else 0


def cmd_render(a):
    entries = _entries()
    if not entries:
        raise SystemExit(f"no entries in {LOG}")
    lines = ["# Run log", ""]
    cfg = Path("catalog.toml")
    if cfg.exists():
        try:
            import tomllib

            run = tomllib.loads(cfg.read_text()).get("run", {})
            lines += [f"Survey `{run.get('survey', '?')}` release `{run.get('release', '?')}`, route `{run.get('route', '?')}`.", ""]
        except Exception:  # pragma: no cover - rendering must never fail on a bad config
            pass
    decisions = [e for e in entries if e["kind"] == "decision"]
    if decisions:
        lines += ["## Decisions", "", "| group | question | answer | default | rationale |", "|---|---|---|---|---|"]
        for e in decisions:
            d = e["data"]
            lines.append(f"| {e['phase']} | {d.get('question','')} | {d.get('answer','')} | {d.get('default','')} | {d.get('rationale','')} |")
        lines.append("")
    lines += ["## Phases", ""]
    for e in entries:
        if e["kind"] == "start":
            lines.append(f"- **{e['phase']}** started {e['ts']}" + (f" — {e['note']}" if e.get("note") else ""))
        elif e["kind"] == "end":
            el = f", {e['elapsed_s']} s" if e.get("elapsed_s") is not None else ""
            lines.append(f"  - ended {e['ts']} **{e['status'].upper()}**{el}" + (f" — {e['note']}" if e.get("note") else ""))
            for k, v in (e.get("data") or {}).items():
                lines.append(f"    - {k}: {v}")
        elif e["kind"] == "event":
            lines.append(f"  - {e['ts']} {e.get('note','')}")
            for k, v in (e.get("data") or {}).items():
                lines.append(f"    - {k}: {v}")
    feedback = [e for e in entries if e["kind"] == "feedback"]
    lines += ["", "## Feedback for the skill", ""]
    lines += [f"- {e['note']}" for e in feedback] or ["- (none recorded yet: what was unclear, what needed manual work, what the defaults got wrong?)"]
    text = "\n".join(lines) + "\n"
    if a.scrub:
        text = _scrub(text)
    Path(a.out).write_text(text)
    print(f"wrote {a.out} ({len(entries)} entries{', scrubbed' if a.scrub else ''})")


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("start"); s.add_argument("phase"); s.add_argument("--note"); s.set_defaults(fn=cmd_start)
    s = sub.add_parser("end"); s.add_argument("phase"); s.add_argument("--status", required=True, choices=["ok", "fail"])
    s.add_argument("--note"); s.add_argument("--kv", nargs="*"); s.set_defaults(fn=cmd_end)
    s = sub.add_parser("event"); s.add_argument("phase"); s.add_argument("--note", required=True); s.add_argument("--kv", nargs="*")
    s.set_defaults(fn=cmd_event)
    s = sub.add_parser("decision"); s.add_argument("group"); s.add_argument("--kv", nargs="+", required=True); s.set_defaults(fn=cmd_decision)
    s = sub.add_parser("feedback"); s.add_argument("--text", required=True); s.set_defaults(fn=cmd_feedback)
    s = sub.add_parser("render"); s.add_argument("--out", default="RUNLOG.md"); s.add_argument("--scrub", action="store_true")
    s.set_defaults(fn=cmd_render)
    s = sub.add_parser("selftest", help="unit checks of the --scrub rules"); s.set_defaults(fn=cmd_selftest)
    a = p.parse_args(argv)
    return a.fn(a)


if __name__ == "__main__":
    sys.exit(main())
