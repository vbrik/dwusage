# dwusage

Tracks how CephFS directory usage (bytes, file and directory counts, owner, times) changes
over time, for dashboards and alerts built by other tools.

Status: milestone 1, in progress. Design:
`docs/superpowers/specs/2026-10-14-dwusage-design.md`.

## Running

Linux only. No installation is needed for development:

    PYTHONPATH=src python3 -m dwusage --help

Or install it with `pip install -e .`. Full protection against symlinks swapped in during a
walk needs openat2 (Linux 5.6 or later).

## Documentation

- `docs/terms.md`: terms used in output, docs and code
- `docs/conventions.md`: conventions for contributors
