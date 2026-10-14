# dwusage

Tracks how CephFS directory usage (bytes, file and directory counts, owner, times) changes
over time, for dashboards and alerts built by other tools.

Status: milestone 1, `dwusage capture` only. Design:
`docs/superpowers/specs/2026-10-14-dwusage-design.md`.

## Running

Linux only. No installation is needed for development:

    PYTHONPATH=src python3 -m dwusage --help

Or install it with `pip install -e .`. Full protection against symlinks swapped in during a
walk needs openat2 (Linux 5.6 or later).

## Capturing

`dwusage capture` records raw CephFS state for offline replay and diagnostics (see
`dwusage capture --help`). Take captures repeatedly while the filesystems are reachable: a
series of captures is what later milestones replay as history. Run as root for a complete
view; otherwise unreadable directories are recorded with `EACCES` errors.

Captures are mode 0600 because they reveal names and sizes of directories other users may
not read. Keep them out of the repository (`*.jsonl.gz` is git-ignored).

Example crontab entries (documentation only: install them yourself if you want them).
`flock -n` skips a run while the previous one is still going (keep the lock file in a
directory only you can write); `%` must be escaped. These depths keep each run to about a
minute; deeper, `/mnt/ceph2/exp` exceeds the default `--max-dirs`:

    0 * * * * cd /path/to/dwusage && flock -n $HOME/dwusage-captures/user.lock env PYTHONPATH=src python3 -m dwusage capture /mnt/ceph1/npx/user /mnt/ceph1/volumes --depth 2 --largest 2000 --workers 8 -o $HOME/dwusage-captures/user-$(date +\%Y\%m\%dT\%H).jsonl.gz 2>>$HOME/dwusage-captures/capture.log
    10 * * * * cd /path/to/dwusage && flock -n $HOME/dwusage-captures/ana.lock env PYTHONPATH=src python3 -m dwusage capture /mnt/ceph1/npx/ana /mnt/ceph1/npx/wipac --depth 3 --largest 2000 --workers 8 -o $HOME/dwusage-captures/ana-$(date +\%Y\%m\%dT\%H).jsonl.gz 2>>$HOME/dwusage-captures/capture.log
    30 * * * * cd /path/to/dwusage && flock -n $HOME/dwusage-captures/exp.lock env PYTHONPATH=src python3 -m dwusage capture /mnt/ceph2/exp --depth 4 --largest 2000 --workers 8 -o $HOME/dwusage-captures/exp-$(date +\%Y\%m\%dT\%H).jsonl.gz 2>>$HOME/dwusage-captures/capture.log

Quick looks:

    zcat cap.jsonl.gz | jq -c 'select(.type=="capture_end")'
    zcat cap.jsonl.gz | jq -r 'select(.type=="dir") | [.path, .xattrs["ceph.dir.rbytes"]] | @tsv'

## Documentation

- `docs/terms.md`: terms used in output, docs and code
- `docs/conventions.md`: conventions for contributors
