# Conventions

- Only `dwusage.fs` reads the tracked filesystem, through `RawFs`: `CephFs` on a live mount,
  `tests/fakefs.py` in tests. Other modules use `os` only for their own files.
- `CephFs` opens every directory below its root with openat2(2) and never follows a symlink
  or enters another mount: the tool runs as root over user-writable trees. All reads use the
  resulting file descriptor. Whether openat2 works is decided once per root, before any
  user-controlled path; without it `CephFs` falls back (weaker) and the CLI warns.
- Names from the filesystem go through `paths.printable` before they appear in messages, so
  they can't control the terminal or forge lines.
- Read CephFS stats with getxattr only. What `read()` and `st_size` return for directories
  depends on the `dirstat` and `rbytes` mount options.
- Times: raw stat times are integer nanoseconds (`*_ns`); observation times (`captured_at`,
  `started_at`) are float seconds since the epoch; durations come from `time.monotonic`.
- Per-directory failures are data, not exceptions: record `describe_oserror()` text in the
  record's `errors` and continue. Only usage, configuration and output problems end a run.
- Capture files keep names exactly (invalid UTF-8 as `\udcXX` surrogate escapes).
- Output files are created mode 0600 under a temporary name and renamed into place; an
  existing destination that isn't a regular file (e.g. `/dev/null`) is refused.
- stdout carries results only. Messages go to stderr, one paragraph each, prefixed `ERROR:` or
  `WARNING:` where they are one.
- Output is deterministic: sort paths with `paths.path_key`; break ties by name.
- Separate deciding from printing: library functions return data; `cli.py` renders it.
- Committed files never contain names or paths from live systems (see
  `AGENTS.md.d/LIVE-SYSTEMS-v1.md`); captures are git-ignored.
- Before handing off: `ruff format`, `ruff check` and `pytest` are clean. No automatic git
  commits.
