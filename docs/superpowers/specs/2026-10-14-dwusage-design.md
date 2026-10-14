# dwusage — CephFS directory usage tracker: design

## Context

dwusage records time series of per-directory usage on CephFS (`/mnt/ceph1`, `/mnt/ceph2`). Separate tools build dashboards (growth trends; who or which group is responsible) and alerts (thresholds, fast growth) from that data.

Constraints:
- Far too many dirs to track them all: ceph1 alone has about 1.6 billion entries and 64 million dirs. Per-subtree configuration decides which dirs are tracked.
- The live filesystems are usually unavailable, so offline capture and replay is a first-class feature and the first milestone. The Milestone 1 plan is `docs/superpowers/plans/2026-10-14-dwusage-m1-capture.md`.
- Production runs as root over user-writable trees. Several instances write to one database.
- The repository is public.
- Project rules come from `AGENTS.md` and `AGENTS.md.d/`:
  - `ruff format` and `ruff check` are clean;
  - unit tests need no live system;
  - a terms file and a conventions file are referenced from the README;
  - deciding is separate from printing;
  - stdout carries results and stderr carries `ERROR:`/`WARNING:` messages;
  - output is deterministic;
  - there is an offline mode;
  - no automatic git commits;
  - no real identifiers in committed files.

## Decisions agreed with the user

| Topic | Decision |
|---|---|
| Unchanged dirs | Every tracked dir gets a sample on every run. |
| Retention | Not in v1. "Pruning" means choosing which dirs to track. |
| Owner | uid and gid always; user and group names when the sampling host can resolve them. |
| Privileges | Production runs as root. Unreadable dirs are recorded as per-dir errors. |
| Database | PostgreSQL in a podman container for shared use; SQLite for tests and local use. One logical schema, plain SQL, no ORM. |
| Instances | One TOML config with named trees. `--tree NAME` runs a subset, so work can be sharded. |
| Config format | TOML (stdlib `tomllib`). |
| Python | `requires-python >= 3.12`. |
| Rule model | Each dir has one owning tree (the deepest root wins). Inside a tree, rules match by glob; precedence is in **Rule model**. |
| Budget rules | Best-first by size: the heaviest dirs are tracked first, with hysteresis. The same code serves `capture --largest`. |
| Discovery | Re-list on every run by default; `rediscover_interval` reuses a selection for a while. |
| rctime culprits | Found and reported only by `capture` and diagnostics, never by `sample`. |
| Identifiers | Real user names and paths never go into committed files; captures are git-ignored and fixtures are anonymized. |

## Facts from the live mounts

These were all measured read-only on 2026-10-14.

1. **Recursive stats cost O(1) per dir.** `ceph.dir.{rbytes,rentries,rfiles,rsubdirs,rctime}` cover a whole subtree. Visiting a dir is cheap; the cost is listing (readdir), which grows with *all* entries, files included.
   - `rentries` and `rsubdirs` count the dir itself: an empty dir has 1 of each.
   - `rbytes` is the sum of file sizes, not pool usage.
2. **rctime is the newest ctime *or mtime* in a subtree, and it never decreases.**
   - A capture of `/mnt/ceph1/npx` found 23 chains of dirs with rctime in the future, at values like `4294967295` and `2147483648`.
   - At 17 chain ends, the rctime equals the newest **mtime** of an entry there, to the nanosecond, while every ctime is normal. At 4 ends nothing is in the future any more, so they are stale. 2 were undetermined, because some subdirs were unreadable as a non-root user.
   - mtime is user-settable (`touch -d`, archives unpacked with bogus dates). So any user can pin rctime, new pinned chains keep appearing, and **rctime can't drive change detection.**
3. **Mount options vary.** `cat <dir>` prints stats here only because of the `dirstat` mount option, and `st_size` of a dir is its entry count. Stats are read with getxattr only.
4. **Unreadable dirs:** as a non-root user, opening a dir for reading fails with `EACCES`. Its stat is still available through an `O_PATH` descriptor.
5. **fd-based reads work and cost nothing extra.** getxattr and scandir work on an fd, and openat2 with `RESOLVE_BENEATH | RESOLVE_NO_SYMLINKS | RESOLVE_NO_XDEV` works. Stat and 12 xattrs for 2,993 dirs took about 1.3 s, fd-based and path-based alike.
6. **Python 3.14 has no `os.statx`** (3.15 has). Birth time comes from statx via ctypes.
7. **rstats propagate lazily.** A parent can briefly disagree with the sum of its children, so invariant tests use a tolerance.

## Architecture

```
src/dwusage/
  cli.py            argparse; each command calls a library function and renders its result  M1+
  xattrs.py         CephFS xattr names and value parsing                                   M1
  rctime.py         pinned-rctime detection (M1); rctime_effective (M5)                    M1, M5
  paths.py          path_key ordering, is_below                                            M1
  fs/base.py        RawFs protocol (inspect, listdir), RawStat, error helpers              M1
  fs/syscalls.py    openat2 and statx via ctypes                                           M1
  fs/ceph.py        CephFs: RawFs over a live mount, fd-based                              M1
  fs/replay.py      RawFs over a capture file                                              M2
  bestfirst.py      best-first expansion by weight (capture --largest, budget rules)       M1
  capture.py        the capture walk                                                       M1
  capture_file.py   capture file format; Output (atomic, mode 0600)                        M1
  config.py         TOML -> frozen dataclasses; validation names the bad key               M3
  policy.py         rule resolution: (tree, path) -> governing rule                        M3
  discovery.py      the tracked set of a tree: depth, transit, exclude (M3), budget (M6)   M3, M6
  model.py          Sample, RunInfo, SCHEMA_VERSION, name encoding for samples             M4
  sampler.py        parallel sampling of the tracked set                                   M4
  state.py          local SQLite: selections, child lists, cache keys                      M5
  lock.py           per-tree flock                                                         M5
  sinks/            jsonl (M4); sql, sqlite, postgres (M7); prometheus, mimir stubs (M8)
tests/  deploy/compose.yaml  docs/terms.md  docs/conventions.md  README.md
```

`RawFs` is the seam that makes everything testable. Every component above `fs/` takes one; tests use an in-memory fake, and replay serves a capture.

## Security

- **No symlinks, no other mounts.** `CephFs` opens every dir relative to its root with openat2 `RESOLVE_BENEATH | RESOLVE_NO_SYMLINKS | RESOLVE_NO_XDEV` and reads only through that fd.
  - A dir that a user swaps for a symlink, at any path component and at any moment, is refused (`ELOOP`), and is recorded as an error.
  - Other mounts are not entered (`EXDEV`).
  - This needs Linux 5.6 or later. Without openat2 (an older kernel, or a seccomp filter), `CephFs` opens with `O_NOFOLLOW` plus a same-device check and warns once that swaps in *intermediate* components go undetected.
- **Outputs are private.** Capture files, JSONL files and SQLite databases are created with mode 0600 under a temporary name, then renamed. They reveal names and sizes of dirs other users can't read. Access to PostgreSQL is controlled by the database.
- **Secrets stay out of config and the command line.** PostgreSQL credentials come from a libpq service file, `.pgpass` or `PGPASSFILE`. DSNs are redacted in messages and in the `runs` table.
- **No identifiers in the repo.** Committed files carry no real names. Captures are git-ignored, and fixtures are anonymized consistently (M2).

## Rule model (policy.py)

**Filesystems** are declared as `[filesystems.NAME] mount = "/mnt/ceph1"`. Stored paths are **fs-relative** (`fs = "ceph1"`, `path = "/npx/user/<user>"`), so data doesn't depend on mount points.

**Trees.** `[trees.NAME]` has `fs`, `root`, a root rule (`depth` or `budget`) and optional `[[trees.NAME.rules]]`.
- A dir belongs to the tree with the deepest root containing it.
- Outer trees stop at inner tree roots and don't sample them. This holds across instances too.

**Rules.**
- `match` is a glob relative to the tree root, matched per path component. `*` matches one component; `**` is not supported.
- Each rule sets exactly one of:
  - `depth = N`: 0 means the dir itself only, 1 adds its children, and so on;
  - `budget = N`, with optional `weight = "rbytes" | "rentries"`, `max_depth` and `hysteresis`;
  - `exclude = true`.

**Resolution** for a dir D:
1. **Exclude is terminal.** If D or an ancestor of D is matched by an `exclude` rule, D is neither tracked nor descended into, and transit never passes through it.
2. **A rule matching D governs D's subtree**, with depth counted from D. It overrides the rule D inherited from its parent, whatever their order in the file. If several rules match D, the last one in file order wins. Example: `match = "*/data"` replaces the inherited rule for `alice/data`, even if a rule for `alice` comes later.
3. **Otherwise D inherits** its parent's rule, with one less remaining depth.
4. **Transit dirs** on the path to a deeper `match` are tracked, so the tracked set stays connected from the tree root.

**`check-config` rejects:**
- unknown keys (with a suggested spelling);
- rules with no mode or with several modes;
- unknown `fs` names;
- trees with the same root;
- rules that can never apply, because their `match` lies under an inner tree's root or under an `exclude`;
- rules whose `match` lies inside a budget rule's subtree, in v1.

**`max_dirs`** is set per tree (default 50k). If discovery would exceed it:
- if a previous selection exists, it keeps that selection and prints a WARNING naming the tree and the rule; the run is `partial` and exits 3;
- if there is none, the tree fails with an ERROR and the other trees still run.

## Budget rules: best-first by size

- `budget = N` tracks N dirs of the rule's subtree, its anchor included. They are chosen **heaviest first** by `weight` (default `rbytes`), down to `max_depth` below the anchor.
- **Algorithm:** `bestfirst.best_first`, which `capture --largest` also uses.
  - Starting from the anchor, it yields the heaviest waiting dir. A dir's children are listed and stat-ed lazily: only when nothing heavier waits, since no child outweighs its parent.
  - The first N dirs yielded are tracked.
  - **Properties:** exactly N dirs (fewer if the subtree is smaller); connected, since a dir comes after its parent; deterministic, since ties are broken by `path_key`; and only dirs whose children could compete are listed.
- **Hysteresis** (`hysteresis`, default 0.25). A dir tracked in the previous selection weighs 1.25× its size, so it is replaced only by a candidate more than 25% heavier. This is the user's "size didn't change much" tolerance.
  - The previous selection is connected, so a boosted dir's parent is boosted too, and the heaviest-first order stays exact.
- Excluded dirs and inner tree roots are never candidates. Dirs with more than `max_list_entries` entries are tracked as leaves, with a note.
- **Cost:** stat calls for the children of listed dirs, bounded by N times the fanout.

## Discovery and sampling

- **Sampling.** Every run samples every tracked dir: stat plus xattrs through one fd, in parallel.
- **Discovery** decides the tracked set.
  - By default it runs on every run (`rediscover_interval = "0"`).
  - The per-tree config option `rediscover_interval = "6h"` (or the CLI's `--rediscover-interval`) reuses the selection for that long; `--rediscover` forces discovery.
  - Discovery re-lists a tracked, listed dir **only if its (ino, mtime, ctime) changed** since its child list was cached. Creating, removing or renaming an entry changes the dir's mtime and ctime. Setting the mtime back (rsync, tar) changes the ctime.
  - Otherwise the cached child list is reused. New dirs therefore appear on the next run, at the cost of one readdir per changed dir.
  - A dir's mtime and ctime may reach a client late, like rstats (lazy, possibly via another MDS), so a new subdir can appear one run late. M5 measures this against a series of captures.
  - No skip ever depends on rctime (see Facts, item 2).
- **Cache validity.** Cached selections are keyed on a hash of the tree's resolved config plus the tool version. A mismatch forces full rediscovery, so `plan` and `sample` agree right after a config edit.
- **Vanishing and identity.**
  - A dir that disappears is dropped at the next discovery. If it vanishes between discovery and sampling, it gets one sample with an `ENOENT` error.
  - A different `ino` at the same path is a new dir. Samples carry `ino`, so consumers can split the series there.
- **Parallelism:**
  - one thread pool (`workers`, default 16);
  - tasks submitted in batches;
  - an optional token bucket, `max_ops_per_sec`, to protect the MDS.
- **State** is a local SQLite file in `state_dir` (default `$XDG_STATE_HOME/dwusage/state.sqlite`, WAL mode). It doesn't depend on the sinks.
  - `--replay` uses an in-memory state only, so replays never touch production state.
- **Locking.** One `flock` per tree on the host. A database lease covers other hosts (see Database).

## Pinned rctime (rctime.py)

- **Detection is per dir.** A dir's `rctime_pinned` is true when its rctime is later than `sampled_at + rctime_future_slack` (default 1 day). Only dirs on a chain are affected; their healthy siblings keep a valid rctime.
- **`rctime_effective` is always populated**, so consumers use one column.
  - For a healthy dir it equals `rctime`.
  - For a pinned dir D it is a lower bound: the newest of the candidates below. Every candidate passes the same future filter, so an entry with a future time can't leak into the result. The candidates are:
    - D's own mtime and ctime;
    - the `rctime` of each healthy subdir;
    - the `rctime_effective` of each pinned subdir, computed along the chain.
  - It misses time changes of plain files directly inside chain dirs, which would need a stat per file.
- **The trace:**
  - It reads subdirs in `path_key` order.
  - Tracked subdirs reuse their samples from this run.
  - Inner tree roots are boundaries: a pinned inner root contributes only its own mtime and ctime.
  - At most `rctime_trace_max_dirs` extra reads of untracked dirs per tree (default 5000). If that cap is hit, `rctime_effective` is the newest candidate read so far (still a lower bound) and one WARNING is printed. The fixed order keeps the result deterministic.
- **What `sample` doesn't do:** it doesn't store, trace or report culprits; `capture` does. Correcting the offending entry never lowers rctime, so per-run reports would only be noise.
- **Raw values are kept.** The raw `rctime` is stored as reported (float seconds; µs precision in the database).

## Sample record (one schema for JSONL and the database)

JSONL has one flat object per line, with `type` set to `run_start`, `sample` or `run_end`. All times are epoch seconds (floats), so jq works directly:

```json
{"schema":1,"type":"sample","run_id":…,"host":"…","fs":"ceph1","tree":"npx-user","path":"/npx/user/<user>",
 "path_escaped":false,"sampled_at":1792000000.12,"rbytes":…,"rentries":…,"rfiles":…,"rsubdirs":…,
 "entries":…,"files":…,"subdirs":…,"rctime":…,"rctime_pinned":false,"rctime_effective":…,
 "uid":…,"gid":…,"user":"<user>","group":"<group>","mode":"0755","ino":…,
 "atime":…,"mtime":…,"ctime":…,"btime":…,"quota_max_bytes":null,"quota_max_files":null,
 "rule":"trees.npx-user","errors":{}}
```

- **Example query:** `jq 'select(.type=="sample" and .rbytes>1e13) | [.path,.rbytes]'`.
- **Errors.** `errors` is an object of step → `describe_oserror()` text, the same as in captures; select clean records with `.errors == {}`. On an error the metric fields are null.
- **Names** in samples and the database are valid UTF-8.
  - Bytes that aren't valid UTF-8 are written as `\xNN`, and the record gets `path_escaped: true`.
  - So one odd name never breaks a database sink. Python's sqlite3 and psycopg reject the surrogate-escaped strings that `os` returns.
  - Capture files keep exact names as `\udcXX` escapes, because only dwusage reads them.
- **Order.** Records are sorted by `(fs, path_key(path))` within each tree.
- `rentries` and `rsubdirs` count the dir itself, as CephFS reports them.

## Database schema (sinks/sql.py)

| Table | Contents |
|---|---|
| `runs` | `run_id` (bigint) PK, host, tool_version, config_hash, trees, started_at, finished_at, status (`running`, `ok`, `partial`, `failed`), n_samples, n_errors |
| `dirs` | `dir_id` PK, `fs`, `path`, `path_escaped`, UNIQUE(fs, path) |
| `principals` | (kind `user` or `group`, id) PK, name, seen_at |
| `rules` | `rule_id` PK, name UNIQUE |
| `samples` | (dir_id, sampled_at) PK, run_id, rule_id, ino, uid, gid, mode, metric columns, rctime, rctime_pinned, rctime_effective, atime/mtime/ctime/btime, quota columns, errors (jsonb in PostgreSQL, text in SQLite) |

- **`samples_v`** joins all the tables back into the flat record for psql and Grafana.
- **Rows stay narrow**, with numbers and foreign keys only.
  - That is about 120 B per row including the primary-key index. For example, 20k tracked dirs sampled hourly is about 175M rows, or 20–25 GB, per year.
  - Retention (a later `prune`, or TimescaleDB policies) will be needed eventually.
- **Migrations:** a `schema_version` table and numbered migration files, applied by `dwusage db init`.
- **Multiple writers:**
  - `dirs`, `principals` and `rules` are filled with `INSERT … ON CONFLICT DO NOTHING` followed by a select.
  - `samples` uses `ON CONFLICT DO NOTHING`, so re-sending a batch is idempotent. Writes go in batches.
  - **Lease:** each tree holds `pg_try_advisory_lock(hash(fs, root))` for the run, so two hosts never sample the same tree at once.
  - **Overlap check:** before sampling, a tree whose root overlaps a root sampled under a *different* `config_hash` in the last 48 h is an ERROR. This stops drifted configs on two hosts from double-counting.
  - SQLite allows one writer, through its file lock.
- **Types:** timestamps are `timestamptz` (µs) in PostgreSQL and REAL epoch seconds in SQLite.

## Sinks

- **Choosing sinks:** `--sink` (repeatable) or `[[sinks]]` in the config. Every sink receives every record.
- **Specs:**
  - `jsonl:-` (stdout, the default);
  - `jsonl:PATH` (mode 0600, atomic);
  - `sqlite:PATH`;
  - `postgresql:///DB?service=NAME` (credentials as in Security);
  - `prometheus:…` and `mimir:…`, which are stubs, as the user asked. They fail at config validation with "not implemented yet".
- **For the future real metric sinks:**
  - Hourly samples need remote-write with explicit timestamps, because scrape staleness is 5 minutes.
  - The series count is about tracked dirs × 20 metrics, and budgets and depths keep it bounded.
- **Sink failure** is fatal for the run: status `failed`, exit 1. To keep the period anyway, add a `jsonl:FILE` sink and import that file later with `dwusage db load FILE`, which is idempotent.
- **Tree failure** (lease held elsewhere, or `max_dirs` exceeded with no previous selection) doesn't stop the other trees. The run is `partial` and exits 3.

## CLI

```
dwusage capture ROOT... -o FILE [--depth N] [--largest N] [--max-dirs N] [--max-list-entries N]
                [--chain-max-dirs N] [--pinned-max-files N] [--workers N]
dwusage plan    [--config F] [--tree NAME]... [--replay CAPTURE] [--format table|jsonl]
dwusage sample  [--config F] [--tree NAME]... [--sink SPEC]... [--replay CAPTURE]
                [--rediscover | --rediscover-interval DURATION] [--workers N]
dwusage check-config [--config F]
dwusage db init --sink SPEC
dwusage db load --sink SPEC FILE...
```

- **Deciding vs printing.** Each command calls a library function that returns a typed result (`Capturer.run`, `plan_trees`, `run_sample`); `cli.py` only parses and renders.
- **`capture`** needs no config; Milestone 1 implements it.
  - Dirs within `--depth` are listed.
  - Pinned dirs are listed at any depth to the chain ends, and their files with a future mtime or ctime are recorded. Each chain end is classified as culprit, stale or undetermined.
  - `--largest N` then visits N more dirs per root, best-first.
  - All limits apply per root.
- **`plan`** writes nothing. It shows each tracked dir with the governing rule and the reason (depth, budget rank, transit, excluded). Approximate values, such as the `rctime_effective` of a pinned dir, are marked `~` and explained once in a footnote.
- **`sample --help`** says up front that it updates the local state cache, even when the only sink is stdout.
- **Replay.** `--replay CAPTURE` uses an in-memory state and `captured_at` as the clock. A path the capture doesn't cover is an ERROR naming the path and the capture option that would have covered it (`--depth` or `--largest`), with exit 1.
- **Exit codes:**
  - 0: done;
  - 1: error;
  - 2: usage error;
  - 3: incomplete or partial (a limit left work undone, or a tree failed);
  - 130: interrupted (Ctrl-C or SIGTERM; no partial output file is left).

## Milestones (TDD; `ruff format`, `ruff check` and `pytest` clean at each)

1. **Capture** (plan: `docs/superpowers/plans/2026-10-14-dwusage-m1-capture.md`):
   - scaffold;
   - fs layer (RawFs, openat2-based CephFs, statx);
   - `bestfirst.py`;
   - the capture walk with pinned chains and `--largest`;
   - the capture file;
   - `dwusage capture`.

   Afterwards the user takes captures while the mounts are reachable. The README documents a crontab, but dwusage never installs one.
2. **Replay reader and fixtures.** `fs/replay.py`, plus test fixtures cut from captures with names replaced consistently (e.g. `<user-1>`). Real names never enter the repo.
3. **Config and policy:** `config.py`, `policy.py`, the depth, transit and exclude parts of `discovery.py`, `check-config` and `plan`.
4. **Sampler and JSONL sink:** `model.py`, `sampler.py`, `sample`, run records, name encoding.
5. **State, rediscovery, lock and `rctime_effective`.**
6. **Budget rules:** `discovery.py` budget selection via `bestfirst`, with hysteresis.
7. **SQL sinks:** shared DDL and migrations, SQLite, PostgreSQL (psycopg 3), `db init`, `db load`, the lease and overlap check, and `deploy/compose.yaml`. PostgreSQL tests run only when `DWUSAGE_TEST_PG_DSN` is set.
8. **Stubs and docs:** Prometheus and Mimir stubs, README, terms and conventions, and an example `dwusage.toml` covering the user's trees.

## Verification

- **Unit tests** (no live filesystem):
  - **rule precedence:** exclude is terminal and blocks transit; a match beats an inherited rule; last match wins; owning tree; dead rules and rules inside a budget subtree are rejected;
  - **budget:** exactly N dirs, connected, deterministic, `max_depth`, hysteresis keeps a dir unless a rival is more than 25% heavier;
  - **discovery:** a dir is re-listed only when its (ino, mtime, ctime) changed; a config hash mismatch forces rediscovery; a vanished dir gives one error row;
  - **rctime:**
    - a healthy dir has `rctime_effective == rctime`;
    - a planted future *mtime*, on a file or on the dir itself, never leaks into the result;
    - when the cap is hit the result is the partial lower bound plus one warning;
    - an inner tree root is a boundary;
  - **replay:** an uncovered path is a fatal error;
  - **names:** odd names produce escaped, flagged paths in samples;
  - **SQLite sink:** round trip, idempotent re-insert, `db load`;
  - **config validation messages.**
- **Invariants on captured fixtures**, with a tolerance for lazy propagation:
  - the tracked set is connected;
  - no dir is owned by two trees;
  - Σ child `rbytes` ≤ parent `rbytes`;
  - 1 + Σ child `rsubdirs` ≤ parent `rsubdirs`.

  (`rentries == rfiles + rsubdirs` can't fail: the client computes it.)
- **End to end on the live mount**, read-only:
  - `capture`, then `plan --replay` and `sample --replay … --sink jsonl:-` through jq;
  - `sample --sink sqlite:…` against the mount;
  - the PostgreSQL container: `db init`, two runs, both present in `samples_v`.

## Out of scope / later

- Retention and downsampling (`prune`).
- Smarter depth heuristics for `npx/ana`.
- Real Prometheus and Mimir sinks.
- Dashboards and alerts.
- Replacing the ctypes statx with `os.statx` once the minimum Python is 3.15.
