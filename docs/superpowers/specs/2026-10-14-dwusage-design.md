# dwusage — CephFS directory usage tracker (design + implementation plan)

## Context

We need time series of per-directory usage on CephFS (`/mnt/ceph1`, `/mnt/ceph2`) to feed dashboards and alerts. Separate tools will build those dashboards and alerts.

Constraints:
- There are far too many dirs to track them all: ceph1 alone has ~1.6 billion entries and 64 million dirs. Config therefore decides which subtrees are tracked and how deep.
- The live filesystems will be unavailable most of the time, so offline capture and replay is a first-class feature and the first milestone.
- The project directory is empty except for `AGENTS.md` and `AGENTS.md.d/`, whose rules apply throughout:
  - `ruff format` and `ruff check` must pass.
  - Unit tests must not need a live system.
  - A terms file and a conventions file, both referenced from the README.
  - `plan()` and `render()` kept separate.
  - Results on stdout, notes on stderr, failures as `ERROR:` plus a non-zero exit.
  - Deterministic output.
  - Offline capture and replay.
  - No automatic git commits.

## Decisions agreed with the user

| Topic | Decision |
|---|---|
| Budget spread | Weighted by size: a child's share of the budget is proportional to its `rbytes` (or `rentries`). |
| Unchanged dirs | A sample is written for every tracked dir on every run. |
| Retention | Not in v1. "Pruning" means choosing which dirs to track; the schema leaves room for a later `prune` command or TimescaleDB policies. |
| Owner | uid and gid always; user and group names when the host can resolve them (cached per run). |
| Privileges | Production runs as root. Unreadable dirs are recorded as per-dir errors. |
| Database | PostgreSQL in a podman container for shared and production use; SQLite for tests and local use. Same logical schema, plain SQL, no ORM. |
| Instances | One TOML config with named trees. `--tree NAME` runs a subset, so work can be sharded across instances. |
| Config format | TOML (stdlib `tomllib`). |
| Python | `requires-python >= 3.12`. |
| Rule model | Each dir has one owning tree (deepest root wins). Inside a tree, a matching rule replaces the inherited rule; if several match, the last wins. Dirs on the path to a deeper match are tracked. |

## Findings from probing the live mounts (they shape the design)

1. **Recursive stats are O(1) per dir.** `ceph.dir.{rbytes,rentries,rfiles,rsubdirs,rctime}` cover the whole subtree. Sampling is therefore cheap; the cost is `readdir` for discovery.
   - The design splits **discovery** (which dirs to track; cached) from **sampling** (`lstat` and `getxattr` on the known set).
   - The user's "don't scan unchanged dirs" maps to "don't re-readdir". It does not mean skipping samples.
2. **Some rctimes are pinned at `4294967295` (UINT32_MAX).** This affects `/npx`, `/npx/user`, `/npx/archive` and `/npx/wipac`. Some descendant has a bogus future ctime, and rctime is a running max, so it never changes again.
   - The user requires these trees to be handled properly: never skipped, with a workaround for the bug. See **Stuck rctime workaround** below.
3. **Mount options vary.** On this host `cat <dir>` returns stats only because of the `dirstat` mount option, and `st_size` is the entry count. The tool reads stats with `os.getxattr` only.
4. **Some dirs are unreadable** as a normal user (`archive`, `host-mounts`, `volumes/_deleting`, `exp/ingest`). Errors are recorded per dir; the run continues.
5. **No `os.statx` in Python 3.14.** Birth time needs a small ctypes `statx` wrapper. It is best-effort: `btime: null` if the syscall is unavailable.
6. **rstats propagate lazily.** A parent's value does not equal the sum of its children at any given instant, so invariant tests use a tolerance.

## Architecture

```
src/dwusage/
  cli.py            argparse subcommands; only parses args and calls render()
  config.py         TOML -> frozen dataclasses; validation names the bad key/path
  model.py          DirStat, Sample, RunInfo, SampleError; SCHEMA_VERSION
  fs/base.py        FsReader Protocol: stat_dir(path) -> DirStat, list_subdirs(path) -> list[str]
  fs/ceph.py        real reader: os.lstat + os.getxattr + os.scandir (d_type, no per-child stat)
  fs/statx.py       ctypes statx for btime (best-effort)
  fs/replay.py      FsReader backed by a capture file; unknown path -> NotCaptured error
  policy.py         rule matching: (tree, path) -> governing rule + remaining depth
  budget.py         size-weighted budget allocation (pure function, heavily unit-tested)
  discovery.py      concurrent walk producing the tracked set (+ the stats it read on the way)
  sampler.py        parallel sampling of a known set; ThreadPoolExecutor + optional rate limit
  rctime.py         stuck-rctime handling: suspect detection, rctime_effective, bad-entry trace
  state.py          local SQLite cache: selections, per-dir child lists, change-detection keys
  capture.py        writes raw capture files (gzip JSONL)
  lock.py           per-tree flock so overlapping cron runs of the same tree fail early
  sinks/base.py     Sink Protocol: open(run) / write(samples) / close(run_summary)
  sinks/jsonl.py    stdout or file
  sinks/sql.py      shared DDL, upsert logic, migrations (numbered .sql files)
  sinks/sqlite.py, sinks/postgres.py   (psycopg 3, optional extra `[postgres]`)
  sinks/prometheus.py, sinks/mimir.py  stubs: config validation fails with "not implemented yet"
tests/              pytest; FakeFs builder, small capture fixtures, sink tests
deploy/compose.yaml postgres:17 container for local use
docs/terms.md, docs/conventions.md, README.md
```

`FsReader` is the seam that makes everything testable. Every component above `fs/` takes a reader, and tests use `FakeFs` or the replay reader.

### Rule model (policy.py)

- **Filesystems** are declared as `[filesystems.NAME] mount = "/mnt/ceph1"`. All stored paths are **fs-relative** (`fs="ceph1"`, `path="/npx/user/alice"`), so data doesn't depend on where a host mounts the filesystem.
- **Trees**: `[trees.NAME]` has `fs`, `root` and a root rule (`depth` or `budget`), plus optional `[[trees.NAME.rules]]`.
  - A dir belongs to the tree with the deepest root containing it, and outer trees stop at inner tree roots.
  - So `npx` (depth 1) and `npx-user` (depth 1) can coexist without double sampling, including across separate instances.
- **Rules**: `match` is a glob relative to the tree root.
  - `*` matches one path component, and fnmatch works per component (e.g. `IC*`). `**` is not supported in v1, which keeps the path to a deeper match finite.
  - Each rule sets exactly one of:
    - `depth = N`: 0 is the dir itself only, 1 adds its children, and so on.
    - `budget = N`, with optional `weight = "rbytes"|"rentries"` and `max_depth`.
    - `exclude = true`.
- **Resolution for dir D**:
  - If some rules match D, the last one in file order governs D's subtree, with depth counted from D.
  - Otherwise D inherits its parent's rule with one less remaining depth.
- **Transit dirs**: discovery also descends where some rule's `match` could still match deeper, even past the inherited depth. Dirs on that path are tracked, so the tracked set stays connected from the tree root.
- **`max_dirs` per tree** (default 50k): if discovery exceeds it, that tree fails with an `ERROR:` naming the tree and the rule that blew up. Nothing is truncated silently.
- Config validation catches:
  - unknown keys, with "did you mean";
  - rules that set more than one mode;
  - `fs` names that don't exist;
  - overlapping trees on the same root.

### Budget allocation (budget.py), size-weighted

`allocate(dir, b)`: D itself uses 1 slot.

1. Stat D's children. These are cheap xattr reads; `ceph.dir.subdirs` gives the count before any readdir.
2. Split `b-1` across the children in proportion to `weight`, using floor plus largest remainder.
3. A child's share is capped at `rsubdirs+1`, and any excess is passed on to its siblings.
4. A child with share 0 is not tracked. A child with share 1 is tracked as a leaf (no readdir).
5. Recurse while `share > 1` and `depth < max_depth`.

Properties:
- At most `b` readdirs.
- Deterministic: ties broken by name.
- Children of a dir whose `ceph.dir.subdirs` exceeds `max_fanout` (default 20k) are not listed; that dir is tracked as a leaf, with a warning.

### Discovery vs sampling, and efficiency (discovery.py, state.py, sampler.py)

- **Selection caching**: a tree's tracked set is stored in `state.py` and reused until `rediscover_after` (default 24h) or `--rediscover`. This saves readdirs, and it stops budget selections from flapping run to run, which would leave gaps in the series.
- **Child-list caching**: on rediscovery, a dir's cached child list is reused when its own `mtime` and `ctime` are unchanged. Creating, deleting or renaming an immediate entry always updates the parent's mtime, so no readdir is needed.
- **Subtree skip**: a whole cached subtree is reused without visiting it only when all of these hold:
  - rctime is trustworthy (not suspect) and unchanged;
  - `rbytes`, `rentries`, `rfiles` and `rsubdirs` are unchanged.
  - A suspect dir is never skipped this way; see the next section.
- **`subdirs == 0`** means no readdir is needed.
- **Sampling** of each tracked dir is one `lstat`, about nine getxattr calls, and an optional statx.
- **Parallelism**: one bounded `ThreadPoolExecutor` (getxattr and scandir release the GIL), set by `workers` (default 16). An optional token bucket, `max_ops_per_sec`, protects the MDS. Discovery is a work queue: expanding a dir submits tasks for its children.
- **State location**: `state_dir` (default `$XDG_STATE_HOME/dwusage/state.sqlite`, WAL mode). It is local and independent of the sinks.
- **Locking**: one `flock` per tree. A second run of the same tree exits with `ERROR: tree X is already being sampled (pid N)`.

### Stuck rctime workaround (rctime.py)

Some dirs (e.g. `/npx`, `/npx/user`) report `rctime = 4294967295` forever. A descendant has a future ctime, and rctime only ever grows, so fixing that file later won't bring it back down. This module ensures such trees are never skipped and still get a usable activity time.

- **Detection is per dir, never per tree.**
  - A dir is `rctime_suspect` when its rctime > `sampled_at + rctime_future_slack` (default 1 day).
  - Only the dirs on the path from the root down to each bad entry are affected; healthy subtrees beside them keep a valid rctime and keep the subtree skip.
- **Change detection for suspect dirs.**
  - The change key leaves rctime out and uses `(rbytes, rentries, rfiles, rsubdirs, mtime, ctime)` instead. The subtree skip is never applied to a suspect dir.
  - Sampling is unaffected, because every tracked dir is sampled every run anyway.
- **Effective rctime** (`rctime_effective`, **always populated**, so consumers use one column and never branch on the flag):
  - For a healthy dir, it equals `rctime`, with `rctime_effective_approx=false`.
  - For a suspect dir D it is a lower bound, with `approx=true`. It is the max of the candidates below; **every candidate passes through the same future-slack filter**, so a dir with a future ctime can't poison the result:
    - D's own mtime and ctime;
    - the rctime of each immediate subdir that is not suspect;
    - the effective rctime of each suspect subdir, found by recursing into it.

  Notes:
  - Reading the subdirs' rctimes costs one getxattr each and needs D's child list, which is cached by mtime. Recursion follows only suspect children, which is normally one or a few chains.
  - **Tracked children reuse their samples from this run** (e.g. the 2,993 user dirs under the `npx-user` root). Only extra reads of untracked dirs count against `rctime_trace_max_dirs` (default 5000 per tree; to be checked against `/npx/user` and `/npx/wipac` captures).
  - If the cap is hit, the field is null and a warning is printed.
  - It misses ctime changes of plain files directly inside chain dirs when no entry was created or removed (that would need a stat per file). Hence lower bound.
- **Locating the bad entries.**
  - The trace ends at the dir(s) whose children are all non-suspect.
  - The culprit is either that dir's **own** ctime (when it's a dir with a future ctime) or one of its files. The tool checks its own ctime first; a bounded scan of file ctimes is left to `capture` and diagnostics.
  - Those dirs are stored in `state.py`, reported once per run on stderr (`WARNING: rctime pinned to 2106-02-07 by an entry in /npx/user/bob/x`), and listed in the `run_end` record.
  - Chains are re-traced only on rediscovery or when the cached endpoint's stats change.
  - **Finding from a live trace (2026-10-14)** on `/mnt/ceph1/npx`:
    - 23 chains were found, ending at e.g. `npx/user/mjeong` (rctime `2147483648`, i.e. 2³¹) and `npx/user/bbenkel/lib` (`4294967291`).
    - At **every** endpoint, no entry has a future ctime now. The entry that set the value was removed or moved, and the value stays because rctime never decreases.
    - So "culprit" reporting is mostly "stale". `rctime_effective` (children plus own times) is the workaround that matters.
- **Raw values are kept.** The raw `rctime` is always stored unchanged next to the flag and the derived value. The tool never "corrects" rctime silently.

### Sample record (one schema for JSONL and DB)

JSONL is one flat object per line. Records have a `type` (`run_start`, `sample` or `run_end`), and all times are epoch seconds (floats), so jq works directly:

```json
{"schema":1,"type":"sample","run_id":"…","host":"…","fs":"ceph1","tree":"npx-user","path":"/npx/user/alice",
 "sampled_at":1792000000.12,"rbytes":…,"rentries":…,"rfiles":…,"rsubdirs":…,"entries":…,"files":…,"subdirs":…,
 "rctime":…,"rctime_suspect":false,"rctime_effective":…,"rctime_effective_approx":false,"uid":…,"gid":…,"user":"alice","group":"icecube","mode":"0755","ino":…,
 "atime":…,"mtime":…,"ctime":…,"btime":…,"quota_max_bytes":null,"quota_max_files":null,
 "rule":"trees.npx-user","error":null}
```

Example query: `jq 'select(.type=="sample" and .rbytes>1e13) | [.path,.rbytes]'`.

Records are emitted sorted by `(fs, path)` for each tree. On a per-dir error (e.g. `EACCES`, or `ENOENT` when the dir vanished), the metric fields are null and `error` holds `"EACCES: …"`. Dashboards can see a disappearance as an error row; the next rediscovery drops the dir.

### Database schema (sinks/sql.py)

| Table | Contents |
|---|---|
| `runs` | `run_id` PK, host, tool_version, config_hash, trees, started_at, finished_at, status, n_samples, n_errors |
| `dirs` | `dir_id` PK, `fs`, `path`, UNIQUE(fs, path). Normalizes repeated paths. |
| `samples` | `(dir_id, sampled_at)` PK, `run_id`, every metric and metadata field above |

Other parts:
- A `samples_v` view that joins in `fs` and `path`, for easy querying in Grafana and psql.
- A `schema_version` table, plus numbered migration files applied by `dwusage db init`.
- Multi-writer safety:
  - `dirs` is filled with `INSERT … ON CONFLICT DO NOTHING` followed by a select.
  - `samples` uses `ON CONFLICT DO NOTHING`, which makes re-sending a batch idempotent.
  - Writes go in batched transactions.
- SQLite gets the same DDL, with types mapped (`timestamptz` becomes REAL epoch).

### Sinks

- `--sink` can be repeated, or sinks can be set in `[[sinks]]` in the config. Every sink receives every record.
- Spec syntax:
  - `jsonl:-` (stdout, the default)
  - `jsonl:PATH`
  - `sqlite:PATH`
  - `postgresql://…`
  - `prometheus:…` and `mimir:…`, which are stubs that fail at validation.
- A sink failure is fatal: other sinks are closed cleanly, the run record is marked failed, and the exit code is 1.
- The stubs' docstrings note the label-cardinality risk of per-path series. Budgets and depths are what keep the series count bounded.

### CLI

```
dwusage capture ROOT... -o FILE.jsonl.gz [--depth N] [--largest N] [--max-dirs N]
                [--chain-max-dirs N] [--endpoint-max-files N] [--workers N]
dwusage plan    [--config F] [--tree NAME]... [--replay CAPTURE] [--format table|jsonl]
dwusage sample  [--config F] [--tree NAME]... [--sink SPEC]... [--replay CAPTURE] [--rediscover] [--workers N]
dwusage check-config [--config F]
dwusage db init --sink SPEC
```

- `plan` shows which dirs would be tracked, with the governing rule and the reason (depth, budget share, transit, excluded). It writes nothing and is the main tool for tuning config.
- `capture` needs no config, so it can be run now. It records raw data for each dir visited:
  - `lstat` fields;
  - every ceph xattr as its raw string, so the parsers are tested on the real format;
  - the child subdir names (`null` beyond the depth limit);
  - `captured_at`;
  - any error.
- The replay reader serves that data, and `sample --replay` uses `captured_at` as `sampled_at`. Several captures taken over days give a real time series for testing dashboards.
- Exit codes: 0 means success (per-dir errors are summarized on stderr and recorded in `run_end`); 1 means a fatal error; 2 means a usage error.

## Implementation milestones (TDD; `ruff format` + `ruff check` + `pytest` green at each)

1. **Scaffold, ceph reader and capture (time-sensitive).**
   - `pyproject.toml` (hatchling, src layout, `dwusage` entry point, `[postgres]` extra), `model.py`, `fs/ceph.py`, `fs/statx.py`, `capture.py`, `cli capture`.
   - Capture must also gather what later milestones need to replay:
     - **Pinned rctime chains**: capture keeps descending into rctime-suspect children past `--depth` until it reaches each chain's endpoint (own cap `--chain-max-dirs`). At endpoints it records the dir's own stat and `lstat`s its files (bounded, `--endpoint-max-files`), so the actual culprit entry is captured. This is the real fixture for the stuck-rctime workaround.
     - **Budget trees**: `--largest N` adds best-first-by-`rbytes` expansion beyond `--depth` (up to N extra dirs per root), so budget allocation with `max_depth` > depth can be replayed.
   - Then the user starts cron captures right away while the mounts are reachable, e.g. hourly for several days, into a directory outside the repo. Captures should be **wider than any one policy** (e.g. `--depth 3 --largest 2000` on `npx/user`, `npx/ana`, `npx/wipac`, `/volumes`, `/exp`), so different policies can be replayed later.
2. **Replay reader and FakeFs.** Write the test fixtures: a trimmed capture subset (the user decides whether real usernames may go in the repo; otherwise anonymize the names).
3. **Config and policy.** `config.py` and `policy.py`, with depth, exclude, transit and owning-tree rules, plus `check-config` and `plan`.
4. **Sampler and JSONL sink.** `sample` command, run records, and deterministic ordering.
5. **State cache and stuck-rctime workaround.**
   - Selection reuse, mtime child-list reuse, subtree skip and the tree lock.
   - `rctime.py`: suspect detection, rctime-free change key, `rctime_effective`, bad-entry trace and its reporting.
   - Test on the real `/npx` capture, which contains a pinned chain.
6. **Budget rules.** `budget.py` and its integration into discovery and `plan`.
7. **SQL sinks.** Shared DDL and migrations, SQLite, PostgreSQL (psycopg 3), `db init`, and `deploy/compose.yaml`. Postgres tests run only when `DWUSAGE_TEST_PG_DSN` is set.
8. **Stubs and docs.** Prometheus and Mimir stubs; README, `docs/terms.md` and `docs/conventions.md`; an example `dwusage.toml` covering the user's trees.

## Verification

- **Unit tests** (no live FS):
  - policy resolution: last-match, inheritance, transit, owning tree, exclude;
  - budget: proportionality, the caps, determinism, and zero/one shares;
  - change detection: mtime reuse;
  - suspect rctime:
    - a suspect dir is never skipped even when its rctime is unchanged;
    - a pinned parent does not mark its healthy siblings suspect;
    - `rctime_effective` and the trace endpoint are correct on a FakeFs with a planted future-ctime entry, both as a file and as a dir (own ctime in the future, which must not leak into the result);
    - for a healthy dir, `rctime_effective == rctime`;
    - tracked children's samples are reused, so the `/npx/user` root fits within the trace cap;
    - when the trace cap is hit, `rctime_effective` is null and a warning is printed;
  - config validation messages;
  - JSONL rendering;
  - SQLite sink round-trip and idempotent re-insert.
- **Invariant tests on the captured fixture**:
  - tracked set is connected;
  - no dir is owned by two trees;
  - sum of children's `rbytes` ≤ parent's `rbytes` × (1 + tolerance);
  - `rentries == rfiles + rsubdirs` (± tolerance).
- **End-to-end on the live mount** (read-only):
  - `dwusage capture /mnt/ceph1/npx/user --depth 1 -o …`;
  - `dwusage plan --replay …`;
  - `dwusage sample --replay … --sink jsonl:-` piped through `jq`;
  - `dwusage sample --sink sqlite:/tmp/x.db` against the live mount;
  - `podman compose -f deploy/compose.yaml up`, then `db init`, then sample into Postgres twice, and check with `psql` that both runs are in `samples_v`.
- `ruff format --check .` and `ruff check .` are clean.

## Out of scope / later

- Retention and downsampling (`prune`).
- Smarter depth heuristics for `npx/ana`.
- A finder for future-ctime files, to locate what pins rctime.
- Real Prometheus/Mimir sinks.
- Dashboards and alerts.
