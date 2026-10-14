"""Command-line interface: parses arguments, calls the library, prints results."""

import argparse
import os
import signal
import socket
import sys
from collections import Counter
from collections.abc import Callable, Sequence
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
from datetime import UTC, datetime
from typing import Final, NoReturn

from dwusage import __version__
from dwusage.capture import CaptureLimits, Capturer, CaptureResult, ChainEnd, check_roots
from dwusage.capture_file import Output, capture_lines
from dwusage.fs.base import describe_oserror
from dwusage.fs.ceph import CephFs
from dwusage.paths import printable

DEFAULT_WORKERS: Final = 16
"""Parallel filesystem operations; enough to hide MDS latency without flooding it."""

EXIT_INCOMPLETE: Final = 3
"""Exit status when a run finished but a limit left part of the requested work undone."""

EXIT_INTERRUPTED: Final = 130

ROOT_WITHOUT_OPENAT2: Final = (
    "openat2 is unavailable (Linux < 5.6 or a seccomp filter); refusing to run as root, where "
    "a directory swapped for a symlink could redirect reads. Use a newer kernel, or run as a "
    "non-root user (unreadable dirs are then recorded as errors)."
)

CAPTURE_DESCRIPTION: Final = """\
Record the stat, CephFS xattrs and subdirectory names of directories under each ROOT, for
replay and diagnostics without access to the filesystem. Read-only except for OUTPUT.

Dirs fewer than --depth levels below ROOT are listed: their subdirs are recorded and
visited. Dirs whose rctime is in the future (pinned) are listed at any depth, down to the end
of each chain, and their files with a future mtime or ctime are recorded: those pin rctime.
--largest N then visits up to N more dirs per ROOT, listing the biggest dirs first.

Output is JSON Lines (capture_start, one "dir" record per directory, capture_end), mode 0600
because it reveals names and sizes of private directories. Example:
  zcat cap.jsonl.gz | jq -r 'select(.type=="dir" and .errors!={}) | .path'
A summary goes to stderr. Unreadable dirs are recorded in .errors and don't fail the run.
As root it refuses to run without openat2 (Linux 5.6 or later), which keeps symlinks swapped
in during the walk from redirecting reads.
Exit status: 0 done, 1 error, 2 usage error, 3 a limit left work undone, 130 interrupted.
"""


def _int_at_least(minimum: int) -> Callable[[str], int]:
    def parse(text: str) -> int:
        try:
            value = int(text)
        except ValueError:
            raise argparse.ArgumentTypeError(f"not an integer: {text!r}") from None
        if value < minimum:
            raise argparse.ArgumentTypeError(f"must be at least {minimum}: {value}")
        return value

    return parse


def build_parser() -> argparse.ArgumentParser:
    """Return the top-level parser with one subparser per command."""
    parser = argparse.ArgumentParser(
        prog="dwusage", description="Track how CephFS directory usage changes over time."
    )
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    commands = parser.add_subparsers(dest="command", required=True, metavar="COMMAND")

    defaults = CaptureLimits()
    nonneg, positive = _int_at_least(0), _int_at_least(1)
    cap = commands.add_parser(
        "capture",
        help="record raw CephFS state for offline replay",
        description=CAPTURE_DESCRIPTION,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    cap.add_argument("roots", nargs="+", metavar="ROOT", help="directory to capture")
    cap.add_argument(
        "-o", "--output", required=True, help="output file (.gz compresses); - for stdout"
    )
    options = (
        ("--depth", nonneg, defaults.depth, "list dirs fewer than N levels below ROOT"),
        ("--largest", nonneg, defaults.largest, "then visit N more dirs per ROOT, biggest first"),
        ("--max-dirs", positive, defaults.max_dirs, "visit at most N dirs per ROOT"),
        (
            "--max-list-entries",
            nonneg,
            defaults.max_list_entries,
            "don't list dirs with more than N entries",
        ),
        (
            "--chain-max-dirs",
            nonneg,
            defaults.chain_max_dirs,
            "visit at most N dirs per ROOT only to follow pinned rctimes",
        ),
        (
            "--pinned-max-files",
            nonneg,
            defaults.pinned_max_files,
            "stat at most N files per pinned dir",
        ),
        ("--workers", positive, DEFAULT_WORKERS, "parallel filesystem operations"),
    )
    for flag, parse, default, text in options:
        metavar = None if flag == "--depth" else "N"
        cap.add_argument(
            flag,
            type=parse,
            default=default,
            metavar=metavar,
            help=f"{text} (default: %(default)s)",
        )
    return parser


def _error(message: str) -> None:
    print(f"ERROR: {message}", file=sys.stderr)


def _date(t: float) -> str:
    try:
        return datetime.fromtimestamp(t, UTC).strftime("%Y-%m-%d")
    except (OverflowError, OSError, ValueError):
        return f"{t:.0f}"


def _chain_summary(result: CaptureResult, output: str) -> str | None:
    if not result.chain_ends:
        return None
    counts = Counter(result.chain_ends.values())
    parts = []
    if n := counts[ChainEnd.CULPRIT]:
        path = next(p for p, end in result.chain_ends.items() if end is ChainEnd.CULPRIT)
        entry = result.records[path].files.future[0]
        which = "mtime" if entry.stat.mtime_ns >= entry.stat.ctime_ns else "ctime"
        newest = max(entry.stat.mtime_ns, entry.stat.ctime_ns) / 1e9
        culprit = printable(path if entry.name == "." else os.path.join(path, entry.name))
        parts.append(
            f"{n} end at entries with a future mtime or ctime "
            f"(e.g. {culprit}, {which} {_date(newest)})"
        )
    if n := counts[ChainEnd.STALE]:
        parts.append(f"{n} are stale (that entry was changed or removed; rctime never decreases)")
    if n := counts[ChainEnd.UNDETERMINED]:
        parts.append(
            f"{n} are undetermined (not listed, an unreadable or vanished subdir, "
            "or a file scan cut short)"
        )
    query = "jq -c 'select(.type==\"capture_end\").chain_ends'"
    if output != "-":
        query = f"{'zcat' if output.endswith('.gz') else 'cat'} {output} | {query}"
    return (
        f"rctime is pinned in the future along {len(result.chain_ends)} "
        f"chain{'s' * (len(result.chain_ends) != 1)}: "
        f"{'; '.join(parts)}. List them: {query}"
    )


def capture_summary(
    result: CaptureResult, *, n_roots: int, output: str, openat2: bool
) -> list[str]:
    """Return the stderr summary of a capture: counts, limits reached, pinned rctimes."""
    lines = [
        f"captured {len(result.records)} dirs under {n_roots} root{'s' * (n_roots != 1)} "
        f"in {result.elapsed_s:.0f}s; {result.n_errors} with errors (see .errors)"
    ]
    if result.truncated_roots:
        lines.append(
            f"WARNING: --max-dirs reached under {', '.join(result.truncated_roots)}; "
            "deeper dirs are missing. Raise --max-dirs, or lower --depth or --largest."
        )
    if result.chain_truncated_roots:
        lines.append(
            f"WARNING: --chain-max-dirs reached under {', '.join(result.chain_truncated_roots)}; "
            "some pinned-rctime chains were not followed to the end."
        )
    if result.oversized:
        n = sum(result.oversized.values())
        lines.append(
            f"WARNING: --max-list-entries kept {n} dir{'s' * (n != 1)} under "
            f"{', '.join(result.oversized)} from being listed; their subdirs are missing. "
            "Raise --max-list-entries."
        )
    if not openat2:
        lines.append(
            "WARNING: openat2 is unavailable (Linux < 5.6 or a seccomp filter); symlinks "
            "swapped into intermediate path components during the capture went undetected."
        )
    if chains := _chain_summary(result, output):
        lines.append(chains)
    return lines


def run_capture(args: argparse.Namespace) -> int:
    """Run `dwusage capture`; return the exit status."""
    roots = [os.path.abspath(root) for root in args.roots]
    limits = CaptureLimits(
        depth=args.depth,
        largest=args.largest,
        max_dirs=args.max_dirs,
        max_list_entries=args.max_list_entries,
        chain_max_dirs=args.chain_max_dirs,
        pinned_max_files=args.pinned_max_files,
    )
    with ExitStack() as cleanup:
        fs = cleanup.enter_context(CephFs(roots))
        problems = check_roots(fs, roots)
        if not problems and not fs.openat2 and os.geteuid() == 0:  # check_roots probed it
            problems.append(ROOT_WITHOUT_OPENAT2)
        try:
            output = cleanup.enter_context(Output(args.output))
        except OSError as exc:
            problems.append(f"cannot write {args.output}: {describe_oserror(exc)}")
        if problems:
            for problem in problems:
                _error(problem)
            return 1
        pool = ThreadPoolExecutor(args.workers)
        writing_stdout = False
        try:
            result = Capturer(fs, limits, pool).run(roots)
            lines = capture_lines(
                result,
                roots=roots,
                limits=limits,
                host=socket.gethostname(),
                version=__version__,
                openat2=fs.openat2,
            )
            writing_stdout = args.output == "-"
            output.write(lines)
        except KeyboardInterrupt:
            _error(
                f"interrupted; {'stdout is incomplete' if writing_stdout else 'nothing written'}"
            )
            raise
        except OSError as exc:  # only writing the output can fail as a whole
            _error(f"cannot write {args.output}: {describe_oserror(exc)}")
            return 1
        finally:
            pool.shutdown(wait=False, cancel_futures=True)
    for line in capture_summary(result, n_roots=len(roots), output=args.output, openat2=fs.openat2):
        print(line, file=sys.stderr)
    limited = result.truncated_roots or result.chain_truncated_roots or result.oversized
    return EXIT_INCOMPLETE if limited else 0


def main(argv: Sequence[str] | None = None) -> int:
    """Run the command line; return the exit status. KeyboardInterrupt propagates."""
    args = build_parser().parse_args(argv)
    match args.command:
        case "capture":
            return run_capture(args)
    raise AssertionError(f"unhandled command {args.command}")


def _interrupt(signum: int, frame: object) -> NoReturn:
    raise KeyboardInterrupt


def entry() -> NoReturn:
    """Console entry point: `main`, but SIGTERM acts like Ctrl-C and either exits at once.

    Worker threads may be blocked in a slow filesystem call; waiting for them would delay
    the exit for minutes, and they hold nothing that needs cleaning up.
    """
    signal.signal(signal.SIGTERM, _interrupt)
    try:
        status = main()
    except KeyboardInterrupt:
        status = EXIT_INTERRUPTED
    sys.stdout.flush()
    sys.stderr.flush()
    os._exit(status)
