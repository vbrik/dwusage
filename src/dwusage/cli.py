"""Command-line interface: parses arguments, calls the library, prints results."""

import argparse
import sys
from collections.abc import Sequence
from typing import NoReturn

from dwusage import __version__


def build_parser() -> argparse.ArgumentParser:
    """Return the top-level parser with one subparser per command."""
    parser = argparse.ArgumentParser(
        prog="dwusage", description="Track how CephFS directory usage changes over time."
    )
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    parser.add_subparsers(dest="command", required=True, metavar="COMMAND")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Run the command line; return the exit status."""
    build_parser().parse_args(argv)
    return 0


def entry() -> NoReturn:
    """Console entry point."""
    sys.exit(main())
