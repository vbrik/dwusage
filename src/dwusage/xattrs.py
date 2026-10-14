"""Names and value formats of the CephFS virtual extended attributes."""

import re
from typing import Final

DIR_STATS: Final = (
    "ceph.dir.entries",
    "ceph.dir.files",
    "ceph.dir.subdirs",
    "ceph.dir.rentries",
    "ceph.dir.rfiles",
    "ceph.dir.rsubdirs",
    "ceph.dir.rbytes",
    "ceph.dir.rctime",
)
"""Immediate and recursive directory statistics, maintained by the MDS.

``rentries`` and ``rsubdirs`` count the directory itself: an empty dir has 1 of each.
"""

OPTIONAL: Final = (
    "ceph.quota.max_bytes",
    "ceph.quota.max_files",
    "ceph.dir.layout.pool",
    "ceph.dir.pin",
)
"""Attributes that may be absent (ENODATA) unless set on, or inherited by, the directory."""

ALL: Final = DIR_STATS + OPTIONAL

_TIME_RE: Final = re.compile(r"(\d+)(?:\.(\d{1,9}))?", re.ASCII)


def parse_time(value: str) -> float:
    """Return a ceph timestamp such as ``1791990850.239100976`` as float seconds since the epoch.

    Raises ValueError for anything other than ``SECONDS[.FRACTION]`` in decimal digits.
    """
    match = _TIME_RE.fullmatch(value)
    if match is None:
        raise ValueError(f"not a ceph timestamp: {value!r}")
    seconds, fraction = match.groups()
    return int(seconds) + (int(fraction) / 10 ** len(fraction) if fraction else 0.0)
