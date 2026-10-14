"""Recognizing CephFS recursive ctimes (rctime) that are pinned in the future.

The MDS sets a directory's rctime to the newest ctime *or mtime* in its subtree, and never
lowers it. mtime is user-settable (``touch -d``, unpacking archives with preserved dates), so
one entry with a future mtime pins the rctime of every ancestor, and the value stays even
after that entry is fixed or removed. Such values are recognized by comparing them with the
observation time.
"""

from typing import Final

FUTURE_SLACK_S: Final = 86_400.0
"""Timestamps more than this many seconds after the observation are bogus; absorbs clock skew."""


def is_future(t: float, now: float, slack: float = FUTURE_SLACK_S) -> bool:
    """Return True if timestamp `t` (seconds since the epoch) is implausibly far after `now`."""
    return t > now + slack
