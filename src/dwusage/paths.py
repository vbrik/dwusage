"""Helpers for the absolute, normalized paths used throughout."""


def path_key(path: str) -> tuple[str, ...]:
    """Return a sort key that keeps every subtree contiguous: "/a", "/a/x", "/a b"."""
    return tuple(path.split("/"))


def is_below(path: str, parent: str) -> bool:
    """Return True if `path` is strictly inside directory `parent`."""
    return path != parent and path.startswith(parent.rstrip("/") + "/")
