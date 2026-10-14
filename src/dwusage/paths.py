"""Helpers for the absolute, normalized paths used throughout."""


def path_key(path: str) -> tuple[str, ...]:
    """Return a sort key that keeps every subtree contiguous: "/a", "/a/x", "/a b"."""
    return tuple(path.split("/"))


def printable(text: str) -> str:
    """Return `text` with every non-printable character escaped, for messages to a terminal.

    Control characters become ``\\xNN``, undecodable bytes (surrogate escapes) ``\\xNN`` of the
    original byte, and other non-printable characters ``\\uNNNN``. A name from the filesystem
    then can't move the cursor, clear the screen or start a forged line.
    """
    out = []
    for char in text:
        code = ord(char)
        if char.isprintable():
            out.append(char)
        elif 0xDC80 <= code <= 0xDCFF:
            out.append(f"\\x{code - 0xDC00:02x}")
        elif code < 0x100:
            out.append(f"\\x{code:02x}")
        else:
            out.append(f"\\u{code:04x}" if code <= 0xFFFF else f"\\U{code:08x}")
    return "".join(out)


def is_below(path: str, parent: str) -> bool:
    """Return True if `path` is strictly inside directory `parent`."""
    return path != parent and path.startswith(parent.rstrip("/") + "/")
