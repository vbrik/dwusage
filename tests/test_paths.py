from dwusage.paths import is_below, path_key, printable


def test_path_key_keeps_subtrees_contiguous():
    paths = ["/r/a b", "/r/a/x", "/r/a", "/r", "/r/a-b"]
    assert sorted(paths, key=path_key) == ["/r", "/r/a", "/r/a/x", "/r/a b", "/r/a-b"]


def test_is_below():
    assert is_below("/r/a", "/r")
    assert is_below("/r", "/")
    assert not is_below("/r", "/r")
    assert not is_below("/", "/")
    assert not is_below("/rx", "/r")


def test_printable_escapes_what_could_control_a_terminal():
    assert printable("/r/ok name/café") == "/r/ok name/café"
    assert printable("/r/a\x1b[2J\nb\x7f") == "/r/a\\x1b[2J\\x0ab\\x7f"
    assert printable("/r/caf\udce9") == "/r/caf\\xe9"  # an undecodable byte
    assert printable("/r/\u2028x") == "/r/\\u2028x"
