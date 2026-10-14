import errno
import json
import os
import stat

import pytest

from dwusage.capture import CaptureLimits
from dwusage.capture_file import SCHEMA, Output, capture_lines, read_jsonl
from fakefs import PINNED, FakeFs
from helpers import capture


def lines_for(fs: FakeFs, **limits: int) -> list[dict]:
    result = capture(fs, ["/r"], **limits)
    return list(
        capture_lines(
            result,
            roots=["/r"],
            limits=CaptureLimits(**limits),
            host="h",
            version="v",
            openat2=True,
        )
    )


def test_record_layout():
    fs = FakeFs()
    fs.mkdir("/r", rctime=PINNED)
    fs.mkfile("/r/bad", mtime=PINNED)
    fs.mkdir("/r/a")
    start, root, a, end = lines_for(fs, depth=1)
    assert start["type"] == "capture_start" and start["schema"] == SCHEMA
    assert start["limits"]["depth"] == 1 and start["roots"] == ["/r"] and start["openat2"]
    assert root["type"] == "dir" and root["subdirs"] == ["a"] and root["reason"] == "root"
    assert root["stat"]["ctime_ns"] == 1000 * 10**9
    assert root["xattrs"]["ceph.dir.rctime"] == "4294967295.000000000"
    assert root["files"]["future"][0]["name"] == "bad"
    assert a["subdirs"] is None and a["files"] is None and a["errors"] == {}
    assert a["skipped"] is None
    assert end == {
        "type": "capture_end",
        "elapsed_s": end["elapsed_s"],
        "n_dirs": 2,
        "n_errors": 0,
        "truncated_roots": (),
        "chain_truncated_roots": (),
        "chain_ends": {"/r": "culprit"},
        "oversized": {},
    }


def test_gzip_file_is_private_and_round_trips(tmp_path):
    fs = FakeFs()
    fs.mkdir("/r/a")
    out = tmp_path / "cap.jsonl.gz"
    lines = lines_for(fs, depth=1)
    with Output(str(out)) as output:
        output.write(lines)
    assert out.read_bytes()[:2] == b"\x1f\x8b"  # gzip magic
    assert stat.S_IMODE(out.stat().st_mode) == 0o600
    assert list(read_jsonl(str(out))) == json.loads(json.dumps(lines))
    assert [p.name for p in tmp_path.iterdir()] == ["cap.jsonl.gz"]


def test_plain_file_and_stdout(tmp_path, capsys):
    lines = [{"type": "x", "n": 1}]
    with Output(str(tmp_path / "cap.jsonl")) as output:
        output.write(lines)
    assert (tmp_path / "cap.jsonl").read_text() == '{"type":"x","n":1}\n'
    with Output("-") as output:
        output.write(lines)
    assert capsys.readouterr().out == '{"type":"x","n":1}\n'


def test_bad_destinations_fail_before_anything_is_written(tmp_path):
    with pytest.raises(IsADirectoryError):
        Output(str(tmp_path))
    with pytest.raises(IsADirectoryError):
        Output(str(tmp_path / "new") + "/")
    with pytest.raises(FileNotFoundError):
        Output(str(tmp_path / "missing" / "cap.jsonl"))
    assert list(tmp_path.iterdir()) == []


def test_unwritten_or_interrupted_output_leaves_nothing(tmp_path):
    with Output(str(tmp_path / "cap.jsonl.gz")):
        pass

    def lines():
        yield {"type": "capture_start"}
        raise KeyboardInterrupt

    with pytest.raises(KeyboardInterrupt), Output(str(tmp_path / "cap.jsonl")) as output:
        output.write(lines())
    assert list(tmp_path.iterdir()) == []


def test_odd_names_round_trip(tmp_path):
    fs = FakeFs()
    fs.mkdir("/r/new\nline")
    fs.mkdir("/r/latin1-\udce9")  # undecodable byte 0xE9, as os.scandir returns it
    out = tmp_path / "cap.jsonl"
    with Output(str(out)) as output:
        output.write(lines_for(fs, depth=1))
    paths = [line["path"] for line in read_jsonl(str(out)) if line["type"] == "dir"]
    assert paths == ["/r", "/r/latin1-\udce9", "/r/new\nline"]
    assert len(out.read_text().splitlines()) == 5  # a newline in a name doesn't split a record


def test_read_jsonl_reports_bad_line(tmp_path):
    path = tmp_path / "bad.jsonl"
    path.write_text('{"type":"capture_start"}\nnot json\n')
    with pytest.raises(json.JSONDecodeError):
        list(read_jsonl(str(path)))


def test_existing_non_regular_destination_is_refused(tmp_path):
    fifo = tmp_path / "fifo"
    os.mkfifo(fifo)
    link = tmp_path / "link"
    link.symlink_to(tmp_path / "elsewhere")
    for path in (fifo, link):
        with pytest.raises(OSError, match="not a regular file"):
            Output(str(path))
    assert stat.S_ISFIFO(fifo.lstat().st_mode) and link.is_symlink()
    assert sorted(p.name for p in tmp_path.iterdir()) == ["fifo", "link"]


def test_existing_regular_file_is_replaced(tmp_path):
    out = tmp_path / "cap.jsonl"
    out.write_text("old\n")
    with Output(str(out)) as output:
        output.write([{"type": "x"}])
    assert out.read_text() == '{"type":"x"}\n'


def test_empty_destination_is_refused(tmp_path, monkeypatch):
    (tmp_path / "cwd").mkdir()
    monkeypatch.chdir(tmp_path / "cwd")
    with pytest.raises(OSError) as exc:
        Output("")
    assert exc.value.errno == errno.EINVAL
    assert [p.name for p in tmp_path.iterdir()] == ["cwd"]
