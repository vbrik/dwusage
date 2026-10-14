import json

import pytest

from dwusage import __version__, cli
from dwusage.capture_file import read_jsonl
from fakefs import PINNED, FakeFs
from helpers import capture


def test_version(capsys):
    with pytest.raises(SystemExit) as exc:
        cli.main(["--version"])
    assert exc.value.code == 0
    assert capsys.readouterr().out.strip() == f"dwusage {__version__}"


def test_command_is_required():
    with pytest.raises(SystemExit) as exc:
        cli.main([])
    assert exc.value.code == 2


def squash(text: str) -> str:
    return " ".join(text.split())


def use(monkeypatch, fs: FakeFs) -> FakeFs:
    monkeypatch.setattr(cli, "CephFs", lambda roots: fs)
    return fs


@pytest.fixture
def fake(monkeypatch):
    fs = FakeFs()
    fs.mkdir("/r", rctime=PINNED)
    fs.mkdir("/r/a")
    fs.mkdir("/r/b", rctime=PINNED)
    fs.mkfile("/r/b/bad", mtime=PINNED)
    return use(monkeypatch, fs)


def test_capture_writes_file_and_summary(fake, tmp_path, capsys):
    out = tmp_path / "c.jsonl.gz"
    assert cli.main(["capture", "/r", "--depth", "1", "-o", str(out)]) == 0
    captured = capsys.readouterr()
    assert captured.out == ""
    err = squash(captured.err)
    assert "captured 3 dirs under 1 root" in err
    assert "along 1 chain: 1 end at entries with a future mtime or ctime" in err
    assert "(e.g. /r/b/bad, mtime 2106-02-07)" in err
    assert f"zcat {out} | jq" in err
    paths = [line["path"] for line in read_jsonl(str(out)) if line["type"] == "dir"]
    assert paths == ["/r", "/r/a", "/r/b"]


def test_capture_to_stdout(fake, capsys):
    assert cli.main(["capture", "/r", "--depth", "0", "-o", "-"]) == 0
    lines = [json.loads(raw) for raw in capsys.readouterr().out.splitlines()]
    assert lines[0]["type"] == "capture_start" and lines[1]["path"] == "/r"


def test_bad_root_is_an_error(fake, tmp_path, capsys):
    assert cli.main(["capture", "/missing", "-o", str(tmp_path / "c.jsonl")]) == 1
    assert capsys.readouterr().err.startswith("ERROR: /missing: ENOENT")
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize("output", ["no/c.jsonl", "", "dir/"])
def test_bad_output_fails_before_capturing(fake, tmp_path, capsys, output):
    (tmp_path / "dir").mkdir()
    assert cli.main(["capture", "/r", "-o", str(tmp_path / output)]) == 1
    assert squash(capsys.readouterr().err).startswith("ERROR: cannot write")
    assert fake.listed == []


@pytest.mark.parametrize("arg", ["--depth=-1", "--workers=0", "--max-dirs=x"])
def test_bad_numbers_are_usage_errors(fake, arg):
    with pytest.raises(SystemExit) as exc:
        cli.main(["capture", "/r", arg, "-o", "-"])
    assert exc.value.code == 2


def test_limit_reached_exits_3_and_names_the_root(fake, capsys):
    assert cli.main(["capture", "/r", "--depth", "1", "--max-dirs", "1", "-o", "-"]) == 3
    assert "WARNING: --max-dirs reached under /r;" in squash(capsys.readouterr().err)


def test_stale_and_undetermined_chains_are_counted(monkeypatch, capsys):
    fs = FakeFs()
    fs.mkdir("/r/stale", rctime=PINNED)
    fs.mkdir("/r/stale/a")
    fs.mkdir("/r/unknown", rctime=PINNED)
    fs.mkdir("/r/unknown/secret", rctime=PINNED, deny=True)
    use(monkeypatch, fs)
    assert cli.main(["capture", "/r", "--depth", "1", "-o", "-"]) == 0
    err = squash(capsys.readouterr().err)
    assert "along 2 chains: 1 are stale" in err and "1 are undetermined" in err


def test_interrupt_removes_partial_output(monkeypatch, tmp_path, capsys):
    class InterruptingFs(FakeFs):
        def inspect(self, path, xattr_names):
            if path == "/r/a":
                raise KeyboardInterrupt
            return super().inspect(path, xattr_names)

    fs = InterruptingFs()
    fs.mkdir("/r/a")
    use(monkeypatch, fs)
    with pytest.raises(KeyboardInterrupt):
        cli.main(["capture", "/r", "-o", str(tmp_path / "c.jsonl")])
    assert capsys.readouterr().err == "ERROR: interrupted; nothing written\n"
    assert list(tmp_path.iterdir()) == []


def test_summary_warns_without_openat2():
    fs = FakeFs()
    fs.mkdir("/r")
    lines = cli.capture_summary(capture(fs, ["/r"]), n_roots=1, output="-", openat2=False)
    assert any(line.startswith("WARNING: openat2 is unavailable") for line in lines)
