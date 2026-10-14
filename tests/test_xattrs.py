import pytest

from dwusage.xattrs import ALL, parse_time


def test_parse_time_real_values():
    assert parse_time("1791990850.239100976") == pytest.approx(1791990850.239100976)
    assert parse_time("4294967295.000000000") == 4294967295.0
    assert parse_time("1697051072") == 1697051072.0
    assert parse_time("5.5") == 5.5


@pytest.mark.parametrize("bad", ["", "abc", "1.2.3", "inf", "nan", "-5.0", "1.", "1e9", " 1"])
def test_parse_time_rejects_other_formats(bad):
    with pytest.raises(ValueError, match="not a ceph timestamp"):
        parse_time(bad)


def test_all_names_are_unique_ceph_xattrs():
    assert len(set(ALL)) == len(ALL)
    assert all(name.startswith("ceph.") for name in ALL)
    assert "ceph.dir.rctime" in ALL and "ceph.quota.max_bytes" in ALL
