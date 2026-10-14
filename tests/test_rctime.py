from dwusage.rctime import FUTURE_SLACK_S, is_future


def test_is_future_allows_clock_skew_up_to_slack():
    now = 1_000_000.0
    assert not is_future(now - 10, now)
    assert not is_future(now + FUTURE_SLACK_S, now)
    assert is_future(now + FUTURE_SLACK_S + 1, now)


def test_pinned_values_seen_on_cephfs_are_future():
    assert is_future(4294967295.0, 1791990850.0)
    assert is_future(2147483648.0, 1791990850.0)
