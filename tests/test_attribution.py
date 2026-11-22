"""Batch traffic attribution against the bounded routing history.

The service-level tests drive a deterministic fake clock so change instants
are exact; the HTTP tests below them exercise validation and response shape
through the real FastAPI app.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from ipaddress import IPv4Address, IPv4Network

import pytest

from app.demo import BACKUP, PRIMARY
from app.service import RouteService

UTC = timezone.utc
T0 = datetime(2026, 11, 22, 10, 0, 0, tzinfo=UTC)
PREFIX = "203.0.113.0/24"
DEST = "203.0.113.10"


class FakeClock:
    def __init__(self, start: datetime) -> None:
        self.now = start

    def __call__(self) -> datetime:
        return self.now

    def advance(self, seconds: float) -> datetime:
        self.now += timedelta(seconds=seconds)
        return self.now


@pytest.fixture()
def clock() -> FakeClock:
    return FakeClock(T0)


@pytest.fixture()
def svc(clock: FakeClock) -> RouteService:
    return RouteService(
        neighbor_priorities={PRIMARY: 10, BACKUP: 20},
        default_neighbor_priority=100,
        grace_period_seconds=30.0,
        history_retention_seconds=600.0,
        clock=clock,
    )


def _baseline(svc: RouteService, clock: FakeClock) -> None:
    """Both neighbours established at T0; primary wins the shared prefix."""
    svc.reconnect(PRIMARY, 1, grace_seconds=0)
    svc.announce(PRIMARY, 1, [(IPv4Network(PREFIX),
                               IPv4Address("10.0.0.1"))])
    svc.end_of_rib(PRIMARY, 1)
    svc.reconnect(BACKUP, 1, grace_seconds=0)
    svc.announce(BACKUP, 1, [(IPv4Network(PREFIX),
                              IPv4Address("10.0.0.2"))])
    svc.end_of_rib(BACKUP, 1)
    clock.advance(60)


# ----------------------------------------------------------- service-level
def test_full_interval_on_one_neighbor_is_attributed(svc, clock):
    _baseline(svc, clock)
    start, end = clock.now, clock.advance(120)

    report = svc.attribution_report([
        (_ip(DEST), start, end, 1000),
        (_ip("203.0.113.99"), start, end, 250),
    ])

    assert len(report.records) == 2
    assert all(r.outcome == "attributed" for r in report.records)
    assert report.neighbor_totals == {PRIMARY: 1250}
    first = report.records[0]
    assert first.neighbor == PRIMARY
    assert first.prefix is not None and str(first.prefix) == PREFIX
    assert str(first.next_hop) == "10.0.0.1"
    assert first.first_change_at is None


def test_half_open_interval_includes_start_and_excludes_end(svc, clock):
    _baseline(svc, clock)
    start = clock.now
    change_at = clock.advance(60)
    # Backup becomes the only route exactly at change_at.
    svc.withdraw(PRIMARY, 1, [_net(PREFIX)])
    clock.advance(60)

    # End exactly on the change instant: change is excluded -> still primary.
    report = svc.attribution_report([(_ip(DEST), start, change_at, 100)])
    assert report.records[0].outcome == "attributed"
    assert report.records[0].neighbor == PRIMARY

    # Start exactly on the change instant: change is included -> backup.
    report = svc.attribution_report(
        [(_ip(DEST), change_at, change_at + timedelta(seconds=30), 100)]
    )
    assert report.records[0].outcome == "attributed"
    assert report.records[0].neighbor == BACKUP


def test_switch_away_and_back_is_changed_route_with_first_change(svc, clock):
    _baseline(svc, clock)
    start = clock.now
    first_change = clock.advance(60)
    svc.withdraw(PRIMARY, 1, [_net(PREFIX)])  # traffic moves to backup
    clock.advance(60)
    svc.announce(PRIMARY, 1, [(_net(PREFIX), _ip("10.0.0.1"))])  # moves back
    end = clock.advance(60)

    report = svc.attribution_report([(_ip(DEST), start, end, 500)])
    record = report.records[0]

    assert record.outcome == "changed_route"
    assert record.first_change_at == first_change
    assert record.neighbor is None
    assert PRIMARY not in report.neighbor_totals
    assert BACKUP not in report.neighbor_totals


def test_no_route_for_whole_interval_is_listed(svc, clock):
    _baseline(svc, clock)
    svc.withdraw(PRIMARY, 1, [_net(PREFIX)])
    svc.withdraw(BACKUP, 1, [_net(PREFIX)])
    start = clock.advance(30)
    end = clock.advance(90)

    report = svc.attribution_report([
        (_ip(DEST), start, end, 400),
        (_ip("192.0.2.77"), start, end, 10),  # never had a route
    ])

    assert [r.outcome for r in report.records] == ["no_route", "no_route"]
    assert report.neighbor_totals == {}


def test_route_appearing_mid_interval_is_changed_route(svc, clock):
    # Backup holds the prefix at first, withdraws it (no route), then announces
    # it again inside the interval: selection goes from a route to none.
    svc.reconnect(BACKUP, 1, grace_seconds=0)
    svc.announce(BACKUP, 1, [(_net(PREFIX), _ip("10.0.0.2"))])
    svc.end_of_rib(BACKUP, 1)
    clock.advance(30)
    start = clock.now
    change_at = clock.advance(60)
    svc.withdraw(BACKUP, 1, [_net(PREFIX)])
    clock.advance(30)
    svc.announce(BACKUP, 1, [(_net(PREFIX), _ip("10.0.0.2"))])
    end = clock.advance(30)

    report = svc.attribution_report([(_ip(DEST), start, end, 9)])
    record = report.records[0]
    assert record.outcome == "changed_route"
    assert record.first_change_at == change_at


def test_route_disappearing_mid_interval_is_changed_route(svc, clock):
    _baseline(svc, clock)
    start = clock.now
    change_at = clock.advance(60)
    svc.withdraw(PRIMARY, 1, [_net(PREFIX)])
    svc.withdraw(BACKUP, 1, [_net(PREFIX)])
    end = clock.advance(60)

    record = svc.attribution_report([(_ip(DEST), start, end, 9)]).records[0]
    assert record.outcome == "changed_route"
    assert record.first_change_at == change_at


def test_route_appearing_from_no_route_is_changed_not_no_route(svc, clock):
    # The archive starts with a withdrawal state, then an announcement
    # re-introduces a route inside the interval.
    svc.reconnect(BACKUP, 1, grace_seconds=0)
    svc.announce(BACKUP, 1, [(_net(PREFIX), _ip("10.0.0.2"))])
    svc.end_of_rib(BACKUP, 1)
    clock.advance(10)
    svc.withdraw(BACKUP, 1, [_net(PREFIX)])
    start = clock.now
    change_at = clock.advance(60)
    svc.announce(BACKUP, 1, [(_net(PREFIX), _ip("10.0.0.2"))])
    end = clock.advance(60)

    record = svc.attribution_report([(_ip(DEST), start, end, 9)]).records[0]
    assert record.outcome == "changed_route"
    assert record.first_change_at == change_at
    assert record.neighbor is None


def test_insufficient_history_before_first_archive(svc, clock):
    _baseline(svc, clock)  # first archive at T0
    report = svc.attribution_report([
        (_ip(DEST), T0 - timedelta(seconds=1), clock.now, 100),
    ])
    assert report.records[0].outcome == "insufficient_history"


def test_insufficient_history_for_future_interval(svc, clock):
    _baseline(svc, clock)
    start = clock.now
    report = svc.attribution_report([
        (_ip(DEST), start, start + timedelta(seconds=1), 100),
    ])
    assert report.records[0].outcome == "insufficient_history"


def test_empty_history_marks_every_record_insufficient(svc, clock):
    start, end = clock.now, clock.advance(60)
    report = svc.attribution_report([(_ip(DEST), start, end, 1)])
    assert report.history_horizon is None
    assert report.records[0].outcome == "insufficient_history"


def test_only_changed_prefixes_are_archived(svc, clock):
    svc.reconnect(PRIMARY, 1, grace_seconds=0)
    svc.announce(PRIMARY, 1, [(_net(PREFIX), _ip("10.0.0.1"))])
    svc.end_of_rib(PRIMARY, 1)
    # Identical re-announcement: same neighbour, next hop, confirmed state.
    svc.announce(PRIMARY, 1, [(_net(PREFIX), _ip("10.0.0.1"))])

    history = svc.history_prefix_count()
    assert history == 1
    entries = _entries(svc, PREFIX, PRIMARY)
    assert len(entries) == 1


def test_pruning_keeps_baseline_effective_at_window_start(svc, clock):
    _baseline(svc, clock)
    baseline_start = T0  # earliest retained entry
    clock.advance(540)  # T0 + 600: T0 entry hits the cutoff
    # This announce triggers pruning; the T0 baseline must survive.
    svc.announce(BACKUP, 1, [(_net("198.51.100.0/24"), _ip("10.0.0.2"))])

    entries = _entries(svc, PREFIX, PRIMARY)
    assert len(entries) == 1
    assert entries[0].changed_at == T0  # still reconstructable at window start

    # An interval starting at the retained baseline remains fully covered.
    report = svc.attribution_report([
        (_ip(DEST), baseline_start, clock.now, 77),
    ])
    assert report.records[0].outcome == "attributed"
    assert report.records[0].neighbor == PRIMARY


def test_pruned_baseline_leaves_older_intervals_insufficient(svc, clock):
    _baseline(svc, clock)
    ancient_start = T0
    ancient_end = T0 + timedelta(seconds=30)
    clock.advance(700)  # well past the 10-minute window
    # A fresh announcement on the primary pair triggers pruning of that
    # pair's T0 baseline; the old interval can no longer be reconstructed.
    svc.announce(PRIMARY, 1, [
        (_net("198.18.0.0/15"), _ip("10.0.0.1")),
    ])

    report = svc.attribution_report([
        (_ip(DEST), ancient_start, ancient_end, 1),
    ])
    assert report.records[0].outcome == "insufficient_history"


def test_withdrawn_prefix_history_entry_is_removed_when_fully_pruned(svc, clock):
    svc.reconnect(PRIMARY, 1, grace_seconds=0)
    svc.announce(PRIMARY, 1, [(_net(PREFIX), _ip("10.0.0.1"))])
    svc.end_of_rib(PRIMARY, 1)
    clock.advance(10)
    svc.withdraw(PRIMARY, 1, [_net(PREFIX)])
    assert svc.history_prefix_count() == 1

    clock.advance(700)  # both entries fall outside the window
    # A later mutation of the same pair triggers pruning; with no retained
    # state the whole archive for the prefix disappears.
    svc.announce(PRIMARY, 1, [
        (_net("198.18.0.0/15"), _ip("10.0.0.1")),
    ])
    assert not any(
        key[0] == _net(PREFIX) for key in svc._freeze_history()
    )


def test_reset_clears_history_and_recording_restarts(svc, clock):
    _baseline(svc, clock)
    svc.reset()
    assert svc.history_prefix_count() == 0

    start, end = clock.now, clock.advance(30)
    report = svc.attribution_report([(_ip(DEST), start, end, 1)])
    assert report.records[0].outcome == "insufficient_history"

    svc.reconnect(PRIMARY, 1, grace_seconds=0)
    svc.announce(PRIMARY, 1, [(_net(PREFIX), _ip("10.0.0.1"))])
    svc.end_of_rib(PRIMARY, 1)
    start2 = clock.now
    end2 = clock.advance(30)
    report = svc.attribution_report([(_ip(DEST), start2, end2, 5)])
    assert report.records[0].outcome == "attributed"
    assert report.neighbor_totals == {PRIMARY: 5}


def test_end_of_rib_batch_purge_shares_one_effective_instant(svc, clock):
    svc.reconnect(PRIMARY, 1, grace_seconds=0)
    svc.announce(PRIMARY, 1, [
        (_net(PREFIX), _ip("10.0.0.1")),
        (_net("198.51.100.0/24"), _ip("10.0.0.1")),
    ])
    svc.end_of_rib(PRIMARY, 1)
    svc.reconnect(BACKUP, 1, grace_seconds=0)
    svc.announce(BACKUP, 1, [(_net(PREFIX), _ip("10.0.0.2"))])
    svc.end_of_rib(BACKUP, 1)

    clock.advance(30)
    svc.reconnect(PRIMARY, 2, grace_seconds=600)  # routes retained pending
    clock.advance(1)
    svc.end_of_rib(PRIMARY, 2)  # batch purge, single instant in history
    purge_at = clock.now
    clock.advance(10)  # let the post-purge part of the window lie in the past

    for prefix in (PREFIX, "198.51.100.0/24"):
        last = _entries(svc, prefix, PRIMARY)[-1]
        assert last.changed_at == purge_at
        assert last.route is None

    # Destination covered only by the purged prefix: no route after the purge.
    start = T0  # every relevant archive reaches back to this baseline
    end = purge_at + timedelta(seconds=10)
    report = svc.attribution_report([
        (_ip("198.51.100.10"), start, end, 1),
    ])
    assert report.records[0].outcome == "changed_route"
    assert report.records[0].first_change_at == purge_at


def test_report_uses_snapshot_taken_at_start(svc, clock):
    _baseline(svc, clock)
    start = clock.now
    end = clock.advance(120)

    # Freeze exactly as attribution_report() does at entry, then mutate the
    # live history: computations driven by the frozen snapshot must ignore
    # the withdrawal that arrived after the report had already started.
    frozen = svc._freeze_history()
    svc.withdraw(PRIMARY, 1, [_net(PREFIX)])
    revision_after = svc.revision

    horizon = min(h.entries[0].changed_at for h in frozen.values())
    record = svc._attribute_one(
        index=0,
        destination=_ip(DEST),
        start=start,
        end=end,
        byte_count=3,
        frozen=frozen,
        horizon=horizon,
        now=clock.now,
    )
    assert record.outcome == "attributed"
    assert record.neighbor == PRIMARY
    assert svc.revision == revision_after  # live table kept taking updates


def test_pending_reconnect_flip_is_its_own_archived_change(svc, clock):
    _baseline(svc, clock)
    start = clock.now
    flip_at = clock.advance(20)
    svc.reconnect(PRIMARY, 2, grace_seconds=600)  # primary routes go pending

    entries = _entries(svc, PREFIX, PRIMARY)
    assert entries[-1].changed_at == flip_at
    assert entries[-1].route is not None and entries[-1].route.pending is True

    # The backup's confirmed equal-prefix route now wins; that is a change.
    end = clock.advance(20)
    record = svc.attribution_report([(_ip(DEST), start, end, 1)]).records[0]
    assert record.outcome == "changed_route"
    assert record.first_change_at == flip_at


def test_grace_expiry_batch_withdrawal_uses_one_effective_instant(svc, clock):
    # The asyncio timer path mirrors End-of-RIB: one batch, one instant.
    svc.reconnect(PRIMARY, 1, grace_seconds=0)
    svc.announce(PRIMARY, 1, [
        (_net(PREFIX), _ip("10.0.0.1")),
        (_net("198.51.100.0/24"), _ip("10.0.0.1")),
    ])
    svc.end_of_rib(PRIMARY, 1)
    svc.reconnect(PRIMARY, 2, grace_seconds=600)  # retained pending
    expiry = clock.advance(600)

    # Drive the same cleanup the timer would, with the fake clock's instant.
    svc._on_grace_expired(PRIMARY, 2, svc._neighbors[PRIMARY].generation)

    for prefix in (PREFIX, "198.51.100.0/24"):
        last = _entries(svc, prefix, PRIMARY)[-1]
        assert last.changed_at == expiry
        assert last.route is None


def test_longest_prefix_rule_is_applied_to_history(svc, clock):
    svc.reconnect(BACKUP, 1, grace_seconds=0)
    svc.announce(BACKUP, 1, [
        (_net(PREFIX), _ip("10.0.0.2")),
        (_net("203.0.113.128/25"), _ip("10.0.0.2")),
    ])
    svc.end_of_rib(BACKUP, 1)
    svc.reconnect(PRIMARY, 1, grace_seconds=0)
    svc.announce(PRIMARY, 1, [
        (_net(PREFIX), _ip("10.0.0.1")),
        (_net("203.0.113.0/25"), _ip("10.0.0.1")),
    ])
    svc.end_of_rib(PRIMARY, 1)
    start, end = clock.now, clock.advance(60)

    report = svc.attribution_report([
        (_ip("203.0.113.10"), start, end, 1),   # primary's more-specific 0/25
        (_ip("203.0.113.200"), start, end, 1),  # backup's more-specific 128/25
    ])
    assert report.records[0].neighbor == PRIMARY
    assert str(report.records[0].prefix) == "203.0.113.0/25"
    assert report.records[1].neighbor == BACKUP
    assert str(report.records[1].prefix) == "203.0.113.128/25"
    assert report.neighbor_totals == {PRIMARY: 1, BACKUP: 1}


# -------------------------------------------------------------- HTTP level
def _report(client, records):
    response = client.post("/v1/attribution/report", json={"records": records})
    assert response.status_code == 200, response.text
    return response.json()


def test_http_endpoint_full_switch_and_back_story(client):
    from tests.conftest import announce, end_of_rib, prime_two_neighbors, reconnect

    prime_two_neighbors(client)
    # Failover to the backup, then the primary comes back.
    reconnect(client, str(PRIMARY), 2, grace=60)
    announce(client, str(PRIMARY), 2, [("203.0.113.0/24", "10.0.0.1")])
    end_of_rib(client, str(PRIMARY), 2)

    body = _report(client, [
        {
            "destination": "203.0.113.10",
            "start": "2000-01-01T00:00:00Z",
            "end": "2000-01-01T00:05:00Z",
            "bytes": 1000,
        }
    ])
    assert body["records"][0]["outcome"] == "insufficient_history"
    assert body["insufficient_history_count"] == 1
    assert body["history_retention_seconds"] == 600.0


def test_http_naive_timestamp_is_rejected(client):
    response = client.post(
        "/v1/attribution/report",
        json={
            "records": [
                {
                    "destination": "203.0.113.10",
                    "start": "2026-11-22T10:00:00",
                    "end": "2026-11-22T10:05:00Z",
                    "bytes": 1,
                }
            ]
        },
    )
    assert response.status_code == 422
    message = str(response.json())
    assert "UTC" in message


def test_http_end_before_start_is_rejected(client):
    response = client.post(
        "/v1/attribution/report",
        json={
            "records": [
                {
                    "destination": "203.0.113.10",
                    "start": "2026-11-22T10:05:00Z",
                    "end": "2026-11-22T10:05:00Z",
                    "bytes": 1,
                }
            ]
        },
    )
    assert response.status_code == 422


def test_http_negative_bytes_is_rejected(client):
    response = client.post(
        "/v1/attribution/report",
        json={
            "records": [
                {
                    "destination": "203.0.113.10",
                    "start": "2026-11-22T10:00:00Z",
                    "end": "2026-11-22T10:05:00Z",
                    "bytes": -1,
                }
            ]
        },
    )
    assert response.status_code == 422


def test_http_attributed_and_neighbor_totals_shape(client):
    import time as _time

    from tests.conftest import prime_two_neighbors

    prime_two_neighbors(client)
    # Choose an interval that starts after priming finishes; sleep so it is
    # already elapsed when the report runs (no future-interval rejection).
    start = datetime.now(UTC) + timedelta(milliseconds=200)
    end = start + timedelta(seconds=1)
    _time.sleep(1.5)
    body = _report(client, [
        {
            "destination": "203.0.113.10",
            "start": start.isoformat(),
            "end": end.isoformat(),
            "bytes": 100,
        },
        {
            "destination": "198.51.100.10",
            "start": start.isoformat(),
            "end": end.isoformat(),
            "bytes": 50,
        },
    ])
    assert body["attributed_count"] == 2
    totals = {item["neighbor"]: item["bytes"] for item in body["neighbor_totals"]}
    assert totals == {str(PRIMARY): 150}
    assert body["records"][0]["outcome"] == "attributed"
    assert body["records"][0]["neighbor"] == str(PRIMARY)


# ---------------------------------------------------------------- helpers
def _ip(raw: str) -> IPv4Address:
    return IPv4Address(raw)


def _net(raw: str) -> IPv4Network:
    return IPv4Network(raw)


def _entries(svc: RouteService, prefix: str, neighbor) -> list:
    return svc._freeze_history()[(_net(prefix), neighbor)].entries
