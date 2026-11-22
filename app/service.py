"""In-memory RIB used for reconnect and failover visibility.

The service is deliberately synchronous and single-threaded.  FastAPI runs the
mutating calls on the event loop, so every public call is atomic with respect
to other requests.  Grace-period expiry is driven by ``asyncio`` timer handles
that are bound to the session generation that armed them: a timer left over
from a superseded session can never change the table.

A bounded **routing history** is kept alongside the live table for batch
traffic attribution.  Only prefix/neighbour pairs whose effective route state
changes are archived, entries older than the retention window (default 10
minutes) are pruned, and the last entry at or before a queried instant is
retained so a route still in effect at the window start can always be
reconstructed.
"""

from __future__ import annotations

import asyncio
import math
from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta, timezone
from ipaddress import IPv4Address, IPv4Network
from typing import Callable, Mapping

from .errors import SessionConflict, UnknownNeighbor


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _same_route_state(left: Route | None, right: Route | None) -> bool:
    """Whether two archived states select traffic identically.

    Only the routing-relevant fields matter: a re-announcement that keeps the
    same neighbour, next hop and confirmation state is not a route change.
    """
    if left is None or right is None:
        return left is None and right is None
    return (
        left.prefix == right.prefix
        and left.neighbor == right.neighbor
        and left.next_hop == right.next_hop
        and left.pending == right.pending
    )


def _same_selection(
    left: tuple[IPv4Address, Route] | None,
    right: tuple[IPv4Address, Route] | None,
) -> bool:
    """Whether two historical selections pick the same neighbour.

    Attribution only depends on which neighbour (if any) carries the traffic.
    A confirmed->pending flip, a different winning prefix or next hop within
    the same neighbour therefore do not count as a route change; a route
    appearing or disappearing (one side ``None``) does.
    """
    if left is None or right is None:
        return left is None and right is None
    return left[0] == right[0]


@dataclass
class Route:
    prefix: IPv4Network
    next_hop: IPv4Address
    neighbor: IPv4Address
    pending: bool
    announced_session: int
    updated_at: datetime


@dataclass(frozen=True)
class RouteState:
    """One archived route state for a prefix/neighbour pair at ``changed_at``.

    A ``None`` route means this neighbour withdrew the prefix (or had it
    batch-purged) at that instant and held no route for it afterwards until
    the next entry.  Other neighbours' routes are archived independently.
    The archived :class:`Route` is a copy, so later in-place pending flips on
    the live route cannot mutate history.
    """

    changed_at: datetime
    route: Route | None


@dataclass
class RouteHistory:
    """Ordered change archive for one ``(prefix, neighbour)`` pair."""

    prefix: IPv4Network
    neighbor: IPv4Address
    entries: list[RouteState] = field(default_factory=list)

    def state_at(self, instant: datetime) -> RouteState | None:
        """Latest entry effective at or before ``instant`` (half-open events)."""
        winner: RouteState | None = None
        for entry in self.entries:
            if entry.changed_at <= instant:
                winner = entry
            else:
                break
        return winner


@dataclass
class NeighborState:
    neighbor: IPv4Address
    session_id: int
    generation: int
    established_at: datetime
    routes: dict[IPv4Network, Route] = field(default_factory=dict)
    end_of_rib_received: bool = False
    grace_expires_at: datetime | None = None
    timer: asyncio.TimerHandle | None = None
    last_cleanup_reason: str | None = None
    last_cleanup_at: datetime | None = None

    @property
    def pending_count(self) -> int:
        return sum(1 for route in self.routes.values() if route.pending)

    @property
    def confirmed_count(self) -> int:
        return sum(1 for route in self.routes.values() if not route.pending)

    @property
    def converging(self) -> bool:
        return self.grace_expires_at is not None and not self.end_of_rib_received


@dataclass(frozen=True)
class ReconnectResult:
    neighbor: IPv4Address
    previous_session_id: int | None
    session_id: int
    retained_pending_routes: int
    grace_seconds: float
    grace_expires_at: datetime | None
    revision: int


@dataclass(frozen=True)
class AnnounceResult:
    neighbor: IPv4Address
    session_id: int
    accepted: int
    added: int
    replaced: int
    route_count: int
    pending_remaining: int
    revision: int


@dataclass(frozen=True)
class WithdrawResult:
    neighbor: IPv4Address
    session_id: int
    removed: int
    not_present: tuple[IPv4Network, ...]
    route_count: int
    pending_remaining: int
    revision: int


@dataclass(frozen=True)
class EndOfRibResult:
    neighbor: IPv4Address
    session_id: int
    purged_pending: int
    pending_remaining: int
    confirmed_remaining: int
    revision: int


@dataclass(frozen=True)
class Match:
    destination: IPv4Address
    found: bool
    prefix: IPv4Network | None = None
    next_hop: IPv4Address | None = None
    neighbor: IPv4Address | None = None
    neighbor_priority: int | None = None
    pending: bool | None = None
    route_session_id: int | None = None


@dataclass(frozen=True)
class LookupResult:
    revision: int
    route_count: int
    matches: tuple[Match, ...]


@dataclass(frozen=True)
class NeighborView:
    neighbor: IPv4Address
    priority: int
    session_id: int
    established_at: datetime
    end_of_rib_received: bool
    grace_expires_at: datetime | None
    converging: bool
    pending_routes: int
    confirmed_routes: int
    total_routes: int
    last_cleanup_reason: str | None


@dataclass(frozen=True)
class AttributionRecord:
    index: int
    destination: IPv4Address
    start: datetime
    end: datetime
    bytes: int
    outcome: str  # attributed | no_route | changed_route | insufficient_history
    neighbor: IPv4Address | None = None
    next_hop: IPv4Address | None = None
    prefix: IPv4Network | None = None
    first_change_at: datetime | None = None


@dataclass(frozen=True)
class AttributionReport:
    revision: int
    computed_at: datetime
    history_horizon: datetime | None
    retention_seconds: float
    records: tuple[AttributionRecord, ...]
    neighbor_totals: Mapping[IPv4Address, int]


class RouteService:
    def __init__(
        self,
        *,
        neighbor_priorities: Mapping[IPv4Address, int] | None = None,
        default_neighbor_priority: int = 100,
        grace_period_seconds: float = 30.0,
        history_retention_seconds: float = 600.0,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        if grace_period_seconds < 0 or not math.isfinite(grace_period_seconds):
            raise ValueError("grace_period_seconds must be a finite, non-negative number")
        if history_retention_seconds <= 0 or not math.isfinite(history_retention_seconds):
            raise ValueError(
                "history_retention_seconds must be a finite, positive number"
            )
        self._priorities: dict[IPv4Address, int] = dict(neighbor_priorities or {})
        self._default_priority = default_neighbor_priority
        self._default_grace = grace_period_seconds
        self._history_retention = float(history_retention_seconds)
        self._clock = clock or _utcnow
        self._neighbors: dict[IPv4Address, NeighborState] = {}
        # One ordered change archive per (prefix, neighbour) pair; only pairs
        # whose effective route state changed get an entry.
        self._history: dict[tuple[IPv4Network, IPv4Address], RouteHistory] = {}
        self._revision = 0

    # ------------------------------------------------------------------ views
    @property
    def revision(self) -> int:
        return self._revision

    def priority_for(self, neighbor: IPv4Address) -> int:
        return self._priorities.get(neighbor, self._default_priority)

    def route_count(self) -> int:
        return sum(len(state.routes) for state in self._neighbors.values())

    def neighbors(self) -> tuple[NeighborView, ...]:
        views = [
            NeighborView(
                neighbor=state.neighbor,
                priority=self.priority_for(state.neighbor),
                session_id=state.session_id,
                established_at=state.established_at,
                end_of_rib_received=state.end_of_rib_received,
                grace_expires_at=state.grace_expires_at,
                converging=state.converging,
                pending_routes=state.pending_count,
                confirmed_routes=state.confirmed_count,
                total_routes=len(state.routes),
                last_cleanup_reason=state.last_cleanup_reason,
            )
            for state in self._neighbors.values()
        ]
        views.sort(key=lambda view: (view.priority, int(view.neighbor)))
        return tuple(views)

    # ------------------------------------------------------------- history
    @property
    def history_retention_seconds(self) -> float:
        return self._history_retention

    def history_horizon(self) -> datetime | None:
        """Oldest instant for which a route state can still be reconstructed.

        Every archive's first retained entry is effective from that instant
        on, so the minimum of those instants bounds every reconstruction.
        """
        instants = [
            history.entries[0].changed_at
            for history in self._history.values()
            if history.entries
        ]
        return min(instants) if instants else None

    def history_prefix_count(self) -> int:
        """Number of distinct prefixes with at least one retained change."""
        return len({history.prefix for history in self._history.values()})

    def _archive(
        self,
        prefix: IPv4Network,
        neighbor: IPv4Address,
        route: Route | None,
        *,
        at: datetime,
    ) -> None:
        """Record one route state change for a ``(prefix, neighbour)`` pair.

        Only pairs whose effective state actually differs from the last
        archived state get an entry; identical re-announcements are skipped.
        A ``None`` route marks a withdrawal or a batch purge.
        """
        key = (prefix, neighbor)
        history = self._history.get(key)
        if history is None:
            history = RouteHistory(prefix=prefix, neighbor=neighbor)
            self._history[key] = history
        last = history.entries[-1] if history.entries else None
        if last is not None and _same_route_state(last.route, route):
            return
        # Defensive copy: the live table mutates Route objects in place
        # (for example the confirmed -> pending flip on a reconnect).
        stored = replace(route) if route is not None else None
        history.entries.append(RouteState(changed_at=at, route=stored))

    def _prune_history(self, *, now: datetime) -> None:
        """Drop entries older than the retention window.

        The entry effective at the window start is kept, so routes still in
        effect at the start of the 10-minute window remain reconstructable.
        Pairs whose whole archive falls outside the window are removed.
        """
        cutoff = now - timedelta(seconds=self._history_retention)
        empty: list[tuple[IPv4Network, IPv4Address]] = []
        for key, history in self._history.items():
            # Newest entry still strictly before the cutoff: it is the state
            # effective when the window started, and is retained as baseline.
            baseline_index = -1
            for index, entry in enumerate(history.entries):
                if entry.changed_at < cutoff:
                    baseline_index = index
                else:
                    break
            if baseline_index >= 0:
                if baseline_index == len(history.entries) - 1:
                    # Every entry predates the window; nothing is retained.
                    history.entries.clear()
                elif baseline_index > 0:
                    del history.entries[:baseline_index]
            if not history.entries:
                empty.append(key)
        for key in empty:
            del self._history[key]

    def _freeze_history(self) -> dict[tuple[IPv4Network, IPv4Address], RouteHistory]:
        """Snapshot the archives so a report ignores updates while it computes."""
        return {
            key: RouteHistory(
                prefix=history.prefix,
                neighbor=history.neighbor,
                entries=list(history.entries),
            )
            for key, history in self._history.items()
            if history.entries
        }

    # --------------------------------------------------------------- mutation
    def reconnect(
        self,
        neighbor: IPv4Address,
        session_id: int,
        *,
        grace_seconds: float | None = None,
        reason: str | None = None,
    ) -> ReconnectResult:
        """Take over the neighbor with a new, higher session id.

        Routes held for the previous session are kept but flagged pending.
        They are removed in one batch when the new session reaches End-of-RIB
        or when this reconnect's grace period expires.
        """
        del reason  # recorded by the caller/logging in a real deployment
        grace = self._default_grace if grace_seconds is None else grace_seconds
        if grace < 0 or not math.isfinite(grace):
            raise ValueError("grace_seconds must be a finite, non-negative number")

        now = self._clock()
        state = self._neighbors.get(neighbor)
        previous_session: int | None = None

        if state is None:
            state = NeighborState(
                neighbor=neighbor,
                session_id=session_id,
                generation=0,
                established_at=now,
            )
            self._neighbors[neighbor] = state
        else:
            if session_id < state.session_id:
                raise SessionConflict(
                    neighbor=neighbor,
                    current_session=state.session_id,
                    received_session=session_id,
                    reason="stale",
                )
            if session_id == state.session_id:
                raise SessionConflict(
                    neighbor=neighbor,
                    current_session=state.session_id,
                    received_session=session_id,
                    reason="not_increasing",
                )
            previous_session = state.session_id
            if state.timer is not None:
                # The superseded timer must never touch the table.
                state.timer.cancel()
                state.timer = None
            # Every route from the old session stays visible, but pending, and
            # that confirmed -> pending transition is archived.  When there is
            # no grace window the immediate purge below appends the withdrawal
            # at the same instant; identical states are deduplicated on archive.
            for route in state.routes.values():
                route.pending = True
                self._archive(route.prefix, neighbor, route, at=now)

            state.session_id = session_id
            state.generation += 1
            state.established_at = now
            state.end_of_rib_received = False
            state.last_cleanup_reason = None
            state.last_cleanup_at = None

        state.grace_expires_at = (
            now + timedelta(seconds=grace) if grace > 0 else None
        )
        self._revision += 1
        retained = state.pending_count

        if grace > 0:
            self._arm_timer(state, grace)
        else:
            # No grace window configured: settle immediately.
            self._purge_pending(state, reason="grace_expired", at=now)
            state.grace_expires_at = None
        self._prune_history(now=now)

        return ReconnectResult(
            neighbor=neighbor,
            previous_session_id=previous_session,
            session_id=session_id,
            retained_pending_routes=retained,
            grace_seconds=grace,
            grace_expires_at=state.grace_expires_at,
            revision=self._revision,
        )

    def announce(
        self,
        neighbor: IPv4Address,
        session_id: int,
        routes: list[tuple[IPv4Network, IPv4Address]],
    ) -> AnnounceResult:
        """Apply IPv4 announcements from the current session immediately."""
        state = self._require_current(neighbor, session_id)
        now = self._clock()
        added = replaced = 0
        for prefix, next_hop in routes:
            if prefix in state.routes:
                replaced += 1
            else:
                added += 1
            route = Route(
                prefix=prefix,
                next_hop=next_hop,
                neighbor=neighbor,
                pending=False,
                # Announcements from the current session are confirmed at once;
                # only routes carried over from a previous session stay pending.
                announced_session=session_id,
                updated_at=now,
            )
            state.routes[prefix] = route
            # Only changed prefixes get an archive entry; identical re-announces
            # are deduplicated inside _archive.
            self._archive(prefix, neighbor, route, at=now)
        self._revision += 1
        self._prune_history(now=now)
        return AnnounceResult(
            neighbor=neighbor,
            session_id=session_id,
            accepted=len(routes),
            added=added,
            replaced=replaced,
            route_count=len(state.routes),
            pending_remaining=state.pending_count,
            revision=self._revision,
        )

    def withdraw(
        self,
        neighbor: IPv4Address,
        session_id: int,
        prefixes: list[IPv4Network],
    ) -> WithdrawResult:
        """Apply withdrawals from the current session immediately."""
        state = self._require_current(neighbor, session_id)
        now = self._clock()
        removed = 0
        not_present: list[IPv4Network] = []
        for prefix in prefixes:
            if prefix in state.routes:
                del state.routes[prefix]
                removed += 1
                self._archive(prefix, neighbor, None, at=now)
            else:
                not_present.append(prefix)
        self._revision += 1
        self._prune_history(now=now)
        return WithdrawResult(
            neighbor=neighbor,
            session_id=session_id,
            removed=removed,
            not_present=tuple(not_present),
            route_count=len(state.routes),
            pending_remaining=state.pending_count,
            revision=self._revision,
        )

    def end_of_rib(self, neighbor: IPv4Address, session_id: int) -> EndOfRibResult:
        """End-of-RIB for the current session: batch-remove pending routes."""
        state = self._require_current(neighbor, session_id)
        now = self._clock()
        if state.timer is not None:
            state.timer.cancel()
            state.timer = None
        # The whole batch is archived with this single effective instant.
        purged = self._purge_pending(state, reason="end_of_rib", at=now)
        state.end_of_rib_received = True
        state.grace_expires_at = None
        self._revision += 1
        self._prune_history(now=now)
        return EndOfRibResult(
            neighbor=neighbor,
            session_id=session_id,
            purged_pending=purged,
            pending_remaining=state.pending_count,
            confirmed_remaining=state.confirmed_count,
            revision=self._revision,
        )

    # ---------------------------------------------------------------- lookups
    def snapshot(self) -> tuple[Route, ...]:
        """All routes in a stable order, with their owning neighbor priority."""
        routes = [route for state in self._neighbors.values() for route in state.routes.values()]
        routes.sort(
            key=lambda route: (
                int(route.prefix.network_address),
                route.prefix.prefixlen,
                self.priority_for(route.neighbor),
                int(route.neighbor),
            )
        )
        return tuple(routes)

    def reset(self) -> tuple[int, int]:
        """Drop all neighbors, routes and history; recording starts over."""
        for state in self._neighbors.values():
            if state.timer is not None:
                state.timer.cancel()
                state.timer = None
        cleared_neighbors = len(self._neighbors)
        cleared_routes = self.route_count()
        self._neighbors.clear()
        # Reset wipes the archives as well: history is recorded afresh.
        self._history.clear()
        self._revision += 1
        return cleared_neighbors, cleared_routes

    def lookup(self, destinations: list[IPv4Address]) -> LookupResult:
        """Resolve a batch of destinations against one shared table snapshot."""
        revision = self._revision
        matches: list[Match] = []

        for destination in destinations:
            selected = self._select_route(
                destination,
                ((state.neighbor, state.routes.values()) for state in self._neighbors.values()),
            )
            if selected is None:
                matches.append(
                    Match(destination=destination, found=False, pending=None)
                )
            else:
                neighbor, route = selected
                matches.append(
                    Match(
                        destination=destination,
                        found=True,
                        prefix=route.prefix,
                        next_hop=route.next_hop,
                        neighbor=neighbor,
                        neighbor_priority=self.priority_for(neighbor),
                        pending=route.pending,
                        route_session_id=route.announced_session,
                    )
                )

        return LookupResult(
            revision=revision,
            route_count=self.route_count(),
            matches=tuple(matches),
        )

    # ----------------------------------------------------------- attribution
    def attribution_report(
        self,
        records: list[tuple[IPv4Address, datetime, datetime, int]],
    ) -> AttributionReport:
        """Attribute traffic records from the routing history.

        The whole batch is evaluated against a frozen snapshot of the history
        taken when the report starts; route updates received while the report
        is computed keep flowing and never change these results.  A record is
        attributed to one neighbour only when that neighbour is selected for
        the entire half-open interval ``[start, end)``.
        """
        computed_at = self._clock()
        revision = self._revision
        frozen = self._freeze_history()
        horizon = min(
            (history.entries[0].changed_at for history in frozen.values()),
            default=None,
        )
        totals: dict[IPv4Address, int] = {}
        outcomes: list[AttributionRecord] = []

        for index, (destination, start, end, byte_count) in enumerate(records):
            outcome = self._attribute_one(
                index=index,
                destination=destination,
                start=start,
                end=end,
                byte_count=byte_count,
                frozen=frozen,
                horizon=horizon,
                now=computed_at,
            )
            outcomes.append(outcome)
            if outcome.outcome == "attributed" and outcome.neighbor is not None:
                totals[outcome.neighbor] = totals.get(outcome.neighbor, 0) + byte_count

        return AttributionReport(
            revision=revision,
            computed_at=computed_at,
            history_horizon=horizon,
            retention_seconds=self._history_retention,
            records=tuple(outcomes),
            neighbor_totals=totals,
        )

    def _attribute_one(
        self,
        *,
        index: int,
        destination: IPv4Address,
        start: datetime,
        end: datetime,
        byte_count: int,
        frozen: Mapping[tuple[IPv4Network, IPv4Address], RouteHistory],
        horizon: datetime | None,
        now: datetime,
    ) -> AttributionRecord:
        base = AttributionRecord(
            index=index,
            destination=destination,
            start=start,
            end=end,
            bytes=byte_count,
            outcome="",
        )
        # Nothing retained, a query before the oldest retained baseline, or a
        # future interval: the full [start, end) cannot be reconstructed.
        if horizon is None or start < horizon or end > now:
            return replace(base, outcome="insufficient_history")

        relevant = [
            history
            for (prefix, _neighbor), history in frozen.items()
            if destination in prefix
        ]
        # An individual archive that only starts after the interval start
        # cannot prove what that neighbour's route was before its first
        # retained entry, even when other archives reach back further.
        if any(history.entries[0].changed_at > start for history in relevant):
            return replace(base, outcome="insufficient_history")

        # Event instants strictly inside the half-open interval, plus start.
        instants = {start}
        for history in relevant:
            for entry in history.entries:
                if start < entry.changed_at < end:
                    instants.add(entry.changed_at)

        selections: list[tuple[datetime, tuple[IPv4Address, Route] | None]] = []
        for instant in sorted(instants):
            selected = self._select_from_history(destination, instant, relevant)
            selections.append((instant, selected))

        first_selection = selections[0][1]
        # Any later selection that differs from the one effective at start
        # (a different neighbour, a route appearing, or one disappearing) is
        # a route change -- including a change that later returns to the
        # initial state. The first differing instant is reported.
        for instant, selected in selections[1:]:
            if not _same_selection(first_selection, selected):
                return replace(
                    base,
                    outcome="changed_route",
                    first_change_at=instant,
                )

        if first_selection is None:
            # No neighbour covered the destination at any point of the window.
            return replace(base, outcome="no_route")
        winner_neighbor, winner_route = first_selection

        return replace(
            base,
            outcome="attributed",
            neighbor=winner_neighbor,
            next_hop=winner_route.next_hop,
            prefix=winner_route.prefix,
        )

    def _select_from_history(
        self,
        destination: IPv4Address,
        instant: datetime,
        relevant: list[RouteHistory],
    ) -> tuple[IPv4Address, Route] | None:
        """Apply the live selection rules to routes effective at ``instant``."""
        candidates: list[tuple[IPv4Address, list[Route]]] = []
        for history in relevant:
            entry = history.state_at(instant)
            if entry is not None and entry.route is not None:
                candidates.append((history.neighbor, [entry.route]))
        return self._select_route(destination, candidates)

    # ---------------------------------------------------------------- helpers
    def _select_route(
        self,
        destination: IPv4Address,
        candidates,
    ) -> tuple[IPv4Address, Route] | None:
        """Longest-prefix, confirmed-over-pending, priority, neighbour tie-break.

        ``candidates`` yields ``(neighbor, routes)`` pairs.  The same ordering is
        used for live lookups and historical reconstructions.
        """
        best: tuple[tuple[int, int, int, int], IPv4Address, Route] | None = None
        for neighbor, routes in candidates:
            priority = self.priority_for(neighbor)
            for route in routes:
                if destination not in route.prefix:
                    continue
                key = (
                    -route.prefix.prefixlen,          # longest match first
                    route.pending,                     # confirmed before pending
                    priority,                          # lower number = preferred
                    int(neighbor),                     # deterministic final tie
                )
                if best is None or key < best[0]:
                    best = (key, neighbor, route)
        if best is None:
            return None
        return best[1], best[2]

    def _require_current(
        self, neighbor: IPv4Address, session_id: int
    ) -> NeighborState:
        state = self._neighbors.get(neighbor)
        if state is None:
            raise UnknownNeighbor(neighbor)
        if session_id < state.session_id:
            raise SessionConflict(
                neighbor=neighbor,
                current_session=state.session_id,
                received_session=session_id,
                reason="stale",
            )
        if session_id > state.session_id:
            raise SessionConflict(
                neighbor=neighbor,
                current_session=state.session_id,
                received_session=session_id,
                reason="not_established",
            )
        return state

    def _purge_pending(
        self, state: NeighborState, *, reason: str, at: datetime | None = None
    ) -> int:
        """Remove every pending route of one neighbor as a single batch.

        The whole batch is archived at one effective instant, so grace cleanup
        and End-of-RIB withdrawals appear in the history atomically.
        """
        effective_at = self._clock() if at is None else at
        doomed = [prefix for prefix, route in state.routes.items() if route.pending]
        for prefix in doomed:
            del state.routes[prefix]
            self._archive(prefix, state.neighbor, None, at=effective_at)
        state.last_cleanup_reason = reason
        state.last_cleanup_at = effective_at
        return len(doomed)

    def _arm_timer(self, state: NeighborState, grace: float) -> None:
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            # No event loop (unit tests, scripted demo): expiry is driven
            # explicitly by end_of_rib() or a later reconnect.
            state.timer = None
            return
        state.timer = loop.call_later(
            grace,
            self._on_grace_expired,
            state.neighbor,
            state.session_id,
            state.generation,
        )

    def _on_grace_expired(
        self, neighbor: IPv4Address, session_id: int, generation: int
    ) -> None:
        state = self._neighbors.get(neighbor)
        if state is None:
            return
        # Stale timer from a superseded session/generation: no table changes.
        if state.session_id != session_id or state.generation != generation:
            return
        if state.end_of_rib_received or state.grace_expires_at is None:
            return
        state.timer = None
        now = self._clock()
        self._purge_pending(state, reason="grace_expired", at=now)
        state.grace_expires_at = None
        self._revision += 1
        self._prune_history(now=now)

    def close(self) -> None:
        """Cancel outstanding timers (application shutdown)."""
        for state in self._neighbors.values():
            if state.timer is not None:
                state.timer.cancel()
                state.timer = None
