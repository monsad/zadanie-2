"""All SQL lives here. The module decides nothing about retry rules.

One connection writes and one reads. SQLite has a single writer anyway, so
funnelling every write through one aiosqlite connection (one thread) turns
lock contention into an in-process queue and ``database is locked`` cannot
happen between our own writers.

Every value reaches SQL as a bound parameter; the only f-string
interpolation is the module constant ``_FENCE``.
"""

import asyncio
import json
import time
from collections.abc import Awaitable, Callable, Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from importlib.resources import files
from typing import Any, Literal

import aiosqlite

from app.domain import ClaimedEvent, Status, Transition

SCHEMA = files("app").joinpath("schema.sql").read_text()

PRAGMAS = (
    "PRAGMA journal_mode = WAL",
    "PRAGMA synchronous = NORMAL",
    "PRAGMA busy_timeout = 5000",
    "PRAGMA foreign_keys = ON",
)

_CLAIM_SQL = """
UPDATE webhook_events
SET status = 'processing',
    lease_token = lower(hex(randomblob(16))),
    locked_until = :now + :lease_seconds,
    locked_by = :worker_id,
    attempts = attempts + 1,
    started_at = :now,
    updated_at = :now
WHERE id IN (
    -- Pinned: without statistics the planner prefers the status index
    -- plus a sort over the whole backlog.
    SELECT id FROM webhook_events INDEXED BY ix_events_ready
    WHERE status IN ('pending', 'retry_scheduled')
      AND next_attempt_at <= :now
    ORDER BY next_attempt_at
    LIMIT :limit
)
RETURNING id, idempotency_key, event_type, payload, attempts,
          max_attempts, lease_token, received_at
"""

# The only f-string SQL fragment; bandit B608 is silenced where it is
# used, and tests/test_static.py checks nothing else is interpolated.
_FENCE = "WHERE id = :id AND lease_token = :token AND status = 'processing'"

_ACTIVE_STATUSES = (
    Status.PENDING,
    Status.RETRY_SCHEDULED,
    Status.PROCESSING,
)

_DAY = 86_400.0
_DEFAULT_IDEMPOTENCY_TTL = 90 * _DAY

type Record = dict[str, Any]


class DatabaseUnavailableError(Exception):
    """The database refused the operation (locked, full disk, closed)."""


DatabaseUnavailable = DatabaseUnavailableError


@dataclass(frozen=True, slots=True, kw_only=True)
class StaleLease:
    id: int
    idempotency_key: str
    lease_token: str
    attempts: int
    max_attempts: int


@dataclass(frozen=True, slots=True)
class InsertResult:
    outcome: Literal["accepted", "duplicate", "conflict", "rejected"]
    record: Record


@dataclass(frozen=True, slots=True)
class QueueStats:
    by_status: Mapping[str, int]
    oldest_ready_at: float | None

    @property
    def depth(self) -> int:
        return sum(self.by_status.get(s, 0) for s in _ACTIVE_STATUSES)


@contextmanager
def _translate_errors() -> Iterator[None]:
    try:
        yield
    except (aiosqlite.OperationalError, ValueError) as exc:
        # ValueError: aiosqlite raises it when the connection is closed.
        raise DatabaseUnavailableError(str(exc)) from exc


def _remnant_record(row: Record) -> Record:
    """Status leftover after the payload row has been deleted."""
    return {
        "id": row["event_id"],
        "idempotency_key": row["idempotency_key"],
        "source": row["source"],
        "event_type": row["event_type"],
        "payload": None,
        "payload_hash": row["payload_hash"],
        "status": row["last_status"],
        "attempts": 0,
        "max_attempts": 0,
        "next_attempt_at": None,
        "lease_token": None,  # nosec B105
        "locked_until": None,
        "locked_by": None,
        "last_error": None,
        "result": None,
        "received_at": row["created_at"],
        "started_at": None,
        "completed_at": None,
        "updated_at": row["created_at"],
    }


class Repository:
    def __init__(
        self,
        path: str,
        clock: Callable[[], float] = time.time,
        *,
        idempotency_ttl_seconds: float = _DEFAULT_IDEMPOTENCY_TTL,
    ) -> None:
        self._path = path
        self._clock = clock
        self._idempotency_ttl = idempotency_ttl_seconds
        self._write_lock = asyncio.Lock()
        self._writer: aiosqlite.Connection | None = None
        self._reader: aiosqlite.Connection | None = None

    async def open(self) -> None:
        self._writer = await self._connect()
        await self._writer.executescript(SCHEMA)
        self._reader = await self._connect()

    async def close(self) -> None:
        for conn in (self._reader, self._writer):
            if conn is not None:
                await conn.close()
        self._reader = self._writer = None

    async def _connect(self) -> aiosqlite.Connection:
        conn = await aiosqlite.connect(self._path, isolation_level=None)
        conn.row_factory = aiosqlite.Row
        for pragma in PRAGMAS:
            await conn.execute(pragma)
        return conn

    @property
    def _writer_conn(self) -> aiosqlite.Connection:
        if self._writer is None:
            raise DatabaseUnavailableError("repository is closed")
        return self._writer

    @property
    def _reader_conn(self) -> aiosqlite.Connection:
        if self._reader is None:
            raise DatabaseUnavailableError("repository is closed")
        return self._reader

    async def _run[T](self, op: Callable[[], Awaitable[T]]) -> T:
        with _translate_errors():
            return await op()

    async def _write[T](self, op: Callable[[], Awaitable[T]]) -> T:
        async with self._write_lock:
            return await self._run(op)

    # --- ingest -----------------------------------------------------------

    async def insert(
        self,
        *,
        idempotency_key: str,
        source: str,
        event_type: str,
        payload_json: str,
        payload_hash: str,
        max_attempts: int,
        max_active: int | None = None,
    ) -> InsertResult:
        """Insert-or-get keyed by the durable idempotency-key registry.

        Event row and key row are written in one transaction. After the
        payload is deleted, the same key still resolves to duplicate or
        conflict and never creates a second job.

        ``max_active`` caps unfinished events. The count runs inside the
        same write transaction, so concurrent requests cannot overshoot it;
        duplicates are resolved first and are never rejected.
        """
        now = self._clock()
        params = {
            "key": idempotency_key,
            "source": source,
            "event_type": event_type,
            "payload": payload_json,
            "hash": payload_hash,
            "max_attempts": max_attempts,
            "now": now,
            "expires_at": now + self._idempotency_ttl,
            "max_active": max_active,
        }

        async def op() -> InsertResult:
            conn = self._writer_conn
            await conn.execute("BEGIN IMMEDIATE")
            try:
                result = await self._insert_in_txn(conn, params)
            except BaseException:
                await conn.execute("ROLLBACK")
                raise
            await conn.execute("COMMIT")
            return result

        return await self._write(op)

    async def _insert_in_txn(
        self, conn: aiosqlite.Connection, params: dict[str, Any]
    ) -> InsertResult:
        key = params["key"]
        payload_hash = params["hash"]
        key_row = await self._fetch_key(conn, key)
        event = await self._fetch(conn, key)
        if key_row is not None:
            if key_row["expires_at"] > params["now"]:
                same = key_row["payload_hash"] == payload_hash
                record = event or _remnant_record(key_row)
                return InsertResult(
                    "duplicate" if same else "conflict", record
                )
            if event is not None:
                same = event["payload_hash"] == payload_hash
                return InsertResult("duplicate" if same else "conflict", event)
            if await self._full(conn, event, params["max_active"]):
                return InsertResult("rejected", {})
            await conn.execute(
                "DELETE FROM webhook_idempotency_keys"
                " WHERE idempotency_key = ?",
                (key,),
            )
        elif await self._full(conn, event, params["max_active"]):
            return InsertResult("rejected", {})
        cursor = await conn.execute(
            """
            INSERT INTO webhook_events (
                idempotency_key, source, event_type, payload,
                payload_hash, status, max_attempts, next_attempt_at,
                received_at, updated_at
            ) VALUES (
                :key, :source, :event_type, :payload, :hash,
                'pending', :max_attempts, :now, :now, :now
            )
            ON CONFLICT (idempotency_key) DO NOTHING
            RETURNING *
            """,
            params,
        )
        row = await cursor.fetchone()
        if row is None:
            existing = event or await self._fetch(conn, key)
            if existing is None:
                raise DatabaseUnavailableError("row vanished during insert")
            await self._ensure_key(conn, existing, params["expires_at"])
            same = existing["payload_hash"] == payload_hash
            return InsertResult("duplicate" if same else "conflict", existing)
        record = dict(row)
        await self._ensure_key(conn, record, params["expires_at"])
        return InsertResult("accepted", record)

    @staticmethod
    async def _full(
        conn: aiosqlite.Connection,
        event: Record | None,
        max_active: int | None,
    ) -> bool:
        """True when a new event would exceed ``max_active``."""
        if max_active is None or event is not None:
            return False
        cursor = await conn.execute(
            "SELECT COUNT(*) FROM webhook_events"
            " WHERE status IN ('pending', 'retry_scheduled', 'processing')"
        )
        (active,) = await cursor.fetchone()
        return active >= max_active

    async def get(self, idempotency_key: str) -> Record | None:
        """Event row, or a remnant from the keys table if the row is gone."""

        async def op() -> Record | None:
            event = await self._fetch(self._reader_conn, idempotency_key)
            if event is not None:
                return event
            key_row = await self._fetch_key(self._reader_conn, idempotency_key)
            if key_row is None or key_row["expires_at"] <= self._clock():
                return None
            return _remnant_record(key_row)

        return await self._run(op)

    async def get_event(self, idempotency_key: str) -> Record | None:
        """Payload row only; ``None`` after DELETE or retention."""
        return await self._run(
            lambda: self._fetch(self._reader_conn, idempotency_key)
        )

    async def delete_event(self, idempotency_key: str) -> bool:
        """Remove the payload row; the key remains until it expires."""

        async def op() -> bool:
            cursor = await self._writer_conn.execute(
                "DELETE FROM webhook_events WHERE idempotency_key = ?",
                (idempotency_key,),
            )
            return cursor.rowcount == 1

        return await self._write(op)

    @staticmethod
    async def _fetch(conn: aiosqlite.Connection, key: str) -> Record | None:
        cursor = await conn.execute(
            "SELECT * FROM webhook_events WHERE idempotency_key = ?",
            (key,),
        )
        row = await cursor.fetchone()
        return dict(row) if row is not None else None

    @staticmethod
    async def _fetch_key(
        conn: aiosqlite.Connection, key: str
    ) -> Record | None:
        cursor = await conn.execute(
            "SELECT * FROM webhook_idempotency_keys WHERE idempotency_key = ?",
            (key,),
        )
        row = await cursor.fetchone()
        return dict(row) if row is not None else None

    @staticmethod
    async def _ensure_key(
        conn: aiosqlite.Connection, event: Record, expires_at: float
    ) -> None:
        await conn.execute(
            """
            INSERT INTO webhook_idempotency_keys (
                idempotency_key, payload_hash, event_id, last_status,
                source, event_type, created_at, expires_at
            ) VALUES (
                :key, :hash, :event_id, :status,
                :source, :event_type, :created_at, :expires_at
            )
            ON CONFLICT (idempotency_key) DO NOTHING
            """,
            {
                "key": event["idempotency_key"],
                "hash": event["payload_hash"],
                "event_id": event["id"],
                "status": event["status"],
                "source": event["source"],
                "event_type": event["event_type"],
                "created_at": event["received_at"],
                "expires_at": expires_at,
            },
        )

    # --- worker -----------------------------------------------------------

    async def claim(
        self, limit: int, lease_seconds: float, worker_id: str
    ) -> list[ClaimedEvent]:
        """Atomically lease up to ``limit`` runnable events."""
        params = {
            "now": self._clock(),
            "lease_seconds": lease_seconds,
            "worker_id": worker_id,
            "limit": limit,
        }

        async def op() -> list[ClaimedEvent]:
            cursor = await self._writer_conn.execute(_CLAIM_SQL, params)
            return [
                ClaimedEvent(
                    id=row["id"],
                    idempotency_key=row["idempotency_key"],
                    event_type=row["event_type"],
                    payload=json.loads(row["payload"]),
                    attempts=row["attempts"],
                    max_attempts=row["max_attempts"],
                    lease_token=row["lease_token"],
                    received_at=row["received_at"],
                )
                for row in await cursor.fetchall()
            ]

        return await self._write(op)

    async def complete(
        self, event: ClaimedEvent, result: Mapping[str, Any]
    ) -> bool:
        """Commit the result. False means the lease was lost (fencing)."""
        return await self._fenced_update(
            f"""
            UPDATE webhook_events
            SET status = 'completed', result = :result,
                completed_at = :now, last_error = NULL,
                lease_token = NULL, locked_until = NULL, updated_at = :now
            {_FENCE}
            """,  # nosec B608
            {
                "id": event.id,
                "token": event.lease_token,
                "result": json.dumps(result),
                "now": self._clock(),
            },
            idempotency_key=event.idempotency_key,
            status=Status.COMPLETED.value,
            event_id=event.id,
        )

    async def fail(
        self, event: ClaimedEvent, transition: Transition, error: str
    ) -> bool:
        return await self._fenced_update(
            f"""
            UPDATE webhook_events
            SET status = :status, last_error = :error,
                next_attempt_at = :next_attempt_at,
                completed_at = :completed_at,
                lease_token = NULL, locked_until = NULL, updated_at = :now
            {_FENCE}
            """,  # nosec B608
            {
                "id": event.id,
                "token": event.lease_token,
                "status": transition.status.value,
                "error": error[:2000],
                "next_attempt_at": transition.next_attempt_at,
                "completed_at": transition.completed_at,
                "now": self._clock(),
            },
            idempotency_key=event.idempotency_key,
            status=transition.status.value,
            event_id=event.id,
        )

    async def release(self, event: ClaimedEvent) -> bool:
        """Return a lease on graceful shutdown without burning an attempt."""
        return await self._fenced_update(
            f"""
            UPDATE webhook_events
            SET status = 'pending', attempts = attempts - 1,
                next_attempt_at = :now,
                lease_token = NULL, locked_until = NULL, updated_at = :now
            {_FENCE}
            """,  # nosec B608
            {"id": event.id, "token": event.lease_token, "now": self._clock()},
            idempotency_key=event.idempotency_key,
            status=Status.PENDING.value,
            event_id=event.id,
        )

    # --- maintenance ------------------------------------------------------

    async def stale_leases(self, *, expired_only: bool) -> list[StaleLease]:
        """Leases to recover.

        At startup every ``processing`` row is an orphan of the previous
        process (single instance), so there is no need to wait for expiry.
        """
        sql = (
            "SELECT id, idempotency_key, lease_token, attempts, max_attempts"
            " FROM webhook_events WHERE status = 'processing'"
        )
        params: dict[str, Any] = {}
        if expired_only:
            sql += " AND locked_until < :now"
            params["now"] = self._clock()

        async def op() -> list[StaleLease]:
            cursor = await self._writer_conn.execute(sql, params)
            rows = await cursor.fetchall()
            return [StaleLease(**dict(row)) for row in rows]

        return await self._write(op)

    async def recover(self, lease: StaleLease, transition: Transition) -> bool:
        return await self._fenced_update(
            f"""
            UPDATE webhook_events
            SET status = :status,
                last_error = COALESCE(last_error, 'lease expired'),
                next_attempt_at = :next_attempt_at,
                completed_at = :completed_at,
                lease_token = NULL, locked_until = NULL, updated_at = :now
            {_FENCE}
            """,  # nosec B608
            {
                "id": lease.id,
                "token": lease.lease_token,
                "status": transition.status.value,
                "next_attempt_at": transition.next_attempt_at,
                "completed_at": transition.completed_at,
                "now": self._clock(),
            },
            idempotency_key=lease.idempotency_key,
            status=transition.status.value,
            event_id=lease.id,
        )

    async def purge(
        self, status: Status, older_than: float, batch_size: int = 1000
    ) -> int:
        """Delete terminal rows in small batches to keep the writer free."""
        if status not in (Status.COMPLETED, Status.DEAD):
            raise ValueError("only terminal events may be purged")
        total = 0
        while True:
            cursor = await self._write(
                lambda: self._writer_conn.execute(
                    """
                    DELETE FROM webhook_events WHERE id IN (
                        SELECT id FROM webhook_events
                        WHERE status = ? AND completed_at < ?
                        LIMIT ?
                    )
                    """,
                    (status.value, older_than, batch_size),
                )
            )
            total += cursor.rowcount
            if cursor.rowcount < batch_size:
                return total

    async def purge_expired_keys(
        self, now: float, batch_size: int = 1000
    ) -> int:
        """Delete expired keys whose payload row is already gone."""
        total = 0
        while True:
            cursor = await self._write(
                lambda: self._writer_conn.execute(
                    """
                    DELETE FROM webhook_idempotency_keys
                    WHERE idempotency_key IN (
                        SELECT k.idempotency_key
                        FROM webhook_idempotency_keys AS k
                        WHERE k.expires_at < ?
                          AND NOT EXISTS (
                              SELECT 1 FROM webhook_events AS e
                              WHERE e.idempotency_key
                                    = k.idempotency_key
                          )
                        LIMIT ?
                    )
                    """,
                    (now, batch_size),
                )
            )
            total += cursor.rowcount
            if cursor.rowcount < batch_size:
                return total

    async def queue_stats(self) -> QueueStats:
        now = self._clock()

        async def op() -> QueueStats:
            conn = self._reader_conn
            cursor = await conn.execute(
                """
                SELECT status, COUNT(*) AS n FROM webhook_events
                WHERE status IN ('pending', 'retry_scheduled', 'processing')
                GROUP BY status
                """
            )
            rows = await cursor.fetchall()
            cursor = await conn.execute(
                """
                SELECT MIN(next_attempt_at) AS oldest FROM webhook_events
                WHERE status IN ('pending', 'retry_scheduled')
                  AND next_attempt_at <= ?
                """,
                (now,),
            )
            oldest = await cursor.fetchone()
            return QueueStats(
                {row["status"]: row["n"] for row in rows},
                oldest["oldest"] if oldest else None,
            )

        return await self._run(op)

    async def ping(self) -> None:
        await self._run(lambda: self._reader_conn.execute("SELECT 1"))

    # --- admin ------------------------------------------------------------

    async def list_events(
        self, *, status: Status | None, limit: int, after_id: int
    ) -> list[Record]:
        """Keyset pagination by ``id``; stable while rows are inserted."""

        async def op() -> list[Record]:
            cursor = await self._reader_conn.execute(
                """
                SELECT * FROM webhook_events
                WHERE (:status IS NULL OR status = :status) AND id > :after
                ORDER BY id
                LIMIT :limit
                """,
                {
                    "status": status.value if status else None,
                    "after": after_id,
                    "limit": limit,
                },
            )
            return [dict(row) for row in await cursor.fetchall()]

        return await self._run(op)

    async def replay(self, idempotency_key: str) -> Record | None:
        """Move a dead event back to the queue with a fresh attempt budget.

        Returns ``None`` when the key is unknown or the event is not dead.
        """
        now = self._clock()

        async def op() -> Record | None:
            cursor = await self._writer_conn.execute(
                """
                UPDATE webhook_events
                SET status = 'pending', attempts = 0, next_attempt_at = :now,
                    completed_at = NULL, result = NULL, updated_at = :now
                WHERE idempotency_key = :key AND status = 'dead'
                RETURNING *
                """,
                {"key": idempotency_key, "now": now},
            )
            row = await cursor.fetchone()
            if row is None:
                return None
            record = dict(row)
            await self._sync_key_status(
                idempotency_key, record["status"], record["id"]
            )
            return record

        return await self._write(op)

    async def _sync_key_status(
        self, key: str, status: str, event_id: int
    ) -> None:
        await self._writer_conn.execute(
            """
            UPDATE webhook_idempotency_keys
            SET last_status = ?, event_id = ?
            WHERE idempotency_key = ?
            """,
            (status, event_id, key),
        )

    async def _fenced_update(
        self,
        sql: str,
        params: dict[str, Any],
        *,
        idempotency_key: str,
        status: str,
        event_id: int,
    ) -> bool:
        async def op() -> bool:
            cursor = await self._writer_conn.execute(sql, params)
            if cursor.rowcount != 1:
                return False
            await self._sync_key_status(idempotency_key, status, event_id)
            return True

        return await self._write(op)
