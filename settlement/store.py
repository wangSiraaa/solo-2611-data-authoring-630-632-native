"""Durable SQLite store.

One file holds both the shared event log and the simulated external payment
receiver.  SQLite gives us the atomicity the task asks for:

* ``BEGIN IMMEDIATE`` serialises writers, so a coverage revision and a payment
  commitment have one reproducible order;
* the commit decision (payment row + ``payment_intent`` event) is one
  transaction, so a crash can never leave "accepted but not recorded";
* the outbox row and its state transition are committed atomically with the
  business effect, so a lost receiver response is recoverable by replaying the
  same idempotent message id.
"""

from __future__ import annotations

import json
import sqlite3
import threading
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

from .core import (
    Event,
    canonical_json,
)


SCHEMA = """
PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS events (
    seq         INTEGER PRIMARY KEY AUTOINCREMENT,
    key         TEXT NOT NULL UNIQUE,
    kind        TEXT NOT NULL,
    payload     TEXT NOT NULL,
    occurred_at TEXT NOT NULL,
    recorded_at TEXT NOT NULL,
    origin      TEXT NOT NULL,
    batch_id    INTEGER REFERENCES batches(id)
);

CREATE INDEX IF NOT EXISTS events_kind_seq ON events(kind, seq);

CREATE TABLE IF NOT EXISTS batches (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    source      TEXT NOT NULL,
    digest      TEXT NOT NULL,
    received_at TEXT NOT NULL
);

-- Simulated external receiver: idempotent on payment_id.
CREATE TABLE IF NOT EXISTS receiver_payments (
    payment_id   TEXT PRIMARY KEY,
    claim_id     TEXT NOT NULL,
    amount       INTEGER NOT NULL CHECK (amount >= 0),
    status       TEXT NOT NULL,
    received_at  TEXT NOT NULL,
    received_seq INTEGER NOT NULL
);

-- Durable outbox: one row per accepted payment, consumed by dispatch.
-- No FK to receiver_payments: the receiver row appears only when the external
-- side actually accepts the message, which is deliberately after commitment.
CREATE TABLE IF NOT EXISTS outbox (
    payment_id   TEXT PRIMARY KEY,
    delivery_seq INTEGER NOT NULL,
    state        TEXT NOT NULL,           -- pending | delivered
    attempts     INTEGER NOT NULL DEFAULT 0,
    last_error   TEXT
);
"""


def _dict_factory(cursor: sqlite3.Cursor, row: tuple) -> dict:
    return {col[0]: row[idx] for idx, col in enumerate(cursor.description)}


class Store:
    def __init__(self, path: str | Path) -> None:
        self.path = str(path)
        self.conn = sqlite3.connect(
            self.path, isolation_level=None, timeout=30.0, check_same_thread=False
        )
        self.conn.row_factory = _dict_factory
        self.conn.execute("PRAGMA busy_timeout = 30000")
        self.conn.executescript(SCHEMA)
        # One in-process writer connection shared by service methods.  The lock
        # is re-entrant so a transaction can call reader helpers that also lock.
        self._lock = threading.RLock()

    def close(self) -> None:
        with self._lock:
            self.conn.close()

    # -- transactions ------------------------------------------------------ #

    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        """Serialised write transaction.

        ``BEGIN IMMEDIATE`` takes the write lock up front; two concurrent
        submitters therefore observe a single total order, which is the
        reproducible "who committed first" basis the task requires.  The
        in-process RLock is needed because SQLite connections are not safe for
        simultaneous use from two threads.
        """
        with self._lock:
            self.conn.execute("BEGIN IMMEDIATE")
            try:
                yield self.conn
            except BaseException:
                self.conn.execute("ROLLBACK")
                raise
            else:
                self.conn.execute("COMMIT")

    # -- meta -------------------------------------------------------------- #

    def get_meta(self, key: str) -> str | None:
        with self._lock:
            row = self.conn.execute("SELECT value FROM meta WHERE key = ?", (key,)).fetchone()
        return row["value"] if row else None

    def set_meta(self, key: str, value: str) -> None:
        with self._lock:
            self.conn.execute(
                "INSERT INTO meta(key, value) VALUES(?, ?) "
                "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                (key, value),
            )

    # -- events ------------------------------------------------------------ #

    def append_event(
        self,
        *,
        key: str,
        kind: str,
        payload: dict,
        occurred_at: str,
        recorded_at: str,
        origin: str,
        batch_id: int | None = None,
    ) -> tuple[Event, bool]:
        """Insert an event once.  Returns (event, created).

        ``key`` is the dedupe key: retries and replays find the existing row and
        get the original event back, with ``created=False``.
        """
        with self._lock:
            existing = self.get_event_by_key(key)
            if existing is not None:
                return existing, False
            cur = self.conn.execute(
                "INSERT INTO events(key, kind, payload, occurred_at, recorded_at, origin, batch_id) "
                "VALUES(?, ?, ?, ?, ?, ?, ?)",
                (key, kind, canonical_json(payload), occurred_at, recorded_at, origin, batch_id),
            )
            event = self.get_event(int(cur.lastrowid))
        assert event is not None
        return event, True

    def get_event(self, seq: int) -> Event | None:
        with self._lock:
            row = self.conn.execute("SELECT * FROM events WHERE seq = ?", (seq,)).fetchone()
        return self._row_to_event(row) if row else None

    def get_event_by_key(self, key: str) -> Event | None:
        with self._lock:
            row = self.conn.execute("SELECT * FROM events WHERE key = ?", (key,)).fetchone()
        return self._row_to_event(row) if row else None

    def events(self, *, realm: str | None = None) -> list[Event]:
        sql = "SELECT * FROM events"
        params: tuple[Any, ...] = ()
        if realm == "warehouse":
            sql += " WHERE key LIKE 'cov-batch:%'"
        elif realm == "claims":
            sql += " WHERE key NOT LIKE 'cov-batch:%'"
        sql += " ORDER BY seq"
        with self._lock:
            rows = self.conn.execute(sql, params).fetchall()
        return [self._row_to_event(r) for r in rows]

    @staticmethod
    def _row_to_event(row: dict) -> Event:
        return Event(
            seq=row["seq"],
            key=row["key"],
            kind=row["kind"],
            payload=json.loads(row["payload"]),
            occurred_at=row["occurred_at"],
            recorded_at=row["recorded_at"],
            origin=row["origin"],
            batch_id=row["batch_id"],
        )

    # -- batches ----------------------------------------------------------- #

    def create_batch(self, *, source: str, digest: str, received_at: str) -> int:
        with self._lock:
            cur = self.conn.execute(
                "INSERT INTO batches(source, digest, received_at) VALUES(?, ?, ?)",
                (source, digest, received_at),
            )
            return int(cur.lastrowid)

    def batches(self) -> list[dict]:
        with self._lock:
            return self.conn.execute("SELECT * FROM batches ORDER BY id").fetchall()

    # -- receiver ---------------------------------------------------------- #

    def receiver_upsert(self, *, payment_id: str, claim_id: str, amount: int, received_at: str) -> tuple[dict, bool]:
        """Idempotent receiver acceptance keyed by payment id."""
        with self._lock:
            existing = self.conn.execute(
                "SELECT * FROM receiver_payments WHERE payment_id = ?", (payment_id,)
            ).fetchone()
            if existing is not None:
                return existing, False
            seqcur = self.conn.execute("SELECT COALESCE(MAX(received_seq), 0) + 1 AS n FROM receiver_payments")
            n = seqcur.fetchone()["n"]
            self.conn.execute(
                "INSERT INTO receiver_payments(payment_id, claim_id, amount, status, received_at, received_seq) "
                "VALUES(?, ?, ?, 'accepted', ?, ?)",
                (payment_id, claim_id, amount, received_at, n),
            )
            row = self.conn.execute(
                "SELECT * FROM receiver_payments WHERE payment_id = ?", (payment_id,)
            ).fetchone()
        assert row is not None
        return row, True

    def receiver_payments(self) -> list[dict]:
        with self._lock:
            return self.conn.execute("SELECT * FROM receiver_payments ORDER BY received_seq").fetchall()

    # -- outbox ------------------------------------------------------------ #

    def outbox_add(self, payment_id: str, delivery_seq: int) -> None:
        with self._lock:
            self.conn.execute(
                "INSERT OR IGNORE INTO outbox(payment_id, delivery_seq, state, attempts) "
                "VALUES(?, ?, 'pending', 0)",
                (payment_id, delivery_seq),
            )

    def outbox_get(self, payment_id: str) -> dict | None:
        with self._lock:
            return self.conn.execute("SELECT * FROM outbox WHERE payment_id = ?", (payment_id,)).fetchone()

    def outbox_rows(self) -> list[dict]:
        with self._lock:
            return self.conn.execute("SELECT * FROM outbox ORDER BY delivery_seq").fetchall()

    def outbox_mark(
        self,
        payment_id: str,
        *,
        state: str,
        attempts: int,
        last_error: str | None,
    ) -> None:
        with self._lock:
            self.conn.execute(
                "UPDATE outbox SET state = ?, attempts = ?, last_error = ? WHERE payment_id = ?",
                (state, attempts, last_error, payment_id),
            )

    def next_delivery_seq(self) -> int:
        with self._lock:
            row = self.conn.execute(
                "SELECT COALESCE(MAX(delivery_seq), 0) + 1 AS n FROM outbox"
            ).fetchone()
        return int(row["n"])

    def count_events(self, *, kind: str | None = None, key_like: str | None = None) -> int:
        sql = "SELECT COUNT(*) AS n FROM events WHERE 1 = 1"
        params: list[Any] = []
        if kind is not None:
            sql += " AND kind = ?"
            params.append(kind)
        if key_like is not None:
            sql += " AND key LIKE ?"
            params.append(key_like)
        with self._lock:
            return int(self.conn.execute(sql, tuple(params)).fetchone()["n"])

    def receiver_for(self, payment_id: str) -> dict | None:
        with self._lock:
            return self.conn.execute(
                "SELECT * FROM receiver_payments WHERE payment_id = ?", (payment_id,)
            ).fetchone()
