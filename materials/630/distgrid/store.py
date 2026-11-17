"""SQLite-backed durable storage.

The database is the only place state lives between processes.  Reopening the
database and replaying the registered change / point rows reconstructs every
in-memory projection from scratch, so history products, reading cuts and
query facets never depend on a long-lived Python object.
"""
from __future__ import annotations

import json
import sqlite3
from pathlib import Path

SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS nodes (
    name TEXT PRIMARY KEY,
    kind TEXT NOT NULL CHECK (kind IN ('feeder', 'junction', 'load'))
);

CREATE TABLE IF NOT EXISTS edges (
    edge_id TEXT PRIMARY KEY,
    u       TEXT NOT NULL,
    v       TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS edge_init (
    edge_id  TEXT PRIMARY KEY,
    closed   INTEGER NOT NULL CHECK (closed IN (0, 1))
);

CREATE TABLE IF NOT EXISTS meters (
    meter_id TEXT PRIMARY KEY,
    load     TEXT NOT NULL UNIQUE,
    modulus  INTEGER NOT NULL,
    rate     INTEGER NOT NULL
);

-- One row per *accepted* change version; old revisions stay forever.
CREATE TABLE IF NOT EXISTS changes (
    change_id    TEXT NOT NULL,
    revision     INTEGER NOT NULL,
    minute       INTEGER NOT NULL,
    actions_json TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    PRIMARY KEY (change_id, revision)
);

-- One row per *accepted* reading version; retracted rows stay forever.
CREATE TABLE IF NOT EXISTS points (
    meter_id     TEXT NOT NULL,
    point_no     INTEGER NOT NULL,
    revision     INTEGER NOT NULL,
    minute       INTEGER NOT NULL,
    value        INTEGER NOT NULL,
    reset        INTEGER NOT NULL CHECK (reset IN (0, 1)),
    retracted    INTEGER NOT NULL CHECK (retracted IN (0, 1)),
    content_hash TEXT NOT NULL,
    PRIMARY KEY (meter_id, point_no, revision)
);

CREATE TABLE IF NOT EXISTS products (
    product_id   TEXT PRIMARY KEY,
    spec_json    TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    created_cut  INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS cuts (
    cut_id    INTEGER PRIMARY KEY,
    at_change INTEGER NOT NULL,
    snapshot  TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS queries (
    query_id   TEXT PRIMARY KEY,
    product_id TEXT NOT NULL,
    cut_id     INTEGER NOT NULL,
    start      INTEGER NOT NULL,
    end        INTEGER NOT NULL,
    max_gap    INTEGER NOT NULL,
    result     TEXT NOT NULL,
    run_at     TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

CREATE TABLE IF NOT EXISTS audit (
    audit_id  INTEGER PRIMARY KEY AUTOINCREMENT,
    kind      TEXT NOT NULL,
    payload   TEXT NOT NULL,
    outcome   TEXT NOT NULL,
    detail    TEXT,
    logged_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);
"""


class Store:
    """Thin typed wrapper around one SQLite file."""

    def __init__(self, path: str | Path):
        self.path = str(path)
        first = not Path(self.path).exists()
        self.conn = sqlite3.connect(self.path)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA foreign_keys = ON")
        self.conn.executescript(SCHEMA)
        self.conn.commit()
        self.fresh = first

    # -- generic helpers --------------------------------------------------
    def commit(self) -> None:
        self.conn.commit()

    def rollback(self) -> None:
        self.conn.rollback()

    def audit(self, kind: str, payload: dict, outcome: str, detail: str | None = None) -> None:
        self.conn.execute(
            "INSERT INTO audit(kind, payload, outcome, detail) VALUES (?, ?, ?, ?)",
            (kind, json.dumps(payload, sort_keys=True), outcome, detail),
        )

    # -- facility ---------------------------------------------------------
    def save_facility(self, nodes: dict, edges: dict, initial_closed: set,
                      meters: dict) -> None:
        rows = self.conn.execute("SELECT COUNT(*) FROM meta").fetchone()[0]
        if rows:
            raise RuntimeError("facility already initialized in this database")
        for name, kind in nodes.items():
            self.conn.execute("INSERT INTO nodes(name, kind) VALUES (?, ?)", (name, kind))
        for edge_id, (u, v) in edges.items():
            self.conn.execute("INSERT INTO edges(edge_id, u, v) VALUES (?, ?, ?)",
                              (edge_id, u, v))
            self.conn.execute("INSERT INTO edge_init(edge_id, closed) VALUES (?, ?)",
                              (edge_id, 1 if edge_id in initial_closed else 0))
        for meter_id, spec in meters.items():
            self.conn.execute(
                "INSERT INTO meters(meter_id, load, modulus, rate) VALUES (?, ?, ?, ?)",
                (meter_id, spec["load"], spec["modulus"], spec["rate"]))
        self.conn.execute("INSERT INTO meta(key, value) VALUES ('facility', '1')")

    def load_facility(self) -> dict:
        nodes = {r["name"]: r["kind"] for r in self.conn.execute("SELECT * FROM nodes")}
        edges = {r["edge_id"]: (r["u"], r["v"])
                 for r in self.conn.execute("SELECT * FROM edges")}
        initial_closed = {r["edge_id"] for r in self.conn.execute(
            "SELECT edge_id FROM edge_init WHERE closed = 1")}
        meters = {r["meter_id"]: {"load": r["load"], "modulus": r["modulus"],
                                  "rate": r["rate"]}
                  for r in self.conn.execute("SELECT * FROM meters")}
        return {"nodes": nodes, "edges": edges,
                "initial_closed": initial_closed, "meters": meters}

    # -- ledger changes ---------------------------------------------------
    def head_revision(self, change_id: str) -> int | None:
        row = self.conn.execute(
            "SELECT MAX(revision) AS r FROM changes WHERE change_id = ?",
            (change_id,)).fetchone()
        return row["r"]

    def change_row(self, change_id: str, revision: int):
        return self.conn.execute(
            "SELECT * FROM changes WHERE change_id = ? AND revision = ?",
            (change_id, revision)).fetchone()

    def all_heads(self) -> list[sqlite3.Row]:
        return self.conn.execute(
            """SELECT c.* FROM changes c
               JOIN (SELECT change_id, MAX(revision) AS r FROM changes
                     GROUP BY change_id) h
                 ON c.change_id = h.change_id AND c.revision = h.r
               ORDER BY c.minute, c.change_id""").fetchall()

    def count_heads(self) -> int:
        row = self.conn.execute(
            "SELECT COUNT(DISTINCT change_id) AS n FROM changes").fetchone()
        return row["n"]

    def insert_change(self, change_id, revision, minute, actions_json, content_hash):
        self.conn.execute(
            "INSERT INTO changes(change_id, revision, minute, actions_json, content_hash)"
            " VALUES (?, ?, ?, ?, ?)",
            (change_id, revision, minute, actions_json, content_hash))

    # -- meter points -----------------------------------------------------
    def point_row(self, meter_id: str, point_no: int, revision: int):
        return self.conn.execute(
            "SELECT * FROM points WHERE meter_id = ? AND point_no = ? AND revision = ?",
            (meter_id, point_no, revision)).fetchone()

    def point_owner(self, point_no: int):
        row = self.conn.execute(
            "SELECT meter_id FROM points WHERE point_no = ? LIMIT 1",
            (point_no,)).fetchone()
        return row["meter_id"] if row else None

    def all_points(self) -> list[sqlite3.Row]:
        return self.conn.execute("SELECT * FROM points").fetchall()

    def meter_point_count(self, meter_id: str) -> int:
        row = self.conn.execute(
            "SELECT COUNT(DISTINCT point_no) AS n FROM points WHERE meter_id = ?",
            (meter_id,)).fetchone()
        return row["n"]

    def insert_point(self, meter_id, point_no, revision, minute, value,
                     reset, retracted, content_hash):
        self.conn.execute(
            "INSERT INTO points(meter_id, point_no, revision, minute, value, reset,"
            " retracted, content_hash) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (meter_id, point_no, revision, minute, value, reset, retracted,
             content_hash))

    # -- cuts / products / queries ---------------------------------------
    def next_cut_id(self) -> int:
        row = self.conn.execute("SELECT COALESCE(MAX(cut_id), 0) + 1 AS n FROM cuts").fetchone()
        return row["n"]

    def insert_cut(self, cut_id: int, at_change: int, snapshot: str) -> None:
        self.conn.execute("INSERT INTO cuts(cut_id, at_change, snapshot) VALUES (?, ?, ?)",
                          (cut_id, at_change, snapshot))

    def cut(self, cut_id: int) -> dict | None:
        row = self.conn.execute("SELECT * FROM cuts WHERE cut_id = ?", (cut_id,)).fetchone()
        return json.loads(row["snapshot"]) if row else None

    def max_cut_id(self) -> int:
        row = self.conn.execute("SELECT COALESCE(MAX(cut_id), 0) AS n FROM cuts").fetchone()
        return row["n"]

    def insert_product(self, product_id: str, spec_json: str,
                       content_hash: str, created_cut: int) -> None:
        self.conn.execute(
            "INSERT OR IGNORE INTO products(product_id, spec_json, content_hash, created_cut)"
            " VALUES (?, ?, ?, ?)",
            (product_id, spec_json, content_hash, created_cut))

    def product(self, product_id: str) -> dict | None:
        row = self.conn.execute("SELECT * FROM products WHERE product_id = ?",
                                (product_id,)).fetchone()
        if not row:
            return None
        spec = json.loads(row["spec_json"])
        spec["content_hash"] = row["content_hash"]
        return spec

    def insert_query(self, query_id, product_id, cut_id, start, end, max_gap, result):
        self.conn.execute(
            "INSERT OR IGNORE INTO queries(query_id, product_id, cut_id, start, end,"
            " max_gap, result) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (query_id, product_id, cut_id, start, end, max_gap,
             json.dumps(result, sort_keys=True)))

    def query_row(self, query_id: str):
        return self.conn.execute("SELECT * FROM queries WHERE query_id = ?",
                                 (query_id,)).fetchone()

    def close(self) -> None:
        self.conn.close()
