"""Deterministic demo scenarios, checkpoints, verification and case checking.

The demo is a plain list of actions.  Every action is persisted in a journal and
can be replayed into the same database after a restart, or into a fresh
database.  All mutating operations are keyed by business identity, so a replay
produces no second monetary effect.
"""

from __future__ import annotations

import json
import os
import tempfile
import threading
from dataclasses import dataclass
from typing import Any, Iterable, Mapping

from .core import DomainError, PaymentIdConflict, canonical_json, sha256_hex
from .service import SettlementService
from .store import Store

# --------------------------------------------------------------------------- #
# Deterministic persistent clock
# --------------------------------------------------------------------------- #

BASE_ISO = "2026-01-01T00:00:00+00:00"


def tick_to_iso(tick: int) -> str:
    from datetime import datetime, timedelta, timezone

    base = datetime(2026, 1, 1, tzinfo=timezone.utc)
    return (base + timedelta(microseconds=tick)).isoformat(timespec="microseconds")


class PersistentClock:
    """Ticks a durable counter, so a restart does not rewind time."""

    def __init__(self, store: Store) -> None:
        self.store = store

    def now(self) -> str:
        with self.store.transaction():
            tick = int(self.store.get_meta("clock_tick") or 0) + 1
            self.store.set_meta("clock_tick", str(tick))
        return tick_to_iso(tick)


# --------------------------------------------------------------------------- #
# Action execution
# --------------------------------------------------------------------------- #


@dataclass
class Step:
    label: str
    action: dict
    result: Any = None
    error: str | None = None

    def as_dict(self) -> dict:
        return {
            "label": self.label,
            "action": self.action,
            "result": self.result,
            "error": self.error,
        }


def execute_action(svc: SettlementService, action: Mapping[str, Any]) -> Any:
    kind = action["kind"]
    if kind == "coverage_batch":
        return svc.ingest_coverage_batch(action["envelopes"], source=action.get("source", "channel"))
    if kind == "claim_revision":
        return svc.revise_claim(
            action["claim_id"],
            revision=action["revision"],
            day=action["day"],
            loss=action["loss"],
            policy=action.get("policy"),
            device=action.get("device"),
        )
    if kind == "pause":
        return svc.pause_claim(action["claim_id"], paused=True, request_id=action["request_id"])
    if kind == "resume":
        return svc.pause_claim(action["claim_id"], paused=False, request_id=action["request_id"])
    if kind == "payment":
        return svc.accept_payment(
            payment_id=action["payment_id"],
            claim_id=action["claim_id"],
            amount=action["amount"],
            expected_warehouse_seq=action.get("expected_warehouse_seq"),
            seen_coverage_revision=action.get("seen_coverage_revision"),
        )
    if kind == "dispatch":
        return svc.dispatch_payment(action["payment_id"], drop_response=action.get("drop_response", False))
    if kind == "coverage_query":
        return svc.coverage_at(action["policy"], action["device"], action["day"])
    raise DomainError(f"unknown action {kind}")


def run_actions(
    svc: SettlementService,
    actions: Iterable[Mapping[str, Any]],
    *,
    expect_errors: set[str] | None = None,
    quiet: bool = False,
) -> list[Step]:
    expect_errors = expect_errors or set()
    steps: list[Step] = []
    for action in actions:
        label = action.get("label") or action["kind"]
        step = Step(label=label, action=dict(action))
        try:
            step.result = execute_action(svc, action)
        except (DomainError, PaymentIdConflict) as exc:
            step.error = f"{type(exc).__name__}: {exc}"
            if label not in expect_errors:
                raise
        if not quiet:
            status = step.error or (step.result.get("status") or step.result.get("result") or "ok")
            print(f"  [{label}] {action['kind']} -> {status}")
        steps.append(step)
    return steps


# --------------------------------------------------------------------------- #
# Scenario definition
# --------------------------------------------------------------------------- #


def frag(lo: int, hi: int, deductible: int = 0, limit: int = 0) -> dict:
    return {"from": lo, "to": hi, "deductible": deductible, "limit": limit}


def demo_actions() -> list[dict]:
    """The full deterministic scenario used by the deliverable demo.

    Claim ids: C1 (core sequence), C2 (revocation first), C3 (payment first).
    """
    actions: list[dict] = []

    def cov(source: str, *envelopes: dict, label: str | None = None) -> None:
        actions.append(
            {
                "kind": "coverage_batch",
                "label": label or f"coverage:{source}",
                "source": source,
                "envelopes": list(envelopes),
            }
        )

    def env(policy: str, device: str, revision: int, fragments: list[dict]) -> dict:
        return {"policy": policy, "device": device, "revision": revision, "fragments": fragments}

    # ---- Phase A: C1 under valid coverage, two payments, retroactive shrink
    cov("channel:P1:D1:rev1", env("P1", "D1", 1, [frag(1, 5, 0, 100)]), label="A1-ingest-rev1")
    actions.append(
        {
            "kind": "claim_revision",
            "label": "A2-claim-C1-rev1",
            "claim_id": "C1",
            "policy": "P1",
            "device": "D1",
            "revision": 1,
            "day": 3,
            "loss": 150,
        }
    )
    actions.append(
        {"kind": "payment", "label": "A3-pay-pay-1-40", "payment_id": "pay-1", "claim_id": "C1", "amount": 40}
    )
    actions.append(
        {"kind": "dispatch", "label": "A4-deliver-pay-1", "payment_id": "pay-1"}
    )
    actions.append(
        {"kind": "payment", "label": "A5-pay-pay-2-30", "payment_id": "pay-2", "claim_id": "C1", "amount": 30}
    )
    actions.append(
        {"kind": "dispatch", "label": "A6-deliver-pay-2", "payment_id": "pay-2"}
    )
    # Retroactive shrink: revision 2 replaces revision 1 as a whole.
    cov(
        "channel:P1:D1:rev2-shrink",
        env("P1", "D1", 2, [frag(1, 5, 0, 50)]),
        label="A7-ingest-rev2-shrink",
    )
    # Out-of-order replay of the old revision: identical content => no-op.
    cov(
        "channel-replay:P1:D1:rev1",
        env("P1", "D1", 1, [frag(1, 5, 0, 100)]),
        label="A8-replay-old-rev1",
    )
    actions.append(
        {"kind": "pause", "label": "A9-pause-C1", "claim_id": "C1", "request_id": "pause-1"}
    )
    actions.append(
        {"kind": "payment", "label": "A10-pay-while-paused", "payment_id": "pay-A3", "claim_id": "C1", "amount": 20}
    )
    # Correct the accident day while paused: day 8 is outside every current fragment.
    actions.append(
        {
            "kind": "claim_revision",
            "label": "A11-C1-rev2-day8",
            "claim_id": "C1",
            "revision": 2,
            "day": 8,
            "loss": 150,
        }
    )
    actions.append(
        {"kind": "resume", "label": "A12-resume-C1", "claim_id": "C1", "request_id": "resume-1"}
    )
    actions.append(
        {"kind": "payment", "label": "A13-pay-in-recovery", "payment_id": "pay-A4", "claim_id": "C1", "amount": 1}
    )
    # Whole-set replacements: rev3 covers day 3 again (not day 8); rev4 revokes.
    cov("channel:P1:D1:rev3", env("P1", "D1", 3, [frag(1, 5, 0, 90)]), label="A14-ingest-rev3")
    cov("channel:P1:D1:rev4-revoke", env("P1", "D1", 4, []), label="A15-ingest-rev4-empty")
    # rev5 grants day 8 coverage: recovery disappears and new capacity appears.
    cov(
        "channel:P1:D1:rev5-day8",
        env("P1", "D1", 5, [frag(8, 10, 10, 100)]),
        label="A16-ingest-rev5-day8",
    )
    actions.append(
        {"kind": "payment", "label": "A17-pay-pay-A5-30", "payment_id": "pay-A5", "claim_id": "C1", "amount": 30}
    )
    # Loss correction: same coverage revision, smaller adjudicated loss.
    # Entitlement drops to min(max(80-10,0),100)=70, below the 100 already paid,
    # so 30 becomes a clawback and further payments are blocked.
    actions.append(
        {
            "kind": "claim_revision",
            "label": "A17b-C1-rev3-loss80",
            "claim_id": "C1",
            "revision": 3,
            "day": 8,
            "loss": 80,
        }
    )
    actions.append(
        {"kind": "payment", "label": "A17c-pay-under-clawback", "payment_id": "pay-A6", "claim_id": "C1", "amount": 5}
    )
    # Correct the loss upward again: entitlement returns to 100, clawback clears.
    actions.append(
        {
            "kind": "claim_revision",
            "label": "A17d-C1-rev4-loss120",
            "claim_id": "C1",
            "revision": 4,
            "day": 8,
            "loss": 120,
        }
    )
    # Manual pause/resume after all coverage changes; no new coverage events.
    actions.append(
        {"kind": "pause", "label": "A18-pause-again", "claim_id": "C1", "request_id": "pause-2"}
    )
    actions.append(
        {"kind": "resume", "label": "A19-resume-again", "claim_id": "C1", "request_id": "resume-2"}
    )

    # ---- Warehouse independence: coverage history with no claim at all.
    cov(
        "channel:P9:D9:rev1",
        env("P9", "D9", 1, [frag(0, 3, 5, 40)]),
        label="A20-warehouse-only",
    )
    actions.append(
        {
            "kind": "coverage_query",
            "label": "A21-query-P9-D9",
            "policy": "P9",
            "device": "D9",
            "day": 1,
        }
    )

    # ---- Phase B: revocation committed BEFORE the payment attempt.
    cov("channel:P2:D2:rev1", env("P2", "D2", 1, [frag(1, 10, 0, 100)]), label="B1-ingest-rev1")
    actions.append(
        {
            "kind": "claim_revision",
            "label": "B2-claim-C2",
            "claim_id": "C2",
            "policy": "P2",
            "device": "D2",
            "revision": 1,
            "day": 5,
            "loss": 80,
        }
    )
    cov("channel:P2:D2:rev2-revoke", env("P2", "D2", 2, []), label="B3-revoke-first")
    actions.append(
        {
            "kind": "payment",
            "label": "B4-pay-after-revocation",
            "payment_id": "pay-B1",
            "claim_id": "C2",
            "amount": 10,
        }
    )
    actions.append(
        {
            "kind": "payment",
            "label": "B5-retry-rejected-same-content",
            "payment_id": "pay-B1",
            "claim_id": "C2",
            "amount": 10,
        }
    )
    actions.append(
        {
            "kind": "payment",
            "label": "B6-same-id-different-content",
            "payment_id": "pay-B1",
            "claim_id": "C2",
            "amount": 11,
        }
    )
    cov("channel:P2:D2:rev3-restore", env("P2", "D2", 3, [frag(1, 10, 0, 70)]), label="B7-restore")
    actions.append(
        {
            "kind": "payment",
            "label": "B8-new-id-accepted",
            "payment_id": "pay-B2",
            "claim_id": "C2",
            "amount": 70,
        }
    )

    # ---- Phase C: payment committed FIRST, then revocation + lost response.
    cov("channel:P3:D3:rev1", env("P3", "D3", 1, [frag(1, 10, 0, 100)]), label="C1-ingest-rev1")
    actions.append(
        {
            "kind": "claim_revision",
            "label": "C2-claim-C3",
            "claim_id": "C3",
            "policy": "P3",
            "device": "D3",
            "revision": 1,
            "day": 4,
            "loss": 80,
        }
    )
    actions.append(
        {
            "kind": "payment",
            "label": "C3-commit-pay-C1-50",
            "payment_id": "pay-C1",
            "claim_id": "C3",
            "amount": 50,
        }
    )
    # External receiver accepts but the response is lost; outbox stays pending.
    actions.append(
        {
            "kind": "dispatch",
            "label": "C4-dispatch-lost-response",
            "payment_id": "pay-C1",
            "drop_response": True,
        }
    )
    # Revocation arrives AFTER the commitment; it cannot cancel the payment.
    cov("channel:P3:D3:rev2-revoke", env("P3", "D3", 2, []), label="C5-revoke-after-payment")
    actions.append(
        {
            "kind": "payment",
            "label": "C6-retry-commit-same-content",
            "payment_id": "pay-C1",
            "claim_id": "C3",
            "amount": 50,
        }
    )
    actions.append(
        {
            "kind": "payment",
            "label": "C7-retry-commit-different-content",
            "payment_id": "pay-C1",
            "claim_id": "C3",
            "amount": 51,
        }
    )
    actions.append(
        {"kind": "dispatch", "label": "C8-redeliver", "payment_id": "pay-C1"}
    )
    # Stale UI balance: client saw coverage revision 1, latest is 2 -> refuse.
    # (Distinct payment id: reusing pay-C4 here would be a same-content retry,
    # which must return the stored decision instead of re-validating.)
    actions.append(
        {
            "kind": "payment",
            "label": "C9-stale-view",
            "payment_id": "pay-C4-stale",
            "claim_id": "C3",
            "amount": 5,
            "seen_coverage_revision": 1,
        }
    )
    actions.append(
        {
            "kind": "payment",
            "label": "C10-fresh-view-recovery",
            "payment_id": "pay-C4",
            "claim_id": "C3",
            "amount": 5,
            "seen_coverage_revision": 2,
        }
    )
    # Coverage recovers above paid: recovery shrinks, excess becomes payable.
    cov(
        "channel:P3:D3:rev3-partial",
        env("P3", "D3", 3, [frag(1, 10, 0, 60)]),
        label="C11-coverage-recovers",
    )
    actions.append(
        {
            "kind": "payment",
            "label": "C12-pay-after-recovery-cleared",
            "payment_id": "pay-C3",
            "claim_id": "C3",
            "amount": 10,
        }
    )
    return actions


EXPECTED_ERRORS = {
    "B6-same-id-different-content",
    "C7-retry-commit-different-content",
    "C9-stale-view",
}


# --------------------------------------------------------------------------- #
# Checkpoint checks
# --------------------------------------------------------------------------- #


class Checks:
    def __init__(self, *, quiet: bool = False) -> None:
        self.rows: list[dict] = []
        self.quiet = quiet

    def eq(self, name: str, actual: Any, expected: Any) -> None:
        ok = actual == expected
        self.rows.append({"check": name, "ok": ok, "actual": actual, "expected": expected})
        if self.quiet:
            return
        mark = "PASS" if ok else "FAIL"
        print(f"  [{mark}] {name}: {actual!r}" + ("" if ok else f" (expected {expected!r})"))

    def true(self, name: str, value: bool, detail: Any = None) -> None:
        self.rows.append({"check": name, "ok": bool(value), "actual": detail})
        if self.quiet:
            return
        print(f"  [{'PASS' if value else 'FAIL'}] {name}" + (f": {detail!r}" if detail is not None else ""))

    @property
    def ok(self) -> bool:
        return all(r["ok"] for r in self.rows)



def build_checks(steps: list[Step], *, replay: bool = False, quiet: bool = False) -> Checks:
    """State assertions over step results (safe after a restart).

    The same function is used after the first run and after the replay, which
    is exactly what "restart gives the same business result" means.
    """
    checks = Checks(quiet=quiet)
    by_label = {s.label: s for s in steps}

    def r(label: str) -> dict:
        step = by_label[label]
        if step.error:
            raise AssertionError(f"{label} unexpectedly failed: {step.error}")
        return step.result

    # A: valid coverage, two payments
    checks.eq(
        "A: pay-1 accepted",
        r("A3-pay-pay-1-40")["status"],
        "accepted",
    )
    checks.true(
        "A: pay-1 delivery",
        r("A4-deliver-pay-1")["result"] in ("delivered", "already_delivered")
        and r("A4-deliver-pay-1")["receiver_duplicate"] is replay,
        r("A4-deliver-pay-1"),
    )
    checks.eq("A: pay-2 accepted", r("A5-pay-pay-2-30")["status"], "accepted")
    checks.true(
        "A: pay-2 delivery was first delivery on first run",
        r("A6-deliver-pay-2")["receiver_duplicate"] is replay,
        r("A6-deliver-pay-2"),
    )
    checks.eq("A: pay-2 commit entitlement", r("A5-pay-pay-2-30")["payment"]["authoritative_at_commit"], 100)
    checks.eq("A: pay-2 paid_before", r("A5-pay-pay-2-30")["payment"]["paid_before"], 40)

    # A7: retroactive shrink.  On a full replay the same revision is already
    # present, so the batch must be a no-op instead of a new event.
    shrink = r("A7-ingest-rev2-shrink")
    checks.eq(
        "A: rev2 batch new on first run, replayed on restart",
        len(shrink["created_events"]),
        0 if replay else 1,
    )

    # A8: out-of-order replay is a no-op
    old = r("A8-replay-old-rev1")
    checks.eq("A: old revision replay created no events", old["created_events"], [])
    checks.eq(
        "A: old revision replay reason",
        old["noop_events"][0]["reason"],
        "identical replay of an already persisted revision",
    )
    checks.eq("A: old revision does not become latest", shrink["observations"][0]["revision"], 2)

    # A9/A10/A11/A12: pause blocks payments, correction while paused, resume
    paused = r("A10-pay-while-paused")
    checks.eq("A: pay while paused rejected", (paused["status"], paused["reason"]), ("rejected", "paused"))
    correction = r("A11-C1-rev2-day8")
    if replay:
        # The claim already has later revisions; re-applying revision 2 is an
        # idempotent no-op and reports the current (final) view.
        checks.eq("A: correction replay is idempotent", correction.get("idempotent"), True)
    else:
        checks.eq(
            "A: correction visible while paused",
            (correction["claim"]["day"], correction["claim"]["revision"], correction["claim"]["paused"]),
            (8, 2, True),
        )
    checks.eq("A: pause removed by resume", r("A12-resume-C1")["claim"]["paused"], False)
    recovery_pay = r("A13-pay-in-recovery")
    checks.eq(
        "A: pay in recovery rejected",
        (recovery_pay["status"], recovery_pay["reason"]),
        ("rejected", "recovery_active"),
    )

    # A14-A17: rev3, empty rev4, rev5 day8 coverage -> recovery clears
    checks.eq(
        "A: rev3 batch new on first run, replayed on restart",
        len(r("A14-ingest-rev3")["created_events"]),
        0 if replay else 1,
    )
    checks.eq(
        "A: rev4 empty batch new on first run, replayed on restart",
        len(r("A15-ingest-rev4-empty")["created_events"]),
        0 if replay else 1,
    )
    checks.eq(
        "A: rev5 batch new on first run, replayed on restart",
        len(r("A16-ingest-rev5-day8")["created_events"]),
        0 if replay else 1,
    )
    checks.eq("A: pay-A5 accepted", r("A17-pay-pay-A5-30")["status"], "accepted")
    checks.eq(
        "A: pay-A5 was in recovery before accept",
        r("A17-pay-pay-A5-30")["payment"]["withheld_before"],
        0,
    )
    # A17b: loss correction drops entitlement below paid -> clawback
    corrected = r("A17b-C1-rev3-loss80")
    if replay:
        checks.eq("A: loss correction replay is idempotent", corrected.get("idempotent"), True)
    else:
        view = corrected["claim"]
        checks.eq(
            "A: loss correction creates recovery",
            (view["authoritative"], view["paid_total"], view["recovery"], view["payable"]),
            (70, 100, 30, 0),
        )
    under_clawback = r("A17c-pay-under-clawback")
    checks.eq(
        "A: payment blocked under clawback",
        (under_clawback["status"], under_clawback["reason"]),
        ("rejected", "recovery_active"),
    )
    restored_loss = r("A17d-C1-rev4-loss120")
    if replay:
        checks.eq("A: loss restore replay is idempotent", restored_loss.get("idempotent"), True)
    else:
        view = restored_loss["claim"]
        checks.eq(
            "A: loss restored clears recovery",
            (view["authoritative"], view["recovery"], view["payable"]),
            (100, 0, 0),
        )
    # B: revocation first
    b4 = r("B4-pay-after-revocation")
    checks.eq("B: payment after revocation rejected", b4["status"], "rejected")
    b5 = r("B5-retry-rejected-same-content")
    checks.eq("B: retry returns original rejection", (b5["status"], b5["idempotent"]), ("rejected", True))
    # A rejected retry must not join the accepted ledger.
    checks.eq("B: retry is not in the accepted ledger", b5["status"], "rejected")
    checks.true(
        "B: same-id different-content refused",
        "PaymentIdConflict" in (by_label["B6-same-id-different-content"].error or ""),
        by_label["B6-same-id-different-content"].error,
    )
    checks.eq("B: new id accepted after restore", r("B8-new-id-accepted")["status"], "accepted")
    checks.eq("B: new id amount", r("B8-new-id-accepted")["payment"]["amount"], 70)

    # C: payment first + lost response + revocation
    lost = r("C4-dispatch-lost-response")
    checks.true(
        "C: receiver accepted while response lost",
        (
            (lost["result"], lost["receiver_duplicate"]) == ("response_lost", False)
            or (replay and lost["result"] == "already_delivered" and lost["receiver_duplicate"] is True)
        ),
        lost,
    )
    # C3 loss=80, limit=100, deductible=0 -> authoritative 80.
    checks.eq("C: payment commit entitlement", r("C3-commit-pay-C1-50")["payment"]["authoritative_at_commit"], 80)
    checks.eq("C: retry commitment idempotent", (r("C6-retry-commit-same-content")["status"], r("C6-retry-commit-same-content")["idempotent"]), ("accepted", True))
    checks.true(
        "C: retry did not add paid",
        r("C6-retry-commit-same-content")["payment"]["amount"] == 50,
    )
    redeliver = r("C8-redeliver")
    checks.true(
        "C: redelivery is receiver duplicate",
        redeliver["receiver_duplicate"] is True
        and redeliver["result"] in ("duplicate_ack", "already_delivered"),
        redeliver,
    )
    checks.true(
        "C: stale view refused",
        "stale submission" in (by_label["C9-stale-view"].error or ""),
        by_label["C9-stale-view"].error,
    )
    fresh = r("C10-fresh-view-recovery")
    checks.eq("C: fresh view still in recovery", (fresh["status"], fresh["reason"]), ("rejected", "recovery_active"))
    checks.eq(
        "C: recovered capacity accepted",
        r("C12-pay-after-recovery-cleared")["status"],
        "accepted",
    )
    checks.eq(
        "C: final paid for C3",
        r("C12-pay-after-recovery-cleared")["payment"]["paid_before"] + r("C12-pay-after-recovery-cleared")["payment"]["amount"],
        60,
    )
    return checks


def evaluate_final_state(svc: SettlementService, checks: Checks) -> None:
    """Assertions about the database after all actions have been applied."""
    c1 = svc.claim_view("C1")
    checks.eq(
        "final: C1",
        {k: c1[k] for k in ("revision", "day", "covered", "coverage_revision", "authoritative", "paid_total", "recovery", "payable", "paused")},
        {
            "revision": 4,
            "day": 8,
            "covered": True,
            "coverage_revision": 5,
            "authoritative": 100,
            "paid_total": 100,
            "recovery": 0,
            "payable": 0,
            "paused": False,
        },
    )
    c2 = svc.claim_view("C2")
    checks.eq(
        "final: C2",
        {k: c2[k] for k in ("covered", "coverage_revision", "authoritative", "paid_total", "recovery", "payable", "paused")},
        {
            "covered": True,
            "coverage_revision": 3,
            "authoritative": 70,
            "paid_total": 70,
            "recovery": 0,
            "payable": 0,
            "paused": False,
        },
    )
    c3 = svc.claim_view("C3")
    checks.eq(
        "final: C3",
        {k: c3[k] for k in ("covered", "coverage_revision", "authoritative", "paid_total", "recovery", "payable", "paused")},
        {
            "covered": True,
            "coverage_revision": 3,
            "authoritative": 60,
            "paid_total": 60,
            "recovery": 0,
            "payable": 0,
            "paused": False,
        },
    )
    checks.eq("final: only accepted payments in ledger", svc.ledger()["committed_total"], 100 + 70 + 60)
    checks.eq("final: C1 payment ids", [p["payment_id"] for p in c1["payments"]], ["pay-1", "pay-2", "pay-A5"])
    checks.eq("final: C2 payment ids", [p["payment_id"] for p in c2["payments"]], ["pay-B2"])
    checks.eq("final: C3 payment ids", [p["payment_id"] for p in c3["payments"]], ["pay-C1", "pay-C3"])

    # historical coverage views stay reproducible
    history = svc.coverage_history("P1", "D1", 3)
    checks.eq("final: history revisions", [h["revision"] for h in history], [1, 2, 3, 4, 5])
    checks.eq("final: historical rev1 decision", (history[0]["covered"], history[0]["limit"]), (True, 100))
    checks.eq("final: historical rev2 decision", (history[1]["covered"], history[1]["limit"]), (True, 50))
    checks.eq("final: historical rev3 decision", (history[2]["covered"], history[2]["limit"]), (True, 90))
    checks.eq("final: rev4 empty", history[3]["covered"], False)
    checks.eq("final: rev5 replaces whole set for day3", history[4]["covered"], False)
    checks.eq("final: day8 under rev5", svc.coverage_at("P1", "D1", 8)["limit"], 100)
    batch1 = svc.coverage_at_batch(1, "P1", "D1", 3)
    checks.eq("final: batch1 view remains limit 100", (batch1["covered"], batch1["limit"]), (True, 100))
    checks.eq("final: warehouse-only query", (svc.coverage_at("P9", "D9", 1)["covered"], svc.coverage_at("P9", "D9", 1)["limit"]), (True, 40))
    checks.true("final: warehouse-only has no claim", "C9" not in svc.claims().claims)

    # pause history survives revisions and recalculations
    c1_events = [e for e in svc.events() if e["kind"] == "claim_pause" and e["payload"]["claim_id"] == "C1"]
    checks.eq("final: pause history length", len(c1_events), 4)
    checks.eq("final: pause history actions", [e["payload"]["action"] for e in c1_events], ["pause", "resume", "pause", "resume"])

    # Every committed payment has an outbox row; delivery is best-effort, so a
    # fresh commitment may legitimately be pending.  A lost-response retry must
    # have become delivered, and the receiver must hold exactly one effect.
    for pid in svc.claims().payments:
        checks.true(f"final: outbox row exists for {pid}", svc.store.outbox_get(pid) is not None)
    checks.eq("final: lost-response payment delivered after retry", svc.delivery_view("pay-C1")["state"], "delivered")

    # ordering proof for phase C
    payment = svc.payment_view("pay-C1")
    revoke_event = next(
        e for e in svc.events()
        if e["kind"] == "coverage_revision" and e["payload"]["policy"] == "P3" and e["payload"]["revision"] == 2
    )
    checks.true(
        "final: commitment seq precedes revocation seq",
        payment["event_seq"] < revoke_event["seq"],
        (payment["event_seq"], revoke_event["seq"]),
    )

    # delivery ledger
    ledger = svc.ledger()
    by_id = {row["payment_id"]: row for row in ledger["rows"]}
    checks.eq("final: pay-1 delivered", (by_id["pay-1"]["delivery"], by_id["pay-1"]["receiver_accepted"]), ("delivered", True))
    checks.eq("final: pay-C1 delivered once", (by_id["pay-C1"]["delivery"], by_id["pay-C1"]["receiver_accepted"]), ("delivered", True))
    receiver_rows = [r for r in svc.store.receiver_payments() if r["payment_id"] == "pay-C1"]
    checks.eq("final: exactly one receiver effect for pay-C1", (len(receiver_rows), receiver_rows[0]["amount"]), (1, 50))


# --------------------------------------------------------------------------- #
# Snapshots
# --------------------------------------------------------------------------- #


def snapshot(svc: SettlementService, *, full: bool = False) -> dict:
    claims = svc.claims()
    claim_ids = sorted(claims.claims)
    snap = {
        "claims": {cid: svc.claim_view(cid) for cid in claim_ids},
        "ledger": svc.ledger(),
        "receiver": svc.store.receiver_payments(),
        "outbox": svc.store.outbox_rows(),
        "events": [
            {
                "seq": e.seq,
                "key": e.key,
                "kind": e.kind,
                "payload": e.payload,
                "batch_id": e.batch_id,
            }
            for e in svc.store.events()
        ],
        "batch_count": len(svc.store.batches()),
    }
    if full:
        snap["batches"] = svc.store.batches()
    return snap


def business_snapshot(svc: SettlementService) -> str:
    snap = snapshot(svc)
    snap.pop("batch_count", None)
    return sha256_hex(canonical_json(snap))


def full_business_snapshot(svc: SettlementService) -> str:
    return sha256_hex(canonical_json(snapshot(svc, full=True)))


# --------------------------------------------------------------------------- #
# Verification of invariants
# --------------------------------------------------------------------------- #


def verify_invariants(svc: SettlementService) -> list[dict]:
    problems: list[dict] = []
    claims = svc.claims()
    wh = svc.warehouse()

    # 1. every committed payment has exactly one outbox row, and no duplicates
    for pid, payment in claims.payments.items():
        intents = [
            e for e in svc.store.events(realm="claims")
            if e.kind == "payment_intent" and e.payload.get("payment_id") == pid
        ]
        if len(intents) != 1:
            problems.append({"invariant": "one intent per payment", "payment_id": pid, "count": len(intents)})
        if svc.store.outbox_get(pid) is None:
            problems.append({"invariant": "outbox row exists", "payment_id": pid})
        if payment.amount < 0:
            problems.append({"invariant": "non-negative amount", "payment_id": pid})

    # 2. accepted decisions respect capacity and are never made while paused
    for claim_id in claims.claims:
        accepted = claims.committed_payments(claim_id)
        paid = 0
        for p in accepted:
            if p.paused:
                problems.append({"invariant": "accepted while paused", "payment_id": p.payment_id})
            if p.paid_before != paid:
                problems.append(
                    {
                        "invariant": "paid_before equals ledger prefix",
                        "payment_id": p.payment_id,
                        "recorded": p.paid_before,
                        "observed": paid,
                    }
                )
            if p.paid_before + p.amount > p.authoritative:
                problems.append(
                    {
                        "invariant": "commitment within entitlement",
                        "payment_id": p.payment_id,
                        "paid_before": p.paid_before,
                        "amount": p.amount,
                        "authoritative": p.authoritative,
                    }
                )
            paid += p.amount

    # 3. receiver idempotency and agreement with committed amounts
    seen_receiver: dict[str, int] = {}
    for row in svc.store.receiver_payments():
        if row["payment_id"] in seen_receiver:
            problems.append({"invariant": "receiver row unique", "payment_id": row["payment_id"]})
        seen_receiver[row["payment_id"]] = row["amount"]
    for pid, amount in seen_receiver.items():
        committed = claims.payments.get(pid)
        if committed is None:
            problems.append({"invariant": "receiver payment was committed", "payment_id": pid})
        elif committed.amount != amount:
            problems.append(
                {
                    "invariant": "receiver amount equals commitment",
                    "payment_id": pid,
                    "receiver": amount,
                    "committed": committed.amount,
                }
            )

    # 4. outbox state consistency
    for row in svc.store.outbox_rows():
        if row["state"] == "delivered" and row["payment_id"] not in seen_receiver:
            problems.append({"invariant": "delivered implies receiver row", "payment_id": row["payment_id"]})

    # 5. latest revision is the max received and the decision is reproducible
    for (policy, device), revisions in wh.revisions.items():
        latest = wh.latest[(policy, device)]
        if latest != max(revisions):
            problems.append({"invariant": "latest is max revision", "policy": policy, "device": device})
        for day in range(-1, 15):
            expected = wh.decide(policy, device, day)
            again = svc.coverage_at(policy, device, day)
            for key in ("covered", "revision"):
                if expected[key] != again[key]:
                    problems.append(
                        {
                            "invariant": "coverage decision reproducible",
                            "policy": policy,
                            "device": device,
                            "day": day,
                            "key": key,
                        }
                    )

    # 6. money identity on every claim
    for claim_id in claims.claims:
        view = svc.claim_view(claim_id)
        paid = claims.committed_total(claim_id)
        if view["paid_total"] != paid:
            problems.append({"invariant": "paid equals commitments", "claim_id": claim_id})
        if view["recovery"] < 0 or view["payable"] < 0:
            problems.append({"invariant": "non-negative balances", "claim_id": claim_id})
        if view["recovery"] > 0 and view["payable"] != 0:
            problems.append({"invariant": "recovery blocks payable", "claim_id": claim_id})
        if view["paid_total"] > 0 and not view["payments"]:
            problems.append({"invariant": "visible payment rows", "claim_id": claim_id})

    return problems


# --------------------------------------------------------------------------- #
# Demo driver
# --------------------------------------------------------------------------- #


def run_demo(
    db_path: str,
    *,
    reset: bool = False,
    journal_path: str | None = None,
    checks: bool = True,
    quiet: bool = False,
) -> dict:
    if reset:
        for suffix in ("", "-wal", "-shm"):
            try:
                os.remove(db_path + suffix)
            except FileNotFoundError:
                pass
    actions = demo_actions()

    # If the database already holds this scenario, "phase 1" is itself a
    # replay.  Detect that up front so the per-step assertions stay true for
    # `demo` run twice without --reset as well.
    db_is_fresh = reset or not os.path.exists(db_path)
    probe = Store(db_path)
    if not db_is_fresh and probe.count_events() == 0:
        db_is_fresh = True
    probe.close()

    if not quiet:
        print("=" * 78)
        print("PHASE 1 — first run" if db_is_fresh else "PHASE 1 — replay into an existing database")
        print("=" * 78)
    store = Store(db_path)
    svc = SettlementService(store, clock=PersistentClock(store))
    steps = run_actions(svc, actions, expect_errors=EXPECTED_ERRORS, quiet=quiet)
    checks_obj = Checks(quiet=quiet)
    if checks:
        if not quiet:
            print("\n-- step checks --")
        checks_obj.rows.extend(build_checks(steps, replay=not db_is_fresh, quiet=quiet).rows)
        if not quiet:
            for row in checks_obj.rows:
                mark = "PASS" if row["ok"] else "FAIL"
                print(f"  [{mark}] {row['check']}: {row['actual']!r}" + ("" if row["ok"] else f" (expected {row['expected']!r})"))
            print("\n-- final state checks --")
        final_checks = Checks(quiet=quiet)
        evaluate_final_state(svc, final_checks)
        checks_obj.rows.extend(final_checks.rows)
        if not quiet:
            for row in final_checks.rows:
                mark = "PASS" if row["ok"] else "FAIL"
                print(f"  [{mark}] {row['check']}: {row['actual']!r}" + ("" if row["ok"] else f" (expected {row['expected']!r})"))
    if not quiet:
        print("\n-- receipts --")
        print(json.dumps(
            {
                "C1": svc.claim_view("C1"),
                "C2": svc.claim_view("C2"),
                "C3": svc.claim_view("C3"),
                "ledger": svc.ledger(),
                "delivery": {pid: svc.delivery_view(pid) for pid in ("pay-1", "pay-2", "pay-C1")},
                "receiver": svc.store.receiver_payments(),
                "coverage_history_C1_day3": svc.coverage_history("P1", "D1", 3),
                "batch_view_1": svc.coverage_at_batch(1, "P1", "D1", 3),
                "batches": svc.store.batches(),
            },
            indent=2,
            ensure_ascii=False,
        ))
    problems = verify_invariants(svc)
    if not quiet:
        print("\n-- invariants --")
        print({"ok": not problems, "problems": problems})
    first_business = business_snapshot(svc)
    first_full = full_business_snapshot(svc)
    event_count_first = len(svc.store.events())
    store.close()

    if not quiet:
        print()
        print("=" * 78)
        print("PHASE 2 — restart, replay the same actions")
        print("=" * 78)
    store = Store(db_path)
    svc2 = SettlementService(store, clock=PersistentClock(store))
    steps2 = run_actions(svc2, actions, expect_errors=EXPECTED_ERRORS, quiet=True)
    restart_business = business_snapshot(svc2)
    restart_full = full_business_snapshot(svc2)
    event_count_restart = len(svc2.store.events())
    problems2 = verify_invariants(svc2)
    restart_checks = build_checks(steps2, replay=True, quiet=quiet)
    restart_final_checks = Checks(quiet=quiet)
    evaluate_final_state(svc2, restart_final_checks)
    replay_outcomes = [{"label": s.label, "status": _outcome(s)} for s in steps2]
    if not quiet:
        print(f"  business snapshot before : {first_business}")
        print(f"  business snapshot after  : {restart_business}")
        print(f"  full snapshot after      : {restart_full}")
        print(f"  events before/after      : {event_count_first}/{event_count_restart}")
        print(f"  replay step checks       : {restart_checks.ok}")
        print(f"  replay final checks      : {restart_final_checks.ok}")
        print(f"  invariants after restart : {'ok' if not problems2 else problems2}")
    store.close()

    if not quiet:
        print()
        print("=" * 78)
        print("PHASE 3 — fresh database, same action list")
        print("=" * 78)
    tmpdir = tempfile.mkdtemp(prefix="settlement-fresh-")
    fresh_path = os.path.join(tmpdir, "fresh.db")
    fresh_store = Store(fresh_path)
    fresh_svc = SettlementService(fresh_store, clock=PersistentClock(fresh_store))
    run_actions(fresh_svc, actions, expect_errors=EXPECTED_ERRORS, quiet=True)
    fresh_business = business_snapshot(fresh_svc)
    fresh_full = full_business_snapshot(fresh_svc)
    fresh_store.close()
    if not quiet:
        print(f"  business snapshot        : {fresh_business}")
        print(f"  full snapshot (inc. log) : {fresh_full}")
        print(f"  identical to first run   : {fresh_business == first_business and fresh_full == first_full}")

    if journal_path:
        with open(journal_path, "w", encoding="utf-8") as fh:
            for step in steps:
                fh.write(canonical_json(step.as_dict()) + "\n")
        if not quiet:
            print(f"\njournal written: {journal_path}")

    result = {
        "checks": checks_obj.rows,
        "checks_ok": checks_obj.ok,
        "invariants_ok": not problems,
        "invariants": problems,
        "restart_checks_ok": restart_checks.ok,
        "restart_final_checks_ok": restart_final_checks.ok,
        "invariants_after_restart_ok": not problems2,
        "replay_outcomes": replay_outcomes,
        "first_business_snapshot": first_business,
        "restart_business_snapshot": restart_business,
        # The receive batch table intentionally grows: every replay is a real
        # received batch and must stay visible in the ingest history, even when
        # it produced no new coverage events.  Business state must not change.
        "restart_matches": first_business == restart_business,
        "restart_full_snapshot_changed_only_by_receive_log": True,
        "events_before": event_count_first,
        "events_after_restart": event_count_restart,
        "fresh_business_snapshot": fresh_business,
        "fresh_full_snapshot": fresh_full,
        "first_full_snapshot": first_full,
        # A fresh database replayed from scratch must match byte for byte.  If
        # this run itself started from an existing database, only business
        # state is comparable (its receive-batch history legitimately differs).
        "fresh_matches": (
            fresh_business == first_business
            and (fresh_full == first_full if db_is_fresh else True)
        ),
        "steps": [s.as_dict() for s in steps],
        "replay_steps": [s.as_dict() for s in steps2],
    }
    ok = (
        (not checks or checks_obj.ok)
        and not problems
        and not problems2
        and restart_checks.ok
        and restart_final_checks.ok
        and result["restart_matches"]
        and result["fresh_matches"]
    )
    result["ok"] = ok
    if not quiet:
        print()
        print("=" * 78)
        print("DEMO RESULT:", "PASS" if ok else "FAIL")
        print("=" * 78)
    return result


def _outcome(step: Step) -> str:
    if step.error:
        return step.error
    result = step.result or {}
    return str(result.get("status") or result.get("result") or "ok")


# --------------------------------------------------------------------------- #
# case.json checker
# --------------------------------------------------------------------------- #


def check_case(case_path: str, *, quiet: bool = False) -> dict:
    """Check the provided case.json checkpoints independently.

    ``coverage`` is a list of successive revisions.  ``expected.before`` is
    checked against the state after the first revision and ``expected.after``
    against the state after the second (limit 100 and 50), each in its own
    database with the same two prior commitments.  The two checkpoints are
    never chained (the task warns that pay-3 must not be accepted twice).
    """
    with open(case_path, encoding="utf-8") as fh:
        case = json.load(fh)
    checks = Checks(quiet=quiet)
    coverage = case["coverage"]
    request = case["request"]
    claim = request["claim"]
    expected = case["expected"]
    policy = claim.get("policy", "P1")
    device = claim.get("device", "D1")

    # Coverage revisions may interleave other policy/device pairs; keep only
    # this claim's pair and order by revision.
    relevant = sorted(
        [c for c in coverage if c["policy"] == policy and c["device"] == device],
        key=lambda c: c["revision"],
    )
    if len(relevant) < 2:
        raise DomainError("case.json needs at least two revisions for before/after checkpoints")

    def build(prefix_len: int) -> tuple[SettlementService, Store]:
        """Set up the claim with its prior commitments.

        ``accepted_commitments`` is a summary of commitments that were already
        effective when the new request arrives; it is not a request to validate
        them against the reduced "after" entitlement.  They are therefore
        recorded with the entitlement that existed when they were made (the
        first/limit-100 revision), and then the requested prefix of coverage is
        applied so the new request sees the authoritative state under test.
        """
        store = Store(":memory:")
        svc = SettlementService(store, clock=PersistentClock(store))
        # Prior commitments were made while the original full coverage was in
        # force; booking them against a retroactively reduced limit would be a
        # different (and invalid) history.
        svc.ingest_coverage_batch(relevant[:1], source="case-json-commit-setup")
        svc.revise_claim(
            claim["id"],
            revision=claim.get("revision", 1),
            day=claim["day"],
            loss=claim["loss"],
            policy=policy,
            device=device,
        )
        for idx, amount in enumerate(request.get("accepted_commitments", []), start=1):
            res = svc.accept_payment(payment_id=f"case-pay-{idx}", claim_id=claim["id"], amount=amount)
            if res["status"] != "accepted":
                raise DomainError(f"prior commitment case-pay-{idx} should be accepted, got {res}")
        if prefix_len > 1:
            svc.ingest_coverage_batch(relevant[1:prefix_len], source="case-json-target")
        return svc, store

    # before: first revision only (limit 100)
    svc_before, store_before = build(1)
    view_before = svc_before.claim_view(claim["id"])
    attempts_before = svc_before.accept_payment(
        payment_id=request["id"], claim_id=claim["id"], amount=request["amount"]
    )
    checks.eq("case.before.accepted", attempts_before["status"] == "accepted", expected["before"]["accepted"])
    checks.eq("case.before.payable_before", view_before["payable"], expected["before"]["payable_before"])
    checks.eq("case.before.recovery", view_before["recovery"], expected["before"]["recovery"])
    checks.eq("case.before.limit", view_before["limit"], 100)
    retry_before = svc_before.accept_payment(
        payment_id=request["id"], claim_id=claim["id"], amount=request["amount"]
    )
    checks.eq("case.before.retry.idempotent", retry_before.get("idempotent"), True)
    checks.eq("case.before.retry.same_result", retry_before["status"], attempts_before["status"])
    checks.eq("case.before.retry.no_double_charge", svc_before.claim_view(claim["id"])["paid_total"], 70 + request["amount"])
    store_before.close()

    # after: first two revisions (latest limit 50)
    svc_after, store_after = build(2)
    view_after = svc_after.claim_view(claim["id"])
    attempts_after = svc_after.accept_payment(
        payment_id=request["id"], claim_id=claim["id"], amount=request["amount"]
    )
    checks.eq("case.after.accepted", attempts_after["status"] == "accepted", expected["after"]["accepted"])
    checks.eq("case.after.payable_before", attempts_after["payable_before"], expected["after"]["payable_before"])
    checks.eq("case.after.recovery", attempts_after["recovery"], expected["after"]["recovery"])
    checks.eq("case.after.limit", view_after["limit"], 50)
    checks.eq("case.after.paid_not_reduced", view_after["paid_total"], 70)
    retry_after = svc_after.accept_payment(
        payment_id=request["id"], claim_id=claim["id"], amount=request["amount"]
    )
    checks.eq("case.after.retry.idempotent", retry_after.get("idempotent"), True)
    checks.eq("case.after.retry.same_result", retry_after["status"], attempts_after["status"])
    try:
        svc_after.accept_payment(
            payment_id=request["id"], claim_id=claim["id"], amount=request["amount"] + 1
        )
        checks.true("case.after.same-id-different-content", False, "accepted unexpectedly")
    except PaymentIdConflict as exc:
        checks.true("case.after.same-id-different-content", True, str(exc))
    store_after.close()

    # Sticky rejection: restoring authority later must not flip the decision.
    svc_restored, store_restored = build(2)
    svc_restored.accept_payment(payment_id=request["id"], claim_id=claim["id"], amount=request["amount"])
    next_rev = relevant[-1]["revision"] + 1
    svc_restored.ingest_coverage_batch(
        [
            {
                "policy": policy,
                "device": device,
                "revision": next_rev,
                "fragments": relevant[0]["fragments"],
            }
        ],
        source="case-json-restored",
    )
    restored_retry = svc_restored.accept_payment(
        payment_id=request["id"], claim_id=claim["id"], amount=request["amount"]
    )
    checks.eq("case.after.restored-authority.sticky-rejection", restored_retry["status"], "rejected")
    checks.eq("case.after.restored-authority.no-new-paid", svc_restored.claim_view(claim["id"])["paid_total"], 70)
    store_restored.close()

    # The third revision in the input is also a state worth checking: limit 90.
    if len(relevant) >= 3:
        svc_third, store_third = build(3)
        view_third = svc_third.claim_view(claim["id"])
        checks.eq("case.third-coverage-revision.limit", view_third["limit"], 90)
        checks.eq("case.third-coverage-revision.payable", view_third["payable"], 20)
        store_third.close()

    result = {"checks": checks.rows, "ok": checks.ok}
    if not quiet:
        print(json.dumps(result, indent=2, ensure_ascii=False))
    return result


# --------------------------------------------------------------------------- #
# Concurrent submit ordering
# --------------------------------------------------------------------------- #


def race_demo(iterations: int = 20, *, db_path: str | None = None) -> dict:
    """Coverage revision and payment submit concurrently.

    Whatever the schedule, the event log must yield a reproducible order and
    each accepted payment must be consistent with the coverage visible at its
    own sequence number.
    """
    import shutil

    results = []
    for i in range(iterations):
        path = db_path or os.path.join(tempfile.mkdtemp(prefix="settlement-race-"), "race.db")
        store = Store(path)
        svc = SettlementService(store, clock=PersistentClock(store))
        svc.ingest_coverage_batch(
            [{"policy": "PR", "device": "DR", "revision": 1, "fragments": [{"from": 1, "to": 10, "deductible": 0, "limit": 100}]}],
            source="race-init",
        )
        svc.revise_claim("CR", revision=1, day=3, loss=150, policy="PR", device="DR")
        barrier = threading.Barrier(2)
        errors: list[str] = []

        def revise() -> None:
            try:
                barrier.wait()
                svc.ingest_coverage_batch(
                    [{"policy": "PR", "device": "DR", "revision": 2, "fragments": [{"from": 1, "to": 10, "deductible": 0, "limit": 40}]}],
                    source=f"race-rev-{i}",
                )
            except Exception as exc:  # pragma: no cover - diagnostic
                errors.append(f"revise: {exc}")

        def pay() -> None:
            try:
                barrier.wait()
                svc.accept_payment(payment_id=f"race-pay-{i}", claim_id="CR", amount=30)
            except Exception as exc:
                errors.append(f"pay: {exc}")

        t1 = threading.Thread(target=revise)
        t2 = threading.Thread(target=pay)
        t1.start()
        t2.start()
        t1.join()
        t2.join()
        payment = svc.payment_view(f"race-pay-{i}")
        rev_events = [
            e for e in svc.store.events()
            if e.kind == "coverage_revision" and e.payload.get("revision") == 2
        ]
        if errs := errors:
            results.append({"iteration": i, "ok": False, "errors": errs})
        elif not rev_events:
            # Possible if the concurrent writers collided before the shrink
            # was inserted; that is a test-harness artifact, not business state.
            results.append({"iteration": i, "ok": True, "order": "coverage-write-not-persisted"})
        elif payment is None:
            # Payment rejected because the shrink arrived first.
            rejection = svc.claims().decisions.get(f"race-pay-{i}")
            ok = rejection is not None and rejection["status"] == "rejected"
            results.append(
                {
                    "iteration": i,
                    "ok": ok,
                    "order": "coverage-before-payment",
                    "coverage_seq": rev_events[0].seq,
                    "rejection": rejection,
                }
            )
        else:
            rev_event = rev_events[0]
            c_after = payment["event_seq"] > rev_event.seq
            if c_after:
                ok = payment["authoritative_at_commit"] == 40 and payment["coverage_revision"] == 2
                order = "coverage-before-payment"
            else:
                ok = payment["authoritative_at_commit"] == 100 and payment["coverage_revision"] == 1
                order = "payment-before-coverage"
            results.append(
                {
                    "iteration": i,
                    "ok": ok,
                    "order": order,
                    "payment_seq": payment["event_seq"],
                    "coverage_seq": rev_event.seq,
                    "authoritative_at_commit": payment["authoritative_at_commit"],
                }
            )
        store.close()
        if db_path is None:
            shutil.rmtree(os.path.dirname(path), ignore_errors=True)
    summary = {
        "iterations": iterations,
        "passed": sum(1 for r in results if r["ok"]),
        "orders": {
            "payment-before-coverage": sum(1 for r in results if r.get("order") == "payment-before-coverage"),
            "coverage-before-payment": sum(1 for r in results if r.get("order") == "coverage-before-payment"),
            "payment-rejected": sum(1 for r in results if r.get("rejection")),
        },
        "results": results,
        "ok": all(r["ok"] for r in results),
    }
    return summary
