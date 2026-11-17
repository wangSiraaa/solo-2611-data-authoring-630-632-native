"""Tests (stdlib unittest only: no third-party dependencies).

Run: python3 -m unittest discover -s tests -v
"""

from __future__ import annotations

import json
import os
import tempfile
import unittest

from settlement.core import (
    ClaimRevisionConflict,
    CoverageRevisionConflict,
    DomainError,
    PaymentIdConflict,
    holdback_from_paid,
    indemnity,
    payable_balance,
    validate_fragments,
    InvalidCoverage,
)
from settlement.scenario import (
    EXPECTED_ERRORS,
    PersistentClock,
    business_snapshot,
    check_case,
    demo_actions,
    execute_action,
    race_demo,
    run_actions,
    run_demo,
    verify_invariants,
)
from settlement.service import SettlementService
from settlement.store import Store


class ServiceFixture(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.mkdtemp(prefix="settlement-test-")
        self.db_path = os.path.join(self.tmp, "test.db")
        self.store = Store(self.db_path)
        self.svc = SettlementService(self.store, clock=PersistentClock(self.store))

    def tearDown(self) -> None:
        self.store.close()

    def ingest(self, revision: int, fragments: list[dict], policy="P", device="D") -> dict:
        return self.svc.ingest_coverage_batch(
            [{"policy": policy, "device": device, "revision": revision, "fragments": fragments}],
            source=f"test-rev{revision}",
        )

    def claim(self, claim_id="K", day=5, loss=150, revision=1, policy="P", device="D") -> dict:
        return self.svc.revise_claim(
            claim_id, revision=revision, day=day, loss=loss, policy=policy, device=device
        )


class MoneyTest(unittest.TestCase):
    def test_indemnity(self) -> None:
        self.assertEqual(indemnity(150, 0, 100), 100)
        self.assertEqual(indemnity(120, 50, 100), 70)
        self.assertEqual(indemnity(30, 50, 100), 0)
        self.assertEqual(indemnity(400, 0, 100), 100)

    def test_recovery_and_payable(self) -> None:
        self.assertEqual(holdback_from_paid(70, 50), 20)
        self.assertEqual(payable_balance(70, 50), 0)
        self.assertEqual(holdback_from_paid(70, 150), 0)
        self.assertEqual(payable_balance(70, 150), 80)
        # recovery is netted before new money is spendable
        self.assertEqual(payable_balance(100, 120), 20)

    def test_fragment_validation(self) -> None:
        with self.assertRaises(InvalidCoverage):
            validate_fragments([{"from": 1, "to": 1, "deductible": 0, "limit": 5}])
        with self.assertRaises(InvalidCoverage):
            validate_fragments(
                [
                    {"from": 1, "to": 5, "deductible": 0, "limit": 5},
                    {"from": 4, "to": 9, "deductible": 0, "limit": 5},
                ]
            )
        self.assertEqual(validate_fragments([]), [])
        # adjacent half-open ranges do not overlap
        validate_fragments(
            [
                {"from": 1, "to": 5, "deductible": 0, "limit": 5},
                {"from": 5, "to": 9, "deductible": 0, "limit": 5},
            ]
        )


class WarehouseTest(ServiceFixture):
    def test_latest_revision_replaces_and_empty_revokes(self) -> None:
        self.ingest(1, [{"from": 1, "to": 10, "deductible": 0, "limit": 100}])
        self.ingest(2, [{"from": 1, "to": 10, "deductible": 0, "limit": 40}])
        self.assertEqual(self.svc.coverage_at("P", "D", 5)["limit"], 40)
        self.ingest(3, [])
        self.assertFalse(self.svc.coverage_at("P", "D", 5)["covered"])
        self.ingest(4, [{"from": 1, "to": 10, "deductible": 0, "limit": 70}])
        self.assertEqual(self.svc.coverage_at("P", "D", 5)["limit"], 70)

    def test_larger_revision_is_not_a_merge(self) -> None:
        self.ingest(1, [
            {"from": 1, "to": 5, "deductible": 0, "limit": 100},
            {"from": 5, "to": 9, "deductible": 0, "limit": 80},
        ])
        self.ingest(2, [{"from": 5, "to": 9, "deductible": 0, "limit": 80}])
        self.assertFalse(self.svc.coverage_at("P", "D", 3)["covered"])
        self.assertEqual(self.svc.coverage_at("P", "D", 6)["limit"], 80)

    def test_identical_replay_is_a_noop(self) -> None:
        env = {"policy": "P", "device": "D", "revision": 1,
               "fragments": [{"from": 1, "to": 10, "deductible": 0, "limit": 100}]}
        first = self.svc.ingest_coverage_batch([env], source="b1")
        replay = self.svc.ingest_coverage_batch([env], source="b2")
        self.assertEqual(len(first["created_events"]), 1)
        self.assertEqual(replay["created_events"], [])
        self.assertEqual(len(replay["noop_events"]), 1)
        self.assertEqual(len(self.store.batches()), 2)  # history is kept
        self.assertEqual(len([e for e in self.store.events(realm="warehouse")]), 1)

    def test_same_revision_different_content_is_a_conflict(self) -> None:
        self.ingest(1, [{"from": 1, "to": 10, "deductible": 0, "limit": 100}])
        with self.assertRaises(CoverageRevisionConflict):
            self.ingest(1, [{"from": 1, "to": 10, "deductible": 0, "limit": 101}])

    def test_out_of_order_arrival(self) -> None:
        self.ingest(2, [{"from": 1, "to": 10, "deductible": 0, "limit": 50}])
        self.ingest(1, [{"from": 1, "to": 10, "deductible": 0, "limit": 100}])
        # revision 1 arrived later but must not replace revision 2
        self.assertEqual(self.svc.coverage_at("P", "D", 5)["limit"], 50)
        self.ingest(3, [{"from": 1, "to": 10, "deductible": 0, "limit": 90}])
        self.assertEqual(self.svc.coverage_at("P", "D", 5)["limit"], 90)

    def test_batch_time_travel_and_history(self) -> None:
        self.ingest(1, [{"from": 1, "to": 10, "deductible": 0, "limit": 100}])
        b1 = self.store.batches()[-1]["id"]
        self.ingest(2, [{"from": 1, "to": 10, "deductible": 0, "limit": 50}])
        historic = self.svc.coverage_at_batch(b1, "P", "D", 5)
        self.assertEqual((historic["covered"], historic["limit"]), (True, 100))
        self.assertEqual(self.svc.coverage_at("P", "D", 5)["limit"], 50)
        history = self.svc.coverage_history("P", "D", 5)
        self.assertEqual([h["revision"] for h in history], [1, 2])
        self.assertEqual([h["limit"] for h in history], [100, 50])

    def test_warehouse_works_without_claims(self) -> None:
        self.ingest(1, [{"from": 0, "to": 3, "deductible": 5, "limit": 40}], policy="W", device="X")
        self.assertEqual(self.svc.claim_view if False else self.svc.coverage_at("W", "X", 1)["deductible"], 5)
        self.assertEqual(self.svc.claims().claims, {})


class ClaimRevisionTest(ServiceFixture):
    def test_newer_revision_is_full_mirror(self) -> None:
        self.ingest(1, [{"from": 1, "to": 10, "deductible": 0, "limit": 100}])
        self.claim(day=3, loss=150)
        self.assertTrue(self.svc.claim_view("K")["covered"])
        self.claim(day=12, loss=200, revision=2)
        view = self.svc.claim_view("K")
        self.assertEqual((view["day"], view["revision"], view["loss"]), (12, 2, 200))
        self.assertFalse(view["covered"])
        self.assertEqual(view["authoritative"], 0)

    def test_revision_idempotency_and_conflict(self) -> None:
        self.ingest(1, [{"from": 1, "to": 10, "deductible": 0, "limit": 100}])
        self.claim(day=3, loss=100)
        same = self.svc.revise_claim("K", revision=1, day=3, loss=100)
        self.assertFalse(same["created"])
        with self.assertRaises(ClaimRevisionConflict):
            self.svc.revise_claim("K", revision=1, day=4, loss=100)
        with self.assertRaises(ClaimRevisionConflict):
            self.svc.revise_claim("K", revision=0, day=3, loss=100)

    def test_claim_revisions_can_change_identity_fields_after_rev0(self) -> None:
        self.ingest(1, [{"from": 1, "to": 10, "deductible": 0, "limit": 100}], policy="Q", device="E")
        self.claim(day=1, loss=10)
        self.svc.revise_claim("K", revision=2, day=2, loss=10, policy="Q", device="E")
        self.assertEqual(self.svc.claim_view("K")["policy"], "Q")


class PaymentTest(ServiceFixture):
    def setUp(self) -> None:
        super().setUp()
        self.ingest(1, [{"from": 1, "to": 10, "deductible": 0, "limit": 100}])
        self.claim(day=3, loss=150)

    def pay(self, pid: str, amount: int, claim="K") -> dict:
        return self.svc.accept_payment(payment_id=pid, claim_id=claim, amount=amount)

    def test_capacity_is_enforced(self) -> None:
        self.assertEqual(self.pay("p1", 60)["status"], "accepted")
        second = self.pay("p2", 60)
        self.assertEqual((second["status"], second["reason"]), ("rejected", "exceeds_payable"))
        self.assertEqual(self.pay("p3", 40)["status"], "accepted")
        self.assertEqual(self.svc.claim_view("K")["paid_total"], 100)
        self.assertEqual(self.pay("p4", 0)["status"], "accepted")

    def test_same_id_same_content_returns_original(self) -> None:
        first = self.pay("p1", 40)
        again = self.pay("p1", 40)
        self.assertTrue(again["idempotent"])
        self.assertEqual(again["payment"]["event_seq"], first["payment"]["event_seq"])
        self.assertEqual(self.svc.claim_view("K")["paid_total"], 40)

    def test_same_id_different_content_refused(self) -> None:
        self.pay("p1", 40)
        with self.assertRaises(PaymentIdConflict):
            self.pay("p1", 41)

    def test_rejection_is_sticky(self) -> None:
        self.ingest(2, [])
        rejected = self.pay("p1", 10)
        self.assertEqual(rejected["status"], "rejected")
        self.ingest(3, [{"from": 1, "to": 10, "deductible": 0, "limit": 100}])
        retry = self.pay("p1", 10)
        self.assertEqual((retry["status"], retry["idempotent"]), ("rejected", True))
        self.assertEqual(self.svc.claim_view("K")["paid_total"], 0)

        with self.assertRaises(PaymentIdConflict):
            self.pay("p1", 11)

    def test_later_revision_cannot_cancel_paid_money(self) -> None:
        self.pay("p1", 100)
        self.ingest(2, [])
        view = self.svc.claim_view("K")
        self.assertEqual((view["paid_total"], view["recovery"], view["payable"]), (100, 100, 0))
        self.assertEqual(self.pay("p2", 5)["reason"], "recovery_active")

    def test_recovery_shrinks_when_entitlement_rises(self) -> None:
        self.pay("p1", 100)
        self.ingest(2, [{"from": 1, "to": 10, "deductible": 0, "limit": 40}])
        self.assertEqual(self.svc.claim_view("K")["recovery"], 60)
        self.ingest(3, [{"from": 1, "to": 10, "deductible": 0, "limit": 70}])
        view = self.svc.claim_view("K")
        self.assertEqual((view["recovery"], view["payable"]), (30, 0))
        self.ingest(4, [{"from": 1, "to": 10, "deductible": 0, "limit": 130}])
        view = self.svc.claim_view("K")
        self.assertEqual((view["recovery"], view["payable"]), (0, 30))
        self.assertEqual(self.pay("p2", 30)["status"], "accepted")

    def test_pause_blocks_payments_only(self) -> None:
        self.pay("p1", 40)
        self.svc.pause_claim("K", paused=True, request_id="r1")
        rejected = self.pay("p2", 10)
        self.assertEqual(rejected["reason"], "paused")
        view = self.svc.claim_view("K")
        self.assertEqual((view["paid_total"], view["authoritative"], view["paused"]), (40, 100, True))
        # revisions and recalculations preserve the manual pause
        self.svc.revise_claim("K", revision=2, day=3, loss=150)
        self.assertTrue(self.svc.claim_view("K")["paused"])
        self.svc.pause_claim("K", paused=False, request_id="r2")
        self.assertEqual(self.pay("p3", 10)["status"], "accepted")

    def test_pause_retry_is_idempotent(self) -> None:
        self.svc.pause_claim("K", paused=True, request_id="r1")
        again = self.svc.pause_claim("K", paused=True, request_id="r1")
        self.assertFalse(again["created"])
        pause_events = [e for e in self.store.events(realm="claims") if e.kind == "claim_pause"]
        self.assertEqual(len(pause_events), 1)

    def test_stale_view_is_refused_but_server_state_is_authoritative(self) -> None:
        self.ingest(2, [{"from": 1, "to": 10, "deductible": 0, "limit": 30}])
        with self.assertRaises(DomainError):
            self.svc.accept_payment(
                payment_id="p1", claim_id="K", amount=100, seen_coverage_revision=1
            )
        # no commitment was recorded
        self.assertEqual(self.svc.claim_view("K")["paid_total"], 0)
        accepted = self.svc.accept_payment(
            payment_id="p2", claim_id="K", amount=30, seen_coverage_revision=2
        )
        self.assertEqual(accepted["status"], "accepted")

    def test_zero_amount_is_allowed_and_needs_no_payable(self) -> None:
        # A zero payment creates no money, so it is a valid commitment even
        # with no coverage and no payable balance -- but a manual pause still
        # blocks new requests, including zero-value ones.
        self.ingest(2, [])
        self.assertEqual(self.pay("p1", 0)["status"], "accepted")
        self.assertEqual(self.pay("p2", 0)["status"], "accepted")
        self.svc.pause_claim("K", paused=True, request_id="pause-0")
        self.assertEqual(self.pay("p3", 0)["status"], "rejected")
        self.assertEqual(self.svc.claim_view("K")["paid_total"], 0)
        self.assertEqual(len(self.svc.claim_view("K")["payments"]), 2)


class DeliveryTest(ServiceFixture):
    def setUp(self) -> None:
        super().setUp()
        self.ingest(1, [{"from": 1, "to": 10, "deductible": 0, "limit": 100}])
        self.claim(day=3, loss=150)
        self.svc.accept_payment(payment_id="p1", claim_id="K", amount=50)

    def test_lost_response_and_idempotent_redelivery(self) -> None:
        lost = self.svc.dispatch_payment("p1", drop_response=True)
        self.assertEqual(lost["result"], "response_lost")
        self.assertEqual(self.svc.delivery_view("p1")["state"], "pending")
        # receiver already holds the money exactly once
        self.assertEqual(len(self.store.receiver_payments()), 1)
        retry = self.svc.dispatch_payment("p1")
        self.assertEqual((retry["result"], retry["receiver_duplicate"]), ("duplicate_ack", True))
        self.assertEqual(self.svc.delivery_view("p1")["state"], "delivered")
        third = self.svc.dispatch_payment("p1")
        self.assertEqual(third["result"], "already_delivered")
        self.assertEqual(len(self.store.receiver_payments()), 1)
        self.assertEqual(self.svc.claim_view("K")["paid_total"], 50)

    def test_dispatch_replay_same_message(self) -> None:
        self.svc.dispatch_payment("p1")
        self.svc.dispatch_payment("p1")
        rows = self.store.receiver_payments()
        self.assertEqual(len(rows), 1)

    def test_settlement_balance_counts_commitments_not_receipts(self) -> None:
        # p1 committed but not dispatched at all
        view = self.svc.claim_view("K")
        self.assertEqual((view["paid_total"], view["payable"]), (50, 50))
        ledger = self.svc.ledger()
        self.assertEqual(ledger["rows"][0]["delivery"], "pending")
        self.assertFalse(ledger["rows"][0]["receiver_accepted"])
        self.assertEqual(ledger["committed_total"], 50)


class RestartTest(ServiceFixture):
    def test_commitment_survives_restart(self) -> None:
        self.ingest(1, [{"from": 1, "to": 10, "deductible": 0, "limit": 100}])
        self.claim(day=3, loss=150)
        self.svc.accept_payment(payment_id="p1", claim_id="K", amount=50)
        self.store.close()

        self.store = Store(self.db_path)
        self.svc = SettlementService(self.store, clock=PersistentClock(self.store))
        again = self.svc.accept_payment(payment_id="p1", claim_id="K", amount=50)
        self.assertTrue(again["idempotent"])
        self.assertEqual(self.svc.claim_view("K")["paid_total"], 50)
        with self.assertRaises(PaymentIdConflict):
            self.svc.accept_payment(payment_id="p1", claim_id="K", amount=60)
        self.assertEqual(self.svc.claim_view("K")["paid_total"], 50)


class ScenarioTest(unittest.TestCase):
    def test_demo_is_repeatable(self) -> None:
        tmp = tempfile.mkdtemp(prefix="settlement-demo-")
        result = run_demo(os.path.join(tmp, "demo.db"), checks=True, quiet=True)
        self.assertTrue(result["checks_ok"], [c for c in result["checks"] if not c["ok"]])
        self.assertTrue(result["invariants_ok"], result["invariants"])
        self.assertTrue(result["restart_checks_ok"])
        self.assertTrue(result["restart_final_checks_ok"])
        self.assertTrue(result["restart_matches"])
        self.assertTrue(result["fresh_matches"])
        self.assertEqual(result["events_before"], result["events_after_restart"])
        self.assertTrue(result["ok"])

    def test_case_json_checkpoints(self) -> None:
        case_path = os.path.join(os.path.dirname(__file__), "..", "case.json")
        result = check_case(case_path)
        self.assertTrue(result["ok"], [c for c in result["checks"] if not c["ok"]])

    def test_invariants_after_demo(self) -> None:
        tmp = tempfile.mkdtemp(prefix="settlement-inv-")
        path = os.path.join(tmp, "demo.db")
        store = Store(path)
        svc = SettlementService(store, clock=PersistentClock(store))
        run_actions(svc, demo_actions(), expect_errors=EXPECTED_ERRORS, quiet=True)
        self.assertEqual(verify_invariants(svc), [])
        store.close()


class ConcurrencyTest(unittest.TestCase):
    def test_coverage_and_payment_race_is_reproducible(self) -> None:
        summary = race_demo(iterations=8)
        self.assertTrue(summary["ok"], summary["results"])
        self.assertEqual(summary["passed"], summary["iterations"])


class RejectionAuditTest(ServiceFixture):
    def test_rejections_never_count_as_paid(self) -> None:
        self.ingest(1, [{"from": 1, "to": 10, "deductible": 0, "limit": 50}])
        self.claim(day=3, loss=150)
        self.svc.accept_payment(payment_id="ok", claim_id="K", amount=50)
        self.svc.accept_payment(payment_id="no", claim_id="K", amount=10)
        self.assertEqual(self.svc.claim_view("K")["paid_total"], 50)
        decisions = {d["payment_id"]: d["status"] for d in self.svc.claims().decisions.values()}
        self.assertEqual(decisions, {"ok": "accepted", "no": "rejected"})


if __name__ == "__main__":
    unittest.main()
