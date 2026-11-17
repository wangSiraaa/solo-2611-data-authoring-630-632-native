#!/usr/bin/env python3
"""Command line entry point.

Examples
--------
    python3 -m settlement demo                      # full repeatable demo
    python3 -m settlement demo --reset              # start from an empty DB
    python3 -m settlement demo --db /tmp/s.db --journal /tmp/s.jsonl
    python3 -m settlement case --case case.json      # check the provided checkpoints
    python3 -m settlement race --iterations 20       # concurrent submit ordering

    # individual operations against a persistent database
    python3 -m settlement ingest --db s.db --file batch.json
    python3 -m settlement claim --db s.db --claim C1 --revision 1 --day 3 --loss 150
    python3 -m settlement pay --db s.db --payment pay-1 --claim C1 --amount 40
    python3 -m settlement dispatch --db s.db --payment pay-1 [--drop-response]
    python3 -m settlement pause --db s.db --claim C1 [--resume]
    python3 -m settlement show --db s.db --claim C1
    python3 -m settlement coverage --db s.db --policy P1 --device D1 --day 3
    python3 -m settlement history --db s.db --policy P1 --device D1 --day 3
    python3 -m settlement events --db s.db
"""

from __future__ import annotations

import argparse
import json
import sys

from .core import DomainError
from .scenario import (
    PersistentClock,
    business_snapshot,
    check_case,
    full_business_snapshot,
    race_demo,
    run_demo,
    verify_invariants,
)
from .service import SettlementService
from .store import Store


def _svc(db: str) -> tuple[Store, SettlementService]:
    store = Store(db)
    return store, SettlementService(store, clock=PersistentClock(store))


def _print(value) -> None:
    print(json.dumps(value, indent=2, ensure_ascii=False, default=str))


def cmd_demo(args: argparse.Namespace) -> int:
    result = run_demo(
        args.db,
        reset=args.reset,
        journal_path=args.journal,
        checks=True,
        # --json means "machine output only"; the check results are still in
        # the JSON payload.
        quiet=args.quiet or args.json,
    )
    if args.json:
        _print({k: v for k, v in result.items() if k not in ("steps", "replay_steps")})
    return 0 if result["ok"] else 1


def cmd_case(args: argparse.Namespace) -> int:
    result = check_case(args.case, quiet=args.json)
    if not args.json:
        return 0 if result["ok"] else 1
    _print(result)
    return 0 if result["ok"] else 1


def cmd_race(args: argparse.Namespace) -> int:
    summary = race_demo(args.iterations)
    print(
        json.dumps(
            {
                "iterations": summary["iterations"],
                "passed": summary["passed"],
                "orders": summary["orders"],
                "ok": summary["ok"],
                "results": summary["results"],
            },
            indent=2,
            ensure_ascii=False,
        )
    )
    return 0 if summary["ok"] else 1


def cmd_ingest(args: argparse.Namespace) -> int:
    with open(args.file, encoding="utf-8") as fh:
        payload = json.load(fh)
    if isinstance(payload, list):
        envelopes = payload
    elif "coverage" in payload:
        envelopes = payload["coverage"]
    elif "envelopes" in payload:
        envelopes = payload["envelopes"]
    else:
        print("input must be a list or an object with 'coverage'/'envelopes'", file=sys.stderr)
        return 2
    store, svc = _svc(args.db)
    try:
        result = svc.ingest_coverage_batch(envelopes, source=args.source)
    except DomainError as exc:
        print(f"rejected: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 2
    finally:
        store.close()
    _print(result)
    return 0


def cmd_claim(args: argparse.Namespace) -> int:
    store, svc = _svc(args.db)
    try:
        if args.show:
            _print(svc.claim_view(args.claim))
            return 0
        result = svc.revise_claim(
            args.claim,
            revision=args.revision,
            day=args.day,
            loss=args.loss,
            policy=args.policy,
            device=args.device,
        )
    except DomainError as exc:
        print(f"rejected: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 2
    finally:
        store.close()
    _print(result)
    return 0


def cmd_pay(args: argparse.Namespace) -> int:
    store, svc = _svc(args.db)
    try:
        result = svc.accept_payment(
            payment_id=args.payment,
            claim_id=args.claim,
            amount=args.amount,
            expected_warehouse_seq=args.expected_warehouse_seq,
            seen_coverage_revision=args.seen_coverage_revision,
        )
    except DomainError as exc:
        print(f"rejected: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 2
    finally:
        store.close()
    _print(result)
    # A rejected request is a valid, persisted decision: exit 2 so scripts can
    # tell it apart from "accepted".
    return 0 if result.get("status") == "accepted" else 2


def cmd_dispatch(args: argparse.Namespace) -> int:
    store, svc = _svc(args.db)
    try:
        result = svc.dispatch_payment(args.payment, drop_response=args.drop_response)
    except DomainError as exc:
        print(f"rejected: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 2
    finally:
        store.close()
    _print(result)
    return 0


def cmd_pause(args: argparse.Namespace) -> int:
    store, svc = _svc(args.db)
    try:
        result = svc.pause_claim(
            args.claim, paused=not args.resume, request_id=args.request_id
        )
    except DomainError as exc:
        print(f"rejected: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 2
    finally:
        store.close()
    _print(result)
    return 0


def cmd_show(args: argparse.Namespace) -> int:
    store, svc = _svc(args.db)
    try:
        _print(svc.claim_view(args.claim))
        _print({"ledger": svc.ledger()})
        _print({"invariants": verify_invariants(svc)})
    finally:
        store.close()
    return 0


def cmd_coverage(args: argparse.Namespace) -> int:
    store, svc = _svc(args.db)
    try:
        if args.batch is not None:
            _print(svc.coverage_at_batch(args.batch, args.policy, args.device, args.day))
        else:
            _print(svc.coverage_at(args.policy, args.device, args.day))
    finally:
        store.close()
    return 0


def cmd_history(args: argparse.Namespace) -> int:
    store, svc = _svc(args.db)
    try:
        _print(
            {
                "history": svc.coverage_history(args.policy, args.device, args.day),
                "batches": store.batches(),
            }
        )
    finally:
        store.close()
    return 0


def cmd_events(args: argparse.Namespace) -> int:
    store, svc = _svc(args.db)
    try:
        _print({"events": svc.events(), "business_snapshot": business_snapshot(svc),
                "full_snapshot": full_business_snapshot(svc)})
    finally:
        store.close()
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="settlement", description=__doc__.splitlines()[0])
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("demo", help="run the repeatable end-to-end demo")
    p.add_argument("--db", default="settlement.db")
    p.add_argument("--reset", action="store_true", help="delete the database first")
    p.add_argument("--journal", help="write one JSON line per executed action")
    p.add_argument("--quiet", action="store_true")
    p.add_argument("--json", action="store_true", help="print a machine-readable summary")
    p.set_defaults(func=cmd_demo)

    p = sub.add_parser("case", help="check case.json checkpoints")
    p.add_argument("--case", default="case.json")
    p.add_argument("--json", action="store_true", help="machine-readable output only")
    p.set_defaults(func=cmd_case)

    p = sub.add_parser("race", help="concurrent coverage-vs-payment ordering")
    p.add_argument("--iterations", type=int, default=20)
    p.set_defaults(func=cmd_race)

    p = sub.add_parser("ingest", help="ingest a coverage batch JSON file")
    p.add_argument("--db", required=True)
    p.add_argument("--file", required=True)
    p.add_argument("--source", default="cli")
    p.set_defaults(func=cmd_ingest)

    p = sub.add_parser("claim", help="revise or show a claim")
    p.add_argument("--db", required=True)
    p.add_argument("--claim", required=True)
    p.add_argument("--revision", type=int)
    p.add_argument("--day", type=int)
    p.add_argument("--loss", type=int)
    p.add_argument("--policy")
    p.add_argument("--device")
    p.add_argument("--show", action="store_true")
    p.set_defaults(func=cmd_claim)

    p = sub.add_parser("pay", help="submit a payment request")
    p.add_argument("--db", required=True)
    p.add_argument("--payment", required=True)
    p.add_argument("--claim", required=True)
    p.add_argument("--amount", type=int, required=True)
    p.add_argument("--expected-warehouse-seq", type=int)
    p.add_argument("--seen-coverage-revision", type=int)
    p.set_defaults(func=cmd_pay)

    p = sub.add_parser("dispatch", help="deliver a commitment to the receiver")
    p.add_argument("--db", required=True)
    p.add_argument("--payment", required=True)
    p.add_argument("--drop-response", action="store_true")
    p.set_defaults(func=cmd_dispatch)

    p = sub.add_parser("pause", help="pause or resume a claim")
    p.add_argument("--db", required=True)
    p.add_argument("--claim", required=True)
    p.add_argument("--resume", action="store_true")
    p.add_argument("--request-id")
    p.set_defaults(func=cmd_pause)

    p = sub.add_parser("show", help="show a claim, ledger and invariants")
    p.add_argument("--db", required=True)
    p.add_argument("--claim", required=True)
    p.set_defaults(func=cmd_show)

    p = sub.add_parser("coverage", help="query current or batch-time coverage")
    p.add_argument("--db", required=True)
    p.add_argument("--policy", required=True)
    p.add_argument("--device", required=True)
    p.add_argument("--day", type=int, required=True)
    p.add_argument("--batch", type=int)
    p.set_defaults(func=cmd_coverage)

    p = sub.add_parser("history", help="coverage revision history and receive batches")
    p.add_argument("--db", required=True)
    p.add_argument("--policy", required=True)
    p.add_argument("--device", required=True)
    p.add_argument("--day", type=int, required=True)
    p.set_defaults(func=cmd_history)

    p = sub.add_parser("events", help="dump the event log and snapshots")
    p.add_argument("--db", required=True)
    p.set_defaults(func=cmd_events)

    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
