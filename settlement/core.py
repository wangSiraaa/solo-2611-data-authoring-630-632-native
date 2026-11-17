"""Core domain logic: event envelopes, coverage history, claim snapshots, money math.

Two applications share one append-only event store but own disjoint namespaces:

* the warehouse (``w:`` event keys) ingests coverage fragments, groups them into
  per-revision batches, merges the latest full revision per policy/device and
  remembers which receive batch produced which decision;
* the claims application (``c:`` event keys) owns claim revisions, payment
  commitments and manual pause/resume events.

Nothing here talks to a database; the store (``settlement.store``) injects the
durable primitives.  This keeps every rule deterministic and replayable.

All money is a single currency in integer cents, all days are UTC day numbers
(the input format uses integer days).
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any, Iterable, Mapping, Sequence

# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #


class DomainError(Exception):
    """Base class for expected, user-facing rejections."""


class DuplicateFragmentConflict(DomainError):
    pass


class CoverageRevisionConflict(DomainError):
    pass


class InvalidCoverage(DomainError):
    pass


class ClaimRevisionConflict(DomainError):
    pass


class InvalidClaimRevision(DomainError):
    pass


class PaymentIdConflict(DomainError):
    pass


class UnknownClaim(DomainError):
    pass


class ClaimPaused(DomainError):
    pass


class InsufficientPayable(DomainError):
    def __init__(self, detail: str, *, payable: int = 0, recovery: int = 0):
        super().__init__(detail)
        self.payable = payable
        self.recovery = recovery


# --------------------------------------------------------------------------- #
# Money
# --------------------------------------------------------------------------- #


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def sha256_hex(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def require_int(name: str, value: Any, *, minimum: int | None = None) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise DomainError(f"{name} must be an integer, got {value!r}")
    if minimum is not None and value < minimum:
        raise DomainError(f"{name} must be >= {minimum}, got {value}")
    return value


def holdback_from_paid(paid: int, authoritative: int) -> int:
    """Money already paid above the current entitlement that must be recovered."""
    return max(0, paid - authoritative)


def payable_balance(paid: int, authoritative: int) -> int:
    """New payment capacity; recovery is netted first and is never re-spendable."""
    return max(0, authoritative - paid)


def indemnity(loss: int, deductible: int, limit: int) -> int:
    return min(max(loss - deductible, 0), limit)


# --------------------------------------------------------------------------- #
# Event envelope
# --------------------------------------------------------------------------- #

REALM_WAREHOUSE = "warehouse"
REALM_CLAIMS = "claims"


@dataclass(frozen=True)
class Event:
    """A single persisted fact.

    ``key`` is global.  Warehouse events are bucket-keyed by
    ``cov-batch:<policy>:<device>:<revision>:<fingerprint>`` so a replay before
    and after a restart is recognised as the same batch; claim events use their
    stable business identity (claim revision, payment id, pause sequence).
    """

    seq: int
    key: str
    kind: str
    payload: dict
    occurred_at: str
    recorded_at: str
    origin: str
    batch_id: int | None = None

    @property
    def event_id(self) -> str:
        return sha256_hex(canonical_json([self.seq, self.key, self.occurred_at, self.payload]))


# --------------------------------------------------------------------------- #
# Coverage
# --------------------------------------------------------------------------- #

DATE_FIELDS = ("from", "to")


def validate_fragments(fragments: Iterable[Mapping[str, Any]]) -> list[dict]:
    """Validate the full fragment set of one revision.

    Fragments are half-open [from, to) and must not overlap *within* the
    revision.  An empty set is a legal revocation of the revision.
    """
    out: list[dict] = []
    for i, frag in enumerate(fragments):
        if not isinstance(frag, Mapping):
            raise InvalidCoverage(f"fragment #{i} must be an object")
        lo = require_int("from", frag.get("from"))
        hi = require_int("to", frag.get("to"))
        if hi <= lo:
            raise InvalidCoverage(f"fragment #{i} has empty/inverted span [{lo},{hi})")
        ded = require_int("deductible", frag.get("deductible"), minimum=0)
        lim = require_int("limit", frag.get("limit"), minimum=0)
        out.append({"from": lo, "to": hi, "deductible": ded, "limit": lim})
    out.sort(key=lambda f: f["from"])
    for a, b in zip(out, out[1:]):
        if a["to"] > b["from"]:
            raise InvalidCoverage(f"fragments overlap at [{b['from']},{a['to']})")
    return out


def coverage_snapshot_for_day(fragments: Sequence[Mapping[str, Any]], day: int) -> dict | None:
    """Coverage decision for one accident day under a concrete fragment set."""
    for frag in fragments:
        if frag["from"] <= day < frag["to"]:
            return {
                "covered": True,
                "from": frag["from"],
                "to": frag["to"],
                "deductible": frag["deductible"],
                "limit": frag["limit"],
            }
    return {"covered": False}


# --------------------------------------------------------------------------- #
# Warehouse fold
# --------------------------------------------------------------------------- #


class Warehouse:
    """Pure fold of warehouse events into the latest complete revision view."""

    def __init__(self) -> None:
        # (policy, device) -> revision -> fragments
        self.revisions: dict[tuple[str, str], dict[int, list[dict]]] = {}
        # latest complete revision per policy/device
        self.latest: dict[tuple[str, str], int] = {}
        # provenance of the latest revision (event seq + event key)
        self.latest_seq: dict[tuple[str, str], int] = {}
        self.latest_key: dict[tuple[str, str], str] = {}

    def apply(self, event: Event) -> None:
        payload = event.payload
        if event.kind != "coverage_revision":
            return
        p, d = payload["policy"], payload["device"]
        rev = payload["revision"]
        fragments = payload["fragments"]
        key = (p, d)
        bucket = self.revisions.setdefault(key, {})
        bucket[rev] = [dict(f) for f in fragments]
        if rev >= self.latest.get(key, rev):
            self.latest[key] = rev
            self.latest_seq[key] = event.seq
            self.latest_key[key] = event.key

    def latest_fragments(self, policy: str, device: str) -> list[dict]:
        rev = self.latest.get((policy, device))
        if rev is None:
            return []
        return self.revisions[(policy, device)][rev]

    def decide(self, policy: str, device: str, day: int, *, revision: int | None = None) -> dict:
        """Explicit coverage decision record.

        ``revision`` lets a historical batch be inspected with the revision it
        delivered; ``None`` means "latest revision known to the warehouse".
        """
        if revision is None:
            rev = self.latest.get((policy, device))
            fragments = self.latest_fragments(policy, device)
        else:
            rev = revision
            fragments = self.revisions.get((policy, device), {}).get(revision, [])
        decision = coverage_snapshot_for_day(fragments, day) if rev is not None else {"covered": False}
        return {
            "policy": policy,
            "device": device,
            "day": day,
            "revision": rev,
            "fragments": fragments,
            "covered": decision["covered"],
            **(decision if decision["covered"] else {}),
        }


# --------------------------------------------------------------------------- #
# Claims fold
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class ClaimState:
    claim_id: str
    policy: str
    device: str
    revision: int
    day: int
    loss: int


@dataclass(frozen=True)
class PaymentAttempt:
    payment_id: str
    claim_id: str
    amount: int
    status: str
    reason: str | None
    coverage_digest: str | None
    coverage_revision: int | None
    coverage_seq: int | None
    authoritative: int | None
    paid_before: int | None
    withheld_before: int | None
    recovery_before: int | None
    claim_revision: int | None
    paused: bool | None
    detail: str | None
    seq: int
    occurred_at: str


@dataclass(frozen=True)
class PauseState:
    paused: bool
    seq: int
    occurred_at: str


class Claims:
    """Pure fold of claims events into claim + payment state."""

    def __init__(self) -> None:
        # claim_id -> revision -> state; ``claims`` keeps the highest revision
        self.claim_revisions: dict[str, dict[int, ClaimState]] = {}
        self.claims: dict[str, ClaimState] = {}
        self.payments: dict[str, PaymentAttempt] = {}
        self.decisions: dict[str, dict] = {}
        self.paused: dict[str, PauseState] = {}
        # Latest warehouse seq known to the claims app; a payment must cite it
        # so "latest authoritative state as of submission" is auditable.
        self.last_warehouse_seq: int = 0

    def apply(self, event: Event) -> None:
        kind = event.kind
        payload = event.payload
        if kind == "warehouse_marker":
            self.last_warehouse_seq = max(self.last_warehouse_seq, int(payload["warehouse_seq"]))
            return
        if kind == "claim_revision":
            state = ClaimState(
                claim_id=payload["claim_id"],
                policy=payload["policy"],
                device=payload["device"],
                revision=payload["revision"],
                day=payload["day"],
                loss=payload["loss"],
            )
            bucket = self.claim_revisions.setdefault(state.claim_id, {})
            bucket[state.revision] = state
            current = self.claims.get(state.claim_id)
            if current is None or state.revision >= current.revision:
                self.claims[state.claim_id] = state
            return
        if kind == "payment_intent":
            attempt = PaymentAttempt(
                payment_id=payload["payment_id"],
                claim_id=payload["claim_id"],
                amount=payload["amount"],
                status="accepted",
                reason=None,
                coverage_digest=payload["coverage_digest"],
                coverage_revision=payload["coverage_revision"],
                coverage_seq=payload["coverage_seq"],
                authoritative=payload["authoritative"],
                paid_before=payload["paid_before"],
                withheld_before=payload["withheld_before"],
                recovery_before=payload["recovery_before"],
                claim_revision=payload["claim_revision"],
                paused=payload["paused"],
                detail=None,
                seq=event.seq,
                occurred_at=event.occurred_at,
            )
            self.payments[attempt.payment_id] = attempt
            self.decisions[attempt.payment_id] = {**payload, "status": "accepted", "seq": event.seq}
            return
        if kind == "payment_rejected":
            # Rejections are kept for audit but never enter payment state, so
            # they can never be counted twice or replayed into a commitment.
            self.decisions[payload["payment_id"]] = {**payload, "status": "rejected", "seq": event.seq}
            return
        if kind == "claim_pause":
            self.paused[payload["claim_id"]] = PauseState(
                paused=payload["action"] == "pause",
                seq=event.seq,
                occurred_at=event.occurred_at,
            )
            return

    # -- derived money ----------------------------------------------------- #

    def is_paused(self, claim_id: str) -> bool:
        state = self.paused.get(claim_id)
        return bool(state and state.paused)

    def committed_payments(self, claim_id: str) -> list[PaymentAttempt]:
        return sorted(
            (p for p in self.payments.values() if p.claim_id == claim_id),
            key=lambda p: (p.seq, p.payment_id),
        )

    def committed_total(self, claim_id: str) -> int:
        return sum(p.amount for p in self.committed_payments(claim_id))

    def committed_before(self, claim_id: str, payment_id: str) -> int:
        """Committed payments excluding the payment with ``payment_id``.

        Re-deriving the balance from the ledger (instead of a stored UI number)
        is what makes exactly-once commitment survive restarts and retries.
        """
        return sum(p.amount for p in self.committed_payments(claim_id) if p.payment_id != payment_id)


# --------------------------------------------------------------------------- #
# Input validation helpers
# --------------------------------------------------------------------------- #


def normalize_coverage_envelope(env: Mapping[str, Any]) -> dict:
    policy = env.get("policy")
    device = env.get("device")
    if not isinstance(policy, str) or not policy:
        raise InvalidCoverage("coverage.policy must be a non-empty string")
    if not isinstance(device, str) or not device:
        raise InvalidCoverage("coverage.device must be a non-empty string")
    revision = require_int("coverage.revision", env.get("revision"), minimum=0)
    fragments = validate_fragments(env.get("fragments", []))
    return {"policy": policy, "device": device, "revision": revision, "fragments": fragments}


def coverage_bucket_key(policy: str, device: str, revision: int, fragments: Sequence[Mapping[str, Any]]) -> str:
    fingerprint = sha256_hex(canonical_json(list(fragments)))
    return f"cov-batch:{policy}:{device}:{revision}:{fingerprint}"


def coverage_digest(
    policy: str,
    device: str,
    day: int,
    revision: int | None,
    fragments: Sequence[Mapping[str, Any]],
) -> str:
    """Digest of the only facts a payment decision depends on.

    Recording it at commitment time lets a later auditor prove which coverage
    version the accept/reject decision saw, even if that revision is superseded
    milliseconds later.
    """
    relevant = [dict(f) for f in fragments if f["from"] <= day < f["to"]]
    return sha256_hex(canonical_json([policy, device, day, revision, relevant]))


def normalize_claim_revision(env: Mapping[str, Any], *, default_policy: str | None, default_device: str | None) -> dict:
    claim_id = env.get("claim_id", env.get("claim"))
    if not isinstance(claim_id, str) or not claim_id:
        raise InvalidClaimRevision("claim id required")
    revision = require_int("claim revision", env.get("revision"), minimum=0)
    day = require_int("claim day", env.get("day"))
    loss = require_int("claim loss", env.get("loss"), minimum=0)
    policy = env.get("policy", default_policy)
    device = env.get("device", default_device)
    if not isinstance(policy, str) or not policy:
        raise InvalidClaimRevision(f"claim {claim_id}: policy is required on revision 0")
    if not isinstance(device, str) or not device:
        raise InvalidClaimRevision(f"claim {claim_id}: device is required on revision 0")
    return {
        "claim_id": claim_id,
        "policy": policy,
        "device": device,
        "revision": revision,
        "day": day,
        "loss": loss,
    }


def payment_intent_key(payment_id: str) -> str:
    return f"payment:{payment_id}"
