"""Coverage-aware claims application.

The claims app folds its own events for claim state, and asks the warehouse for
the *current* coverage decision using the highest warehouse sequence number it
has durably observed.  A payment therefore never validates against a stale UI
number: it re-reads the authoritative coverage and re-derives paid totals from
the commitment ledger inside the same write transaction.

Ordering guarantee: every write (coverage revision, claim revision, pause,
payment intent) starts with ``BEGIN IMMEDIATE``.  Two writers are serialised by
SQLite, their events receive monotonic ``seq`` values and the event log is the
single reproducible record of submission order.  Coverage ingested before a
payment is visible to that payment; coverage ingested after it cannot cancel
the commitment; it only moves the settlement into recovery.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Mapping

from .core import (
    ClaimRevisionConflict,
    Claims,
    CoverageRevisionConflict,
    DomainError,
    PaymentIdConflict,
    UnknownClaim,
    Warehouse,
    canonical_json,
    coverage_bucket_key,
    coverage_digest,
    holdback_from_paid,
    indemnity,
    normalize_claim_revision,
    normalize_coverage_envelope,
    payable_balance,
    payment_intent_key,
    require_int,
    sha256_hex,
)
from .store import Store


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds")


class Clock:
    """Injectable clock so demos and tests are deterministic."""

    def __init__(self, fixed: str | None = None) -> None:
        self.fixed = fixed

    def now(self) -> str:
        return self.fixed or utc_now()


class SettlementService:
    def __init__(self, store: Store, *, clock: Clock | None = None) -> None:
        self.store = store
        self.clock = clock or Clock()

    # ------------------------------------------------------------------ #
    # Fold helpers
    # ------------------------------------------------------------------ #

    def warehouse(self) -> Warehouse:
        wh = Warehouse()
        for event in self.store.events(realm="warehouse"):
            wh.apply(event)
        return wh

    def claims(self) -> Claims:
        claims = Claims()
        for event in self.store.events(realm="claims"):
            claims.apply(event)
        return claims

    # ------------------------------------------------------------------ #
    # Warehouse ingestion
    # ------------------------------------------------------------------ #

    def ingest_coverage_batch(
        self,
        envelopes: list[Mapping[str, Any]],
        *,
        source: str,
    ) -> dict:
        """Ingest one received batch (a list of coverage envelopes).

        Each envelope carries the full fragment set of one revision (possibly
        empty = revocation).  The batch is recorded even when it produced no new
        events, which is how an out-of-order replay is shown to be a no-op while
        remaining visible in the receive history.

        Rules enforced here:

        * fragments inside one revision must not overlap;
        * a revision is an immutable full mirror: replaying identical content
          is deduped, replaying *different* content under the same revision is a
          conflict (never silently last-writer-wins);
        * a larger revision replaces the whole smaller revision, not merges.
        """
        normalized = [normalize_coverage_envelope(env) for env in envelopes]
        # Reject conflicting duplicates inside one batch before touching disk.
        seen: dict[tuple[str, str, int], str] = {}
        unique: dict[tuple[str, str, int, str], dict] = {}
        for env in normalized:
            ident = (env["policy"], env["device"], env["revision"])
            body = canonical_json(env["fragments"])
            if ident in seen and seen[ident] != body:
                raise CoverageRevisionConflict(
                    f"batch contains revision {ident[2]} of {ident[0]}/{ident[1]} twice with different content"
                )
            seen[ident] = body
            unique[(env["policy"], env["device"], env["revision"], body)] = env
        normalized = sorted(
            unique.values(), key=lambda e: (e["policy"], e["device"], e["revision"], canonical_json(e["fragments"]))
        )
        digest = sha256_hex(canonical_json(normalized))
        now = self.clock.now()

        with self.store.transaction():
            wh = self.warehouse()
            for env in normalized:
                existing = wh.revisions.get((env["policy"], env["device"]), {}).get(env["revision"])
                if existing is not None and existing != env["fragments"]:
                    raise CoverageRevisionConflict(
                        f"{env['policy']}/{env['device']} revision {env['revision']} already received "
                        f"with different content; a revision is an immutable full mirror"
                    )
            batch_id = self.store.create_batch(source=source, digest=digest, received_at=now)
            created_events: list[int] = []
            noop_events: list[dict] = []
            observations: list[dict] = []
            for env in normalized:
                bucket = coverage_bucket_key(env["policy"], env["device"], env["revision"], env["fragments"])
                payload = {
                    "policy": env["policy"],
                    "device": env["device"],
                    "revision": env["revision"],
                    "fragments": env["fragments"],
                }
                event, created = self.store.append_event(
                    key=bucket,
                    kind="coverage_revision",
                    payload=payload,
                    occurred_at=now,
                    recorded_at=now,
                    origin="warehouse",
                    batch_id=batch_id,
                )
                if created:
                    created_events.append(event.seq)
                else:
                    noop_events.append(
                        {
                            "key": bucket,
                            "policy": env["policy"],
                            "device": env["device"],
                            "revision": env["revision"],
                            "reason": "identical replay of an already persisted revision",
                            "existing_seq": event.seq,
                        }
                    )
                observations.append(
                    {
                        "policy": env["policy"],
                        "device": env["device"],
                        "revision": env["revision"],
                        "event_seq": event.seq,
                        "created": created,
                        "fragments": env["fragments"],
                    }
                )
            if created_events:
                warehouse_seq = max(created_events)
                self.store.set_meta("last_warehouse_seq", str(warehouse_seq))
                # A claims-realm marker makes the observed warehouse position an
                # explicit, replayable fact rather than a hidden side channel.
                self.store.append_event(
                    key=f"warehouse-marker:batch:{batch_id}",
                    kind="warehouse_marker",
                    payload={
                        "batch_id": batch_id,
                        "warehouse_seq": warehouse_seq,
                        "event_seqs": [obs["event_seq"] for obs in observations if obs["created"]],
                    },
                    occurred_at=now,
                    recorded_at=now,
                    origin="claims",
                )
        return {
            "batch_id": batch_id,
            "digest": digest,
            "created_events": created_events,
            "noop_events": noop_events,
            "observations": observations,
        }

    def ingest_canonical_form(self, envelopes: list[Mapping[str, Any]]) -> dict:
        """Thin wrapper mirroring a channel replay of the same logical batch."""
        return self.ingest_coverage_batch(envelopes, source="canonical-replay")

    # ------------------------------------------------------------------ #
    # Coverage queries
    # ------------------------------------------------------------------ #

    def coverage_at(self, policy: str, device: str, day: int) -> dict:
        """Current coverage decision (latest received revision)."""
        wh = self.warehouse()
        decision = wh.decide(policy, device, day)
        key = (policy, device)
        decision["source_event_seq"] = wh.latest_seq.get(key)
        decision["source_event_key"] = wh.latest_key.get(key)
        return decision

    def coverage_at_batch(
        self,
        batch_id: int,
        policy: str,
        device: str,
        day: int,
        *,
        revision: int | None = None,
    ) -> dict:
        """Coverage as it stood at the moment a specific receive batch landed.

        Only warehouse events delivered by that batch or an earlier one are
        folded, so the answer is reproducible even after later revisions arrive.
        """
        batch = next((b for b in self.store.batches() if b["id"] == batch_id), None)
        if batch is None:
            raise DomainError(f"unknown batch {batch_id}")
        wh = Warehouse()
        last_seq = 0
        own_events: list[int] = []
        for event in self.store.events(realm="warehouse"):
            if event.batch_id is not None and event.batch_id > batch_id:
                continue
            wh.apply(event)
            last_seq = event.seq
            if event.batch_id == batch_id:
                own_events.append(event.seq)
        decision = wh.decide(policy, device, day, revision=revision)
        decision["batch_id"] = batch_id
        decision["batch_received_at"] = batch["received_at"]
        decision["batch_event_seqs"] = own_events
        decision["warehouse_seq_at_batch"] = last_seq
        return decision

    def coverage_history(self, policy: str, device: str, day: int) -> list[dict]:
        """Every revision ever received for a policy/device, with its decision.

        ``warehouse.decide`` with an explicit revision is O(1) per revision;
        the fold itself is O(events).  The receive history is listed so the
        cost is visible: each batch is a whole-set replacement, not a diff.
        """
        wh = self.warehouse()
        bucket = wh.revisions.get((policy, device), {})
        seq_by_rev: dict[int, int] = {}
        batch_by_rev: dict[int, int | None] = {}
        key_by_rev: dict[int, str] = {}
        for event in self.store.events(realm="warehouse"):
            payload = event.payload
            if payload.get("policy") != policy or payload.get("device") != device:
                continue
            rev = payload["revision"]
            if rev not in seq_by_rev or event.seq >= seq_by_rev[rev]:
                seq_by_rev[rev] = event.seq
                batch_by_rev[rev] = event.batch_id
                key_by_rev[rev] = event.key
        out: list[dict] = []
        for rev in sorted(bucket):
            decision = wh.decide(policy, device, day, revision=rev)
            out.append(
                {
                    "revision": rev,
                    "event_seq": seq_by_rev.get(rev),
                    "batch_id": batch_by_rev.get(rev),
                    "event_key": key_by_rev.get(rev),
                    "fragments": bucket[rev],
                    "covered": decision["covered"],
                    **({"deductible": decision["deductible"], "limit": decision["limit"]} if decision["covered"] else {}),
                }
            )
        return out

    # ------------------------------------------------------------------ #
    # Claim revisions
    # ------------------------------------------------------------------ #

    def revise_claim(
        self,
        claim_id: str,
        *,
        revision: int,
        day: int,
        loss: int,
        policy: str | None = None,
        device: str | None = None,
    ) -> dict:
        claims = self.claims()
        current = claims.claims.get(claim_id)
        if current is not None:
            if revision <= current.revision and revision in claims.claim_revisions.get(claim_id, {}):
                existing = claims.claim_revisions[claim_id][revision]
                same = (
                    existing.day == day
                    and existing.loss == loss
                    and (policy is None or policy == existing.policy)
                    and (device is None or device == existing.device)
                )
                if same:
                    return {
                        "claim": self.claim_view(claim_id),
                        "created": False,
                        "idempotent": True,
                        "event_seq": None,
                    }
                raise ClaimRevisionConflict(
                    f"claim {claim_id}: revision {revision} already exists with different content"
                )
            if revision < current.revision:
                raise ClaimRevisionConflict(
                    f"claim {claim_id}: revision {revision} is older than current {current.revision} "
                    f"and was never recorded"
                )
            policy = policy or current.policy
            device = device or current.device
        env = normalize_claim_revision(
            {
                "claim_id": claim_id,
                "revision": revision,
                "day": day,
                "loss": loss,
                "policy": policy,
                "device": device,
            },
            default_policy=policy,
            default_device=device,
        )
        key = f"claim:{claim_id}:rev:{revision}"
        now = self.clock.now()
        with self.store.transaction():
            event, created = self.store.append_event(
                key=key,
                kind="claim_revision",
                payload=env,
                occurred_at=now,
                recorded_at=now,
                origin="claims",
            )
        return {"claim": self.claim_view(claim_id), "created": created, "event_seq": event.seq}

    def pause_claim(self, claim_id: str, *, paused: bool, request_id: str | None = None) -> dict:
        claims = self.claims()
        if claim_id not in claims.claims:
            raise UnknownClaim(f"claim {claim_id} does not exist")
        now = self.clock.now()
        if request_id is None:
            # Default to a monotonic sequence so a genuine pause/resume pair
            # is never mistaken for a retry of itself.
            existing = self.store.count_events(kind="claim_pause", key_like=f"pause:{claim_id}:%")
            request_id = f"auto-{existing + 1}"
        key = f"pause:{claim_id}:{request_id}"
        payload = {"claim_id": claim_id, "action": "pause" if paused else "resume", "request_id": request_id}
        with self.store.transaction():
            event, created = self.store.append_event(
                key=key,
                kind="claim_pause",
                payload=payload,
                occurred_at=now,
                recorded_at=now,
                origin="claims",
            )
        return {"claim": self.claim_view(claim_id), "created": created, "event_seq": event.seq}

    # ------------------------------------------------------------------ #
    # Payment commitment
    # ------------------------------------------------------------------ #

    def _decision_inputs(self, claim, claims: Claims, wh: Warehouse) -> dict:
        """Authoritative state a payment decision is allowed to see."""
        decision = wh.decide(claim.policy, claim.device, claim.day)
        if decision["covered"]:
            auth = indemnity(claim.loss, decision["deductible"], decision["limit"])
        else:
            auth = 0
        key = (claim.policy, claim.device)
        return {
            "coverage": decision,
            "authoritative": auth,
            "coverage_revision": decision["revision"],
            "coverage_seq": wh.latest_seq.get(key),
            "coverage_digest": coverage_digest(
                claim.policy, claim.device, claim.day, decision["revision"], decision["fragments"]
            ),
        }

    def _rejected_result_from_decision(self, d: Mapping[str, Any]) -> dict:
        paid_before = int(d.get("paid_before") or 0)
        auth = int(d.get("authoritative") or 0)
        return {
            "status": "rejected",
            "reason": d.get("reason"),
            "detail": d.get("detail"),
            "payment_id": d["payment_id"],
            "claim_id": d["claim_id"],
            "amount": d["amount"],
            "authoritative": auth,
            "paid": paid_before,
            "payable_before": payable_balance(paid_before, auth),
            "recovery": holdback_from_paid(paid_before, auth),
            "coverage_revision": d.get("coverage_revision"),
            "coverage_digest": d.get("coverage_digest"),
            "claim_revision": d.get("claim_revision"),
            "paused": d.get("paused"),
        }

    def _replay_decision(
        self, claims: Claims, payment_id: str, claim_id: str, amount: int
    ) -> dict | None:
        """Same id + same content must return the original decision verbatim.

        This makes the decision itself sticky: a rejection caused by a
        revocation cannot later flip to acceptance just because coverage was
        restored, and an acceptance cannot be duplicated by a retry.
        """
        d = claims.decisions.get(payment_id)
        if d is None:
            return None
        if d.get("claim_id") != claim_id or d.get("amount") != amount:
            raise PaymentIdConflict(
                f"payment {payment_id} was already decided as {d.get('status')} for "
                f"claim {d.get('claim_id')} amount {d.get('amount')}; refusing different content"
            )
        if d.get("status") == "accepted":
            return {
                "status": "accepted",
                "idempotent": True,
                "payment": self.payment_view(payment_id),
                "delivery": self.delivery_view(payment_id),
            }
        return {**self._rejected_result_from_decision(d), "idempotent": True}

    def accept_payment(
        self,
        *,
        payment_id: str,
        claim_id: str,
        amount: int,
        expected_warehouse_seq: int | None = None,
        submitted_at: str | None = None,
        seen_coverage_revision: int | None = None,
    ) -> dict:
        """Atomically decide and persist a payment commitment.

        Rules, in one serialised transaction:

        1. idempotency: same payment id + same amount returns the original
           decision; same id + different amount is rejected;
        2. claim must exist; amount must be an integer >= 0;
        3. manual pause blocks new payments only;
        4. the authoritative entitlement is recomputed from the *latest*
           warehouse revision and the claim's latest revision;
        5. ``committed_before + amount <= entitlement`` must hold, where
           ``committed_before`` excludes this payment id (so a retry that lost
           its response cannot double count);
        6. accept => payment_intent event + receiver row + outbox row commit
           together.
        """
        require_int("amount", amount, minimum=0)
        claims = self.claims()
        replayed = self._replay_decision(claims, payment_id, claim_id, amount)
        if replayed is not None:
            return replayed
        claim = claims.claims.get(claim_id)
        if claim is None:
            raise UnknownClaim(f"claim {claim_id} does not exist")
        wh = self.warehouse()
        latest_warehouse_seq = max((e.seq for e in self.store.events(realm="warehouse")), default=0)
        if expected_warehouse_seq is not None and expected_warehouse_seq != latest_warehouse_seq:
            raise DomainError(
                f"stale submission: client expected warehouse seq {expected_warehouse_seq}, "
                f"latest is {latest_warehouse_seq}; re-read coverage and retry"
            )
        if seen_coverage_revision is not None:
            current = wh.latest.get((claim.policy, claim.device))
            if seen_coverage_revision != current:
                raise DomainError(
                    f"stale submission: client saw coverage revision {seen_coverage_revision}, "
                    f"latest is {current}; re-read coverage and retry"
                )

        now = submitted_at or self.clock.now()
        with self.store.transaction():
            # Re-read inside the write lock: another writer may have committed
            # while we were building the fold above.  This is what makes the
            # "coverage revision vs payment" order reproducible.
            claims = self.claims()
            replayed = self._replay_decision(claims, payment_id, claim_id, amount)
            if replayed is not None:
                return replayed
            claim = claims.claims.get(claim_id)
            if claim is None:
                raise UnknownClaim(f"claim {claim_id} does not exist")
            wh = self.warehouse()
            inputs = self._decision_inputs(claim, claims, wh)
            paid_before = claims.committed_total(claim_id)
            withheld_before = holdback_from_paid(paid_before, inputs["authoritative"])
            recovery_before = withheld_before
            paused = claims.is_paused(claim_id)
            capacity = payable_balance(paid_before, inputs["authoritative"])

            base = {
                "payment_id": payment_id,
                "claim_id": claim_id,
                "amount": amount,
                "coverage_digest": inputs["coverage_digest"],
                "coverage_revision": inputs["coverage_revision"],
                "coverage_seq": inputs["coverage_seq"],
                "authoritative": inputs["authoritative"],
                "paid_before": paid_before,
                "withheld_before": withheld_before,
                "recovery_before": recovery_before,
                "claim_revision": claim.revision,
                "paused": paused,
                "submitted_at": now,
            }

            if paused:
                self.store.append_event(
                    key=f"rejection:{payment_id}:{sha256_hex(canonical_json([base, 'paused']))}",
                    kind="payment_rejected",
                    payload={**base, "reason": "paused", "detail": "manual pause blocks new payments"},
                    occurred_at=now,
                    recorded_at=now,
                    origin="claims",
                )
                return self._rejection_result(
                    "paused", base, "manual pause blocks new payments", capacity, recovery_before
                )
            if recovery_before > 0:
                self.store.append_event(
                    key=f"rejection:{payment_id}:{sha256_hex(canonical_json([base, 'recovery']))}",
                    kind="payment_rejected",
                    payload={**base, "reason": "recovery_active", "detail": "clawback outstanding"},
                    occurred_at=now,
                    recorded_at=now,
                    origin="claims",
                )
                return self._rejection_result(
                    "recovery_active", base, "clawback outstanding", 0, recovery_before
                )
            if amount > capacity:
                self.store.append_event(
                    key=f"rejection:{payment_id}:{sha256_hex(canonical_json([base, 'capacity']))}",
                    kind="payment_rejected",
                    payload={
                        **base,
                        "reason": "exceeds_payable",
                        "detail": f"requested {amount} > payable {capacity}",
                    },
                    occurred_at=now,
                    recorded_at=now,
                    origin="claims",
                )
                return self._rejection_result(
                    "exceeds_payable",
                    base,
                    f"requested {amount} > payable {capacity}",
                    capacity,
                    recovery_before,
                )

            event, created = self.store.append_event(
                key=payment_intent_key(payment_id),
                kind="payment_intent",
                payload=base,
                occurred_at=now,
                recorded_at=now,
                origin="claims",
            )
            delivery_seq = self.store.next_delivery_seq()
            self.store.outbox_add(payment_id, delivery_seq)
        return {
            "status": "accepted",
            "idempotent": not created,
            "payment": self.payment_view(payment_id),
            "delivery": self.delivery_view(payment_id),
        }

    def _rejection_result(
        self, reason: str, base: dict, detail: str, capacity: int, recovery: int
    ) -> dict:
        return {
            "status": "rejected",
            "reason": reason,
            "detail": detail,
            "payment_id": base["payment_id"],
            "claim_id": base["claim_id"],
            "amount": base["amount"],
            "authoritative": base["authoritative"],
            "paid": base["paid_before"],
            "payable_before": capacity,
            "recovery": recovery,
            "coverage_revision": base["coverage_revision"],
            "coverage_digest": base["coverage_digest"],
            "claim_revision": base["claim_revision"],
            "paused": base["paused"],
        }

    # ------------------------------------------------------------------ #
    # Delivery (simulated external receiver)
    # ------------------------------------------------------------------ #

    def dispatch_payment(self, payment_id: str, *, drop_response: bool = False) -> dict:
        """Send an accepted commitment to the idempotent receiver.

        The receiver dedupes on payment id.  ``drop_response`` accepts the
        money but throws away the acknowledgement: the outbox stays ``pending``,
        the balance is unchanged (commitment already counted it) and a later
        retry re-sends the same id, so the receiver replies "duplicate" instead
        of charging twice.
        """
        row = self.store.outbox_get(payment_id)
        if row is None:
            raise DomainError(f"payment {payment_id} was never committed; nothing to deliver")
        now = self.clock.now()
        attempts = row["attempts"]

        if row["state"] == "delivered":
            recv = self._receiver_row(payment_id)
            return {
                "payment_id": payment_id,
                "result": "already_delivered",
                "receiver": recv,
                "receiver_duplicate": True,
                "attempts": attempts,
            }

        with self.store.transaction():
            recv, created = self.store.receiver_upsert(
                payment_id=payment_id,
                claim_id=self.payment_view(payment_id)["claim_id"],
                amount=self.payment_view(payment_id)["amount"],
                received_at=now,
            )
            attempts += 1
            if drop_response:
                self.store.outbox_mark(
                    payment_id,
                    state="pending",
                    attempts=attempts,
                    last_error="response_lost: receiver accepted but ack was dropped",
                )
                return {
                    "payment_id": payment_id,
                    "result": "response_lost",
                    "receiver": recv,
                    "receiver_duplicate": not created,
                    "attempts": attempts,
                }
            self.store.outbox_mark(payment_id, state="delivered", attempts=attempts, last_error=None)
        return {
            "payment_id": payment_id,
            "result": "delivered" if created else "duplicate_ack",
            "receiver": recv,
            "receiver_duplicate": not created,
            "attempts": attempts,
        }

    def dispatch_all_pending(self) -> list[dict]:
        return [
            self.dispatch_payment(row["payment_id"])
            for row in self.store.outbox_rows()
            if row["state"] == "pending"
        ]

    def _receiver_row(self, payment_id: str) -> dict | None:
        return self.store.receiver_for(payment_id)

    # ------------------------------------------------------------------ #
    # Views
    # ------------------------------------------------------------------ #

    def payment_view(self, payment_id: str) -> dict | None:
        claims = self.claims()
        p = claims.payments.get(payment_id)
        if p is None:
            return None
        outbox = self.store.outbox_get(payment_id)
        receiver = self._receiver_row(payment_id)
        return {
            "payment_id": p.payment_id,
            "claim_id": p.claim_id,
            "amount": p.amount,
            "event_seq": p.seq,
            "committed_at": p.occurred_at,
            "claim_revision": p.claim_revision,
            "coverage_revision": p.coverage_revision,
            "coverage_seq": p.coverage_seq,
            "coverage_digest": p.coverage_digest,
            "authoritative_at_commit": p.authoritative,
            "paid_before": p.paid_before,
            "withheld_before": p.withheld_before,
            "recovery_before": p.recovery_before,
            "ledger_status": "committed",
            "delivery_status": outbox["state"] if outbox else "missing",
            "delivery_attempts": outbox["attempts"] if outbox else 0,
            "delivery_error": outbox["last_error"] if outbox else None,
            "receiver_status": receiver["status"] if receiver else "not_received",
            "receiver_seq": receiver["received_seq"] if receiver else None,
        }

    def delivery_view(self, payment_id: str) -> dict | None:
        outbox = self.store.outbox_get(payment_id)
        if outbox is None:
            return None
        receiver = self._receiver_row(payment_id)
        return {
            "payment_id": payment_id,
            "state": outbox["state"],
            "attempts": outbox["attempts"],
            "last_error": outbox["last_error"],
            "receiver": (
                {
                    "status": receiver["status"],
                    "received_seq": receiver["received_seq"],
                    "amount": receiver["amount"],
                }
                if receiver
                else None
            ),
        }

    def claim_view(self, claim_id: str) -> dict:
        claims = self.claims()
        claim = claims.claims.get(claim_id)
        if claim is None:
            raise UnknownClaim(f"claim {claim_id} does not exist")
        wh = self.warehouse()
        inputs = self._decision_inputs(claim, claims, wh)
        paid = claims.committed_total(claim_id)
        withheld = holdback_from_paid(paid, inputs["authoritative"])
        recovery = withheld
        payable = payable_balance(paid, inputs["authoritative"])
        payments = [
            {
                "payment_id": p.payment_id,
                "amount": p.amount,
                "event_seq": p.seq,
                "claimed_at": p.occurred_at,
                "claim_revision": p.claim_revision,
                "coverage_revision": p.coverage_revision,
                "authoritative_at_commit": p.authoritative,
                "receiver_status": (
                    self._receiver_row(p.payment_id)["status"] if self._receiver_row(p.payment_id) else "not_received"
                ),
                "delivery_status": (
                    self.store.outbox_get(p.payment_id)["state"]
                    if self.store.outbox_get(p.payment_id)
                    else "missing"
                ),
            }
            for p in claims.committed_payments(claim_id)
        ]
        return {
            "claim_id": claim_id,
            "policy": claim.policy,
            "device": claim.device,
            "revision": claim.revision,
            "day": claim.day,
            "loss": claim.loss,
            "covered": inputs["coverage"]["covered"],
            "coverage_revision": inputs["coverage_revision"],
            "coverage_source_seq": inputs["coverage_seq"],
            "coverage_digest": inputs["coverage_digest"],
            "deductible": inputs["coverage"].get("deductible"),
            "limit": inputs["coverage"].get("limit"),
            "authoritative": inputs["authoritative"],
            "paid_total": paid,
            "withheld": withheld,
            "recovery": recovery,
            "payable": payable,
            "paused": claims.is_paused(claim_id),
            "pause_event_seq": claims.paused.get(claim_id).seq if claims.paused.get(claim_id) else None,
            "payments": payments,
            "payment_decisions": [
                d for d in sorted(claims.decisions.values(), key=lambda x: x["seq"]) if d.get("claim_id") == claim_id
            ],
        }

    def ledger(self) -> dict:
        """Three separate facts per payment: commitment, delivery, receipt.

        The settlement balance is derived only from ``committed`` rows; a
        pending delivery or a missing acknowledgement never changes it.
        """
        claims = self.claims()
        rows = []
        for p in sorted(claims.payments.values(), key=lambda x: x.seq):
            view = self.payment_view(p.payment_id)
            outbox = self.store.outbox_get(p.payment_id)
            rows.append(
                {
                    "payment_id": p.payment_id,
                    "claim_id": p.claim_id,
                    "amount": p.amount,
                    "committed": True,
                    "committed_seq": p.seq,
                    "committed_at": p.occurred_at,
                    "delivery": outbox["state"] if outbox else "missing",
                    "delivery_attempts": outbox["attempts"] if outbox else 0,
                    "receiver_accepted": view["receiver_status"] == "accepted" if view else False,
                    "receiver_status": view["receiver_status"] if view else "missing",
                }
            )
        return {
            "committed_total": sum(r["amount"] for r in rows),
            "committed_count": len(rows),
            "delivery_pending_total": sum(r["amount"] for r in rows if r["delivery"] == "pending"),
            "receiver_accepted_total": sum(r["amount"] for r in rows if r["receiver_accepted"]),
            "rows": rows,
        }

    def events(self) -> list[dict]:
        return [
            {
                "seq": e.seq,
                "key": e.key,
                "kind": e.kind,
                "origin": e.origin,
                "batch_id": e.batch_id,
                "occurred_at": e.occurred_at,
                "recorded_at": e.recorded_at,
                "payload": e.payload,
            }
            for e in self.store.events()
        ]

    # ------------------------------------------------------------------ #
    # Deterministic replay of claim actions after a restart
    # ------------------------------------------------------------------ #

    def replay_claim_action(self, action: Mapping[str, Any]) -> dict:
        """Apply one recorded action idempotently (used by tests/demo)."""
        kind = action["kind"]
        if kind == "claim_revision":
            return self.revise_claim(
                action["claim_id"],
                revision=action["revision"],
                day=action["day"],
                loss=action["loss"],
                policy=action.get("policy"),
                device=action.get("device"),
            )
        if kind in ("pause", "resume"):
            return self.pause_claim(
                action["claim_id"], paused=(kind == "pause"), request_id=action.get("request_id")
            )
        if kind == "payment":
            return self.accept_payment(
                payment_id=action["payment_id"],
                claim_id=action["claim_id"],
                amount=action["amount"],
                expected_warehouse_seq=action.get("expected_warehouse_seq"),
            )
        if kind == "dispatch":
            return self.dispatch_payment(
                action["payment_id"], drop_response=action.get("drop_response", False)
            )
        raise DomainError(f"unknown replay action kind {kind}")
