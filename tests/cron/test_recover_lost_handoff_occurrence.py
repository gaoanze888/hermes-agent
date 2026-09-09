"""Regression: a restart-safe handoff whose successor never adopts is refired, not lost (#106607).

When the original gateway owner dies while its replacement worker is still initializing (past the
adoption grace), ``recover_interrupted_executions`` terminalizes the unadopted handoff as
``unknown``. Before #106607, the scheduled occurrence was silently consumed: ``claim_job_for_fire``
had already stamped ``fire_claim`` and advanced ``next_run_at`` at dispatch, so the next tick saw an
already-advanced ``next_run_at`` and the stale claim blocked a reclaim within its TTL. The fix clears
the stale claim and restores ``next_run_at`` to now so the scheduler refires the lost occurrence.

Non-handoff interruptions (``handoff_pending=0``) DID run (to an unknown extent) and must not refire,
so they leave ``fire_claim`` and ``next_run_at`` untouched (no double side-effects).
"""
from __future__ import annotations


def _point_ledger(monkeypatch, tmp_path):
    import cron.executions as executions

    monkeypatch.setattr(executions, "EXECUTIONS_FILE", tmp_path / "cron" / "executions.db")
    return executions


def test_unadopted_handoff_refires_lost_occurrence(monkeypatch, tmp_path):
    from cron.jobs import create_job, claim_job_for_fire, get_job

    # Isolate the jobs store so the requeue touches a temp jobs.json, not the real one.
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    executions = _point_ledger(monkeypatch, tmp_path)

    # A real recurring job, dispatched exactly once: claim stamps fire_claim and advances
    # next_run_at (simulating the original owner firing the scheduled occurrence).
    job = create_job(prompt="x", schedule="every 5m", name="lost")
    jid = job["id"]
    original_next = get_job(jid)["next_run_at"]
    assert claim_job_for_fire(jid) is True
    dispatched = get_job(jid)
    assert dispatched["fire_claim"] is not None
    assert dispatched["next_run_at"] != original_next
    advanced_next = dispatched["next_run_at"]

    # The execution ledger records the dispatched attempt with a pending restart-safe handoff.
    record = executions.create_execution(jid, source="builtin")
    pending = executions.mark_execution_handoff_pending(record["id"])
    assert pending is not None

    # The original owner is dead and the replacement worker never adopted within the grace window.
    monkeypatch.setattr(executions, "_PROCESS_ID", "replacement-gateway")
    monkeypatch.setattr(executions, "_owner_is_live", lambda _pid, _started: False)
    monkeypatch.setattr(
        executions.time,
        "time",
        lambda: pending["handoff_started_at"]
        + executions.HANDOFF_ADOPTION_GRACE_SECONDS
        + 1,
    )

    # Recovery terminalizes the unadopted handoff AND re-arms the job so the lost occurrence
    # is refired rather than silently consumed.
    assert executions.recover_interrupted_executions() == 1
    assert executions.get_execution(record["id"])["status"] == "unknown"

    rearmed = get_job(jid)
    assert rearmed["fire_claim"] is None, (
        "stale claim must be cleared so the occurrence can refire"
    )
    assert rearmed["next_run_at"] != advanced_next, (
        "next_run_at must be restored so the occurrence refires"
    )

    # The lost occurrence is reclaimable: the scheduler can fire it again (proving it was not consumed).
    assert claim_job_for_fire(jid) is True


def test_non_handoff_interruption_does_not_refire(monkeypatch, tmp_path):
    from cron.jobs import create_job, claim_job_for_fire, get_job

    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    executions = _point_ledger(monkeypatch, tmp_path)

    # A dispatched job whose owner died mid-run (no handoff in flight). The occurrence DID run
    # (to an unknown extent), so it must NOT be refired (no double side-effects).
    job = create_job(prompt="x", schedule="every 5m", name="interrupted")
    jid = job["id"]
    assert claim_job_for_fire(jid) is True
    claimed = get_job(jid)
    assert claimed["fire_claim"] is not None
    advanced_next = claimed["next_run_at"]

    record = executions.create_execution(jid, source="builtin")
    # handoff_pending stays 0 (no mark_execution_handoff_pending call)

    monkeypatch.setattr(executions, "_PROCESS_ID", "replacement-gateway")
    monkeypatch.setattr(executions, "_owner_is_live", lambda _pid, _started: False)

    assert executions.recover_interrupted_executions() == 1
    assert executions.get_execution(record["id"])["status"] == "unknown"

    # The job is NOT re-armed: the stale claim stays and next_run_at stays advanced — the
    # interrupted occurrence is not refired (recovery stays "not a retry queue").
    untouched = get_job(jid)
    assert untouched["fire_claim"] == claimed["fire_claim"], (
        "non-handoff interruption must not clear the claim"
    )
    assert untouched["next_run_at"] == advanced_next, (
        "non-handoff interruption must not restore next_run_at"
    )
