"""Local-only adapter for the existing CP2B Push proof and registration facts."""
from private_queue import QueueStop, request_identity


class VerifiedPushProvider:
    def __init__(self, proof, api, event, read_run, read_recovery_pin, *,
                 run, attempt, workflow, code, bootstrap, source, started):
        self.proof, self.api, self.event = proof, api, event
        self.read_run, self.recovery_head = read_run, read_recovery_pin
        self.run, self.attempt, self.workflow, self.code = run, attempt, workflow, code
        self.bootstrap, self.source, self.started = bootstrap, source, started

    def run_facts(self, run, attempt):
        p = self.proof
        if ((run, attempt) != (self.run, self.attempt)
                or any(type(x) is not int or x <= 0 for x in (run, attempt, self.workflow))
                or not p.oid(self.code)):
            raise QueueStop("PUSH_RUN_PIN")
        try:
            meta = self.read_run(run, attempt)
            expected = (run, attempt, self.workflow, p.REPO_ID, p.OWNER,
                        p.REF.removeprefix("refs/heads/"), self.source, "push")
            observed = (meta["id"], meta["run_attempt"], meta["workflow_id"],
                        meta["repository"]["id"], meta["actor"]["id"], meta["head_branch"],
                        meta["head_sha"], meta["event"])
            if observed != expected:
                raise QueueStop("PUSH_RUN_MISMATCH")
            p.prove(self.api, self.event, self.source, self.bootstrap, self.started)
        except QueueStop:
            raise
        except p.Stop as exc:
            raise QueueStop(str(exc)) from None
        except Exception:
            raise QueueStop("PUSH_PROVIDER_UNRESOLVED") from None
        return dict(run=run, attempt=attempt, workflow=self.workflow, code=self.code,
                    repository=p.REPO_ID, actor=p.OWNER, ref=p.REF, event="push",
                    forced=False, deleted=False, before=self.bootstrap, tip=self.source)

    def verify_binding(self, binding):
        facts = self.run_facts(self.run, self.attempt)
        expected = (facts["before"], facts["tip"],
                    request_identity(facts["repository"], facts["before"], facts["tip"]))
        if (binding.raw_parent, binding.raw_source, binding.owner_request_sha256) != expected:
            raise QueueStop("PUSH_SOURCE_BINDING")
        return expected[2]
