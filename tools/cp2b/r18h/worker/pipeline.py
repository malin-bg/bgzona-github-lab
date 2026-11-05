"""Inactive PRIVATE admission, using existing E4G/D3 contracts in disposable Labs.

PushEvent is trusted workflow context, not a JSON proof of actor authentication.
The injected Backend supplies the independent repository/session/object read port.
No default connection, writer, live mode, queue or publication authority is added.
Returned objects and provenance are PRIVATE capabilities, never public artifacts.
"""
from dataclasses import dataclass
from pathlib import PurePosixPath
import time
import uuid

import github_backend as gb
import intake_plan as planner
import privacy_gate
from authority_coordinator import LocalWitness, SourceBinding, DerivedFile
from request_executor import RequestOperation, EDITABLE, binding as accepted_binding
from git_transport import Lab, MAIN, git, value, files
from importer import data, parse, digest, text


class PipelineStop(RuntimeError):
    def __init__(self, code):
        super().__init__(code)
        self.code = code


def need(ok, code):
    if not ok:
        raise PipelineStop(code)


@dataclass(frozen=True, repr=False)
class PushEvent:
    repository_id: int
    repository: str
    actor_id: int
    parent: str
    commit: str
    ref: str = MAIN
    event: str = "push"
    forced: bool = False
    deleted: bool = False


@dataclass(frozen=True, repr=False)
class Prepared:
    operation: RequestOperation
    binding: SourceBinding
    proofs: tuple

    def summary(self):
        return {"status": "PRIVATE_REQUEST_PREPARED", "publication_authorized": False,
                "authority_accepted": False, "live_activation": False}


def _commit(repo, buffers, parent, timestamp, index):
    """Materialize deterministic PRIVATE source objects, never authority commits."""
    env = {"GIT_INDEX_FILE": str(index), "GIT_AUTHOR_DATE": timestamp,
           "GIT_COMMITTER_DATE": timestamp}
    git(repo, "read-tree", "--empty", env_extra=env)
    entries = []
    for name, raw in sorted(buffers.items()):
        identity = value(repo, "hash-object", "-w", "--stdin", stdin=raw)
        entries.append(b"100644 " + identity.encode() + b"\t" + name.encode() + b"\0")
    if entries:
        git(repo, "update-index", "-z", "--index-info", stdin=b"".join(entries), env_extra=env)
    tree = value(repo, "write-tree", env_extra=env)
    args = ["commit-tree", tree, "-m", "E4N PRIVATE derived source"]
    if parent:
        args += ["-p", parent]
    return value(repo, *args, env_extra=env)


def _verified_queue(queue, lab, backend, pin, event):
    # Import lazily because the queue reuses PushEvent from this module.
    from private_queue import PrivateQueue, request_identity

    need(type(queue) is PrivateQueue and queue.lab.root == lab.root and
         queue.backend is backend and queue.repository == pin and
         queue.owner_actor_id == event.actor_id, "QUEUED_SOURCE_UNVERIFIED")
    snapshot = queue.snapshot()
    rows = snapshot["requests"]
    need(rows and rows[0]["state"] == "pending" and
         (rows[0]["source"], rows[0]["parent"], rows[0]["request"]) ==
         (event.commit, event.parent, request_identity(pin.numeric_id, event.parent, event.commit)),
         "QUEUED_SOURCE_UNVERIFIED")
    return snapshot


def prepare(lab, source, event, *, backend, owner_actor_id, domain,
            interrupt=lambda point: None, queued=None, registration=None):
    """Read independent state, prepare exact private bytes, and bind existing planner.

    A queued request must be the verified pending queue head. The actual
    branch tip is observed separately from its immutable commit.
    Current boundary requires a synchronized accepted editing mirror at parent.
    Mismatched mirrors and lifecycle ambiguity STOP. Resume is through the existing
    accepted E4G/D3 request; this function does not invent a second recovery journal.
    """
    try:
        return _prepare(lab, source, event, backend, owner_actor_id, domain, interrupt, queued, registration)
    except PipelineStop:
        raise
    except Exception:
        # Neither provider exception text nor raw paths may reach diagnostics.
        raise PipelineStop("PRIVATE_INTAKE_UNRESOLVED") from None


def _prepare(lab, source, event, backend, owner_actor_id, domain, interrupt, queued, registration):
    need(type(lab) is Lab and type(domain) is LocalWitness and domain.lab.root == lab.root,
         "ISOLATED_DOMAIN_REQUIRED")
    need(type(backend) is gb.Backend and type(event) is PushEvent, "TRUSTED_INPUT_REQUIRED")
    source_ref, reader = MAIN, backend
    if registration is not None:
        from private_queue import TrustedRegistration, request_identity
        need(type(registration) is TrustedRegistration and (registration.cp2b_dynamic or registration.standard)
             and registration.queue is queued, "REGISTRATION_TRUST_SCOPE")
        verified = registration.verify()
        need(verified and verified[0][1:] ==
             (request_identity(event.repository_id, event.parent, event.commit),
              event.parent, event.commit), "QUEUED_SOURCE_UNVERIFIED")
        source_ref, reader = queued.source_ref, queued.source_reader
    need(gb.positive(owner_actor_id) and type(event.actor_id) is int and
         event.actor_id == owner_actor_id and event.event == "push" and event.ref == source_ref and
         event.forced is False and event.deleted is False, "OWNER_EVENT_REJECTED")
    pin = queued.repository if registration is not None and registration.standard else backend.repos["intake"]
    need(type(event.repository_id) is int and event.repository_id == pin.numeric_id and
         event.repository == pin.name and pin.visibility == "private", "REPOSITORY_MISMATCH")
    need(gb.oid(event.commit) and gb.oid(event.parent) and event.commit != event.parent,
         "SOURCE_IDENTITY")
    queue_snapshot = None if queued is None else _verified_queue(queued, lab, backend, pin, event)
    source = lab.remote(source)
    observed_tip = reader._ref("intake", source_ref)
    if queue_snapshot is None:
        need(observed_tip == event.commit, "SOURCE_MOVED")
    else:
        from private_queue import QueueStop

        try:
            queued._on_main(event.commit, observed_tip)
            # Also reject a rewind/fork that discarded a later registered row.
            queued._on_main(queue_snapshot["discovery_cursor"], observed_tip)
        except QueueStop:
            raise PipelineStop("QUEUED_SOURCE_DIVERGENT") from None

    def source_stable():
        return reader._ref("intake", source_ref) == observed_tip and (
            queued is None or (value(source, "rev-parse", source_ref) == observed_tip and
                               queued.snapshot() == queue_snapshot))

    _, parents = reader._commit("intake", event.commit)
    need(parents == [event.parent], "SOURCE_PARENT")
    reader._commit("intake", event.parent)
    # Git verifies the entire local object graph; provider OIDs bind it remotely.
    git(source, "fsck", "--full", "--strict", "--no-reflogs")
    need(value(source, "rev-parse", source_ref) == observed_tip and
         value(source, "show", "-s", "--format=%P", event.commit).split() == parents,
         "SOURCE_MIRROR_MISMATCH")
    historical_workflow = None
    if registration is not None and registration.standard:
        from push_artifact import WORKFLOW_PATH
        historical_workflow = WORKFLOW_PATH
    elif registration is not None:
        from registration_entry import BEFORE, HISTORY_SOURCE, WORKFLOW
        if (event.parent, event.commit) == (BEFORE, HISTORY_SOURCE):
            historical_workflow = WORKFLOW
    _source_scope(source, event.parent, event.commit, allowed_control=historical_workflow)
    from private_queue import request_identity
    owner = request_identity(pin.numeric_id, event.parent, event.commit)
    return _prepare_views(lab, source, backend, pin, domain, None, event.commit, event.parent,
                          event.parent, owner, source_stable, interrupt, source_reader=reader)


def _prepare_views(lab, source, backend, pin, domain, context, source_commit, raw_parent,
                   raw_base, owner, source_stable, interrupt, *, range_proof=None, source_reader=None):
    reader = backend if source_reader is None else source_reader
    old = files(source, raw_base, EDITABLE)
    new = files(source, source_commit, EDITABLE)
    for commit, view in ((raw_base, old), (source_commit, new)):
        planner._manifest({name: digest(raw) for name, raw in view.items()})
        need(sum(map(len, view.values())) <= planner.MAX_BYTES and
             all(len(raw) <= planner.MAX_FILE for raw in view.values()), "SOURCE_LIMIT")
        for name, raw in view.items():
            _, verified = reader._path("intake", commit, name)
            need(verified == raw, "SOURCE_BYTES_MISMATCH")
    context = domain.read() if context is None else context
    record = context["latest"]
    accepted = files(context["recovered"], record["primary_main"])
    baseline = {name: raw for name, raw in accepted.items() if name.startswith(EDITABLE)}
    need(old == baseline, "EDITING_MIRROR_CONFLICT")
    registry = parse(accepted["automation-state/registry.json"])
    planner._bundle_rows(new, registry)
    groups, replacements = planner._incoming_groups(new)
    roles = {}
    destinations = {}
    for (collection, stem), group in groups.items():
        # Keep accepted incoming paths; canonicalize newly submitted raw names.
        fresh = not any(name in old for name in group.values())
        safe = "input-" + digest(data([owner, collection, stem]))
        for role, name in group.items():
            roles[name] = role
            suffix = PurePosixPath(name).suffix.lower()
            destinations[name] = ("incoming/" + collection + "/" + safe +
                                  (".og" if role == "og" else "") + suffix) if fresh else name
    for name in replacements.values():
        roles[name] = "original"
        p = PurePosixPath(name)
        destinations[name] = str(p.parent / ("input-" + digest(data([owner, name])) + p.suffix.lower())) if name not in old else name
    derived = {}
    bindings = []
    proofs = []
    for name, raw in sorted(new.items()):
        destination = destinations.get(name, name)
        need(destination not in derived, "DERIVED_PATH_COLLISION")
        if old.get(name) == raw:
            derived[destination] = baseline[name]
            continue
        p = PurePosixPath(name)
        role = roles.get(name)
        if role is None:
            role = "txt" if p.suffix.lower() == ".txt" else "og" if p.name in ("og.png", "og.jpg") else None
        need(role in ("original", "og", "txt"), "UNSUPPORTED_CHANGED_FILE")
        if role == "txt":
            text(raw)
            output = raw
        else:
            result = privacy_gate.prepare(raw, p.suffix, role)
            need(result.evidence["status"] == "SANITIZED_AND_VERIFIED", "PRIVACY_STOP")
            output = result.output
            checked = privacy_gate.prepare(output, p.suffix, role)
            need(checked.evidence["status"] == "SANITIZED_AND_VERIFIED" and
                 checked.output == output, "PRIVACY_READBACK")
        raw_oid, verified = reader._path("intake", source_commit, name)
        need(raw == verified, "SOURCE_BYTES_MISMATCH")
        raw_pin = gb.ObjectPin(raw_oid, digest(raw), len(raw))
        from hashlib import sha1
        out_pin = gb.ObjectPin(sha1(b"blob " + str(len(output)).encode() + b"\0" + output).hexdigest(),
                               digest(output), len(output))
        proof_sha = None
        if role != "txt":
            actors = dict(backend.policy.actors)
            proof = gb.Derivation(owner, source_commit, raw_pin, out_pin, role, privacy_gate.POLICY,
                                  "SANITIZED_AND_VERIFIED", actors["independent_verify"],
                                  backend.policy.verifier_code_oid)
            from dataclasses import asdict
            proof_sha = digest(data(asdict(proof)))
            proofs.append((proof_sha, proof, raw, output))
        bindings.append(DerivedFile(destination, name, raw_pin, out_pin, role, proof_sha))
        derived[destination] = output
    interrupt("after-private-derivation")
    need(source_stable() and
         value(domain.anchor, "rev-parse", MAIN) == context["tip"], "STATE_MOVED")
    if range_proof is not None and derived == baseline:
        rows, recovery_head, mirror_record = range_proof
        return PreparedRange(rows, recovery_head, owner, mirror_record, None, None, None,
                             None, (), ())
    job = lab.path(lab.root / "private-intake" / uuid.uuid4().hex)
    job.mkdir(parents=True)
    repo = job / "derived.git"
    repo.mkdir()
    git(repo, "init", "--bare", "--initial-branch=main")
    timestamp = "@" + value(source, "show", "-s", "--format=%at", source_commit) + " +0000"
    parent = _commit(repo, baseline, None, timestamp, job / "index")
    commit = _commit(repo, derived, parent, timestamp, job / "index")
    git(repo, "update-ref", MAIN, commit)
    before = dict(schema=1, repository={"id": pin.numeric_id, "name": pin.name}, commit=parent,
                  authority_commit=record["primary_main"], witness_commit=record["primary_witness"],
                  ledger_record_sha256=digest(data(record)), registry=registry,
                  files={name: digest(raw) for name, raw in baseline.items()})
    operations = []
    for name in sorted(set(baseline) | set(derived)):
        a, b = baseline.get(name), derived.get(name)
        if a != b:
            operations.append(dict(op="add" if a is None else "delete" if b is None else "modify",
                                   path=name, before_sha256=digest(a) if a is not None else None,
                                   after_sha256=digest(b) if b is not None else None))
    change = dict(schema=1, repository=before["repository"], parent=parent, commit=commit,
                  before_state_sha256=digest(data(before)),
                  files={name: digest(raw) for name, raw in derived.items()}, operations=operations)
    evidence = planner.SourceEvidence(pin.numeric_id, pin.name, digest(data(before)),
                                     planner.change_digest(change), digest(data(record)), int(time.time()), "private")
    operation = RequestOperation(lab, repo, before, change, evidence, interrupt)
    need({f.path for f in bindings} == {name for name, raw in derived.items() if baseline.get(name) != raw},
         "DERIVED_BINDING_MISMATCH")
    binding = SourceBinding(owner, source_commit, raw_parent, repo, commit, parent,
                            operation.request, tuple(bindings))
    need(source_stable() and
         value(domain.anchor, "rev-parse", MAIN) == context["tip"], "STATE_MOVED")
    if range_proof is not None:
        rows, recovery_head, mirror_record = range_proof
        return PreparedRange(rows, recovery_head, owner, mirror_record, commit, parent,
                             operation.request, repo, tuple(bindings), tuple(proofs))
    return Prepared(operation, binding, tuple(proofs))


@dataclass(frozen=True, repr=False)
class PreparedRange:
    """PRIVATE preparation evidence only. No executable D3 binding."""
    rows: tuple
    recovery_head: str
    request: str
    ledger_record: str
    derived_source: str | None
    derived_parent: str | None
    derived_request: str | None
    derived_repo: object
    files: tuple
    proofs: tuple

    @property
    def no_op(self):
        return self.derived_source is None


def _prepare_range(registration, source, domain, mirror_record, interrupt):
    from private_queue import QueueStop, TrustedRegistration

    need(type(registration) is TrustedRegistration, "REGISTRATION_TRUST_SCOPE")
    queue = registration.queue
    need(type(domain) is LocalWitness and domain.lab.root == queue.lab.root,
         "ISOLATED_DOMAIN_REQUIRED")
    need(gb.sha(mirror_record), "MIRROR_PIN_REQUIRED")
    try:
        recovery_head = registration.provider.recovery_head()
        rows = tuple(registration.verify())
        snapshot = queue.snapshot()
        need(len(rows) == 2 and snapshot["acceptance_cursor"] is None and
             tuple((r["sequence"], r["request"], r["parent"], r["source"])
                   for r in snapshot["requests"]) == rows and
             all(r["state"] == "pending" and r["outcome"] is None for r in snapshot["requests"]),
             "RANGE_QUEUE_CONFLICT")
        need(rows[0][2] == queue.baseline and rows[0][3] == rows[1][2],
             "RANGE_SOURCE_IDENTITY")
        backend = queue.backend
        pin = backend.repos["intake"]
        need(pin == queue.repository, "REPOSITORY_MISMATCH")
        source = queue.lab.remote(source)
        source_commit = rows[-1][3]
        authority_refs = (("assets", MAIN), ("assets", gb.WITNESS),
                          ("control", backend.policy.ledger_ref))
        refs = tuple(backend._ref(*ref) for ref in authority_refs)
        context = domain.read()
        record = context["latest"]
        prior = accepted_binding(context["recovered"], record["primary_main"])
        need(prior is None or (prior["final"] and prior["phase"] == "staged" and
                               prior["step"] == prior["steps"]), "RANGE_AUTHORITY_CONFLICT")
        need(digest(data(record)) == mirror_record and record["phase"] == "staged" and
             refs == (record["primary_main"], record["primary_witness"], context["tip"]),
             "RANGE_AUTHORITY_CONFLICT")

        def stable():
            return (backend._ref("intake", MAIN) == source_commit and
                    value(source, "rev-parse", MAIN) == source_commit and
                    registration.provider.recovery_head() == recovery_head and
                    tuple(registration.verify()) == rows and queue.snapshot() == snapshot and
                    registration.provider.recovery_head() == recovery_head and
                    tuple(backend._ref(*ref) for ref in authority_refs) == refs and
                    backend._ref("intake", MAIN) == source_commit and
                    value(source, "rev-parse", MAIN) == source_commit)

        need(stable(), "STATE_MOVED")
        git(source, "fsck", "--full", "--strict", "--no-reflogs")
        for _, _, parent, commit in rows:
            need(backend._commit("intake", commit)[1] == [parent] and
                 value(source, "show", "-s", "--format=%P", commit).split() == [parent],
                 "SOURCE_PARENT")
            _source_scope(source, parent, commit)
        owner = digest(data({"format": "bgzona-private-range-r1", "repository": pin.numeric_id,
                             "record": mirror_record, "rows": rows}))
        return _prepare_views(queue.lab, source, backend, pin, domain, context, source_commit,
                              rows[-1][2], rows[0][2], owner, stable, interrupt,
                              range_proof=(rows, recovery_head, mirror_record))
    except (PipelineStop, QueueStop):
        raise
    except Exception:
        raise PipelineStop("PRIVATE_INTAKE_UNRESOLVED") from None


def _source_scope(source, parent, commit, *, allowed_control=None):
    changes = git(source, "diff-tree", "--no-commit-id", "--name-only", "-r", "-z",
                  parent, commit).stdout.decode("utf-8", "strict").split("\0")[:-1]
    media = [name for name in changes if name != allowed_control]
    need(media and all(name.startswith(EDITABLE) for name in media), "SOURCE_SCOPE")

