"""Inactive preparation boundary from the durable PRIVATE queue to E4N R2."""
import github_backend as gb
import pipeline
from authority_coordinator import LocalWitness
from importer import data, digest
from private_queue import PrivateQueue, QueueStop, request_identity


def prepare_next(queue, source, domain, *, mirror_record_sha256):
    return _prepare_next(queue, source, domain, mirror_record_sha256=mirror_record_sha256)


def _prepare_next(queue, source, domain, *, mirror_record_sha256, _registered_event=None, _registration=None):
    """Prepare the oldest pending row against the caller's trusted D3 pin.

    Preparation leaves the row pending. The returned capability is PRIVATE,
    and separate execution/acceptance gates are still required.
    """
    if type(queue) is not PrivateQueue or type(domain) is not LocalWitness or domain.lab.root != queue.lab.root:
        raise QueueStop("QUEUED_PIPELINE_SCOPE")
    if not gb.sha(mirror_record_sha256):
        raise QueueStop("MIRROR_PIN_REQUIRED")
    snapshot = queue.snapshot()
    rows = snapshot["requests"]
    if not rows:
        raise QueueStop("QUEUE_EMPTY")
    row = rows[0]
    if row["state"] != "pending":
        raise QueueStop("QUEUE_HEAD_CONFLICT")
    event = _registered_event or pipeline.PushEvent(queue.repository.numeric_id, queue.repository.name,
                               queue.owner_actor_id, row["parent"], row["source"])
    identity = request_identity(event.repository_id, event.parent, event.commit)
    if identity != row["request"]:
        raise QueueStop("QUEUED_SOURCE_IDENTITY")
    try:
        record = domain.read()["latest"]
        record_sha = digest(data(record))
    except Exception:
        raise QueueStop("MIRROR_UNVERIFIED") from None
    if record_sha != mirror_record_sha256:
        raise QueueStop("MIRROR_PIN_MISMATCH")

    prepared = pipeline.prepare(queue.lab, source, event, backend=queue.backend,
                                owner_actor_id=queue.owner_actor_id, domain=domain, queued=queue,
                                 registration=_registration)
    binding = prepared.binding
    if (binding.owner_request_sha256, binding.raw_parent, binding.raw_source) != (
            identity, event.parent, event.commit):
        raise QueueStop("QUEUED_SOURCE_IDENTITY")
    before = prepared.operation.before
    if (before["ledger_record_sha256"], before["authority_commit"], before["witness_commit"]) != (
            mirror_record_sha256, record["primary_main"], record["primary_witness"]):
        raise QueueStop("MIRROR_PIN_MISMATCH")
    current = queue.snapshot()
    if current["requests"][:1] != [row] or current["acceptance_cursor"] != snapshot["acceptance_cursor"]:
        raise QueueStop("QUEUE_HEAD_MOVED")
    return prepared


def prepare_registered_next(registration, source, domain, *, mirror_record_sha256):
    """Prepare the proven FIFO head. This seam grants no D3 acceptance."""
    from private_queue import TrustedRegistration

    if type(registration) is not TrustedRegistration:
        raise QueueStop("REGISTRATION_TRUST_SCOPE")
    proven = registration.verify()
    if not proven:
        raise QueueStop("QUEUE_EMPTY")
    queue = registration.queue
    _, identity, parent, commit = proven[0]
    event = pipeline.PushEvent(queue.repository.numeric_id, queue.repository.name,
                               queue.owner_actor_id, parent, commit, ref=queue.source_ref)
    prepared = _prepare_next(queue, source, domain, mirror_record_sha256=mirror_record_sha256,
                            _registered_event=event,
                             _registration=registration if registration.cp2b_dynamic or registration.standard else None)
    if registration.verify() != proven or prepared.binding.owner_request_sha256 != identity:
        raise QueueStop("REGISTRATION_QUEUE_MISMATCH")
    return prepared



def prepare_registered_range(registration, source, domain, *, mirror_record_sha256,
                             interrupt=lambda point: None):
    """Prepare exactly two proven pending rows, without authority or queue writes."""
    return pipeline._prepare_range(registration, source, domain, mirror_record_sha256, interrupt)


def stage_registered_next(registration, source, domain, *, primary, mirror_record_sha256,
                          append_guard, reserve=True, recoverable=False):
    """Local standard FIFO checkpoint. It cannot reconcile or append to D3."""
    from authority_coordinator import Coordinator, LocalAppendGuard
    from git_transport import MAIN, value
    from private_queue import TrustedRegistration

    if (type(registration) is not TrustedRegistration or type(append_guard) is not LocalAppendGuard
            or append_guard.hosted_probe or append_guard.hosted_standard or type(reserve) is not bool
            or type(recoverable) is not bool or (recoverable and not reserve)
            or type(domain) is not LocalWitness or domain.lab.root != registration.queue.lab.root):
        raise QueueStop("GUARDED_INTAKE_SCOPE")
    queue = registration.queue
    recovery = append_guard.recovery
    if (recovery.port is not registration.recovery.port or recovery.repository != queue.repository
            or recovery.expected_head != registration.provider.recovery_head()):
        raise QueueStop("GUARD_RECOVERY_PIN_MISMATCH")

    context = domain.read()
    if digest(data(context["latest"])) != mirror_record_sha256:
        raise QueueStop("MIRROR_PIN_MISMATCH")
    append_guard.check(domain, context)
    rows = tuple(registration.verify())
    prepared = prepare_registered_next(registration, source, domain,
                                       mirror_record_sha256=mirror_record_sha256)

    def recheck(point):
        if point != "after-backup-before-acceptance":
            return
        if (tuple(registration.verify()) != rows or
                registration.provider.recovery_head() != recovery.expected_head or
                queue.backend._ref("intake", MAIN) != prepared.binding.raw_source or
                value(queue.lab.remote(source), "rev-parse", MAIN) != prepared.binding.raw_source):
            raise QueueStop("GUARDED_INTAKE_STATE_MOVED")

    recheck("after-backup-before-acceptance")
    if recoverable:
        _persist_handoff(registration, recovery, prepared, mirror_record_sha256)
        recheck("after-backup-before-acceptance")
    if not reserve:
        current = domain.read()
        if current["tip"] != context["tip"] or current["latest"] != context["latest"]:
            raise QueueStop("GUARDED_INTAKE_STATE_MOVED")
        append_guard.check(domain, current)
        return prepared, None
    coordinator = Coordinator(domain.lab, primary, domain.anchor, domain.archives,
                              domain.genesis, domain.library_id, domain.primary_genesis, recheck,
                              operation=prepared.operation, append_guard=append_guard)
    return prepared, coordinator.run(stop_before_append=True)



_HANDOFF_FORMAT = "bgzona-standard-handoff-r1"


def _handoff_slot(request, mirror):
    scope = digest(data([_HANDOFF_FORMAT, request]))
    return scope, "private-recovery/" + scope + "/" + mirror + "/checkpoint.json"


def _handoff_payload(binding, before, change):
    from base64 import b64encode
    from dataclasses import asdict
    from git_transport import git

    repo = binding.derived_repo
    pack = git(repo, "pack-objects", "--stdout", "--revs",
               stdin=(binding.derived_source + "\n").encode("ascii")).stdout
    fields = {key: value for key, value in asdict(binding).items() if key != "derived_repo"}
    return data(dict(format=_HANDOFF_FORMAT, binding=fields, before=before,
                     change=change, pack=b64encode(pack).decode("ascii")))


def _persist_handoff(registration, recovery, prepared, mirror):
    payload = _handoff_payload(prepared.binding, prepared.operation.before, prepared.operation.change)
    scope, path = _handoff_slot(prepared.binding.owner_request_sha256, mirror)
    try:
        pin = registration.provider.recovery_head()
        snapshot = recovery.read(pin)
        if pin != recovery.expected_head or path in snapshot["files"]:
            raise QueueStop("HANDOFF_RECOVERY_CONFLICT")
        gb.recovery_tree(snapshot["files"] | {path: payload})
        saved = recovery.persist(scope, mirror, payload)
        if registration.provider.recovery_head() != saved or recovery.read(saved)["files"].get(path) != payload:
            raise QueueStop("HANDOFF_READBACK")
    except gb.RecoveryStop as exc:
        raise QueueStop(str(exc)) from None


def restore_registered_handoff(registration, domain, *, append_guard, mirror_record_sha256,
                               destination):
    """Inspect a stopped attempt in a clean Lab. Returns bytes, never send rights."""
    from authority_coordinator import LocalAppendGuard, PortStop

    def check(condition, code):
        if not condition:
            raise QueueStop(code)

    try:
        check(type(registration) is TrustedRegistration and type(append_guard) is LocalAppendGuard
              and not append_guard.hosted_probe and not append_guard.hosted_standard
              and type(domain) is LocalWitness
              and domain.lab.root == registration.queue.lab.root, "HANDOFF_SCOPE")
        recovery = append_guard.recovery
        check(not recovery.writable and recovery.port is registration.recovery.port
              and recovery.repository == registration.queue.repository, "HANDOFF_SCOPE")
        pin, rows, _, _, _, _ = registration._read()
        check(rows and pin == recovery.expected_head, "HANDOFF_RECOVERY_PIN")
        _, request, parent, source = rows[0]
        context = domain.read()
        mirror = mirror_record_sha256
        check(gb.sha(mirror) and digest(data(context["latest"])) == mirror, "MIRROR_PIN_MISMATCH")
        snapshot = recovery.read(pin)
        _, path = _handoff_slot(request, mirror)
        payload = snapshot["files"].get(path)
        check(type(payload) is bytes, "HANDOFF_MISSING")
        binding = gb.parse(payload)["binding"]
        scope = append_guard.scope(domain)
        marker = snapshot["files"].get("private-recovery/" + scope + "/" + mirror + "/checkpoint.json")
        check(type(marker) is bytes, "HANDOFF_GUARD_MISSING")
        row = gb.parse(marker)
        check(row["request"] == binding["derived_request_sha256"] and row["source"] == binding["derived_source"]
              and row["index"] == context["latest"]["index"] + 1, "HANDOFF_GUARD_MISMATCH")
        try:
            append_guard.check(domain, context)
        except PortStop as exc:
            check(str(exc) == "APPEND_OUTCOME_UNKNOWN", "HANDOFF_GUARD_UNVERIFIED")
        else:
            raise QueueStop("HANDOFF_REPLAY_NOT_BLOCKED")

        repo = verify_handoff(payload, registration, rows[0], domain, context, destination)
        check(registration.provider.recovery_head() == pin and recovery.read(pin) == snapshot
              and domain.read()["tip"] == context["tip"], "HANDOFF_STATE_MOVED")
        return payload, repo
    except QueueStop:
        raise
    except Exception:
        raise QueueStop("HANDOFF_UNRESOLVED") from None


def verify_handoff(payload, registration, registered_row, domain, context, destination, *, check_proofs=False):
    """Verify the existing packet against independently authenticated source and authority."""
    from base64 import b64decode
    from pathlib import Path, PurePosixPath
    from request_executor import EDITABLE
    from intake_plan import change_digest
    import privacy_gate
    from git_transport import files, git, value

    def check(condition, code):
        if not condition:
            raise QueueStop(code)

    _, request, parent, source = registered_row
    mirror = digest(data(context["latest"]))
    packet = gb.parse(payload)
    check(type(packet) is dict and set(packet) == {"format", "binding", "before", "change", "pack"}
          and data(packet) == payload and packet["format"] == _HANDOFF_FORMAT, "HANDOFF_FORMAT")
    binding = packet["binding"]
    check((binding["owner_request_sha256"], binding["raw_parent"], binding["raw_source"])
          == (request, parent, source), "HANDOFF_SOURCE")
    derived, derived_parent = binding["derived_source"], binding["derived_parent"]
    check(gb.oid(derived) and gb.oid(derived_parent) and gb.sha(binding["derived_request_sha256"])
          and packet["before"]["commit"] == packet["change"]["parent"] == derived_parent
          and packet["change"]["commit"] == derived
          and packet["before"]["ledger_record_sha256"] == mirror, "HANDOFF_BINDING")
    repo = domain.lab.path(Path(destination))
    repo.mkdir()
    git(repo, "init", "--bare", "--initial-branch=main")
    git(repo, "index-pack", "--stdin", "--strict", stdin=b64decode(packet["pack"], validate=True))
    reachable = {line.split()[0] for line in value(repo, "rev-list", "--objects", derived).splitlines()}
    stored = set(value(repo, "cat-file", "--batch-all-objects",
                       "--batch-check=%(objectname)").splitlines())
    check(reachable == stored and len(stored) <= 4096, "HANDOFF_OBJECT_SET")
    check(value(repo, "show", "-s", "--format=%P", derived) == derived_parent,
          "HANDOFF_DERIVED_PARENT")
    old, new = files(repo, derived_parent, EDITABLE), files(repo, derived, EDITABLE)
    check({name: digest(raw) for name, raw in old.items()} == packet["before"]["files"]
          and {name: digest(raw) for name, raw in new.items()} == packet["change"]["files"],
          "HANDOFF_DERIVED_BYTES")
    accepted = files(context["recovered"], context["latest"]["primary_main"])
    baseline = {name: raw for name, raw in accepted.items() if name.startswith(EDITABLE)}
    check(old == baseline, "HANDOFF_EDITING_MIRROR")
    repository = registration.queue.repository
    identity = dict(id=repository.numeric_id, name=repository.name)
    before = dict(schema=1, repository=identity, commit=derived_parent,
                  authority_commit=context["latest"]["primary_main"],
                  witness_commit=context["latest"]["primary_witness"], ledger_record_sha256=mirror,
                  registry=gb.parse(accepted["automation-state/registry.json"]),
                  files={name: digest(raw) for name, raw in old.items()})
    operations = []
    for name in sorted(set(old) | set(new)):
        a, b = old.get(name), new.get(name)
        if a != b:
            operations.append(dict(op="add" if a is None else "delete" if b is None else "modify",
                                   path=name, before_sha256=None if a is None else digest(a),
                                   after_sha256=None if b is None else digest(b)))
    change = dict(schema=1, repository=identity, parent=derived_parent, commit=derived,
                  before_state_sha256=digest(data(before)), operations=operations,
                  files={name: digest(raw) for name, raw in new.items()})
    check(data(before) == data(packet["before"]) and data(change) == data(packet["change"])
          and binding["derived_request_sha256"] == digest(data(dict(
              schema=1, repository=identity, before_sha256=digest(data(before)),
              change_sha256=change_digest(change)))), "HANDOFF_REQUEST")
    changed = {name for name, raw in new.items() if old.get(name) != raw}
    check(len(binding["files"]) == len(changed)
          and {entry["path"] for entry in binding["files"]} == changed, "HANDOFF_DERIVED_FILES")
    for entry in binding["files"]:
        raw_oid, raw = registration.queue.source_reader._path("intake", source, entry["private_path"])
        output = new[entry["path"]]
        role = entry["role"]
        if role == "txt":
            check(raw == output and entry["proof_sha256"] is None, "HANDOFF_TXT")
        else:
            check(role in {"original", "og"} and gb.sha(entry["proof_sha256"]),
                  "HANDOFF_PRIVACY_ROLE")
            result = privacy_gate.prepare(raw, PurePosixPath(entry["private_path"]).suffix, role)
            check(result.evidence["status"] == "SANITIZED_AND_VERIFIED"
                  and result.output == output, "HANDOFF_PRIVACY")
        oid = value(repo, "hash-object", "--stdin", stdin=output)
        if check_proofs and role != "txt":
            from dataclasses import asdict
            policy = registration.queue.backend.policy
            proof = gb.Derivation(request, source, gb.ObjectPin(raw_oid, digest(raw), len(raw)),
                                  gb.ObjectPin(oid, digest(output), len(output)), role,
                                  privacy_gate.POLICY, "SANITIZED_AND_VERIFIED",
                                  dict(policy.actors)["independent_verify"], policy.verifier_code_oid)
            check(entry["proof_sha256"] == digest(data(asdict(proof))), "HANDOFF_PRIVACY_PROOF")
        check(entry["raw"] == dict(oid=raw_oid, sha256=digest(raw), size=len(raw))
              and entry["output"] == dict(oid=oid, sha256=digest(output), size=len(output)),
              "HANDOFF_DERIVED_BINDING")
    return repo
