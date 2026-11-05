"""PRIVATE-only verifier for a scoped R18H E4M reservation."""
from pathlib import Path
import time

import github_backend as gb
from authority_coordinator import LocalAppendGuard, LocalWitness
from e4f_github import ProviderStop, attest_private_guard, ledger_cas_payload, require
from git_transport import MAIN, git, value
from importer import data, digest
from intake_plan import SourceEvidence, change_digest
from private_queue import TrustedRegistration
from queued_pipeline import verify_handoff
from request_executor import RequestOperation


def check_recovery(recovery, snapshot, handoff_head, handoff_parent, scope):
    """Check exact E4M ancestry and append-only contents from the trusted pins."""
    pin = recovery.port.standard_handoff
    rid = recovery.repository.numeric_id
    current = snapshot
    prefix = 'private-recovery/' + scope + '/'
    for _ in range(256):
        at_handoff = current['head'] == handoff_head
        require(len(current['parents']) == 1, 'E4F_GUARD_RECOVERY_ANCESTRY')
        parent, = current['parents']
        older = recovery.port.snapshot(rid, parent)
        require(set(older['files']) < set(current['files']) and
                len(current['files']) == len(older['files']) + 1 and
                all(current['files'][k] == v for k, v in older['files'].items()),
                'E4F_GUARD_RECOVERY_ANCESTRY')
        path, = current['files'].keys() - older['files'].keys()
        if at_handoff:
            require(parent == handoff_parent and path == pin.path,
                    'E4F_GUARD_HANDOFF_ANCESTRY')
            return
        row = gb.parse(current['files'][path])
        require(set(row) == {'version', 'base', 'index', 'request', 'source', 'record'} and
                type(row['version']) is int and row['version'] == 1 and
                type(row['index']) is int and row['index'] > 0 and
                all(gb.sha(row[k]) for k in ('base', 'request', 'record')) and
                gb.oid(row['source']) and data(row) == current['files'][path] and
                path == prefix + row['base'] + '/checkpoint.json',
                'E4F_GUARD_RECOVERY_ANCESTRY')
        current = older
    require(False, 'E4F_GUARD_RECOVERY_ANCESTRY')


def attest(mode, **trusted_inputs):
    """Return only the scoped verdict or a safe diagnostic code."""
    try:
        return _attest(mode, **trusted_inputs)
    except ProviderStop:
        raise
    except Exception:
        raise ProviderStop('E4F_STANDARD_UNRESOLVED') from None


def _attest(mode, *, guard, registration, domain, destination, record,
           ledger_parent, record_sha256, registration_head, handoff_head, handoff_parent):
    """Independently verify the PRIVATE handoff and the public D3 candidate.

    Setup supplies pins from the trusted workflow and verified provider state,
    never from the artifact or from the handoff under verification.
    """
    require(type(guard) is LocalAppendGuard and guard.hosted_standard and
            type(registration) is TrustedRegistration and not registration.recovery.writable and
            type(domain) is LocalWitness and registration.queue.lab.root == domain.lab.root and
            all(gb.oid(x) for x in (ledger_parent, registration_head, handoff_head, handoff_parent)) and
            gb.sha(record_sha256) and digest(data(record)) == record_sha256,
            'E4F_GUARD_SCOPE')
    require(mode in ('reserve', 'observe') and guard.recovery.writable == (mode == 'reserve'),
            'E4F_GUARD_SCOPE')
    guard.validate()
    recovery = guard.recovery
    pin = recovery.port.standard_handoff
    seen_registration, rows, _, _, _, _ = registration._read()
    require(seen_registration == registration_head and rows and rows[0][1] == pin.request,
            'E4F_GUARD_REGISTRATION')
    context = domain.read()
    index = record['index']
    require(type(index) is int and 0 < index <= len(context['records']),
            'E4F_GUARD_RECORD')
    prefix = context['records'][:index]
    parents = value(context['repo'], 'rev-list', '--reverse', context['tip']).splitlines()
    require(len(parents) == len(context['records']) and parents[index - 1] == ledger_parent and
            (index == len(parents) or context['records'][index] == record),
            'E4F_GUARD_LEDGER_PARENT')
    previous = prefix[-1]
    ledger_cas_payload(ledger_parent, previous, record)
    mirror, = [r for r in prefix if digest(data(r)) == pin.mirror]
    snapshot = recovery.read(recovery.expected_head)
    check_recovery(recovery, snapshot, handoff_head, handoff_parent, guard.scope(domain))
    payload = snapshot['files'][pin.path]
    require(len(payload) == pin.size and digest(payload) == pin.sha256, 'E4F_GUARD_HANDOFF')
    mirror_context = dict(context, latest=mirror)
    restored = verify_handoff(payload, registration, rows[0], domain, mirror_context,
                              Path(destination), check_proofs=True)

    packet = gb.parse(payload)
    before, change = packet['before'], packet['change']
    git(restored, 'update-ref', MAIN, change['commit'])
    repo = registration.queue.repository
    evidence = SourceEvidence(repo.numeric_id, repo.name, digest(data(before)), change_digest(change),
                              pin.mirror, int(time.time()), 'private')
    operation = RequestOperation(domain.lab, restored, before, change, evidence, lambda _: None)
    require(operation.request == packet['binding']['derived_request_sha256'],
            'E4F_GUARD_REQUEST')
    prior = dict(context, latest=previous, records=prefix, tip=ledger_parent)
    require(domain.verify_backup(prior, record)['recoverable'] is True, 'E4F_GUARD_BACKUP')
    operation.bind(dict(prior, latest=record, records=prefix + [record]))
    require(value(domain.anchor, 'rev-parse', MAIN) == context['tip'] and
            registration.provider.recovery_head() == seen_registration and
            recovery.read(recovery.expected_head) == snapshot, 'E4F_GUARD_STATE_MOVED')
    return attest_private_guard(guard, mode, domain, prior, operation, record)
