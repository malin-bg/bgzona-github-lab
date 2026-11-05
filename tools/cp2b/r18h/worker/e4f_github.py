"""Bounded GitHub transport for the existing D3 NativeAtomicPort.

The transport does not mint a D3 intent or an E4M claim. The coordinator must
supply its sealed intent and durable replay barrier before enabling this port.
"""
import json
import re
import time
from urllib.error import HTTPError
from urllib.request import HTTPRedirectHandler, Request, build_opener

from github_backend import NativeReply, SessionIdentity, oid, sha
from importer import data, digest
from git_transport import MAIN, WITNESS

ASSETS = 'malin-bg/bgzona-assets'
CONTROL = 'malin-bg/bgzona-github-lab'
CONTROL_ID = 1426765395
LEDGER = 'refs/heads/automation-ledger'
ATOMIC = 'mutation($input:UpdateRefsInput!){updateRefs(input:$input){clientMutationId}}'
LEDGER_CAS = ('mutation($input:CreateCommitOnBranchInput!)'
              '{createCommitOnBranch(input:$input){commit{oid}}}')


class ProviderStop(RuntimeError):
    pass


def require(ok, code):
    if not ok:
        raise ProviderStop(code)


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        return None


def github(path, token=None, body=None):
    headers = {'Accept': 'application/vnd.github+json', 'User-Agent': 'bgzona-e4f/1', 'X-GitHub-Api-Version': '2026-03-10'}
    if token:
        headers['Authorization'] = 'Bearer ' + token
    if body is not None:
        headers['Content-Type'] = 'application/json'
    req = Request('https://api.github.com' + path, headers=headers,
                  data=None if body is None else data(body))
    try:
        with build_opener(NoRedirect).open(req, timeout=15) as response:
            raw = response.read(2_000_001)
            require(len(raw) <= 2_000_000, 'E4F_API_LIMIT')
            return response.status, json.loads(raw)
    except HTTPError as e:
        return e.code, None
    except Exception:
        raise ProviderStop('E4F_PROVIDER_UNRESOLVED') from None


class GitHubNativeAtomic:
    """One exact non-forced same-repository pair. No standalone entrypoint."""

    def __init__(self, *, token, assets_id, assets_node, actor, code, ledger_commit,
                 record_sha256, before, after, request=github):
        require(type(token) is str and token and not any(c.isspace() for c in token) and
                type(assets_id) is int and assets_id > 0 and type(actor) is int and actor > 0 and
                re.fullmatch(r'R_[A-Za-z0-9_-]{8,100}', assets_node) and oid(code) and
                oid(ledger_commit) and sha(record_sha256) and
                type(before) is tuple and type(after) is tuple and len(before) == len(after) == 2 and
                all(oid(x) and x != '0' * 40 for x in (*before, *after)) and
                all(a != b for a, b in zip(before, after)), 'E4F_NATIVE_SCOPE')
        self.token, self.assets_id, self.assets_node = token, assets_id, assets_node
        self.actor, self.code, self.ledger_commit = actor, code, ledger_commit
        self.record_sha256, self.before, self.after = record_sha256, before, after
        self.request, self.sent = request, False

    def session(self):
        now = int(time.time())
        return SessionIdentity(self.actor, self.code, frozenset({(self.assets_id, 'atomic_pair')}),
                               now, now + 30, 'HOSTED_E4F_NATIVE')

    def _get(self, repo, suffix):
        status, row = self.request('/repos/' + repo + suffix, self.token if repo == ASSETS else None)
        require(status == 200 and type(row) is dict, 'E4F_NATIVE_READ')
        return row

    def node_id(self, numeric_id):
        row = self._get(ASSETS, '')
        require(numeric_id == self.assets_id and
                (row['full_name'], row['id'], row['node_id'], row['private'], row['archived']) ==
                (ASSETS, self.assets_id, self.assets_node, False, False), 'E4F_NATIVE_IDENTITY')
        return self.assets_node

    def _ref(self, repo, ref):
        row = self._get(repo, '/git/ref/' + ref.removeprefix('refs/'))
        require(row['ref'] == ref and row['object']['type'] == 'commit' and
                oid(row['object']['sha']), 'E4F_NATIVE_REF')
        return row['object']['sha']

    def _ledger(self):
        from base64 import b64decode
        from hashlib import sha1
        repository = self._get(CONTROL, '')
        require((repository['full_name'], repository['id'], repository['private'], repository['archived']) ==
                (CONTROL, CONTROL_ID, False, False), 'E4F_NATIVE_LEDGER_IDENTITY')
        require(self._ref(CONTROL, LEDGER) == self.ledger_commit, 'E4F_NATIVE_LEDGER_MOVED')
        row = self._get(CONTROL, '/contents/anchor.json?ref=' + self.ledger_commit)
        require((row['path'], row['encoding'], row['type']) == ('anchor.json', 'base64', 'file'),
                'E4F_NATIVE_LEDGER_BYTES')
        raw = b64decode(''.join(row['content'].splitlines()), validate=True)
        blob = sha1(f'blob {len(raw)}\0'.encode() + raw).hexdigest()
        require(digest(raw) == self.record_sha256 and row['size'] == len(raw) and row['sha'] == blob,
                'E4F_NATIVE_LEDGER_BYTES')
        record = json.loads(raw)
        authority = record['primary_main']
        require(data(record) == raw and oid(authority) and record['primary_witness'] == self.after[1],
                'E4F_NATIVE_LEDGER_TARGET')
        if authority != self.after[0]:
            self._ancestor(authority, self.after[0])
            require(self._protected(authority) == self._protected(self.after[0]),
                    'E4F_NATIVE_LEDGER_TARGET')

    def _ancestor(self, old, new):
        row = self._get(ASSETS, f'/compare/{old}...{new}')
        require(row['status'] == 'ahead' and row['ahead_by'] > 0 and
                row['base_commit']['sha'] == row['merge_base_commit']['sha'] == old and
                self._get(ASSETS, '/git/commits/' + new)['sha'] == new, 'E4F_NATIVE_NON_FF')

    def _protected(self, commit):
        row = self._get(ASSETS, '/git/commits/' + commit)
        tree = row['tree']['sha']
        require(row['sha'] == commit and oid(tree), 'E4F_NATIVE_PROTECTED')
        row = self._get(ASSETS, '/git/trees/' + tree)
        require(row['sha'] == tree and row['truncated'] is False, 'E4F_NATIVE_PROTECTED')
        paths = {x['path']: x['sha'] for x in row['tree'] if x['path'] in ('automation-state', 'objects')
                 and x['type'] == 'tree' and x['mode'] == '040000' and oid(x['sha'])}
        require(set(paths) == {'automation-state', 'objects'}, 'E4F_NATIVE_PROTECTED')
        return paths

    def update_refs(self, query, variables):
        client = digest(data([self.assets_node, self.before, self.after]))
        expected = {'input': {'repositoryId': self.assets_node, 'clientMutationId': client,
                    'refUpdates': [{'name': ref, 'beforeOid': old, 'afterOid': new, 'force': False}
                                   for ref, old, new in zip((MAIN, WITNESS), self.before, self.after)]}}
        require(not self.sent and query == ATOMIC and variables == expected, 'E4F_NATIVE_MUTATION_SCOPE')
        self.node_id(self.assets_id)
        self._ledger()
        for old, new in zip(self.before, self.after):
            self._ancestor(old, new)
        require(tuple(self._ref(ASSETS, r) for r in (MAIN, WITNESS)) == self.before, 'E4F_NATIVE_PAIR_MOVED')
        require(self._ref(CONTROL, LEDGER) == self.ledger_commit, 'E4F_NATIVE_LEDGER_MOVED')
        self.sent = True
        status, reply = self.request('/graphql', self.token, {'query': ATOMIC, 'variables': expected})
        return NativeReply(status, reply)


def ledger_cas_payload(tip, previous, record):
    """Prepare only the D2 ledger append for createCommitOnBranch CAS."""
    from base64 import b64encode
    from types import SimpleNamespace
    from authority_coordinator import LocalWitness
    scope = SimpleNamespace(library_id=previous['library_id'])
    try:
        LocalWitness.validate_record(scope, previous)
        LocalWitness.validate_record(scope, record)
    except (RuntimeError, TypeError, ValueError, KeyError):
        raise ProviderStop('E4F_LEDGER_RECORD') from None
    require(oid(tip) and type(previous['index']) is int and
            record['index'] == previous['index'] + 1 and
            record['previous'] == digest(data(previous)) and
            record['library_id'] == previous['library_id'] and
            record['registry_seq'] > previous['registry_seq'] and
            all(record['counters'][k] >= v for k, v in previous['counters'].items()),
            'E4F_LEDGER_NOT_SUCCESSOR')
    raw = b64encode(data(record)).decode('ascii')
    return {'query': LEDGER_CAS, 'variables': {'input': {
        'branch': {'repositoryNameWithOwner': CONTROL, 'branchName': 'automation-ledger'},
        'expectedHeadOid': tip, 'message': {'headline': 'Advance independent D3 ledger'},
        'fileChanges': {'additions': [{'path': 'anchor.json', 'contents': raw},
            {'path': f'records/{record["index"]:08d}.json', 'contents': raw}]}}}}



# The public worker sees only a fresh, opaque authorization from a pinned
# PRIVATE attester. It never opens the PRIVATE recovery bundle.
PRIVATE = 'malin-bg/bgzona-private-intake'
PRIVATE_ID = 1429436685
GUARD_WORKFLOW_PATH = '.github/workflows/e4f-d3-guard.yml'
GUARD_BRANCH = 'main'
GRANT_JOB = 'guard'
GRANT_STEP = 'CORE-GUARD fresh reservation'
OBSERVE_STEP = 'CORE-GUARD verified existing reservation'


class PrivateGuardAttester:
    """Use the existing E4F R App (PRIVATE Contents read, Actions write)."""

    def __init__(self, token, workflow_id, workflow_commit, actor_id, control_run,
                 *, request=github, clock=time.monotonic, sleep=time.sleep,
                 observe_workflow_id=None):
        require(observe_workflow_id is None or type(observe_workflow_id) is int and observe_workflow_id>0,
                'E4F_GUARD_SCOPE')
        self.observe_workflow_id=observe_workflow_id
        require(type(token) is str and token and not any(c.isspace() for c in token) and
                oid(workflow_commit) and
                all(type(n) is int and n > 0 for n in (workflow_id, actor_id, control_run)),
                'E4F_GUARD_SCOPE')
        self.token, self.workflow_id, self.commit = token, workflow_id, workflow_commit
        self.actor, self.control_run = actor_id, control_run
        self.request, self.clock, self.sleep = request, clock, sleep

    def attest(self, mode, tip, record_sha):
        from secrets import token_hex
        require(mode in ('reserve', 'observe') and oid(tip) and sha(record_sha),
                'E4F_GUARD_SCOPE')
        challenge = token_hex(32)
        inputs = {'mode': mode, 'ledger_parent': tip, 'record_sha256': record_sha,
                  'control_run': str(self.control_run), 'challenge': challenge}
        proof = digest(data(inputs))
        inputs['proof'] = proof
        observe_split = mode=='observe' and self.observe_workflow_id is not None
        workflow_id = self.observe_workflow_id if observe_split else self.workflow_id
        workflow_path = GUARD_WORKFLOW_PATH.replace('.yml','-observe.yml') if observe_split else GUARD_WORKFLOW_PATH
        root = '/repos/' + PRIVATE
        status, result = self.request(root + f'/actions/workflows/{workflow_id}/dispatches',
                                      self.token, {'ref': GUARD_BRANCH, 'inputs': inputs,
                                                   'return_run_details': True})
        require(status == 200 and type(result) is dict and
                type(result.get('workflow_run_id')) is int and result['workflow_run_id'] > 0,
                'E4F_GUARD_DISPATCH_UNRESOLVED')
        run_id = result['workflow_run_id']
        deadline = self.clock() + 120
        while True:
            status, row = self.request(root + f'/actions/runs/{run_id}', self.token)
            require(status in (200, 404), 'E4F_GUARD_RUN_UNRESOLVED')
            if status == 200:
                require((row['id'], row['run_attempt'], row['workflow_id'], row['head_sha'],
                         row['head_branch'], row['path'].split('@')[0], row['event'],
                         row['repository']['id'], row['actor']['id'], row['display_title']) ==
                        (run_id, 1, workflow_id, self.commit, GUARD_BRANCH, workflow_path,
                         'workflow_dispatch', PRIVATE_ID, self.actor, 'E4F D3 guard ' + proof),
                        'E4F_GUARD_RUN_IDENTITY')
                if row['status'] == 'completed':
                    require(row['conclusion'] == 'success', 'E4F_GUARD_RUN_STOP')
                    break
            require(self.clock() < deadline, 'E4F_GUARD_RUN_UNRESOLVED')
            self.sleep(2)
        status, jobs = self.request(root + f'/actions/runs/{run_id}/attempts/1/jobs', self.token)
        require(status == 200 and jobs['total_count'] == len(jobs['jobs']) == 1,
                'E4F_GUARD_JOB_IDENTITY')
        job, = jobs['jobs']
        require((job['run_id'], job['run_attempt'], job['name'], job['conclusion']) ==
                (run_id, 1, GRANT_JOB, 'success'), 'E4F_GUARD_JOB_IDENTITY')
        evidence = [step['name'] for step in job['steps'] if step['conclusion'] == 'success' and
                    step['name'] in (GRANT_STEP, OBSERVE_STEP)]
        require(len(evidence) == 1 and (mode == 'reserve' or evidence == [OBSERVE_STEP]),
                'E4F_GUARD_JOB_EVIDENCE')
        return 'fresh' if evidence == [GRANT_STEP] else 'existing'


class GitHubLedgerAppend:
    """One verified D2 append with a durable E4M fence in the PRIVATE repo."""

    def __init__(self, token, attester, *, request=github):
        require((token is None or (type(token) is str and token and not any(c.isspace() for c in token))) and
                type(attester) is PrivateGuardAttester, 'E4F_LEDGER_SCOPE')
        self.token, self.attester, self.request = token, attester, request

    def _get(self, suffix):
        status, row = self.request('/repos/' + CONTROL + suffix)
        require(status == 200 and type(row) is dict, 'E4F_LEDGER_READ')
        return row

    def _head(self):
        row = self._get('/git/ref/heads/automation-ledger')
        require(row['ref'] == LEDGER and row['object']['type'] == 'commit' and
                oid(row['object']['sha']), 'E4F_LEDGER_REF')
        return row['object']['sha']

    def _tree(self, commit):
        row = self._get('/git/commits/' + commit)
        tree = row['tree']['sha']
        require(row['sha'] == commit and oid(tree) and len(row['parents']) <= 1 and
                all(oid(p['sha']) for p in row['parents']), 'E4F_LEDGER_COMMIT')
        listing = self._get('/git/trees/' + tree + '?recursive=1')
        require(listing['sha'] == tree and listing['truncated'] is False and
                len(listing['tree']) <= 258, 'E4F_LEDGER_TREE')
        files = {}
        for item in listing['tree']:
            path = item['path']
            if path == 'records':
                require((item['type'], item['mode']) == ('tree', '040000'), 'E4F_LEDGER_TREE')
                continue
            require((item['type'], item['mode']) == ('blob', '100644') and oid(item['sha']) and
                    (path == 'anchor.json' or re.fullmatch(r'records/[0-9]{8}\.json', path)) and
                    path not in files, 'E4F_LEDGER_TREE')
            files[path] = item['sha']
        return tuple(p['sha'] for p in row['parents']), files

    def _record(self, blob_id):
        from base64 import b64decode
        from hashlib import sha1
        row = self._get('/git/blobs/' + blob_id)
        require(row['sha'] == blob_id and row['encoding'] == 'base64' and
                type(row['size']) is int and 0 < row['size'] <= 65_536, 'E4F_LEDGER_BLOB')
        raw = b64decode(''.join(row['content'].splitlines()), validate=True)
        require(len(raw) == row['size'] and
                sha1(f'blob {len(raw)}\0'.encode() + raw).hexdigest() == blob_id, 'E4F_LEDGER_BLOB')
        record = json.loads(raw)
        require(data(record) == raw, 'E4F_LEDGER_BYTES')
        return record

    def observe(self, tip, previous, record):
        """Find the exact append even if a later verified successor is present."""
        ledger_cas_payload(tip, previous, record)
        repo = self._get('')
        require((repo['full_name'], repo['id'], repo['private'], repo['archived']) ==
                (CONTROL, CONTROL_ID, False, False), 'E4F_LEDGER_IDENTITY')
        head = self._head()
        chain = []
        here = head
        while here != tip:
            require(len(chain) < 32, 'E4F_LEDGER_ANCESTRY')
            parents, paths = self._tree(here)
            require(len(parents) == 1, 'E4F_LEDGER_ANCESTRY')
            chain.append((here, paths))
            here = parents[0]
        _, prior_files = self._tree(tip)
        index = previous['index']
        require(0 <= index < 256 and set(prior_files) ==
                {'anchor.json', *(f'records/{i:08d}.json' for i in range(index + 1))} and
                prior_files.get('anchor.json') == prior_files.get(f'records/{index:08d}.json') and
                self._record(prior_files['anchor.json']) == previous, 'E4F_LEDGER_PREDECESSOR')
        current = previous
        applied = None
        for commit, files in reversed(chain):
            path = f'records/{current["index"] + 1:08d}.json'
            require(set(files) == set(prior_files) | {path} and files['anchor.json'] == files[path] and
                    all(files[k] == v for k, v in prior_files.items() if k != 'anchor.json'),
                    'E4F_LEDGER_NOT_EXACT_APPEND')
            successor = self._record(files[path])
            ledger_cas_payload(tip, current, successor)
            if applied is None:
                require(successor == record, 'E4F_LEDGER_CONFLICT')
                applied = commit
            prior_files, current = files, successor
        require(self._head() == head, 'E4F_LEDGER_MOVED')
        return applied

    def recover(self, tip, previous, record):
        """An independent reader only observes; it cannot authorize another append."""
        require(self.token is None, 'E4F_LEDGER_SCOPE')
        ledger_cas_payload(tip, previous, record)
        try:
            permit = self.attester.attest('observe', tip, digest(data(record)))
            seen = self.observe(tip, previous, record)
            require(permit == 'existing' and seen is not None, 'E4F_LEDGER_UNRESOLVED')
        except Exception:
            raise ProviderStop('E4F_LEDGER_UNRESOLVED') from None
        return 'OBSERVED', seen

    def append(self, tip, previous, record):
        """A fresh attester invocation is the only route to a POST."""
        require(self.token is not None, 'E4F_LEDGER_SCOPE')
        payload = ledger_cas_payload(tip, previous, record)
        seen = self.observe(tip, previous, record)
        record_sha = digest(data(record))
        permit = self.attester.attest('observe' if seen else 'reserve', tip, record_sha)
        if seen:
            require(permit == 'existing' and self.observe(tip, previous, record) == seen,
                    'E4F_LEDGER_UNRESOLVED')
            return 'OBSERVED', seen
        if permit != 'fresh':
            seen = self.observe(tip, previous, record)
            require(seen is not None, 'E4F_LEDGER_UNRESOLVED')
            return 'OBSERVED', seen
        require(self.observe(tip, previous, record) is None, 'E4F_LEDGER_MOVED')
        reply = None
        try:
            status, response = self.request('/graphql', self.token, payload)
            if status == 200 and set(response) == {'data'}:
                answer = response['data']['createCommitOnBranch']['commit']['oid']
                if oid(answer):
                    reply = answer
        except Exception:
            pass  # An HTTP outcome is never authorization to repeat the CAS.
        try:
            seen = self.observe(tip, previous, record)
            require(seen is not None and (reply is None or reply == seen), 'E4F_LEDGER_UNRESOLVED')
        except Exception:
            raise ProviderStop('E4F_LEDGER_UNRESOLVED') from None
        return ('APPENDED' if reply == seen else 'OBSERVED'), seen



def attest_private_guard(guard, mode, domain, context, operation, record):
    """Reserve through the existing E4M guard, or read an exact durable marker.

    The PRIVATE caller supplies the independently recovered, verified D3
    context. Only a successful new reservation grants an append.
    """
    from authority_coordinator import LocalAppendGuard
    require(type(guard) is LocalAppendGuard and mode in ('reserve', 'observe'),
            'E4F_GUARD_SCOPE')
    guard.validate()
    recovery = guard.recovery
    require(mode != 'observe' or not recovery.writable, 'E4F_GUARD_SCOPE')
    ledger_cas_payload(context['tip'], context['latest'], record)
    require(sha(operation.request) and oid(operation.change['commit']), 'E4F_GUARD_OPERATION')
    scope, base, marker = guard.marker(domain, context, operation, record)
    path = f'private-recovery/{scope}/{base}/checkpoint.json'
    pinned = recovery.read(recovery.expected_head)
    stored = pinned['files'].get(path)
    if stored is not None:
        require(stored == marker, 'E4F_GUARD_MARKER')
        return 'existing'
    require(mode == 'reserve' and recovery.writable, 'E4F_GUARD_UNRESOLVED')
    guard.reserve(domain, context, operation, record)
    return 'fresh'
