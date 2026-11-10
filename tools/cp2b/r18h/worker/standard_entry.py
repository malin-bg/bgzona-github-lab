"""R18H first DEV source/registration entrypoint. It has no public write token."""
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
import base64
import json
import os
import subprocess
import sys

from e4f_github import ProviderStop, github, require
from git_transport import Lab, MAIN, files, git, value
from importer import data, digest, parse
import github_backend as gb
import push_artifact as artifact
import push_probe as proof
from media_preflight import GitReader, media_snapshot, verify_delta
from private_queue import PrivateQueue, TrustedRegistration
from queued_pipeline import prepare_registered_next, _handoff_payload
from registration_entry import read_archive
from request_executor import EDITABLE

HERE = Path(__file__).resolve().parent
PIN = gb.RepoPin('intake', gb.CP2B_REPO_ID, *gb.NAMES['intake'])
SOURCE_REF = gb.STANDARD_SOURCE_REF
PATH = artifact.WORKFLOW_PATH
BOOTSTRAP = HERE / 'bootstrap-pins.json'


def fetch_source(folder, token):
    """Fetch only the isolated PRIVATE ref to a new bare, untrusted-object store."""
    folder.mkdir()
    git(folder, 'init', '--bare', '--initial-branch=main')
    env = {k: v for k, v in os.environ.items() if not k.startswith('GIT_')}
    encoded = base64.b64encode(('x-access-token:' + token).encode()).decode()
    env.update(GIT_CONFIG_NOSYSTEM='1', GIT_CONFIG_GLOBAL=os.devnull, GIT_TERMINAL_PROMPT='0',
               GIT_CONFIG_COUNT='1', GIT_CONFIG_KEY_0='http.https://github.com/.extraheader',
               GIT_CONFIG_VALUE_0='AUTHORIZATION: basic ' + encoded)
    args = ['git', '-C', str(folder), '-c', 'http.followRedirects=false', '-c',
            'protocol.allow=never', '-c', 'protocol.https.allow=always', 'fetch',
            '--no-tags', '--no-recurse-submodules', 'https://github.com/' + PIN.name + '.git',
            SOURCE_REF + ':' + SOURCE_REF]
    run = subprocess.run(args, env=env, capture_output=True, timeout=90)
    require(run.returncode == 0, 'STANDARD_SOURCE_FETCH')


class SourceReader(GitReader):
    def __init__(self, repo, api, before, after):
        super().__init__(lambda *args: git(repo, *args).stdout, before, after, dynamic=True)
        self.api, self.repo = api, repo
        self.allow_history(after, before)

    def _ref(self, role, ref):
        require((role, ref)==('intake', SOURCE_REF), 'STANDARD_SOURCE_SCOPE')
        row, _ = self.api.get('/git/ref/' + SOURCE_REF.removeprefix('refs/'))
        require(row['ref']==SOURCE_REF and row['object']['type']=='commit' and
                gb.oid(row['object']['sha']), 'STANDARD_SOURCE_REF')
        return row['object']['sha']


def c3_source_pins(api, repo, pins, source):
    c3 = pins.get('c3')
    if c3 is None:
        return pins
    parent = value(repo, 'show', '-s', '--format=%P', source)
    require(gb.oid(parent), 'C3_SOURCE_PARENT')
    row, _ = api.get('/git/commits/' + parent)
    require(row['sha'] == parent and [p['sha'] for p in row['parents']] == [c3['source_previous']]
            and row['tree']['sha'] == value(repo, 'show', '-s', '--format=%T', parent),
            'C3_BOOTSTRAP_PARENT')
    changed = git(repo, 'diff-tree', '-r', '--no-renames', '--no-commit-id', '--name-only',
                  '-z', c3['source_previous'], parent).stdout.decode().split(chr(0))
    require(set(changed) == {PATH, '', *c3['add'], *c3['remove']}, 'C3_BOOTSTRAP_DELTA')
    reader = SourceReader(repo, api, parent, source)
    require(media_snapshot(reader, parent) == c3['mirror'], 'C3_BOOTSTRAP_MIRROR')
    return pins | {'source_parent':parent, 'recovery_ref':c3['recovery_ref']}


def source_identity(api, repo, pins, source, run, attempt, event=None):
    require(type(run) is int and run>0 and attempt==1 and gb.oid(source) and
            pins['source_ref']==SOURCE_REF and gb.oid(pins['source_parent']), 'STANDARD_SOURCE_SCOPE')
    before = pins['source_parent']
    reader = SourceReader(repo, api, before, source)
    require(value(repo, 'rev-parse', SOURCE_REF)==source==reader._ref('intake', SOURCE_REF)
            and reader._commit('intake', source)[1]==[before], 'STANDARD_SOURCE_PARENT')
    row, _ = api.get('')
    require((row['id'], row['full_name'], row['owner']['id'], row['private'])==
            (PIN.numeric_id, PIN.name, gb.CP2B_OWNER_ID, True), 'STANDARD_REPOSITORY')
    remote, _ = api.get('/git/commits/' + source)
    require(remote['sha']==source and [r['sha'] for r in remote['parents']]==[before],
            'STANDARD_SOURCE_PARENT')
    meta, _ = api.get(f'/actions/runs/{run}/attempts/{attempt}')
    if event is None:
        listing, more = api.get(f'/actions/runs/{run}/artifacts?per_page=100')
        require(not more, 'STANDARD_ARTIFACT_WINDOW')
        item = artifact.select(listing['artifacts'], run, attempt, source, ref=SOURCE_REF)
        packet = artifact.decode(read_archive(api, item['id']), item)
        activities, more = api.get('/activity?per_page=100')
        require(not more, 'STANDARD_ACTIVITY_WINDOW')
        event = artifact.verify(packet, run, attempt, source, meta, activities, [before], ref=SOURCE_REF)
    else:
        require((meta['id'], meta['run_attempt'], meta['workflow_id'], meta['repository']['id'],
                 meta['actor']['id'], meta['head_branch'], meta['head_sha'], meta['event'],
                 meta['path'].split('@',1)[0])==
                (run,attempt,artifact.WORKFLOW,PIN.numeric_id,gb.CP2B_OWNER_ID,
                 SOURCE_REF.removeprefix('refs/heads/'),source,'push',PATH), 'STANDARD_SOURCE_RUN')
        artifact.encode(event, run, attempt, artifact.WORKFLOW, source, ref=SOURCE_REF)
        proof.prove(api, event, source, before, datetime.now(timezone.utc), ref=SOURCE_REF)
    require(event['before']==before and event['after']==source, 'STANDARD_SOURCE_IDENTITY')
    from dataclasses import asdict
    c3 = pins.get('c3')
    pins_=verify_delta(reader, before, source, workflow_change=c3 is None)
    if c3 is None:
        require({path:asdict(obj) for path,obj in pins_.items()}==
                parse((HERE/'media-pins.json').read_bytes()),'STANDARD_TEST_MEDIA_PIN')
    else:
        path = c3['txt']
        require(set(pins_) == {path} and pins_[path].sha256 == c3['txt_sha256'],
                'C3_EXACT_TXT_EDIT')
    require(reader._ref('intake', SOURCE_REF)==source, 'STANDARD_SOURCE_MOVED')
    return reader, event


def check_source_yaml(repo, source, code, manifest):
    require(gb.oid(code) and gb.sha(manifest), 'STANDARD_WORKER_PIN')
    template = (HERE / 'source.template.yml').read_bytes()
    for marker, value_ in ((b'__R18H_WORKER__',code),(b'__R18H_MANIFEST__',manifest)):
        require(template.count(marker)==1, 'STANDARD_SOURCE_TEMPLATE')
        template = template.replace(marker,value_.encode(),1)
    require(git(repo,'show',source+':'+PATH).stdout==template, 'STANDARD_SOURCE_TEMPLATE')


def registration(api, lab, source_repo, pins, *, source, run, attempt, code, manifest,
                 verifier_actor, handoff=None):
    reader, original = source_identity(api, source_repo, pins, source, run, attempt)
    check_source_yaml(source_repo, source, code, manifest)
    before = original['before']
    require(gb.positive(verifier_actor) and gb.oid(pins['recovery_bootstrap']) and
            pins['recovery_ref']==(pins['c3']['recovery_ref'] if 'c3' in pins else gb.STANDARD_RECOVERY_REF),
            'STANDARD_RECOVERY_SCOPE')
    policy = gb.Policy((PIN,), (('independent_verify',verifier_actor),), code, code,
                       pins['library_id'], gb.CP2B_OWNER_ID, ())
    backend = gb.Backend(policy)
    queue = PrivateQueue(lab, lab.root/'queue', backend, repository=PIN, owner_actor_id=gb.CP2B_OWNER_ID,
                         baseline=before, source_ref=SOURCE_REF, cp2b_reader=reader)
    if not queue.directory.exists():
        queue.initialize()

    def http(write=None):
        return gb.PrivateRecoveryHTTP(PIN, gb.CP2B_OWNER_ID, code, api.token, writable=write is not None,
                                      standard_bootstrap=pins['recovery_bootstrap'],
                                      standard_handoff=handoff, standard_write=write,
                                      standard_ref=pins['recovery_ref'])

    ro = http()
    node = ro.node_id(PIN.numeric_id)

    def recovery_head():
        return ro.head(PIN.numeric_id, pins['recovery_ref'])

    def run_facts(r,a):
        require((r,a)==(run,attempt), 'STANDARD_SOURCE_RUN')
        return dict(run=run,attempt=attempt,workflow=artifact.WORKFLOW,code=code,repository=PIN.numeric_id,
                    actor=gb.CP2B_OWNER_ID,ref=SOURCE_REF,event='push',forced=False,deleted=False,
                    before=before,tip=source)

    provider = SimpleNamespace(run_facts=run_facts,recovery_head=recovery_head)
    def client(expected, write=None):
        return gb.PrivateRecovery(PIN,node,gb.CP2B_OWNER_ID,code,ro if write is None else http(write),
                   expected_head=expected,writable=write is not None,ref=pins['recovery_ref'])

    def reg(recovery):
        return TrustedRegistration(queue,recovery,provider,workflow=artifact.WORKFLOW,code=code)
    current=recovery_head()
    return reg(client(current)), client, reg



def register_chained(reg, client, reg_factory, run, attempt):
    """Use the existing durable envelope chain; return a read-only verified client."""
    require(reg.standard_chain and not reg.recovery.writable, "STANDARD_REGISTRATION_SCOPE")
    reg.queue._accepted_view = None
    head, proven, runs, cursor, previous, _ = reg._read()
    reg.restore()
    facts = reg._facts(run, attempt)
    if run in runs:
        require(runs[run]["facts"] == facts, "STANDARD_REGISTRATION_RUN_CONFLICT")
        reader = reg_factory(client(head))
        require(reader.restore() == [tuple(row) for row in proven], "STANDARD_REGISTRATION_READBACK")
        return reader
    packet = reg._envelope(facts, cursor, len(proven), previous)
    path = f"private-recovery/{reg.scope}/{reg._slot(facts)}/checkpoint.json"
    writer = reg_factory(client(head, (head, path, digest(data(packet)))))
    require(writer.register(run, attempt) == "REGISTERED", "STANDARD_REGISTRATION_UNRESOLVED")
    reader = reg_factory(client(reg.provider.recovery_head()))
    require(not reader.recovery.writable and
            reader.restore() == [tuple(row) for row in proven + packet["rows"]],
            "STANDARD_REGISTRATION_READBACK")
    return reader

def register_one(reg, client, reg_factory, run, attempt):
    if reg.standard_chain:
        return register_chained(reg, client, reg_factory, run, attempt)
    head, rows, _, cursor, previous, _ = reg._read()
    require(head==reg.recovery.port.standard_bootstrap and not rows and previous is None,
            'STANDARD_REGISTRATION_EXISTING_STOP')
    facts=reg.provider.run_facts(run,attempt)
    packet=reg._envelope(facts,cursor,0,None)
    path=f'private-recovery/{reg.scope}/{reg._slot(facts)}/checkpoint.json'
    writer=reg_factory(client(head,(head,path,digest(data(packet)))))
    require(writer.register(run,attempt)=='REGISTERED','STANDARD_REGISTRATION_UNRESOLVED')
    reader=reg_factory(client(reg.provider.recovery_head()))
    require(reader.verify()==[tuple(packet['rows'][0])] and
            reader.recovery.expected_head!=head, 'STANDARD_REGISTRATION_READBACK')
    return reader


def prepare_handoff(reg, source_repo, domain, pins):
    from authority_coordinator import LocalWitness
    require(type(domain) is LocalWitness, 'STANDARD_AUTHORITY_SCOPE')
    context = domain.read()
    pins = pins | pins.get('c3', {})
    initial = context['latest']
    require(context['tip']==pins['ledger'] and digest(data(initial))==pins['record_sha256'] and
            (initial['primary_main'],initial['primary_witness'],initial['registry_sha256'])==
            (pins['authority'],pins['witness'],pins['registry']), 'STANDARD_AUTHORITY_PIN')
    mirror = files(source_repo, pins['source_parent'], EDITABLE)
    registry = files(context['recovered'], pins['authority'])
    require(mirror=={k:v for k,v in registry.items() if k.startswith(EDITABLE)} and
            len(mirror)==pins['mirror_files'] and sum(map(len,mirror.values()))==pins['mirror_bytes'],
            'STANDARD_MIRROR_PIN')
    prepared=prepare_registered_next(reg,source_repo,domain,mirror_record_sha256=pins['record_sha256'])
    accepted = reg.queue._accepted_view if reg.standard_chain else None
    payload=_handoff_payload(prepared.binding,prepared.operation.before,prepared.operation.change,
                             base=accepted.handoff_base if accepted is not None else None)
    pin=gb.StandardHandoffPin(prepared.binding.owner_request_sha256,pins['record_sha256'],
                              digest(payload),len(payload))
    return prepared, pin, payload


def source_main(env):
    from auto_intake import source_main as dynamic_source
    if env.get("STANDARD_ROUTE") == "auto":
        return dynamic_source(env)
    api=proof.GitHub(env['GH_TOKEN'])
    pins=parse(BOOTSTRAP.read_bytes())
    root=Path(env['RUNNER_TEMP'])
    require(env['GITHUB_ACTIONS']=='true' and env['GITHUB_EVENT_NAME']=='push' and
            env['GITHUB_REF']==SOURCE_REF and env['GITHUB_REPOSITORY']==PIN.name and
            env['GITHUB_REPOSITORY_ID']==str(PIN.numeric_id),'STANDARD_TRIGGER')
    run,attempt,source=int(env['GITHUB_RUN_ID']),int(env['GITHUB_RUN_ATTEMPT']),env['GITHUB_SHA']
    event=json.loads(Path(env['GITHUB_EVENT_PATH']).read_bytes())
    repo=root/'r18h-source.git'
    fetch_source(repo,api.token)
    pins=c3_source_pins(api,repo,pins,source)
    check_source_yaml(repo,source,env['WORKER_COMMIT'],env['MANIFEST_SHA256'])
    source_identity(api,repo,pins,source,run,attempt,event)
    artifact.emit(env['GITHUB_EVENT_PATH'],root/artifact.FILE,run,attempt,artifact.WORKFLOW,
                  source,ref=SOURCE_REF)
    print('PROPOSED_ONLY registered=False rows=1')

def verified_dispatch(api, env, path, source_run, *, normal=False):
    require(env['GITHUB_ACTIONS']=='true' and env['GITHUB_EVENT_NAME']=='workflow_dispatch' and
            env['GITHUB_REF']=='refs/heads/main' and env['GITHUB_RUN_ATTEMPT']=='1' and
            env['GITHUB_REPOSITORY']==PIN.name and env['GITHUB_REPOSITORY_ID']==str(PIN.numeric_id) and
            env['GITHUB_WORKFLOW_REF']==f'{PIN.name}/{path}@refs/heads/main' and
            gb.oid(env['GITHUB_WORKFLOW_SHA']), 'STANDARD_DISPATCH')
    workflow, _=api.get('/actions/workflows/'+path.rsplit('/',1)[1])
    run=int(env['GITHUB_RUN_ID'])
    actor=int(env['TRUSTED_DISPATCH_ACTOR'])
    meta, _=api.get(f'/actions/runs/{run}/attempts/1')
    require((meta['id'],meta['workflow_id'],meta['run_attempt'],meta['head_sha'],meta['event'],
             meta['repository']['id'],meta['head_branch'],meta['actor']['id'],
             meta['path'].split('@',1)[0])==
            (run,workflow['id'],1,env['GITHUB_WORKFLOW_SHA'],'workflow_dispatch',
             PIN.numeric_id,'main',actor,path), 'STANDARD_DISPATCH_IDENTITY')
    if path.endswith('e4f-standard-prepare.yml') and not normal:
        rows,more=api.get(f"/actions/workflows/{workflow['id']}/runs?event=workflow_dispatch&per_page=100")
        matches=[r for r in rows['workflow_runs'] if r['display_title']==f'R18H prepare {source_run}']
        require(not more and len(matches)==1 and matches[0]['id']==run and
                matches[0]['run_attempt']==1, 'STANDARD_DUPLICATE_WRITER')


def lab_read_token():
    # The existing job token authenticates only reads of its own public Lab.
    # PRIVATE and DEV jobs never forward their token to another repository.
    from e4f_github import CONTROL
    if (os.environ.get("GITHUB_REPOSITORY")==CONTROL and
            os.environ.get("AUTO_PHASE") in ("intake","assets","dev")):
        return os.environ.get("GH_TOKEN")
    if os.environ.get("GITHUB_REPOSITORY")==PIN.name:
        # Existing ephemeral job token, only to api.github.com GETs of the fixed public Lab.
        # No credential is sent by public_asset() to browser/redirect download hosts.
        paths=(".github/workflows/e4f-standard-prepare.yml",
               ".github/workflows/e4f-d3-guard.yml",".github/workflows/e4f-d3-guard-observe.yml")
        token=os.environ.get("GH_TOKEN")
        require(os.environ.get("GITHUB_ACTIONS")=="true" and
                os.environ.get("GITHUB_SERVER_URL")=="https://github.com" and
                os.environ.get("GITHUB_REPOSITORY_ID")==str(PIN.numeric_id) and
                os.environ.get("GITHUB_REF")=="refs/heads/main" and
                os.environ.get("GITHUB_EVENT_NAME")=="workflow_dispatch" and
                os.environ.get("GITHUB_RUN_ATTEMPT")=="1" and
                os.environ.get("GITHUB_ACTOR_ID")=="353092585" and
                os.environ.get("GITHUB_WORKFLOW_REF") in
                  tuple(PIN.name+"/"+p+"@refs/heads/main" for p in paths) and
                gb.oid(os.environ.get("GITHUB_WORKFLOW_SHA")) and
                type(token) is str and bool(token) and not any(c.isspace() for c in token),
                "STANDARD_LAB_READ_CONTEXT")
        return token
    return None


def backup_releases(records):
    """Bounded, fresh bulk metadata reads. No byte cache or prior acceptance."""
    from e4f_github import CONTROL
    tags=set()
    for record in records:
        backup=record['backup'];identity=backup['sha256']
        require(gb.sha(identity) and backup['file']==identity+'.bundle','STANDARD_BACKUP_PIN')
        tags.add('r18h-d2-'+identity if record['index']==0 else 'r18h-d3-'+digest(data(record)))
    require(1<=len(tags)<=256,'STANDARD_LEDGER_HISTORY')
    selected={}
    # A finite 16-GET metadata budget covers the 256-record contract plus
    # projection and historical releases. Missing evidence always stops.
    for page in range(1,17):
        status,rows=github('/repos/'+CONTROL+f'/releases?per_page=100&page={page}',lab_read_token())
        require(status==200 and type(rows) is list and len(rows)<=100,'STANDARD_BACKUP_LIST')
        for row in rows:
            require(type(row) is dict,'STANDARD_BACKUP_LIST')
            tag=row.get('tag_name')
            if tag not in tags:continue
            require(tag not in selected or selected[tag]==row,'STANDARD_BACKUP_LIST_CONFLICT')
            selected[tag]=row
        if set(selected)==tags:return selected
        if len(rows)<100 or page==16:
            # Reuse the original strict tag reader for at most the two newly
            # published transitions of this bounded job, not an unbounded retry.
            missing=tags-set(selected)
            require(len(missing)<=2,'STANDARD_BACKUP_LIST_LIMIT' if page==16 else 'STANDARD_BACKUP_LIST_MISSING')
            for tag in sorted(missing):selected[tag]=public_release(tag)
            return selected
    raise ProviderStop('STANDARD_BACKUP_LIST_LIMIT')


def public_release(tag, *, metadata=None):
    from e4f_github import CONTROL
    require(any(tag.startswith(prefix) and gb.sha(tag[len(prefix):]) for prefix in ('r18h-d2-','r18h-d3-','r18h-dev-')),'STANDARD_BACKUP_TAG')
    if metadata is None:
        status, release=github('/repos/'+CONTROL+'/releases/tags/'+tag,lab_read_token())
    else:
        require(type(metadata) is dict and tag in metadata,'STANDARD_BACKUP_LIST_MISSING')
        status,release=200,metadata[tag]
    obj=release if type(release) is dict else {}
    checks=dict(http_200=status==200,object=type(release) is dict,
                tag_matches=obj.get('tag_name')==tag,draft_false=obj.get('draft') is False,
                prerelease_false=obj.get('prerelease') is False,immutable=obj.get('immutable') is True)
    valid=all(checks.values())
    if not valid:
        print(json.dumps(dict(release_tag=tag,http_status=status,metadata_checks=checks),
                         sort_keys=True),flush=True)
    require(valid,'STANDARD_BACKUP_RELEASE')
    return release


def public_asset(release, name, size, limit):
    from e4f_github import CONTROL, NoRedirect
    from urllib.parse import urlsplit
    from urllib.request import Request, build_opener
    from urllib.error import HTTPError
    require(type(size) is int and 0<size<=limit and '/' not in name and '\\' not in name,
            'STANDARD_BACKUP_PIN')
    tag=release['tag_name']
    expected_url=f'https://github.com/{CONTROL}/releases/download/{tag}/{name}'
    matches=[a for a in release['assets'] if a['name']==name]
    require(len(matches)==1 and (matches[0]['size'],matches[0]['state'],matches[0]['browser_download_url'])==
            (size,'uploaded',expected_url),'STANDARD_BACKUP_ASSET')
    opener=build_opener(NoRedirect)
    try:
        response=opener.open(Request(expected_url),timeout=30)
    except HTTPError as exc:
        redirected=exc.headers.get('Location','')
        u=urlsplit(redirected)
        permitted = (exc.code in (301,302,303,307,308) and u.scheme=='https' and
                     u.netloc in ('release-assets.githubusercontent.com','objects.githubusercontent.com'))
        if not permitted:
            # Fixed fields only. Never emit Location, query, userinfo or exception text.
            host = u.hostname or ''
            if len(host)>253 or any(c not in 'abcdefghijklmnopqrstuvwxyz0123456789.-' for c in host):
                host = 'INVALID_HOST'
            print(json.dumps({'asset_id': matches[0]['id'], 'http_status': exc.code,
                              'redirect_hostname': host}, sort_keys=True), flush=True)
        require(permitted, 'STANDARD_BACKUP_REDIRECT')
        response=opener.open(Request(redirected),timeout=30)
    with response:
        raw=response.read(size+1)
    require(len(raw)==size,'STANDARD_BACKUP_READBACK')
    return raw


def public_baseline(lab, pins, *, extra=None, normal=False):
    """Read the public ledger and verified immutable backup objects into scratch."""
    from authority_coordinator import LocalWitness
    from e4f_github import CONTROL, CONTROL_ID, LEDGER

    prefix='/repos/'+CONTROL
    status, repository=github(prefix,lab_read_token())
    require(status==200 and (repository['id'],repository['full_name'],repository['private'])==
            (CONTROL_ID,CONTROL,False),'STANDARD_LEDGER_REPOSITORY')
    status,row=github(prefix+'/git/ref/'+LEDGER.removeprefix('refs/'),lab_read_token())
    require(status==200 and row['ref']==LEDGER and row['object']['type']=='commit' and
            gb.oid(row['object']['sha']),'STANDARD_LEDGER_HEAD')
    tip=row['object']['sha']
    anchor=lab.root/'ledger.git'
    anchor.mkdir()
    git(anchor,'init','--bare','--initial-branch=main')
    cmd=['git','-C',str(anchor),'-c','http.followRedirects=false','-c',
         'protocol.allow=never','-c','protocol.https.allow=always','fetch','--no-tags',
         '--no-recurse-submodules','https://github.com/'+CONTROL+'.git',LEDGER+':'+MAIN]
    clean={k:v for k,v in os.environ.items() if not k.startswith('GIT_')}
    clean.update(GIT_CONFIG_NOSYSTEM='1',GIT_CONFIG_GLOBAL=os.devnull,GIT_TERMINAL_PROMPT='0')
    result=subprocess.run(cmd,capture_output=True,env=clean,timeout=90)
    require(result.returncode==0 and value(anchor,'rev-parse',MAIN)==tip,'STANDARD_LEDGER_FETCH')
    history=value(anchor,'rev-list','--reverse',MAIN).splitlines()
    c3 = pins.get('c3')
    require(1<=len(history)<=(256 if normal else (4 if c3 else 3)) and history[0]==pins['ledger'],'STANDARD_LEDGER_HISTORY')
    files_=files(anchor,tip,('records/',))
    paths=[f'records/{i:08d}.json' for i in range(len(history))]
    require(set(files_)==set(paths),'STANDARD_LEDGER_HISTORY')
    rows=[parse(files_[p]) for p in paths]
    require(digest(data(rows[0]))==pins['record_sha256'] and rows[0]['backup']==pins['backup'],
            'STANDARD_LEDGER_GENESIS')
    if c3:
        require(len(rows) >= 3 and history[2] == c3['ledger'] and
                digest(data(rows[2])) == c3['record_sha256'] and
                (rows[2]['primary_main'], rows[2]['primary_witness'], rows[2]['registry_sha256']) ==
                (c3['authority'], c3['witness'], c3['registry']), 'C3_LEDGER_PREDECESSOR')
    if extra is not None and extra not in rows:
        require(extra['index']==len(rows) and extra['previous']==digest(data(rows[-1])),
                'STANDARD_BACKUP_PREDECESSOR')
        rows.append(extra)
    metadata=backup_releases(rows)
    directory=lab.root/'independent-archives'
    directory.mkdir()
    for record in rows:
        backup=record['backup']
        identity=backup['sha256']
        require(gb.sha(identity) and backup['file']==identity+'.bundle','STANDARD_BACKUP_PIN')
        tag='r18h-d2-'+identity if record['index']==0 else 'r18h-d3-'+digest(data(record))
        release=public_release(tag,metadata=metadata)
        allowed={backup['file']} if record['index']==0 else {backup['file'],'proposal.json'}
        require(set(a['name'] for a in release['assets'])==allowed and
                len(release['assets'])==len(allowed),'STANDARD_BACKUP_ASSET')
        raw=public_asset(release,backup['file'],backup['bytes'],64_000_000)
        require(digest(raw)==identity,'STANDARD_BACKUP_READBACK')
        with (directory/backup['file']).open('xb') as f:
            f.write(raw)
    status,readback=github(prefix+'/git/ref/'+LEDGER.removeprefix('refs/'),lab_read_token())
    require(status==200 and readback==row,'STANDARD_LEDGER_MOVED')
    return LocalWitness(lab,anchor,directory,pins['record_sha256'],pins['library_id'],pins['witness'])

def control_run(control_run, allowed_titles=None):
    from e4f_github import CONTROL, CONTROL_ID
    root='/repos/'+CONTROL
    path='.github/workflows/shared-media.yml'
    status,ref=github(root+'/git/ref/heads/automation-control')
    require(status==200 and ref['ref']=='refs/heads/automation-control' and
            ref['object']['type']=='commit' and gb.oid(ref['object']['sha']),
            'STANDARD_CONTROL_REF')
    status,row=github(root+f'/actions/runs/{control_run}/attempts/1')
    require(status==200 and (row['id'],row['run_attempt'],row['event'],row['head_sha'],
            row['head_branch'],row['repository']['id'],row['path'].split('@',1)[0])==
            (control_run,1,'workflow_dispatch',ref['object']['sha'],'automation-control',CONTROL_ID,path),
            'STANDARD_CONTROL_RUN')
    require(row['actor']['id']==gb.CP2B_OWNER_ID and
            (allowed_titles is None or row['display_title'] in allowed_titles),
            'STANDARD_CONTROL_ACTOR')
    return row


def proposal(record_sha, *, c3=False, normal=False):
    require(gb.sha(record_sha),'STANDARD_PROPOSAL_PIN')
    tag='r18h-d3-'+record_sha
    release=public_release(tag)
    assets=release['assets']
    named=[a for a in assets if a['name']=='proposal.json']
    require(len(named)==1 and len(assets)==2,'STANDARD_PROPOSAL_ASSET')
    raw=public_asset(release,'proposal.json',named[0]['size'],32_000)
    obj=parse(raw)
    require(data(obj)==raw and set(obj) in ({'schema','source_run','ledger_parent','previous','record','handoff'},
            {'schema','source_run','ledger_parent','previous','record','handoff','execution'})
            and obj['schema']==1 and gb.positive(obj['source_run']) and gb.oid(obj['ledger_parent']) and
            digest(data(obj['record']))==record_sha and (type(obj['record']['index']) is int and obj['record']['index']>=3 if normal else obj['record']['index'] in ((3,) if c3 else (1,2))),
            'STANDARD_PROPOSAL')
    from e4f_github import ledger_cas_payload
    ledger_cas_payload(obj['ledger_parent'],obj['previous'],obj['record'])
    return obj


def c3_control_run(run, pins):
    row = control_run(run, {'R18H c3'})
    require((row['workflow_id'], row['run_number'], row['status']) ==
            (408732936, pins['c3']['control_run_number'], 'in_progress'), 'C3_CONTROL_RUN')
    return row


def guard_main(env):
    params=json.loads(Path(env['GITHUB_EVENT_PATH']).read_bytes())['inputs']
    from auto_intake import is_auto_control, private_guard
    if is_auto_control(int(params["control_run"])):
        return private_guard(env)
    import standard_guard
    from authority_coordinator import LocalAppendGuard
    api=proof.GitHub(env['GH_TOKEN'])
    params=json.loads(Path(env['GITHUB_EVENT_PATH']).read_bytes())['inputs']
    expected={'mode','ledger_parent','record_sha256','control_run','challenge','proof'}
    require(set(params)==expected and params['mode']==env['GUARD_MODE'] and
            gb.sha(params['challenge']) and gb.sha(params['proof']) and
            digest(data({k:v for k,v in params.items() if k!='proof'}))==params['proof'],
            'STANDARD_GUARD_DISPATCH')
    mode=params['mode']
    path='.github/workflows/e4f-d3-guard'+('-observe' if mode=='observe' else '')+'.yml'
    verified_dispatch(api,env,path,None)
    actions=('accept',) if mode=='reserve' else ('accept','recover')
    titles={f"R18H {action} {params['record_sha256']}" for action in actions}
    pins=parse(BOOTSTRAP.read_bytes())
    c3=pins.get('c3')
    if c3:
        c3_control_run(int(params['control_run']), pins)
    else:
        control_run(int(params['control_run']),titles)
    item=proposal(params['record_sha256'],c3=c3 is not None)
    if c3:
        require(item['ledger_parent']==c3['ledger'] and item['record']['phase']=='staged' and
                digest(data(item['previous']))==c3['record_sha256'], 'C3_GUARD_PREDECESSOR')
    require(item['ledger_parent']==params['ledger_parent'],'STANDARD_GUARD_LEDGER')
    p=item['handoff']
    require(set(p)=={'request','mirror','sha256','size','head','parent'},'STANDARD_HANDOFF_PIN')
    pin=gb.StandardHandoffPin(*(p[k] for k in ('request','mirror','sha256','size')))
    root=Path(env['RUNNER_TEMP'])
    lab=Lab.create(root/'r18h-guard')
    raw=lab.root/'source.git'
    fetch_source(raw,api.token)
    run=item['source_run']
    source,_=api.get(f'/actions/runs/{run}/attempts/1')
    pins=c3_source_pins(api,raw,pins,source['head_sha'])
    reg,client,_=registration(api,lab,raw,pins,source=source['head_sha'],run=run,attempt=1,
                    code=env['WORKER_COMMIT'],manifest=env['MANIFEST_SHA256'],
                    verifier_actor=int(env['TRUSTED_DISPATCH_ACTOR']),handoff=pin)
    head=reg.recovery.expected_head
    domain=public_baseline(lab,pins,extra=item['record'])
    write=None
    if mode=='reserve':
        packet=parse(reg.recovery.read(head)['files'][pin.path])
        binding=packet['binding']
        operation=SimpleNamespace(request=binding['derived_request_sha256'],change=packet['change'])
        scope,base,marker=LocalAppendGuard.marker(domain,{'latest':item['previous']},operation,item['record'])
        write=(head,f'private-recovery/{scope}/{base}/checkpoint.json',digest(marker))
    guard=LocalAppendGuard(client(head,write),hosted_standard=True)
    result=standard_guard.attest(mode,guard=guard,registration=reg,domain=domain,
          destination=lab.root/'restored.git',record=item['record'],ledger_parent=item['ledger_parent'],
          record_sha256=params['record_sha256'],registration_head=head,handoff_head=p['head'],
          handoff_parent=p['parent'])
    require(result in ('fresh','existing'),'STANDARD_GUARD_VERDICT')
    with open(env['GITHUB_OUTPUT'],'a',encoding='ascii') as f:
        f.write('guard='+result+'\n')
    print('STANDARD_CORE_GUARD_VERIFIED')

def next_main(env):
    """Prepare only the roll-forward after the first independently accepted record."""
    from intake_plan import SourceEvidence, change_digest
    from queued_pipeline import verify_handoff
    from request_executor import RequestOperation

    api=proof.GitHub(env['GH_TOKEN'])
    params=json.loads(Path(env['GITHUB_EVENT_PATH']).read_bytes())['inputs']
    prev=proposal(params['previous_record'])
    run=prev['source_run']
    verified_dispatch(api,env,'.github/workflows/e4f-standard-next.yml',run)
    require(prev['record']['index']==1 and prev['record']['phase']=='accepted','STANDARD_ROLL_FORWARD')
    pins=parse(BOOTSTRAP.read_bytes())
    root=Path(env['RUNNER_TEMP'])
    lab=Lab.create(root/'r18h-next')
    raw=lab.root/'source.git'
    fetch_source(raw,api.token)
    src,_=api.get(f'/actions/runs/{run}/attempts/1')
    p=prev['handoff']
    pin=gb.StandardHandoffPin(*(p[k] for k in ('request','mirror','sha256','size')))
    reg,_,_=registration(api,lab,raw,pins,source=src['head_sha'],run=run,attempt=1,
        code=env['WORKER_COMMIT'],manifest=env['MANIFEST_SHA256'],
        verifier_actor=int(env['TRUSTED_DISPATCH_ACTOR']),handoff=pin)
    domain=public_baseline(lab,pins)
    context=domain.read()
    require(context['latest']==prev['record'] and len(context['records'])==2,'STANDARD_ROLL_FORWARD_LEDGER')
    head,rows,*_=reg._read()
    snap=reg.recovery.read(head)
    import standard_guard
    standard_guard.check_recovery(reg.recovery,snap,p['head'],p['parent'],
                                  standard_guard.LocalAppendGuard.scope(domain))
    payload=snap['files'][pin.path]
    mirror=dict(context,latest=context['records'][0])
    restored=verify_handoff(payload,reg,rows[0],domain,mirror,lab.root/'restored.git',check_proofs=True)
    packet=parse(payload)
    before,change=packet['before'],packet['change']
    git(restored,'update-ref',MAIN,change['commit'])
    evidence=SourceEvidence(PIN.numeric_id,PIN.name,digest(data(before)),change_digest(change),
                            pin.mirror,int(datetime.now(timezone.utc).timestamp()),'private')
    op=RequestOperation(lab,restored,before,change,evidence,lambda _: None)
    op.bind(context)
    require(op.pending and op.request==packet['binding']['derived_request_sha256'],'STANDARD_ROLL_FORWARD')
    item=produce_candidate(domain,op,context,pins['witness'],run,p,root/'r18h-candidate')
    require(item['record']['index']==2 and item['record']['phase']=='staged',
            'STANDARD_FINAL_BACKUP')
    print('STANDARD_FINAL_CANDIDATE')


def preparation(api, lab, source_repo, domain, pins, *, source, run, attempt, code, manifest,
                verifier_actor):
    reg, client, factory=registration(api,lab,source_repo,pins,source=source,run=run,attempt=attempt,
                                      code=code,manifest=manifest,verifier_actor=verifier_actor)
    # An independent authority/mirror check precedes even registration I/O.
    context=domain.read()
    admission=pins.get('c3',pins)
    require(context['tip']==admission['ledger'] and digest(data(context['latest']))==admission['record_sha256'],
            'STANDARD_AUTHORITY_PIN')
    baseline=files(context['recovered'],admission['authority'],EDITABLE)
    require(files(source_repo,pins['source_parent'],EDITABLE)==baseline and
            len(baseline)==admission['mirror_files'], 'STANDARD_MIRROR_PIN')
    reader=register_one(reg,client,factory,run,attempt)
    prepared,pin,payload=prepare_handoff(reader,source_repo,domain,pins)
    parent=reader.recovery.expected_head
    ro, pinned_client, pinned_factory=registration(api,lab,source_repo,pins,source=source,run=run,
        attempt=attempt,code=code,manifest=manifest,verifier_actor=verifier_actor,handoff=pin)
    expected_rows=reader.verify()
    require(ro.recovery.expected_head==parent and ro.verify()==expected_rows,'STANDARD_RECOVERY_MOVED')
    writer=pinned_client(parent,(parent,pin.path,pin.sha256))
    scope=pin.path.split('/')[1]
    head=writer.persist(scope,pin.mirror,payload)
    observed=pinned_client(head).read(head)
    require(observed['parents']==[parent] and observed['files'][pin.path]==payload and
            pinned_factory(pinned_client(head)).verify()==expected_rows,'STANDARD_HANDOFF_READBACK')
    return prepared,pin,head,parent


def produce_candidate(domain, operation, context, witness, run, handoff, out):
    from git_transport import Session
    previous=context['latest']
    operation.bind(context)
    session=Session(domain.lab,context['recovered'],witness)
    candidate=session.prepare(operation)
    require(candidate is not None,'STANDARD_NO_CANDIDATE')
    record=domain.record(session.repo,candidate['authority'],candidate['witness'],candidate['base'],
                         previous,previous['index']+1)
    require(domain.verify_backup(context,record)['recoverable'] is True,'STANDARD_BACKUP')
    out.mkdir()
    item={'schema':1,'source_run':run,'ledger_parent':context['tip'],
          'previous':previous,'record':record,'handoff':handoff}
    if isinstance(operation,__import__('request_executor').RequestOperation):
        from e4f_github import execution_hint
        item['execution']=execution_hint(operation,previous,record)
    (out/'proposal.json').write_bytes(data(item))
    (out/record['backup']['file']).write_bytes((domain.archives/record['backup']['file']).read_bytes())
    return item


def prepare_main(env):
    params=json.loads(Path(env['GITHUB_EVENT_PATH']).read_bytes())['inputs']
    if params.get("route") == "auto":
        from auto_intake import private_prepare
        return private_prepare(env)
    api=proof.GitHub(env['GH_TOKEN'])
    params=json.loads(Path(env['GITHUB_EVENT_PATH']).read_bytes())['inputs']
    run=int(params['source_run'])
    verified_dispatch(api,env,'.github/workflows/e4f-standard-prepare.yml',run)
    source,_=api.get(f'/actions/runs/{run}/attempts/1')
    root=Path(env['RUNNER_TEMP'])
    lab=Lab.create(root/'r18h-prepare')
    pins=parse(BOOTSTRAP.read_bytes())
    c3=pins.get('c3')
    require(params.get("route","")=="","STANDARD_DISPATCH_ROUTE")
    params.pop("route",None)
    if c3:
        require(set(params)=={'source_run','control_run'} and params['control_run'].isdigit(),'C3_PREPARE_SCOPE')
        c3_control_run(int(params['control_run']),pins)
    repo=lab.root/'source.git'
    fetch_source(repo,api.token)
    pins=c3_source_pins(api,repo,pins,source['head_sha'])
    domain=public_baseline(lab,pins)
    prepared,pin,head,parent=preparation(api,lab,repo,domain,pins,source=source['head_sha'],
          run=run,attempt=1,code=env['WORKER_COMMIT'],manifest=env['MANIFEST_SHA256'],
          verifier_actor=int(env['TRUSTED_DISPATCH_ACTOR']))
    handoff={'request':pin.request,'mirror':pin.mirror,'sha256':pin.sha256,'size':pin.size,
             'head':head,'parent':parent}
    item=produce_candidate(domain,prepared.operation,domain.read(),pins['witness'],
                           run,handoff,root/'r18h-candidate')
    if c3:
        require(item['record']['index']==3 and item['record']['phase']=='staged' and
                len(prepared.operation.steps)==1 and
                (prepared.operation.steps[0]['kind'],prepared.operation.steps[0]['target_id'])==
                ('metadata','bgzona-m-00019'), 'C3_PREPARE_PLAN')
    print('STANDARD_HANDOFF_DURABLE')


if __name__=='__main__':
    try:
        actions={'source':source_main,'prepare':prepare_main,'next':next_main,'guard':guard_main}
        require(len(sys.argv)==2 and sys.argv[1] in actions,'STANDARD_COMMAND')
        actions[sys.argv[1]](os.environ)
    except ProviderStop as exc:
        print('STOP '+str(exc),file=sys.stderr)
        sys.exit(1)
    except Exception:
        print('STOP STANDARD_UNRESOLVED',file=sys.stderr)
        sys.exit(1)
