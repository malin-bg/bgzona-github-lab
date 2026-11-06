"""Hosted independent ledger writer, Assets materialization and accepted DEV input."""
from pathlib import Path
from io import BytesIO
from zipfile import ZipFile, ZipInfo, ZIP_DEFLATED
import base64
import json
import os
import subprocess
import sys

import github_backend as gb
from e4f_github import (ASSETS, CONTROL, CONTROL_ID, LEDGER, ATOMIC, GitHubLedgerAppend,
                        GitHubNativeAtomic, PrivateGuardAttester, ProviderStop, github, require)
from git_transport import Lab, MAIN, WITNESS, Session, files
from importer import data, digest, parse
from standard_entry import BOOTSTRAP, PIN, control_run, proposal, public_asset, public_baseline, public_release

ASSETS_ID=1173521221
DEV='malin-bg/bgzona-win-dev'
STAGES=('refs/heads/r18h-staging-authority','refs/heads/r18h-staging-witness')
CONTROL_REF='refs/heads/automation-control'
RESUME_SOURCE_RUN=42194020254
RESUME_PRIVATE_RUN=42198381507
RESUME_SOURCE='0259760e45c7041ab3789560ff8ed6048c093eeb'
RESUME_WORKER='61216eb28fa155f7186bbcb03d2e23edc2f57b2e'
RESUME_MANIFEST='936b02a8825d74164b8f9fff511ea2a3baf70a5633170edae183d0d302f40c8b'
RESUME_SOURCE_YAML='9b8e1d39466f3f9b4a628895282aea7269d050df553f3855d3e8796a22ef538c'
RESUME_PRIVATE_SHA='58293804386a29159946dab2bdd1c02c547819fb'
RESUME_ACTOR=353092585
RESUME_REGISTRATION='4b6841993efe58080f3212366b78ac0d3b905e44'
RESUME_HANDOFF='21407c9ecbd52690001bb7fc519ad9fe536bff7b'
RESUME_CANDIDATE='4856e887ca71096a7f533c5e42e11dfe09a19e58059045903964d737455fdf2d'
RESUME_SOURCE_ARTIFACT=(13490857727,'a658d82a2c04489032691fab7495636275b2a38e6af8a6ab42894ef52840ea2a')
RESUME_PRIVATE_ARTIFACT=(13493237223,'3278798b68a0284c43116c80ddc0c760e9a8853c6d850828683d9a6721f33b78')


def context(env):
    require(env['GITHUB_ACTIONS']=='true' and env['GITHUB_REPOSITORY']==CONTROL and
            env['GITHUB_REPOSITORY_ID']==str(CONTROL_ID) and env['GITHUB_REF']==CONTROL_REF and
            env['GITHUB_REF_PROTECTED']=='true' and env['GITHUB_ACTOR_ID']==str(gb.CP2B_OWNER_ID) and
            env['GITHUB_RUN_ATTEMPT']=='1' and env['GITHUB_EVENT_NAME']=='workflow_dispatch' and
            env['GITHUB_WORKFLOW_REF']==f'{CONTROL}/.github/workflows/shared-media.yml@{CONTROL_REF}',
            'E4F_CONTROL_SCOPE')
    control_run(int(env['GITHUB_RUN_ID']))
    params=parse(Path(env['GITHUB_EVENT_PATH']).read_bytes())['inputs']
    require(type(params) is dict and 'mode' in params and
            set(params)<={'mode','record_sha256','source_run','private_run'}, 'E4F_CONTROL_INPUT')
    params={'record_sha256':'','source_run':'','private_run':''}|params
    if params['mode']=='resume-prepare':
        require((params['source_run'],params['private_run'],params['record_sha256'])==
                (str(RESUME_SOURCE_RUN),str(RESUME_PRIVATE_RUN),''), 'E4F_RESUME_SCOPE')
        return params
    require(params['private_run']=='','E4F_CONTROL_INPUT')
    if params['mode']=='prepare':
        require(params['source_run'].isdigit() and int(params['source_run'])>0 and
                params['record_sha256']=='', 'E4F_CONTROL_INPUT')
    else:
        require(gb.sha(params['record_sha256']) and params['source_run']=='','E4F_CONTROL_INPUT')
    return params


def attach_guard(env):
    token=env['PRIVATE_ACTIONS_TOKEN']
    rows=[]
    for name in ('e4f-d3-guard.yml','e4f-d3-guard-observe.yml'):
        status,row=github(f'/repos/{PIN.name}/actions/workflows/{name}',token)
        require(status==200 and row['path']=='.github/workflows/'+name and
                row['state']=='active' and gb.positive(row['id']),'E4F_GUARD_WORKFLOW')
        rows.append(row['id'])
    return PrivateGuardAttester(token,rows[0],env['PRIVATE_WORKFLOW_COMMIT'],
        int(env['PRIVATE_DISPATCH_ACTOR']),int(env['GITHUB_RUN_ID']),observe_workflow_id=rows[1])


def final_state(env, item):
    pins=parse(BOOTSTRAP.read_bytes())
    require(item['record']['index']==2 and item['record']['phase']=='staged','E4F_FINAL_D3_RECORD')
    lab=Lab.create(Path(env['RUNNER_TEMP'])/'r18h-control-final')
    domain=public_baseline(lab,pins)
    accepted=domain.read()
    require(len(accepted['records'])==3 and accepted['latest']==item['record'] and
            accepted['records'][1]==item['previous'],'E4F_FINAL_D3_RECORD')
    return pins,lab,accepted


def assets_ref(ref, token=None, missing=False):
    status,row=github('/repos/'+ASSETS+'/git/ref/'+ref.removeprefix('refs/'),token)
    if missing and status==404:
        return None
    require(status==200 and row['ref']==ref and row['object']['type']=='commit' and
            gb.oid(row['object']['sha']),'E4F_ASSETS_REF')
    return row['object']['sha']


def assets_identity(token=None):
    status,row=github('/repos/'+ASSETS,token)
    require(status==200 and (row['id'],row['full_name'],row['private'],row['archived'])==
            (ASSETS_ID,ASSETS,False,False),'E4F_ASSETS_IDENTITY')
    return row['node_id']


def stage_assets(repo, token, after):
    current=tuple(assets_ref(r,token,missing=True) for r in STAGES)
    if current==after:
        return
    require(current==(None,None),'E4F_ASSETS_STAGING_CONFLICT')
    env={k:v for k,v in os.environ.items() if not k.startswith('GIT_')}
    encoded=base64.b64encode(('x-access-token:'+token).encode()).decode()
    env.update(GIT_CONFIG_NOSYSTEM='1',GIT_CONFIG_GLOBAL=os.devnull,GIT_TERMINAL_PROMPT='0',
               GIT_CONFIG_COUNT='1',GIT_CONFIG_KEY_0='http.https://github.com/.extraheader',
               GIT_CONFIG_VALUE_0='AUTHORIZATION: basic '+encoded)
    command=['git','-C',str(repo),'-c','http.followRedirects=false','-c',
             'protocol.allow=never','-c','protocol.https.allow=always','push','--atomic',
             '--no-verify','https://github.com/'+ASSETS+'.git',
             *(oid+':'+ref for oid,ref in zip(after,STAGES))]
    try:
        subprocess.run(command,env=env,capture_output=True,timeout=60)
    except (OSError,subprocess.TimeoutExpired):
        pass
    require(tuple(assets_ref(r,token,missing=True) for r in STAGES)==after,
            'E4F_ASSETS_STAGING_UNRESOLVED')


def unique_materialization(env, record_sha):
    root='/repos/'+CONTROL
    status,workflow=github(root+'/actions/workflows/shared-media.yml',env['GH_TOKEN'])
    require(status==200 and gb.positive(workflow['id']),'E4F_NATIVE_RUN')
    status,listing=github(root+f"/actions/workflows/{workflow['id']}/runs?per_page=100",env['GH_TOKEN'])
    title='R18H assets '+record_sha
    rows=[r for r in listing['workflow_runs'] if r['display_title']==title]
    require(status==200 and listing['total_count']==len(listing['workflow_runs']) and
            len(rows)==1 and rows[0]['id']==int(env['GITHUB_RUN_ID']) and
            rows[0]['run_attempt']==1,'E4F_NATIVE_DUPLICATE_WRITER')


def materialize(env, mode, item):
    pins,_,accepted=final_state(env,item)
    record=accepted['latest']
    before=(pins['authority'],pins['witness'])
    after=(record['primary_main'],record['primary_witness'])
    token=env['ASSETS_APP_TOKEN'] if mode=='assets' else None
    node=assets_identity(token)
    current=tuple(assets_ref(r,token) for r in (MAIN,WITNESS))
    if current==after:
        print('E4F_ASSETS_OBSERVED',*after)
        return
    require(mode=='assets' and current==before,'E4F_ASSETS_UNRESOLVED')
    identity=digest(data(record))
    unique_materialization(env,identity)
    stage_assets(accepted['recovered'],token,after)
    provider=GitHubNativeAtomic(token=token,assets_id=ASSETS_ID,assets_node=node,
        actor=gb.CP2B_OWNER_ID,code=env['GITHUB_WORKFLOW_SHA'],ledger_commit=accepted['tip'],
        record_sha256=identity,before=before,after=after)
    variables={'input':{'repositoryId':node,'clientMutationId':digest(data([node,before,after])),
        'refUpdates':[{'name':ref,'beforeOid':a,'afterOid':b,'force':False}
                      for ref,a,b in zip((MAIN,WITNESS),before,after)]}}
    try:
        provider.update_refs(ATOMIC,variables)
    except (ProviderStop,OSError):
        pass
    require(tuple(assets_ref(r,token) for r in (MAIN,WITNESS))==after,
            'E4F_ASSETS_UNRESOLVED')
    print('E4F_ASSETS_OBSERVED',*after)


def dev_input(env, mode, item):
    pins,lab,accepted=final_state(env,item)
    record=accepted['latest']
    pair=(record['primary_main'],record['primary_witness'])
    assets_identity()
    require(tuple(assets_ref(r) for r in (MAIN,WITNESS))==pair,'E4F_DEV_ASSETS_MOVED')
    session=Session(lab,accepted['recovered'],pins['witness'])
    projection=lab.root/'projection'
    session.store.project(projection)
    final=parse(files(accepted['recovered'],pair[0])['automation-state/registry.json'])
    base=parse(files(accepted['recovered'],pins['authority'])['automation-state/registry.json'])
    require(len(base['items'])==45 and all(final['items'].get(k)==v for k,v in base['items'].items()) and
            set(final['items'])-set(base['items'])=={'bgzona-m-00019'} and
            final['items']['bgzona-m-00019']['page_url']=='/poslaniya/p-00019/',
            'E4F_DEV_BASELINE_CHANGED')
    items=[v for v in final['items'].values() if v['state']=='published']
    counts={'published':len(items),'kolaji':sum(v['collection']=='kolaji' for v in items),
            'poslaniya':sum(v['collection']=='poslaniya' for v in items),
            'slides':sum(v['collection']=='kolaji' and v['slideshow'] for v in items)}
    paths=sorted(p for p in projection.rglob('*') if p.is_file())
    payload=BytesIO()
    with ZipFile(payload,'w') as archive:
        for path in paths:
            archive.writestr(ZipInfo(path.relative_to(projection).as_posix()),path.read_bytes(),
                             compress_type=ZIP_DEFLATED)
    raw=payload.getvalue()
    record_sha=digest(data(record))
    name='r18h-dev-'+record_sha+'.zip'
    folder=Path(env['RUNNER_TEMP'])/'r18h-dev-input'
    folder.mkdir()
    if mode=='project':
        immutable_release(env,'r18h-dev-'+record_sha,{name:raw})
        (folder/name).write_bytes(raw)
        print('E4F_DEV_PROJECTED',record_sha,digest(raw))
        return
    release=public_release('r18h-dev-'+record_sha)
    asset,=release['assets']
    require(asset['name']==name and gb.positive(asset['id']) and gb.positive(release['id']) and
            public_asset(release,name,len(raw),64_000_000)==raw,'E4F_DEV_RELEASE_READBACK')
    lock=dict(schema=1,repository=DEV,archive_name=name,archive_sha256=digest(raw),bytes=len(raw),
              members=len(paths),source_catalog_sha256=digest((projection/'artifact/catalog.json').read_bytes()),
              expected_files_sha256=digest((projection/'expected-files.json').read_bytes()),
              snapshot=counts,provider={'release_id':release['id'],'asset_id':asset['id'],'tag':release['tag_name']})
    state=dict(channel='dev',ledger_commit=accepted['tip'],record_sha256=record_sha,
               authority_commit=pair[0],witness_commit=pair[1],assets_tip=pair[0],
               registry_sha256=record['registry_sha256'],input=lock)
    cid=digest(data(state))
    status,ledger=github('/repos/'+CONTROL+'/git/ref/'+LEDGER.removeprefix('refs/'))
    require(status==200 and ledger['ref']==LEDGER and ledger['object']['sha']==accepted['tip'] and
            tuple(assets_ref(r) for r in (MAIN,WITNESS))==pair,'E4F_DEV_STATE_MOVED')
    (folder/'input-trust.json').write_bytes(data(dict(schema=1,cid=cid,control_id=CONTROL_ID,
                  assets_id=ASSETS_ID,ledger_genesis=pins['record_sha256'],accepted=state)))
    print('E4F_DEV_ACCEPTED_CID',cid)


def release_write(env, method, path, body, content_type):
    from urllib.error import HTTPError
    from urllib.request import Request, build_opener
    from e4f_github import NoRedirect
    from urllib.parse import urlsplit

    u=urlsplit(path)
    base='/repos/'+CONTROL+'/releases'
    allowed=(u.netloc=='api.github.com' and
             ((method=='POST' and u.path==base) or (method=='PATCH' and
               u.path.startswith(base+'/') and u.path.rsplit('/',1)[-1].isdigit()))
             or u.netloc=='uploads.github.com' and method=='POST' and
               u.path.startswith(base+'/') and u.path.endswith('/assets') and
               u.path.split('/')[-2].isdigit())
    require(u.scheme=='https' and allowed,'E4F_RELEASE_SCOPE')
    request=Request(path,data=body,method=method,headers={
          'Authorization':'Bearer '+env['GH_TOKEN'],'Content-Type':content_type,
          'Accept':'application/vnd.github+json','X-GitHub-Api-Version':'2026-03-10'})
    try:
        with build_opener(NoRedirect).open(request,timeout=45) as response:
            raw=response.read(1_000_001)
            require(response.status in (200,201) and len(raw)<=1_000_000,'E4F_RELEASE_UNRESOLVED')
            return parse(raw)
    except (OSError,HTTPError):
        raise ProviderStop('E4F_RELEASE_UNRESOLVED') from None


def immutable_release(env, tag, payloads):
    from urllib.parse import quote

    base='/repos/'+CONTROL+'/releases'
    require(all(name=='proposal.json' or name.endswith(('.bundle','.zip')) for name in payloads) and
            all(0<len(raw)<=64_000_000 for raw in payloads.values()),'E4F_RELEASE_SCOPE')
    status,_=github(base+'/tags/'+tag,env['GH_TOKEN'])
    require(status==404,'E4F_RELEASE_EXISTING_STOP')
    record=release_write(env,'POST','https://api.github.com'+base,data({
            'tag_name':tag,'name':tag,'target_commitish':env['GITHUB_WORKFLOW_SHA'],
            'draft':True,'prerelease':False}),'application/json')
    rid=record['id']
    require(gb.positive(rid) and record['tag_name']==tag and record['draft'] is True,
            'E4F_RELEASE_DRAFT')
    for name,raw in sorted(payloads.items()):
        item=release_write(env,'POST','https://uploads.github.com'+base+f'/{rid}/assets?name='+quote(name,safe=''),
                           raw,'application/octet-stream')
        require(item['name']==name and item['size']==len(raw) and item['state']=='uploaded',
                'E4F_RELEASE_UPLOAD')
    release_write(env,'PATCH','https://api.github.com'+base+f'/{rid}',
                  data({'draft':False}),'application/json')
    observed=public_release(tag)
    require(observed['id']==rid and {a['name'] for a in observed['assets']}==set(payloads),
            'E4F_RELEASE_READBACK')
    require(all(public_asset(observed,name,len(raw),64_000_000)==raw for name,raw in payloads.items()),
            'E4F_RELEASE_READBACK')
    return observed


def verify_resume_source(api, env):
    from media_preflight import WORKFLOW as source_path
    from registration_entry import pinned_file
    import push_artifact

    require((env['PRIVATE_WORKFLOW_COMMIT'],int(env['PRIVATE_DISPATCH_ACTOR']))==
            (RESUME_PRIVATE_SHA,RESUME_ACTOR),'E4F_RESUME_WORKER_PIN')
    pins=parse(BOOTSTRAP.read_bytes())
    row,_=api.get(f'/actions/runs/{RESUME_SOURCE_RUN}/attempts/1')
    require((row['id'],row['run_attempt'],row['workflow_id'],row['event'],row['head_sha'],
             row['head_branch'],row['repository']['id'],row['actor']['id'],
             row['path'].split('@',1)[0],row['status'],row['conclusion'])==
            (RESUME_SOURCE_RUN,1,push_artifact.WORKFLOW,'push',RESUME_SOURCE,
             pins['source_ref'].removeprefix('refs/heads/'),PIN.numeric_id,
             gb.CP2B_OWNER_ID,source_path,'completed','success'),'E4F_RESUME_SOURCE_RUN')
    row,_=api.get('/git/ref/'+pins['source_ref'].removeprefix('refs/'))
    require((row['ref'],row['object']['type'],row['object']['sha'])==
            (pins['source_ref'],'commit',RESUME_SOURCE),'E4F_RESUME_SOURCE_REF')
    row,_=api.get('/git/commits/'+RESUME_SOURCE)
    require(row['sha']==RESUME_SOURCE and [p['sha'] for p in row['parents']]==[pins['source_parent']],
            'E4F_RESUME_SOURCE_PARENT')
    workflow=pinned_file(api,source_path,RESUME_SOURCE)
    require(digest(workflow)==RESUME_SOURCE_YAML,'E4F_RESUME_SOURCE_WORKER')
    prepare=pinned_file(api,'.github/workflows/e4f-standard-prepare.yml',RESUME_PRIVATE_SHA)
    prepared_env=parse(prepare)['jobs']['prepare']['env']
    require((prepared_env['WORKER_COMMIT'],prepared_env['MANIFEST_SHA256'],
             prepared_env['TRUSTED_DISPATCH_ACTOR'])==(RESUME_WORKER,RESUME_MANIFEST,str(RESUME_ACTOR)),
            'E4F_RESUME_WORKER_PIN')
    artifact,sha=RESUME_SOURCE_ARTIFACT
    row,_=api.get(f'/actions/artifacts/{artifact}')
    link=row['workflow_run']
    require((row['id'],row['name'],row['expired'],row['digest'],link['id'],link['repository_id'],
             link['head_sha'])==(artifact,push_artifact.name(RESUME_SOURCE_RUN,1),False,'sha256:'+sha,
             RESUME_SOURCE_RUN,PIN.numeric_id,RESUME_SOURCE),'E4F_RESUME_SOURCE_ARTIFACT')


def verify_resume_refs(api, item, pins):
    from private_queue import request_identity
    from queued_pipeline import _handoff_slot

    h=item['handoff']
    request=request_identity(PIN.numeric_id,pins['source_parent'],RESUME_SOURCE)
    require((h['request'],h['mirror'],h['head'],h['parent'],item['ledger_parent'],
             digest(data(item['record'])))==(request,pins['record_sha256'],RESUME_HANDOFF,
             RESUME_REGISTRATION,pins['ledger'],RESUME_CANDIDATE),'E4F_RESUME_HANDOFF')
    for child,parent in ((RESUME_REGISTRATION,pins['recovery_bootstrap']),
                         (RESUME_HANDOFF,RESUME_REGISTRATION)):
        row,_=api.get('/git/commits/'+child)
        require(row['sha']==child and [p['sha'] for p in row['parents']]==[parent],
                'E4F_RESUME_RECOVERY_ANCESTRY')
    _,slot=_handoff_slot(request,pins['record_sha256'])
    listing,_=api.get('/git/trees/'+row['tree']['sha']+'?recursive=1')
    blobs={x['path']:x['size'] for x in listing['tree'] if x['type']=='blob'}
    require(listing['truncated'] is False and len(blobs)==3 and
            blobs.get(slot)==h['size'] and 12_000_000<h['size']<=16_000_000,
            'E4F_RESUME_RECOVERY_TREE')
    row,_=api.get('/git/ref/'+pins['recovery_ref'].removeprefix('refs/'))
    require((row['ref'],row['object']['type'],row['object']['sha'])==
            (pins['recovery_ref'],'commit',RESUME_HANDOFF),'E4F_RESUME_RECOVERY_REF')
    status,row=github('/repos/'+CONTROL+'/git/ref/'+LEDGER.removeprefix('refs/'))
    require(status==200 and (row['ref'],row['object']['type'],row['object']['sha'])==
            (LEDGER,'commit',pins['ledger']),'E4F_RESUME_LEDGER_REF')


def private_candidate(env, mode, source_run, previous, *, existing_run=None):
    import time
    from push_probe import GitHub
    from registration_entry import read_archive
    from stat import S_IFMT, S_IFREG
    from e4f_github import ledger_cas_payload

    token=env['PRIVATE_ACTIONS_TOKEN']
    api=GitHub(token)
    worker_commit=env['PRIVATE_WORKFLOW_COMMIT']
    private_path='.github/workflows/e4f-standard-'+mode+'.yml'
    path='/actions/workflows/'+private_path.rsplit('/',1)[1]
    wf,_=api.get(path)
    require(gb.oid(worker_commit) and wf['path']==private_path and wf['state']=='active' and
            gb.positive(wf['id']),'E4F_PRIVATE_PREPARE_PIN')
    args={'source_run':str(source_run)} if mode=='prepare' else {'previous_record':previous}
    if existing_run is not None:
        require((mode,source_run,existing_run)==('prepare',RESUME_SOURCE_RUN,RESUME_PRIVATE_RUN),
                'E4F_RESUME_SCOPE')
        verify_resume_source(api,env)
        run=existing_run
    else:
        prefix='/repos/'+PIN.name
        status,resp=github(prefix+f"/actions/workflows/{wf['id']}/dispatches",token,
                           {'ref':'main','inputs':args,'return_run_details':True})
        require(status==200 and gb.positive(resp['workflow_run_id']),'E4F_PRIVATE_DISPATCH_UNRESOLVED')
        run=resp['workflow_run_id']
    end=time.monotonic()+360
    title='R18H '+mode+' '+next(iter(args.values()))
    while True:
        try:
            row,_=api.get(f'/actions/runs/{run}/attempts/1')
        except OSError as exc:
            if getattr(exc,'code',None)!=404 or time.monotonic()>=end:
                raise ProviderStop('E4F_PRIVATE_RUN_UNRESOLVED') from None
            time.sleep(3)
            continue
        require(row['id']==run,'E4F_PRIVATE_RUN_PIN')
        if row['status']=='completed':
            require((row['run_attempt'],row['workflow_id'],row['event'],row['head_sha'],
                     row['head_branch'],row['repository']['id'],row['actor']['id'],
                     row['path'].split('@',1)[0],row['display_title'])==
                    (1,wf['id'],'workflow_dispatch',worker_commit,'main',PIN.numeric_id,
                     int(env['PRIVATE_DISPATCH_ACTOR']),private_path,title),'E4F_PRIVATE_RUN_PIN')
            require(row['conclusion']=='success','E4F_PRIVATE_PREPARE_STOP')
            break
        # A run ID alone grants no trust. Allow only bounded GETs until final metadata.
        require(existing_run is None and time.monotonic()<end,'E4F_PRIVATE_RUN_UNRESOLVED')
        time.sleep(3)
    jobs,_=api.get(f'/actions/runs/{run}/attempts/1/jobs')
    require(jobs['total_count']==len(jobs['jobs'])==1 and
            (jobs['jobs'][0]['name'],jobs['jobs'][0]['conclusion'])==('prepare','success'),
            'E4F_PRIVATE_PREPARE_JOB')
    listing,more=api.get(f'/actions/runs/{run}/artifacts?per_page=100')
    matches=[a for a in listing['artifacts'] if a['name']==f'r18h-d3-candidate-{run}']
    require(not more and len(matches)==1,'E4F_PRIVATE_ARTIFACT')
    asset=matches[0]
    link=asset['workflow_run']
    limit=16_000_000
    require(gb.positive(asset['id']) and asset['expired'] is False and
            0<asset['size_in_bytes']<=limit and
            (link['id'],link['repository_id'],link['head_sha'])==(run,PIN.numeric_id,worker_commit),
            'E4F_PRIVATE_ARTIFACT')
    if existing_run is not None:
        artifact,sha=RESUME_PRIVATE_ARTIFACT
        require((asset['id'],asset['digest'])==(artifact,'sha256:'+sha),'E4F_RESUME_ARTIFACT_PIN')
    wire=read_archive(api,asset['id'],limit=limit)
    require(len(wire)==asset['size_in_bytes'] and asset['digest']=='sha256:'+digest(wire),
            'E4F_PRIVATE_ARTIFACT_DIGEST')
    with ZipFile(BytesIO(wire)) as archive:
        rows=archive.infolist()
        require(len(rows)==2 and all(S_IFMT(r.external_attr>>16) in (0,S_IFREG) and
                    0<r.file_size<=limit for r in rows),'E4F_PRIVATE_ARTIFACT_SHAPE')
        manifest=[r for r in rows if r.filename=='proposal.json']
        require(len(manifest)==1 and manifest[0].file_size<=32_000,'E4F_PRIVATE_ARTIFACT_SHAPE')
        blob=archive.read(manifest[0])
        item=parse(blob)
        require(data(item)==blob and set(item)=={'schema','source_run','ledger_parent','previous','record','handoff'}
                and item['schema']==1 and item['source_run']==source_run,'E4F_PRIVATE_PROPOSAL')
        record=item['record']
        if mode=='next':
            require(record['index']==2 and digest(data(item['previous']))==previous and
                    record['phase']=='staged','E4F_PRIVATE_PROPOSAL')
        else:
            require(record['index']==1 and record['phase']=='accepted','E4F_PRIVATE_PROPOSAL')
        ledger_cas_payload(item['ledger_parent'],item['previous'],record)
        backup=record['backup']
        files_=[r for r in rows if r.filename==backup['file']]
        require(len(files_)==1 and files_[0].file_size==backup['bytes'],'E4F_PRIVATE_BACKUP')
        raw=archive.read(files_[0])
    require(digest(raw)==backup['sha256'],'E4F_PRIVATE_BACKUP')
    pins=parse(BOOTSTRAP.read_bytes())
    lab=Lab.create(Path(env['RUNNER_TEMP'])/'r18h-control-proposal')
    domain=public_baseline(lab,pins)
    ctx=domain.read()
    require(ctx['tip']==item['ledger_parent'] and ctx['latest']==item['previous'],
            'E4F_PRIVATE_PROPOSAL_BASE')
    (domain.archives/backup['file']).write_bytes(raw)
    require(domain.verify_backup(ctx,record)['recoverable'],'E4F_PRIVATE_BACKUP')
    identity=digest(data(record))
    if existing_run is not None:
        verify_resume_refs(api,item,pins)
    immutable_release(env,'r18h-d3-'+identity,{'proposal.json':blob,backup['file']:raw})
    print('E4F_D3_PROPOSAL_READY',identity)


def main(env):
    params=context(env)
    mode=params['mode']
    if mode=='resume-prepare':
        private_candidate(env,'prepare',int(params['source_run']),None,
                          existing_run=int(params['private_run']))
        return
    if mode=='prepare':
        private_candidate(env,mode,int(params['source_run']),None)
        return
    item=proposal(params['record_sha256'])
    if mode=='next':
        private_candidate(env,mode,item['source_run'],params['record_sha256'])
    elif mode in ('accept','recover'):
        attester=attach_guard(env)
        token=env['GH_TOKEN'] if mode=='accept' else None
        ledger=GitHubLedgerAppend(token,attester)
        action=ledger.append if mode=='accept' else ledger.recover
        status,commit=action(item['ledger_parent'],item['previous'],item['record'])
        print('E4F_LEDGER_READBACK',status,commit,params['record_sha256'])
    elif mode in ('assets','assets-read'):
        materialize(env,mode,item)
    else:
        require(mode in ('project','dev-pin'),'E4F_CONTROL_COMMAND')
        dev_input(env,mode,item)


if __name__=='__main__':
    try:
        main(os.environ)
    except ProviderStop as exc:
        print('STOP '+str(exc),file=sys.stderr)
        sys.exit(1)
    except Exception:
        print('STOP E4F_CONTROL_UNRESOLVED',file=sys.stderr)
        sys.exit(1)
