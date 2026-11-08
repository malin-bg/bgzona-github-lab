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
                        GitHubNativeAtomic, PrivateGuardAttester, ProviderStop, RESUME_CANDIDATE,
                        github, require)
from git_transport import Lab, MAIN, WITNESS, Session, files
from importer import data, digest, parse
from standard_entry import (BOOTSTRAP, PIN, c3_control_run, control_run, proposal, public_asset,
                            public_baseline, public_release)

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
    require(params.get("schedule_run","")=="","E4F_CONTROL_INPUT")
    params.pop("schedule_run",None)
    require(type(params) is dict and 'mode' in params and
            set(params)<={'mode','record_sha256','source_run','private_run'}, 'E4F_CONTROL_INPUT')
    params={'record_sha256':'','source_run':'','private_run':''}|params
    if params['mode']=='c3':
        require(all(params[k]=='' for k in ('source_run','private_run','record_sha256')), 'C3_CONTROL_INPUT')
        return params
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


def attach_guard(env, *, c3=False):
    token=env['PRIVATE_ACTIONS_TOKEN']
    rows=[]
    for name in ('e4f-d3-guard.yml','e4f-d3-guard-observe.yml'):
        status,row=github(f'/repos/{PIN.name}/actions/workflows/{name}',token)
        require(status==200 and row['path']=='.github/workflows/'+name and
                row['state']=='active' and gb.positive(row['id']),'E4F_GUARD_WORKFLOW')
        rows.append(row['id'])
    require(not c3 or rows==[408733315,408733314], 'C3_GUARD_WORKFLOW_IDS')
    return PrivateGuardAttester(token,rows[0],env['PRIVATE_WORKFLOW_COMMIT'],
        int(env['PRIVATE_DISPATCH_ACTOR']),int(env['GITHUB_RUN_ID']),observe_workflow_id=rows[1])


def final_state(env, item, *, c3=False, normal=False):
    pins=parse(BOOTSTRAP.read_bytes())
    index=item['record']['index'] if normal else (3 if c3 else 2)
    require(item['record']['index']==index and item['record']['phase']=='staged','E4F_FINAL_D3_RECORD')
    lab=Lab.create(Path(env['RUNNER_TEMP'])/'r18h-control-final')
    domain=public_baseline(lab,pins,normal=normal)
    accepted=domain.read()
    require(len(accepted['records'])==index+1 and accepted['latest']==item['record'] and
            accepted['records'][index-1]==item['previous'],'E4F_FINAL_D3_RECORD')
    if normal:
        from request_executor import binding
        bound=binding(accepted["recovered"],item["record"]["primary_main"])
        require(bound is not None and bound["final"] and bound["phase"]=="staged",
                "AUTO_FINAL_REQUIRED")
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


def stage_assets(repo, token, after, before=(None,None)):
    current=tuple(assets_ref(r,token,missing=True) for r in STAGES)
    if current==after:
        return
    require(current==before,'E4F_ASSETS_STAGING_CONFLICT')
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


def materialize(env, mode, item, *, c3=False, normal=False):
    pins,_,accepted=final_state(env,item,c3=c3,normal=normal)
    record=accepted['latest']
    baseline=pins['c3'] if c3 else pins
    before=(baseline['authority'],baseline['witness'])
    if normal:
        from auto_intake import last_final
        predecessor=last_final(accepted,exclude_latest=True)
        before=(predecessor["primary_main"],predecessor["primary_witness"])
    after=(record['primary_main'],record['primary_witness'])
    token=env['ASSETS_APP_TOKEN'] if mode=='assets' else None
    node=assets_identity(token)
    current=tuple(assets_ref(r,token) for r in (MAIN,WITNESS))
    if current==after:
        print('E4F_ASSETS_OBSERVED',*after)
        return
    require(mode=='assets' and current==before,'E4F_ASSETS_UNRESOLVED')
    identity=digest(data(record))
    if normal:
        from auto_intake import control_run as auto_control
        auto_control(int(env["GITHUB_RUN_ID"]))
    elif c3:
        c3_control_run(int(env['GITHUB_RUN_ID']),pins)
    else:
        unique_materialization(env,identity)
    stage_assets(accepted['recovered'],token,after,before if (c3 or normal) else (None,None))
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


def dev_input(env, mode, item, *, c3=False, normal=False):
    pins,lab,accepted=final_state(env,item,c3=c3,normal=normal)
    record=accepted['latest']
    pair=(record['primary_main'],record['primary_witness'])
    assets_identity()
    require(tuple(assets_ref(r) for r in (MAIN,WITNESS))==pair,'E4F_DEV_ASSETS_MOVED')
    session=Session(lab,accepted['recovered'],pins['witness'])
    projection=lab.root/'projection'
    session.store.project(projection)
    final=parse(files(accepted['recovered'],pair[0])['automation-state/registry.json'])
    baseline=pins['c3'] if c3 else pins
    base=parse(files(accepted['recovered'],baseline['authority'])['automation-state/registry.json'])
    if normal:
        from auto_intake import stable_registry
        stable_registry(base,final)
    elif c3:
        target='bgzona-m-00019'
        current=final['items'][target]
        old=base['items'][target]
        require(len(base['items'])==46 and
                final['items']==base['items'] | {target:old | {
                    'description':current['description'],'description_hash':baseline['txt_sha256']}} and
                final['counters']==base['counters'], 'C3_PROJECTION_METADATA_ONLY')
    else:
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
    if normal:
        from auto_intake import last_final
        prior=last_final(accepted,exclude_latest=True)
        old_registry=parse(files(accepted["recovered"],prior["primary_main"])["automation-state/registry.json"])
        catalog=parse((projection/"artifact/catalog.json").read_bytes())
        items=[]
        for row in catalog["items"]:
            key=row["id"]
            if row["state"]!="published" or final["items"][key]==old_registry["items"].get(key): continue
            old=old_registry["items"].get(key)
            current=final["items"][key]
            changed_original=old is None or old["revisions"][-1]["sha256"]!=current["revisions"][-1]["sha256"]
            changed_og=changed_original or (old or {}).get("og")!=current["og"]
            public_row=row|dict(clean_original=changed_original,clean_og=changed_og)
            if current["og"]: public_row["og_sha256"]=current["og"]["sha256"]
            items.append(public_row)
        (folder/"public-verification.json").write_bytes(data(dict(cid=cid,items=items)))
    print('E4F_DEV_ACCEPTED_CID',cid)
    return (folder/'input-trust.json').read_bytes()


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


def private_candidate(env, mode, source_run, previous, *, existing_run=None, c3=False, normal=False):
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
    if normal:
        require(mode=="prepare" and existing_run is None and wf["id"]==408733317,"AUTO_PREPARE_WORKFLOW")
        args.update(route="auto",control_run=env["GITHUB_RUN_ID"])
    if c3:
        require(mode=='prepare' and existing_run is None and wf['id']==408733317, 'C3_PREPARE_WORKFLOW')
        args['control_run']=env['GITHUB_RUN_ID']
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
        if normal:
            require(record["index"]==item["previous"]["index"]+1 and
                    record["phase"] in ("accepted","staged"),"AUTO_PRIVATE_PROPOSAL")
        elif c3:
            pins=parse(BOOTSTRAP.read_bytes())['c3']
            require((item['ledger_parent'],digest(data(item['previous'])),record['index'],record['phase'],
                     record['primary_base'])==(pins['ledger'],pins['record_sha256'],3,'staged',pins['authority']),
                     'C3_PRIVATE_PROPOSAL')
        elif mode=='next':
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
    domain=public_baseline(lab,pins,normal=normal)
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
    return item


def c3_source(env, pins):
    import time
    from push_probe import GitHub
    from registration_entry import pinned_file

    api=GitHub(env['PRIVATE_ACTIONS_TOKEN'])
    path='/git/ref/heads/cp2b-private-standard-source-r1'
    parent=env['C3_EDITORIAL_PARENT']
    baseline=pins['c3']
    def source_ref():
        row,_=api.get(path)
        require(row['ref']==pins['source_ref'] and row['object']['type']=='commit' and
                gb.oid(row['object']['sha']), 'C3_SOURCE_REF')
        return row['object']['sha']
    require(gb.oid(parent) and source_ref()==parent,'C3_ARM_BEFORE_PUSH')
    row,_=api.get('/git/ref/'+baseline['recovery_ref'].removeprefix('refs/'))
    require(row['ref']==baseline['recovery_ref'] and row['object']['type']=='commit' and
            row['object']['sha']==pins['recovery_bootstrap'], 'C3_RECOVERY_NOT_FRESH')
    status,ledger=github('/repos/'+CONTROL+'/git/ref/'+LEDGER.removeprefix('refs/'))
    require(status==200 and ledger['ref']==LEDGER and ledger['object']['sha']==baseline['ledger']
            and tuple(assets_ref(r) for r in (MAIN,WITNESS))==(baseline['authority'],baseline['witness']),
            'C3_ARM_BASELINE')
    query='/actions/workflows/407190822/runs?event=push&branch=cp2b-private-standard-source-r1&per_page=100'
    print('C3_ARMED',parent,flush=True)
    deadline=time.monotonic()+420
    pinned=None
    while time.monotonic()<deadline:
        head=source_ref()
        if head!=parent:
            require(pinned in (None,head),'C3_CONCURRENT_PUSH')
            pinned=head
            commit,_=api.get('/git/commits/'+head)
            require(commit['sha']==head and [p['sha'] for p in commit['parents']]==[parent],'C3_SOURCE_PARENT')
            delta,more=api.get('/compare/'+parent+'...'+head)
            require(not more and delta['total_commits']==1 and
                    [(f['filename'],f['status']) for f in delta['files']]==[(baseline['txt'],'modified')],
                    'C3_SOURCE_DELTA')
            require(digest(pinned_file(api,baseline['txt'],head))==baseline['txt_sha256'],'C3_TXT_HASH')
            listing,more=api.get(query)
            hits=[r for r in listing['workflow_runs'] if r['head_sha']==head]
            require(not more and len(hits)<=1,'C3_SOURCE_RUN_COUNT')
            if hits and hits[0]['status']=='completed':
                row=hits[0]
                require((row['workflow_id'],row['run_attempt'],row['repository']['id'],row['head_branch'],
                         row['path'].split('@')[0],row['event'],row['actor']['id'],row['conclusion'])==
                        (407190822,1,PIN.numeric_id,'cp2b-private-standard-source-r1',
                         '.github/workflows/cp2b-push-proof.yml','push',gb.CP2B_OWNER_ID,'success')
                        and source_ref()==head, 'C3_SOURCE_RUN')
                return row['id']
        else:
            require(pinned is None,'C3_SOURCE_REWOUND')
        time.sleep(8)
    raise ProviderStop('C3_ARM_EXPIRED')


def c3_dispatch_dev(env, trust):
    import time

    token=env['DEV_ACTIONS_TOKEN']
    dev_sha=env['C3_DEV_MAIN']
    cid=parse(trust)['cid']
    require(gb.oid(dev_sha) and gb.sha(cid) and 0<len(trust)<=8192,'C3_DEV_INPUT')
    root='/repos/'+DEV
    status,branch=github(root+'/git/ref/heads/main',token)
    require(status==200 and branch['ref']=='refs/heads/main' and branch['object']['sha']==dev_sha,'C3_DEV_HEAD')
    status,wf=github(root+'/actions/workflows/232539432',token)
    require(status==200 and (wf['id'],wf['path'],wf['state'])==
            (232539432,'.github/workflows/hugo.yml','active'), 'C3_DEV_TARGET')
    status,row=github(root+'/actions/workflows/232539432/dispatches',token,
                      {'ref':'main','inputs':{'accepted_cid':cid,'input_trust':trust.decode('utf-8')},
                       'return_run_details':True})
    require(status==200 and type(row) is dict and gb.positive(row.get('workflow_run_id')),
            'C3_DEV_DISPATCH_UNRESOLVED')
    run=row['workflow_run_id']
    print('C3_DEV_RUN',run,cid,flush=True)
    deadline=time.monotonic()+660
    while time.monotonic()<deadline:
        status,r=github(root+f'/actions/runs/{run}',token)
        require(status in (200,404),'C3_DEV_RUN_UNRESOLVED')
        if status==200 and r['status']=='completed':
            require((r['id'],r['workflow_id'],r['run_attempt'],r['repository']['id'],r['head_branch'],
                     r['head_sha'],r['actor']['id'],r['event'],r['display_title'],r['conclusion'])==
                    (run,232539432,1,1149689018,'main',dev_sha,353092585,'workflow_dispatch','DEV '+cid,
                     'success'),'C3_DEV_RUN_RESULT')
            status,j=github(root+f'/actions/runs/{run}/attempts/1/jobs',token)
            require(status==200 and {x['name']:x['conclusion'] for x in j['jobs']}==
                    {'build':'success','deploy':'success'} and any(
                        step['name']=='Verify public DEV CID, page and OG against the seal' and
                        step['conclusion']=='success' for x in j['jobs'] if x['name']=='deploy'
                        for step in x['steps']), 'C3_DEV_PUBLIC_READBACK')
            print('C3_PUBLIC_DEV_VERIFIED',cid,run,flush=True)
            return
        time.sleep(8)
    raise ProviderStop('C3_DEV_RUN_UNRESOLVED')


def c3_main(env):
    pins=parse(BOOTSTRAP.read_bytes())
    c3_control_run(int(env['GITHUB_RUN_ID']),pins)
    phase=env['C3_PHASE']
    if phase=='intake':
        run=c3_source(env,pins)
        item=private_candidate(env,'prepare',run,None,c3=True)
        key=digest(data(item['record']))
        require(proposal(key,c3=True)==item,'C3_PROPOSAL_READBACK')
        ledger=GitHubLedgerAppend(env['GH_TOKEN'],attach_guard(env,c3=True))
        status,commit=ledger.append(item['ledger_parent'],item['previous'],item['record'])
        require(status in ('APPENDED','OBSERVED') and gb.oid(commit),'C3_D3_READBACK')
        with open(env['GITHUB_OUTPUT'],'a',encoding='ascii') as f:
            f.write('record='+key+'\n')
        return
    key=env['C3_RECORD']
    item=proposal(key,c3=True)
    require(item['ledger_parent']==pins['c3']['ledger'] and
            digest(data(item['previous']))==pins['c3']['record_sha256'], 'C3_D3_PREDECESSOR')
    if phase=='assets':
        materialize(env,'assets',item,c3=True)
    else:
        require(phase=='dev','C3_CONTROL_PHASE')
        for mode in ('project','dev-pin'):
            temp=Path(env['RUNNER_TEMP'])/('c3-'+mode)
            temp.mkdir()
            raw=dev_input(env|{'RUNNER_TEMP':str(temp)},mode,item,c3=True)
        c3_dispatch_dev(env,raw)


def main(env):
    inputs=parse(Path(env["GITHUB_EVENT_PATH"]).read_bytes()).get("inputs",{})
    if inputs.get("mode") in ("auto", "auto-resume", "f1-resume"):
        from auto_intake import main as auto_main
        return auto_main(env)
    params=context(env)
    mode=params['mode']
    if mode=='c3':
        c3_main(env)
        return
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
        if mode=='accept' and params['record_sha256']==RESUME_CANDIDATE:
            status,commit=ledger.append_reserved(item,env['GITHUB_WORKFLOW_SHA'])
        else:
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
