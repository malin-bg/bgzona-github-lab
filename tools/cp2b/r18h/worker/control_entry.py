"""Hosted independent ledger writer, Assets materialization and accepted DEV input."""
from pathlib import Path
from io import BytesIO
from zipfile import ZipFile, ZipInfo, ZIP_DEFLATED
import base64
import json
import os
import subprocess
import sys
from uuid import uuid4

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
PROD='malin-bg/bgzona-win'
PROD_ID=1128312231
PROD_WORKFLOW=249807408
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

# One owner-approved consumer completion of the independently staged R4 result.
PROD_R4=dict(writer=42904773089,writer_sha='045e25e873248f078bf084fa565dafe06d89b396',
    previous_run=42907618181,previous_sha='cf903a04b0730721e13d4c8dc6d22af918d97531',
    ledger='90430d4be6280869992dcff046a66ad079b44ebf',record='2ac6ee4df8e427e380bcbeb840ab28a552dbbdab5e05d11fb765727f0b021a0b',
    assets='ac94046822d9fba54f41141c8da1e304ea2c83a2',witness='f883b5e50661eae0dc251037ec49bd64e4ecc7b1',
    registry='2e4cb59d276fe1dda1a0b46a27606a28d0e78a20badecb18669cdd5a22317493',
    dev_cid='6cfd3430adf5612a3a3b4896516e29844a5a49ecc6af320b7a1b809324bcb64f',
    cid='738478204838ae1a4ceae2176ff35c1995e257dfea49844fb3cb64dc0a70d2bd',
    old_cid='350b2d579fb56b5ca8394f40cefca5873700ebe1b1573aebdc9689a57df517c3',
    old_registry='89947b1091b14cd7f19c13671d3204edd0b9630635f1148a740d516b580909b9',
    old_pages=42838778343,old_pages_sha='caec5cf004778ebaef0855d401a58e4683ba65ee',
    deployment=7685475408,release=440945104,asset=733050580,
    archive='c406e4352cfe8b8f993540adc9da9166c082a936b698bacba0a27ed683146045',
    workflow='21d9e7c94ee9c194b7fc4a746af94c57fac3408e3e324bfdcfb31a4f33b82bf5')
PROD_COMPLETION_ROLE="${{ inputs.mode == 'prod' && 'true' || 'false' }}"


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


def temporary_lab(env, label):
    require(label in ('r18h-control-final','r18h-control-proposal'),'E4F_TEMP_SCOPE')
    return Lab.create(Path(env['RUNNER_TEMP'])/(label.rsplit('-',1)[-1]+'-'+uuid4().hex))

def final_state(env, item, *, c3=False, normal=False):
    pins=parse(BOOTSTRAP.read_bytes())
    index=item['record']['index'] if normal else (3 if c3 else 2)
    require(item['record']['index']==index and item['record']['phase']=='staged','E4F_FINAL_D3_RECORD')
    lab=temporary_lab(env,'r18h-control-final')
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


def dev_input(env, mode, item, *, c3=False, normal=False, all_public=False):
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
            if row["state"]!="published" or not all_public and final["items"][key]==old_registry["items"].get(key): continue
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


def verify_m1_completed_prepare(api, env, run):
    import auto_intake as auto
    from registration_entry import pinned_file
    require(run==auto.M1_FINAL_PREPARE and int(env['GITHUB_RUN_NUMBER'])==auto.M1_NUMBER and
            env['PRIVATE_DISPATCH_ACTOR']=='353092585','M1_COMPLETED_PREPARE_SCOPE')
    row=auto.control_run(int(env['GITHUB_RUN_ID']),auto.M1_SOURCE,auto.M1_RELAY,
                         private_api=api,private_main=env['PRIVATE_WORKFLOW_COMMIT'])
    require(row['source_sha']==auto.M1_SOURCE_SHA and row['head_sha']==env['GITHUB_WORKFLOW_SHA'],
            'M1_COMPLETED_CONTROL_PIN')
    for ref,want in (('heads/main',env['PRIVATE_WORKFLOW_COMMIT']),
                     ('heads/cp2b-private-standard-source-r1',auto.M1_SOURCE_SHA),
                     ('heads/cp2b-private-standard-recovery-c2',auto.M1_RECOVERY_STEP8)):
        head,_=api.get('/git/ref/'+ref)
        require(head['ref']=='refs/'+ref and head['object']['type']=='commit' and
                head['object']['sha']==want,'M1_COMPLETED_PRIVATE_STATE')
    require(auto.public('/git/ref/heads/automation-ledger')['object']['sha']==auto.M1_ACCEPTED_LEDGER,
            'M1_ACCEPTED_PREFIX_MOVED')
    for ref,want in ((MAIN,'923d8f8f67361ca2f725a232eaba02a8cac20bcc'),
                     (WITNESS,'50e4a18cf85943114687f031fe03b556b48ac52e'),
                     (STAGES[0],'923d8f8f67361ca2f725a232eaba02a8cac20bcc'),
                     (STAGES[1],'50e4a18cf85943114687f031fe03b556b48ac52e')):
        require(assets_ref(ref)==want,'M1_COMPLETED_ASSETS_STATE')
    # Only workflow code is read here. PRIVATE handoff bytes stay on PRIVATE runners.
    code=parse(pinned_file(api,'.github/workflows/e4f-standard-prepare.yml',auto.M1_FINAL_PRIVATE))
    worker=code['jobs']['prepare']['env']
    require((worker['WORKER_COMMIT'],worker['MANIFEST_SHA256'],worker['TRUSTED_DISPATCH_ACTOR'])==
            (auto.M1_FINAL_WORKER,auto.M1_FINAL_MANIFEST,'353092585'),'M1_COMPLETED_IMMUTABLE_WORKER')

def private_candidate(env, mode, source_run, previous, *, existing_run=None, c3=False, normal=False):
    import time
    from push_probe import GitHub
    from registration_entry import read_archive
    from stat import S_IFMT, S_IFREG
    from e4f_github import ledger_cas_payload

    token=env['PRIVATE_ACTIONS_TOKEN']
    api=GitHub(token)
    worker_commit=env['PRIVATE_WORKFLOW_COMMIT']
    import auto_intake as auto
    m1_resume=normal and (mode,source_run,existing_run)==('prepare',auto.M1_SOURCE,auto.M1_FINAL_PREPARE)
    p4_resume=normal and (mode,source_run,existing_run)==('prepare',auto.P4_SOURCE,auto.P4_PRIVATE_RUN)
    private_path='.github/workflows/e4f-standard-'+mode+'.yml'
    path='/actions/workflows/'+private_path.rsplit('/',1)[1]
    wf,_=api.get(path)
    require(gb.oid(worker_commit) and wf['path']==private_path and wf['state']=='active' and
            gb.positive(wf['id']),'E4F_PRIVATE_PREPARE_PIN')
    args={'source_run':str(source_run)} if mode=='prepare' else {'previous_record':previous}
    if normal:
        require(mode=="prepare" and (existing_run is None or m1_resume or p4_resume) and wf["id"]==408733317,"AUTO_PREPARE_WORKFLOW")
        args.update(route="auto",control_run=env["GITHUB_RUN_ID"])
    if c3:
        require(mode=='prepare' and existing_run is None and wf['id']==408733317, 'C3_PREPARE_WORKFLOW')
        args['control_run']=env['GITHUB_RUN_ID']
    if p4_resume:
        verify_p4_completed(api,env,existing_run)
        worker_commit=auto.P4_ORIGIN_MAIN;run=existing_run
    elif m1_resume:
        verify_m1_completed_prepare(api,env,existing_run)
        worker_commit=auto.M1_FINAL_PRIVATE
        run=existing_run
    elif existing_run is not None:
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
    from e4f_github import wait_actions_run,PRIVATE_JOB_SECONDS
    title='R18H '+mode+' '+next(iter(args.values()))
    def read_run():
        try:return 200,api.get(f'/actions/runs/{run}/attempts/1')[0]
        except OSError as exc:
            code=getattr(exc,'code',None)
            if code is not None:return code,None
            raise
    row=wait_actions_run(read_run,run,'E4F_PRIVATE_RUN',PRIVATE_JOB_SECONDS)
    require((row['run_attempt'],row['workflow_id'],row['event'],row['head_sha'],
             row['head_branch'],row['repository']['id'],row['actor']['id'],
             row['path'].split('@',1)[0],row['display_title'])==
            (1,wf['id'],'workflow_dispatch',worker_commit,'main',PIN.numeric_id,
             int(env['PRIVATE_DISPATCH_ACTOR']),private_path,title),'E4F_PRIVATE_RUN_PIN')
    require(row['conclusion']=='success','E4F_PRIVATE_PREPARE_STOP')
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
    if p4_resume:
        require((asset['id'],asset['digest'])==(auto.P4_ARTIFACT,'sha256:'+auto.P4_ARTIFACT_SHA),'P4_COMPLETED_ARTIFACT_PIN')
    elif m1_resume:
        require((asset['id'],asset['digest'])==(auto.M1_FINAL_ARTIFACT,'sha256:'+auto.M1_FINAL_ARTIFACT_SHA),
                'M1_COMPLETED_ARTIFACT_PIN')
    elif existing_run is not None:
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
        require(data(item)==blob and set(item) in ({'schema','source_run','ledger_parent','previous','record','handoff'},
                    {'schema','source_run','ledger_parent','previous','record','handoff','execution'})
                and item['schema']==1 and item['source_run']==source_run,'E4F_PRIVATE_PROPOSAL')
        record=item['record']
        if p4_resume:
            require(digest(blob)==auto.P4_PROPOSAL and digest(data(record))==auto.P4_RECORD11 and
                    item['ledger_parent']==auto.P4_PREFIX and digest(data(item['previous']))==auto.P4_RECORD10,'P4_COMPLETED_CANDIDATE_PIN')
        elif m1_resume:
            require(digest(blob)==auto.M1_FINAL_PROPOSAL and digest(data(record))==auto.M1_FINAL_RECORD and
                    item['handoff']==auto.M1_HANDOFF and item['ledger_parent']==auto.M1_ACCEPTED_LEDGER and
                    digest(data(item['previous']))==auto.M1_PREFIX_RECORD,'M1_COMPLETED_CANDIDATE_PIN')
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
    lab=temporary_lab(env,'r18h-control-proposal')
    domain=public_baseline(lab,pins,normal=normal)
    ctx=domain.read()
    require(ctx['tip']==item['ledger_parent'] and ctx['latest']==item['previous'],
            'E4F_PRIVATE_PROPOSAL_BASE')
    (domain.archives/backup['file']).write_bytes(raw)
    require(domain.verify_backup(ctx,record)['recoverable'],'E4F_PRIVATE_BACKUP')
    identity=digest(data(record))
    if 'execution' in item:
        from request_executor import binding
        bound=binding(ctx['recovered'],record['primary_main']);hint=item['execution']
        require(set(hint)=={'schema','request','plan','steps','transitions','remaining'} and hint['schema']==1 and
                (hint['request'],hint['plan'],hint['steps'])==(bound['request'],bound['plan'],bound['steps']) and
                type(hint['remaining']) is int and type(hint['transitions']) is int and
                bound['steps']<=hint['transitions']<=2*bound['steps'] and
                0<=hint['remaining']<hint['transitions'] and bound['final']==(hint['remaining']==0) and
                record['index']+hint['remaining']<=255,'AUTO_EXECUTION_HINT')
    if p4_resume:
        from request_executor import binding
        bound=binding(ctx['recovered'],record['primary_main'])
        require((bound['request'],bound['plan'],bound['step'],bound['steps'],bound['phase'],bound['final'])==
                (auto.P4_REQUEST,auto.P4_PLAN,2,2,'accepted',False),'P4_COMPLETED_E4G_PIN')
    elif m1_resume:
        from request_executor import binding
        require(len(ctx['records'])==9 and digest(data(ctx['records'][7]))==auto.M1_RECORD and
                digest(data(ctx['records'][8]))==auto.M1_PREFIX_RECORD,'M1_COMPLETED_ACCEPTED_PREFIX')
        bound=binding(ctx['recovered'],record['primary_main'])
        require(bound is not None and (bound['request'],bound['plan'],bound['step'],bound['steps'],bound['phase'],bound['final'])==
                (auto.M1_MARKER['request'],auto.M1_PLAN,2,2,'staged',True),'M1_COMPLETED_E4G_FINAL')
        verify_m1_completed_prepare(api,env,existing_run)
    elif existing_run is not None:
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
    from e4f_github import wait_actions_run
    r=wait_actions_run(lambda:github(root+f'/actions/runs/{run}',token),run,'C3_DEV_RUN',25*60)
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



def prod_input(env,item, *, all_public=False):
    """Reuse the verified immutable DEV projection; only the consumer CID changes."""
    from urllib.request import Request,build_opener
    from e4f_github import NoRedirect
    dev=dev_input(env,'dev-pin',item,normal=True,all_public=all_public)
    config=parse(dev);accepted=config['accepted'];wanted=config['cid']
    try:
        with build_opener(NoRedirect).open(Request('https://dev.bgzona.win/media-version.json?cid='+wanted,
                headers={'Cache-Control':'no-cache','User-Agent':'BGZONA-Public-Verification/1.0 (+https://bgzona.win/)'}),timeout=15) as response:
            require(response.status==200,'PROD_DEV_HTTP');wire=response.read(32001)
    except Exception:raise ProviderStop('PROD_DEV_NOT_ACCEPTED') from None
    require(len(wire)<=32000 and parse(wire)==dict(cid=wanted,channel='dev',
            registry_sha256=accepted['registry_sha256']),'PROD_DEV_NOT_ACCEPTED')
    # No authority mutation, new D3 append, filtering or private content.
    accepted['channel']='prod';accepted['input']['repository']=PROD
    config['cid']=digest(data(accepted))
    packet=parse((Path(env['RUNNER_TEMP'])/'r18h-dev-input/public-verification.json').read_bytes())
    require(packet['cid']==wanted,'PROD_DEV_PACKET');packet['cid']=config['cid']
    print('PROD_ACCEPTED_CID',config['cid'],accepted['record_sha256'],flush=True)
    return data(config),packet


def prod_workflow(repo,commit,token=None):
    from auto_intake import consumer_workflow as shared
    return shared(repo,commit,token)


def prod_consumer(row):
    from auto_intake import prod_consumer as shared
    return shared(row)


def prod_existing_readback(env,run,config,tip,origin):
    """Complete verification of the original Pages artifact. GETs only; no deployment."""
    from urllib.request import Request,build_opener
    from urllib.error import HTTPError
    from urllib.parse import urlsplit
    from pathlib import PurePosixPath
    import tarfile
    from e4f_github import NoRedirect
    token=env['PROD_ACTIONS_TOKEN'];root='/repos/'+PROD;cid=config['cid'];lock=config['accepted']['input']
    require(env['GITHUB_JOB']=='prod-only' and origin is not None and origin['conclusion']=='failure' and
            origin['record_sha256']==config['accepted']['record_sha256'] and origin['prod_pin']==run['head_sha'] and
            (run['workflow_id'],run['run_attempt'],run['event'],run['repository']['id'],run['actor']['id'],
             run['head_branch'],run['status'],run['conclusion'],run['display_title'])==
            (PROD_WORKFLOW,1,'workflow_dispatch',PROD_ID,353092585,'main','completed','failure','PROD '+cid),
            'PROD_EXISTING_IDENTITY')
    for pending in ('in_progress','queued','waiting'):
        status,active=github(root+'/actions/workflows/'+str(PROD_WORKFLOW)+'/runs?status='+pending+'&per_page=100',token)
        require(status==200 and active['total_count']==0,'PROD_CONCURRENT')
    current=prod_workflow(PROD,env['PROD_WORKFLOW_COMMIT'],token)
    if run['head_sha']!=tip:
        status,comparison=github(root+'/compare/'+run['head_sha']+'...'+tip,token)
        require(status==200 and comparison['status']=='ahead' and comparison['merge_base_commit']['sha']==run['head_sha'] and
                0<comparison['total_commits']<=256 and [r['filename'] for r in comparison['files']]==['.github/workflows/hugo.yml'],
                'PROD_EXISTING_SOURCE_CHANGED')
    old=prod_workflow(PROD,run['head_sha'],token)
    old['jobs']['deploy']['steps'][1]['run']=current['jobs']['deploy']['steps'][1]['run']
    require(old==current,'PROD_EXISTING_SOURCE_CHANGED')
    status,jobs=github(root+f"/actions/runs/{run['id']}/attempts/1/jobs?per_page=100",token)
    require(status==200 and jobs['total_count']==len(jobs['jobs'])==2 and
            {j['name']:j['conclusion'] for j in jobs['jobs']}==dict(build='success',deploy='failure') and
            all(j['run_id']==run['id'] and j['head_sha']==run['head_sha'] and j['status']=='completed' for j in jobs['jobs']),
            'PROD_EXISTING_JOBS')
    build,=[j for j in jobs['jobs'] if j['name']=='build'];deploy,=[j for j in jobs['jobs'] if j['name']=='deploy']
    require(all(any(s['name']==name and s['conclusion']=='success' for s in build['steps']) for name in
            ('Trusted PROD input, current-source build, acceptance and seal','Verify accepted output immediately before upload',
             'Upload exactly accepted Pages output')) and
            any(s['name']=='Deploy this run accepted Pages artifact' and s['conclusion']=='success' for s in deploy['steps']) and
            [s['name'] for s in deploy['steps'] if s['conclusion']=='failure']==['Verify public PROD CID and files against the seal'],
            'PROD_EXISTING_NOT_DEPLOYED')
    status,listing=github(root+f"/actions/runs/{run['id']}/artifacts?per_page=100",token)
    require(status==200 and listing['total_count']==len(listing['artifacts']),'PROD_ARTIFACT_WINDOW')
    assets=[a for a in listing['artifacts'] if a['name']=='github-pages'];require(len(assets)==1,'PROD_ARTIFACT_IDENTITY');asset,=assets
    require(asset['expired'] is False and 0<asset['size_in_bytes']<=64_000_000 and
            (asset['workflow_run']['id'],asset['workflow_run']['repository_id'],asset['workflow_run']['head_repository_id'],
             asset['workflow_run']['head_branch'],asset['workflow_run']['head_sha'])==
            (run['id'],PROD_ID,PROD_ID,'main',run['head_sha']) and
            asset['digest'].startswith('sha256:') and gb.sha(asset['digest'][7:]),'PROD_ARTIFACT_IDENTITY')
    request=Request('https://api.github.com'+root+f"/actions/artifacts/{asset['id']}/zip",headers={
            'Authorization':'Bearer '+token,'Accept':'application/vnd.github+json','X-GitHub-Api-Version':'2026-03-10'})
    opener=build_opener(NoRedirect)
    try:
        with opener.open(request,timeout=20) as response:
            require(response.status==200,'PROD_ARTIFACT_HTTP');raw=response.read(64_000_001)
    except HTTPError as error:
        location=error.headers.get('Location','');target=urlsplit(location)
        require(error.code==302 and target.scheme=='https' and not target.username and not target.password and
                target.port in (None,443) and target.hostname and target.hostname.endswith('.blob.core.windows.net'),
                'PROD_ARTIFACT_REDIRECT')
        # The repository token is never forwarded to the signed artifact host.
        with opener.open(Request(location),timeout=30) as response:
            require(response.status==200,'PROD_ARTIFACT_HTTP');raw=response.read(64_000_001)
    require(len(raw)==asset['size_in_bytes'] and digest(raw)==asset['digest'][7:],'PROD_ARTIFACT_SHA')
    with ZipFile(BytesIO(raw)) as archive:
        require(archive.namelist()==['artifact.tar'] and archive.getinfo('artifact.tar').file_size<=256_000_000,'PROD_ARTIFACT_SET')
        tar=archive.read('artifact.tar')
    buffers={}
    with tarfile.open(fileobj=BytesIO(tar),mode='r:') as archive:
        members=archive.getmembers();require(len(members)<=10000 and sum(m.size for m in members)<=256_000_000,'PROD_ARTIFACT_LIMIT')
        for member in members:
            path=PurePosixPath(member.name)
            require(not path.is_absolute() and '..' not in path.parts and '\\' not in member.name,'PROD_ARTIFACT_PATH')
            if member.isdir():continue
            name=path.as_posix();require(member.isfile() and 0<=member.size<=16_000_000 and
                    name.casefold() not in {n.casefold() for n in buffers},'PROD_ARTIFACT_FILE')
            buffers[name]=archive.extractfile(member).read()
    release=public_release(lock['provider']['tag']);payload=public_asset(release,lock['archive_name'],lock['bytes'],64_000_000)
    require(release['id']==lock['provider']['release_id'] and
            any(a['id']==lock['provider']['asset_id'] and a['name']==lock['archive_name'] for a in release['assets']) and
            digest(payload)==lock['archive_sha256'],'PROD_PROJECTION_IDENTITY')
    with ZipFile(BytesIO(payload)) as archive:
        cat=archive.read('artifact/catalog.json');expected=archive.read('expected-files.json')
    require(digest(cat)==lock['source_catalog_sha256'] and digest(expected)==lock['expected_files_sha256'],'PROD_PROJECTION_IDENTITY')
    paths={'/','/kolaji/','/poslaniya/','/media-version.json'};expected=parse(expected)
    for row in parse(cat)['items']:
        if row['state']!='published':continue
        original=row['original'];og=row.get('og') or original;paths.update((row['page_url'],original,og))
        for path in (original,og):require(digest(buffers[path.lstrip('/')])==expected[path.lstrip('/')],'PROD_ARTIFACT_MEDIA_SHA')
    require(parse(buffers['media-version.json'])==dict(cid=cid,channel='prod',registry_sha256=config['accepted']['registry_sha256']),
            'PROD_ARTIFACT_CID')
    pairs=[[p,digest(buffers[p.lstrip('/')+('index.html' if p.endswith('/') else '')])] for p in sorted(paths)]
    command=current['jobs']['deploy']['steps'][1]['run'];prefix="set -euo pipefail\npython -B - <<'PY'\n"
    require(command.startswith(prefix) and command.endswith('\nPY'),'PROD_HTTP_CHECKER_SOURCE')
    checker=command[len(prefix):-3];compile(checker,'approved-PROD-HTTP-checker','exec')
    result=subprocess.run([sys.executable,'-I','-B','-c',checker],env={'CID':cid,'CHECKS':data(pairs).decode()},
                          capture_output=True,text=True,encoding='utf8',timeout=150)
    print(result.stdout,end='',flush=True)
    require(result.returncode==0 and result.stdout.splitlines().count('PUBLIC_PROD_VERIFIED '+cid+' '+str(len(pairs)))==1,
            'PROD_EXISTING_HTTP_UNVERIFIED')
    require(prod_source(env,token)==tip,'PROD_SOURCE_MOVED')
    print('PROD_EXISTING_DELIVERY_VERIFIED',run['id'],asset['id'],asset['digest'],digest(checker.encode()),len(pairs),flush=True)
    return run['id']

def prod_source(env,token):
    from auto_intake import consumer_source as shared
    return shared(env,token)


def dispatch_prod(env,trust, *, verify_only=False, complete=False):
    """Explicit verify/publish roles. Unknown dispatch is never resent."""
    import time
    from urllib.parse import quote
    config=parse(trust);cid=config['cid'];a=config['accepted'];token=env['PROD_ACTIONS_TOKEN'];root='/repos/'+PROD
    require(a['channel']=='prod' and a['input']['repository']==PROD and digest(data(a))==cid and
            gb.sha(cid) and 0<len(trust)<=8192,'PROD_DISPATCH_INPUT')
    require(not verify_only or env.get('GITHUB_JOB')=='prod-only' and env.get('PROD_VERIFY_ONLY')=='true','PROD_VERIFY_ROLE')
    tip=prod_source(env,token);status,wf=github(root+'/actions/workflows/'+str(PROD_WORKFLOW),token)
    require(status==200 and (wf['id'],wf['path'],wf['state'])==(PROD_WORKFLOW,'.github/workflows/hugo.yml','active'),'PROD_WORKFLOW')
    require(not complete or not verify_only,'PROD_COMPLETION_ROLE')
    recovered=prod_completion(env,config,tip) if complete else None
    published=prod_published_source(env,config) if verify_only else None
    title='VERIFY PROD '+cid+':'+str(published['run']) if verify_only else 'PROD '+cid
    run=None
    if not verify_only:
        status,origin=github('/repos/'+CONTROL+'/git/commits/'+a['ledger_commit'])
        require(status==200 and origin['sha']==a['ledger_commit'],'PROD_LEDGER_COMMIT')
        created=quote('>='+origin['committer']['date'],safe='')
        status,history=github(root+'/actions/workflows/'+str(PROD_WORKFLOW)+
            '/runs?event=workflow_dispatch&branch=main&created='+created+'&per_page=100',token)
        require(status==200 and history['total_count']==len(history['workflow_runs']),'PROD_HISTORY_WINDOW')
        hits=[r for r in history['workflow_runs'] if r['display_title']==title]
        require(len(hits)<=1,'PROD_DUPLICATE')
        if hits:
            prior,=hits
            if complete:
                require(all(prior[k]==recovered[k] for k in
                            ('id','workflow_id','run_number','run_attempt','head_sha','status','conclusion','display_title')),
                        'PROD_COMPLETION_PRIOR_MOVED')
            else:
                require((prior['run_attempt'],prior['head_sha'],prior['status'],prior['conclusion'])==(1,tip,'completed','success'),
                        'PROD_PRIOR_UNRESOLVED');run=prior['id']
    if run is None:
        prod_idle(token)
        params=dict(accepted_cid=cid,input_trust=trust.decode(),verify_only='true' if verify_only else 'false',
                    published_source=published['source'] if verify_only else '',published_run=str(published['run']) if verify_only else '')
        if complete:
            marker=Path(env['RUNNER_TEMP'])/('prod-complete-'+cid+'.sent')
            try:
                with marker.open('xb') as handle:handle.write(data(dict(control_run=int(env['GITHUB_RUN_ID']),cid=cid)))
            except FileExistsError:raise ProviderStop('PROD_COMPLETION_ALREADY_SENT') from None
        status,reply=github(root+'/actions/workflows/'+str(PROD_WORKFLOW)+'/dispatches',token,
                           dict(ref='main',inputs=params,return_run_details=True))
        require(status==200 and isinstance(reply,dict) and gb.positive(reply.get('workflow_run_id')),'PROD_DISPATCH_UNKNOWN')
        run=reply['workflow_run_id']
    print('PROD_VERIFY_ONLY_RUN' if verify_only else 'PROD_RUN',run,cid,flush=True)
    from e4f_github import wait_actions_run
    row=wait_actions_run(lambda:github(root+f'/actions/runs/{run}/attempts/1',token),run,'PROD_RUN',20*60 if verify_only else 30*60)
    require((row['id'],row['workflow_id'],row['run_attempt'],row['repository']['id'],row['head_branch'],row['head_sha'],
                         row['actor']['id'],row['event'],row['display_title'],row['conclusion'])==
                        (run,PROD_WORKFLOW,1,PROD_ID,'main',tip,353092585,'workflow_dispatch',title,'success'),'PROD_RUN_IDENTITY')
    status,jobs=github(root+f'/actions/runs/{run}/attempts/1/jobs?per_page=100',token)
    require(status==200 and jobs['total_count']==len(jobs['jobs'])==3 and
                        {j['name']:j['conclusion'] for j in jobs['jobs']}==dict(build='success',deploy='skipped' if verify_only else 'success',verify='success') and
                        any(s['name']=='Verify bound build source' and s['conclusion']=='success' for j in jobs['jobs'] if j['name']=='build' for s in j['steps']) and
                        any(s['name']=='Verify public PROD CID and files against the seal' and s['conclusion']=='success' for j in jobs['jobs'] if j['name']=='verify' for s in j['steps']),
                        'PROD_SEAL_READBACK')
    require(prod_source(env,token)==tip,'PROD_SOURCE_MOVED')
    if verify_only:require(prod_published_source(env,config)==published,'PROD_PUBLISHED_SOURCE_MOVED')
    return run
def prod_completion_claim(current,history):
    require(type(history) is dict and type(history.get('total_count')) is int and
            history['total_count']==len(history['workflow_runs']) and
            len({r['id'] for r in history['workflow_runs']})==history['total_count'],'PROD_COMPLETION_HISTORY')
    rows=history['workflow_runs'];title='BGZONA prod '+PROD_R4['record']+':complete'
    claim=[r for r in rows if r['display_title']==title]
    require(current['run_number']>29 and len(claim)==1 and claim[0]['id']==current['id'] and
            all(claim[0][k]==current[k] for k in ('workflow_id','head_sha','run_number','run_attempt')) and
            claim[0]['run_attempt']==1 and all(r['id']==current['id'] or r['status']=='completed' and r['run_number']<=29 for r in rows),
            'PROD_COMPLETION_REPLAY_OR_CONCURRENT')



def prod_event_object(pairs):
    obj=dict(pairs)
    require(len(obj)==len(pairs),'PROD_CONTROL_DUPLICATE_INPUT')
    return obj


def prod_completion(env,config,tip):
    """Positive proof for exactly R4; this grants no D3/Assets authority."""
    require(env['GITHUB_JOB']=='prod-only' and env.get('PROD_VERIFY_ONLY')=='false' and
            env['GITHUB_EVENT_NAME']=='workflow_dispatch' and env['GITHUB_ACTOR_ID']==str(gb.CP2B_OWNER_ID) and
            env['GITHUB_REPOSITORY']==CONTROL and env['GITHUB_REPOSITORY_ID']==str(CONTROL_ID) and
            env['GITHUB_REF']==CONTROL_REF and env['GITHUB_REF_PROTECTED']=='true' and
            env['GITHUB_RUN_ATTEMPT']=='1' and env['GITHUB_ACTIONS']=='true' and
            env['GITHUB_WORKFLOW_REF']==CONTROL+'/.github/workflows/shared-media.yml@'+CONTROL_REF,
            'PROD_COMPLETION_SCOPE')
    params=json.loads(Path(env['GITHUB_EVENT_PATH']).read_bytes(),object_pairs_hook=prod_event_object)['inputs']
    require(type(params) is dict and params.get('mode')=='prod-complete' and
            params.get('record_sha256')==PROD_R4['record'] and
            set(params)<={'mode','record_sha256','source_run','private_run','relay_run'} and
            all(params.get(k,'')=='' for k in ('source_run','private_run','relay_run')),'PROD_COMPLETION_INPUT')
    current=control_run(int(env['GITHUB_RUN_ID']))
    title='BGZONA prod '+PROD_R4['record']+':complete'
    require((current['workflow_id'],current['head_sha'],current['status'],current['display_title'])==
            (408732936,env['GITHUB_WORKFLOW_SHA'],'in_progress',title),'PROD_COMPLETION_IDENTITY')
    status,history=github('/repos/'+CONTROL+'/actions/workflows/408732936/runs?per_page=100')
    require(status==200 and type(history) is dict and
            type(history.get('total_count')) is int and history['total_count']==len(history['workflow_runs']) and
            len({r['id'] for r in history['workflow_runs']})==history['total_count'],'PROD_COMPLETION_HISTORY')
    prod_completion_claim(current,history)
    rows=history['workflow_runs']
    writers=[r for r in rows if r['display_title'].startswith(('BGZONA auto ','BGZONA auto-resume ','BGZONA f1-resume '))]
    require(writers and max(writers,key=lambda r:r['run_number'])['id']==PROD_R4['writer'],'PROD_COMPLETION_WRITER')
    status,writer=github('/repos/'+CONTROL+f"/actions/runs/{PROD_R4['writer']}/attempts/1")
    require(status==200 and (writer['id'],writer['workflow_id'],writer['run_number'],writer['run_attempt'],
            writer['repository']['id'],writer['actor']['id'],writer['event'],writer['head_branch'],
            writer['path'].split('@',1)[0],writer['head_sha'],writer['status'],writer['conclusion'],writer['display_title'])==
            (PROD_R4['writer'],408732936,29,1,CONTROL_ID,gb.CP2B_OWNER_ID,'workflow_dispatch','automation-control',
             '.github/workflows/shared-media.yml',PROD_R4['writer_sha'],'completed','failure',
             'BGZONA auto-resume 42852332535:42852410172'),'PROD_COMPLETION_WRITER_IDENTITY')
    prior_writer,=[r for r in rows if r['id']==writer['id']]
    identity_fields=('workflow_id','run_number','run_attempt','head_sha','status','conclusion','display_title')
    require(all(prior_writer[k]==writer[k] for k in identity_fields),'PROD_COMPLETION_WRITER_HISTORY')
    workflow=prod_workflow(CONTROL,PROD_R4['writer_sha'])
    status,jobs=github('/repos/'+CONTROL+f"/actions/runs/{writer['id']}/attempts/1/jobs?per_page=100")
    expected={name:('success' if name in ('auto-intake','auto-assets','auto-dev') else
                   'failure' if name=='auto-prod' else 'skipped') for name in workflow['jobs']}
    require(status==200 and jobs['total_count']==len(jobs['jobs'])==len(expected) and
            {j['name']:j['conclusion'] for j in jobs['jobs']}==expected and
            all((j['run_id'],j['head_sha'],j['status'],j['run_attempt'])==
                (writer['id'],PROD_R4['writer_sha'],'completed',1) for j in jobs['jobs']),
            'PROD_COMPLETION_WRITER_JOBS')
    status,rules=github('/repos/'+CONTROL+'/rulesets/26657290')
    require(status==200 and rules['enforcement']=='active' and
            rules['conditions']['ref_name']==dict(include=[CONTROL_REF],exclude=[]) and
            len(rules['rules'])==3 and {r['type'] for r in rules['rules']}=={'update','deletion','non_fast_forward'},
            'PROD_COMPLETION_PROTECTION')
    accepted=config['accepted'];lock=accepted['input']
    require(config['cid']==PROD_R4['cid'] and
            (accepted['ledger_commit'],accepted['record_sha256'],accepted['authority_commit'],accepted['assets_tip'],
             accepted['witness_commit'],accepted['registry_sha256'])==
            (PROD_R4['ledger'],PROD_R4['record'],PROD_R4['assets'],PROD_R4['assets'],PROD_R4['witness'],PROD_R4['registry']) and
            lock['archive_sha256']==PROD_R4['archive'] and
            lock['provider']==dict(release_id=PROD_R4['release'],asset_id=PROD_R4['asset'],tag='r18h-dev-'+PROD_R4['record']),
            'PROD_COMPLETION_ACCEPTED_INPUT')
    for repo,ref,wanted in ((CONTROL,LEDGER,PROD_R4['ledger']),
                            (ASSETS,MAIN,PROD_R4['assets']),(ASSETS,WITNESS,PROD_R4['witness'])):
        status,row=github('/repos/'+repo+'/git/ref/'+ref.removeprefix('refs/'))
        require(status==200 and (row['ref'],row['object']['type'],row['object']['sha'])==(ref,'commit',wanted),
                'PROD_COMPLETION_ACCEPTED_REF')
    require(tip==env['PROD_WORKFLOW_COMMIT'] and tip!=PROD_R4['previous_sha'] and
            digest(data(prod_workflow(PROD,tip,env['PROD_ACTIONS_TOKEN'])))==PROD_R4['workflow'],
            'PROD_COMPLETION_WORKFLOW')
    status,comparison=github('/repos/'+PROD+'/compare/'+PROD_R4['previous_sha']+'...'+tip,env['PROD_ACTIONS_TOKEN'])
    require(status==200 and comparison['status']=='ahead' and comparison['total_commits']==1 and
            comparison['merge_base_commit']['sha']==PROD_R4['previous_sha'] and
            [f['filename'] for f in comparison['files']]==['.github/workflows/hugo.yml'],'PROD_COMPLETION_SOURCE')
    status,prior=github('/repos/'+PROD+f"/actions/runs/{PROD_R4['previous_run']}/attempts/1",env['PROD_ACTIONS_TOKEN'])
    require(status==200 and (prior['id'],prior['workflow_id'],prior['run_attempt'],prior['repository']['id'],
            prior['actor']['id'],prior['event'],prior['head_branch'],prior['path'].split('@',1)[0],
            prior['head_sha'],prior['status'],prior['conclusion'],prior['display_title'])==
            (PROD_R4['previous_run'],PROD_WORKFLOW,1,PROD_ID,353092585,'workflow_dispatch','main',
             '.github/workflows/hugo.yml',PROD_R4['previous_sha'],'completed','failure','PROD '+PROD_R4['cid']),
            'PROD_COMPLETION_PRIOR_IDENTITY')
    status,jobs=github('/repos/'+PROD+f"/actions/runs/{prior['id']}/attempts/1/jobs?per_page=100",env['PROD_ACTIONS_TOKEN'])
    require(status==200 and jobs['total_count']==len(jobs['jobs'])==3 and
            {j['name']:j['conclusion'] for j in jobs['jobs']}==dict(build='failure',deploy='skipped',verify='skipped') and
            all((j['run_id'],j['head_sha'],j['status'],j['run_attempt'])==
                (prior['id'],PROD_R4['previous_sha'],'completed',1) for j in jobs['jobs']),
            'PROD_COMPLETION_PRIOR_JOBS')
    build,=[j for j in jobs['jobs'] if j['name']=='build']
    expected_steps={s['name']:'skipped' for s in prod_workflow(PROD,PROD_R4['previous_sha'],env['PROD_ACTIONS_TOKEN'])['jobs']['build']['steps']}
    expected_steps.update({'Checkout current source':'success','Verify bound build source':'failure'})
    for name,conclusion in expected_steps.items():
        steps=[s for s in build['steps'] if s['name']==name]
        require(len(steps)==1 and steps[0]['status']=='completed' and steps[0]['conclusion']==conclusion,
                'PROD_COMPLETION_PREBUILD_REQUIRED')
    require(all(not j.get('steps') for j in jobs['jobs'] if j['name'] in ('deploy','verify')),
            'PROD_COMPLETION_PREBUILD_REQUIRED')
    status,history=github('/repos/'+PROD+'/actions/workflows/'+str(PROD_WORKFLOW)+'/runs?branch=main&per_page=100',
                          env['PROD_ACTIONS_TOKEN'])
    require(status==200 and type(history) is dict and type(history.get('total_count')) is int and
            history['total_count']==len(history['workflow_runs']) and
            len({r['id'] for r in history['workflow_runs']})==history['total_count'] and
            sum(r['id']==prior['id'] for r in history['workflow_runs'])==1 and
            all(r['status']=='completed' and r['run_number']<=prior['run_number'] for r in history['workflow_runs']) and
            [r['id'] for r in history['workflow_runs'] if r['display_title']=='PROD '+PROD_R4['cid']]==[prior['id']],
            'PROD_COMPLETION_PRIOR_HISTORY')
    prior_summary,=[r for r in history['workflow_runs'] if r['id']==prior['id']]
    require(all(prior_summary[k]==prior[k] for k in identity_fields),'PROD_COMPLETION_PRIOR_HISTORY')
    status,deployments=github('/repos/'+PROD+'/deployments?environment=github-pages&per_page=100',env['PROD_ACTIONS_TOKEN'])
    require(status==200 and type(deployments) is list and 0<len(deployments)<100 and
            (deployments[0]['id'],deployments[0]['sha'],deployments[0]['environment'])==
            (PROD_R4['deployment'],PROD_R4['old_pages_sha'],'github-pages') and
            all(d['created_at']<prior['created_at'] for d in deployments),'PROD_COMPLETION_DEPLOYMENTS')
    old=dict(cid=PROD_R4['old_cid'],accepted=dict(registry_sha256=PROD_R4['old_registry']))
    require(prod_published_source(env,old)==dict(run=PROD_R4['old_pages'],source=PROD_R4['old_pages_sha']),
            'PROD_COMPLETION_PUBLIC_BASELINE')
    return prior


def prod_main(env):
    require(env['GITHUB_ACTIONS']=='true' and env['GITHUB_REPOSITORY']==CONTROL and env['GITHUB_REPOSITORY_ID']==str(CONTROL_ID) and
            env['GITHUB_REF']==CONTROL_REF and env['GITHUB_REF_PROTECTED']=='true' and env['GITHUB_ACTOR_ID']==str(gb.CP2B_OWNER_ID) and
            env['GITHUB_RUN_ATTEMPT']=='1' and env['GITHUB_EVENT_NAME']=='workflow_dispatch' and
            env['GITHUB_WORKFLOW_REF']==CONTROL+'/.github/workflows/shared-media.yml@'+CONTROL_REF and
            env['GITHUB_JOB']=='prod-only','PROD_CONTROL_CONTEXT')
    params=json.loads(Path(env['GITHUB_EVENT_PATH']).read_bytes(),object_pairs_hook=prod_event_object)['inputs']
    require(type(params) is dict and set(params)<={'mode','record_sha256','source_run','private_run','relay_run'} and
            params.get('mode') in ('prod','prod-complete') and
            gb.sha(params.get('record_sha256')) and all(params.get(k,'')=='' for k in ('source_run','private_run','relay_run')),'PROD_CONTROL_INPUT')
    complete=params['mode']=='prod-complete'
    require(env.get('PROD_VERIFY_ONLY')==('false' if complete else 'true') and
            (not complete or params['record_sha256']==PROD_R4['record']),'PROD_CONTROL_ROLE')
    row=control_run(int(env['GITHUB_RUN_ID']))
    require(row['head_sha']==env['GITHUB_WORKFLOW_SHA'] and row['status']=='in_progress' and
            row['display_title']=='BGZONA prod '+params['record_sha256']+(':complete' if complete else ''),
            'PROD_CONTROL_IDENTITY')
    status,history=github('/repos/'+CONTROL+'/actions/workflows/408732936/runs?per_page=100')
    require(status==200 and all(r['id']==row['id'] or r['status']=='completed' for r in history['workflow_runs']),'PROD_CONTROL_CONCURRENT')
    writers=[r for r in history['workflow_runs'] if r['id']!=row['id'] and
             r['display_title'].startswith(('BGZONA auto ','BGZONA auto-resume ','BGZONA f1-resume '))]
    if complete:prod_completion_claim(row,history)
    else:
        require(writers and max(writers,key=lambda r:r['run_number'])['conclusion']=='success','PROD_WRITER_UNRESOLVED')
    for prior in history['workflow_runs']:
        if prior['id']!=row['id'] and prior['display_title'].startswith('BGZONA prod '):prod_consumer(prior)
    item=proposal(params['record_sha256'],normal=True);trust,packet=prod_input(env,item,all_public=True)
    dispatch_prod(env,trust,verify_only=not complete,complete=complete)
    from auto_intake import verify_public_result
    checked=verify_public_result(packet,base='https://bgzona.win')
    print('PROD_COMPLETION_ACCEPTED' if complete else 'PROD_VERIFY_ONLY_ACCEPTED',packet['cid'],len(checked),flush=True)

def main(env):
    inputs=parse(Path(env["GITHUB_EVENT_PATH"]).read_bytes()).get("inputs",{})
    if inputs.get('mode') in ('prod','prod-complete'):return prod_main(env)
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


def prod_idle(token):
    """Typed fail-closed API checks, distinct from actual concurrency."""
    for state in ('in_progress','queued','waiting'):
        try:
            status,row=github('/repos/'+PROD+'/actions/workflows/'+str(PROD_WORKFLOW)+'/runs?status='+state+'&per_page=100',token)
        except ProviderStop:
            print('PROD_CONCURRENCY_API',data(dict(check=state,http_status=None,error='TRANSPORT')).decode(),flush=True)
            raise ProviderStop('PROD_CONCURRENCY_API_FAILURE') from None
        count=row.get('total_count') if isinstance(row,dict) else None
        runs=row.get('workflow_runs') if isinstance(row,dict) else None
        print('PROD_CONCURRENCY_API',data(dict(check=state,http_status=status,total_count=count,
              returned_count=len(runs) if isinstance(runs,list) else None)).decode(),flush=True)
        require(status==200,'PROD_CONCURRENCY_API_FAILURE')
        require(type(count) is int and count>=0 and isinstance(runs,list) and len(runs)<=count,'PROD_CONCURRENCY_RESPONSE')
        require(count==0,'PROD_CONCURRENT')

def prod_live_version(config):
    from auto_intake import public_prod_version as shared
    return shared(config)

def prod_published_source(env,config):
    """The most recent actual Pages step, including pushes and failed post-checks."""
    token=env['PROD_ACTIONS_TOKEN'];root='/repos/'+PROD;tip=prod_source(env,token)
    prod_idle(token);prod_live_version(config)
    for page in range(1,11):
        status,history=github(root+'/actions/workflows/'+str(PROD_WORKFLOW)+
                '/runs?branch=main&per_page=100&page='+str(page),token)
        require(status==200 and isinstance(history,dict) and type(history.get('total_count')) is int and
                isinstance(history.get('workflow_runs'),list),'PROD_SOURCE_HISTORY')
        for run in history['workflow_runs']:
            require(run['status']=='completed','PROD_CONCURRENT')
            status,jobs=github(root+f"/actions/runs/{run['id']}/attempts/{run['run_attempt']}/jobs?per_page=100",token)
            require(status==200 and jobs['total_count']==len(jobs['jobs']),'PROD_SOURCE_JOBS')
            deployed=[s for j in jobs['jobs'] if j['name']=='deploy' for s in j.get('steps',[])
                      if s['name']=='Deploy this run accepted Pages artifact' and s['conclusion']=='success']
            if not deployed:continue
            require((run['workflow_id'],run['run_attempt'],run['repository']['id'],run['head_branch'],run['path'].split('@',1)[0])==
                    (PROD_WORKFLOW,1,PROD_ID,'main','.github/workflows/hugo.yml') and
                    run['actor']['id'] in (gb.CP2B_OWNER_ID,353092585) and run['event'] in ('push','workflow_dispatch') and
                    len(deployed)==1 and any(j['name']=='build' and j['conclusion']=='success' for j in jobs['jobs']),
                    'PROD_PUBLISHED_IDENTITY')
            source=run['head_sha'];require(gb.oid(source),'PROD_PUBLISHED_SOURCE')
            if source!=tip:
                status,comparison=github(root+'/compare/'+source+'...'+tip,env.get('GH_TOKEN'))
                require(status==200 and comparison['status']=='ahead' and comparison['merge_base_commit']['sha']==source and
                        0<comparison['total_commits']<=256,'PROD_PUBLISHED_ANCESTRY')
            print('PROD_PUBLISHED_SOURCE',run['id'],source,config['cid'],run['conclusion'],flush=True)
            return dict(run=run['id'],source=source)
        if len(history['workflow_runs'])<100:break
    raise ProviderStop('PROD_PUBLISHED_SOURCE_NOT_FOUND')

def ensure_current_public(env,item):
    """Positive public proof before any new registration/CAS; no publication token."""
    from auto_intake import verify_public_result
    temp=Path(env['RUNNER_TEMP'])/('public-ready-'+uuid4().hex);temp.mkdir()
    trust,packet=prod_input(env|{'RUNNER_TEMP':str(temp)},item,all_public=True)
    prod_live_version(parse(trust));checked=verify_public_result(packet,base='https://bgzona.win')
    print('AUTO_PUBLIC_BASELINE_VERIFIED',packet['cid'],len(checked),flush=True)


def verify_p4_completed(api,env,run):
    import auto_intake as auto
    from registration_entry import pinned_file
    from e4f_github import _failed_job_log
    require((run,int(env['GITHUB_RUN_NUMBER']),env['GITHUB_EVENT_NAME'])==
            (auto.P4_PRIVATE_RUN,auto.P4_NUMBER,'workflow_dispatch'),'P4_COMPLETED_SCOPE')
    row=auto.control_run(int(env['GITHUB_RUN_ID']),auto.P4_SOURCE,auto.P4_RELAY,
                         private_api=api,private_main=env['PRIVATE_WORKFLOW_COMMIT'])
    require(row['source_sha']==auto.P4_SOURCE_SHA and row['head_sha']==env['GITHUB_WORKFLOW_SHA'],'P4_CONTROL_PIN')
    _failed_job_log(env['GH_TOKEN'],job=129956590856,
       expected_sha='36e136d75c154ed64a31378e9080f99d7ae4d329f76b10f71fb87d4f1c556346',
       expected_line=b'2026-11-10T16:32:36.0376774Z STOP E4F_PRIVATE_RUN_UNRESOLVED')
    require(api.get('/git/ref/heads/cp2b-private-standard-recovery-c2')[0]['object']['sha']==auto.P4_RECOVERY and
            auto.public('/git/ref/heads/automation-ledger')['object']['sha']==auto.P4_PREFIX,'P4_STATE_MOVED')
    wf=parse(pinned_file(api,'.github/workflows/e4f-standard-prepare.yml',auto.P4_ORIGIN_MAIN))
    require((wf['jobs']['prepare']['env']['WORKER_COMMIT'],wf['jobs']['prepare']['env']['MANIFEST_SHA256'])==
            ('d0d50e811809bfe07e2457a51fc7940d34b6728c','5652eb60b1e4930ee676a60404bda96e302300000d296eb3370f4d6964c53b6d'),
            'P4_PREPARED_WORKER_PIN')
    auto.p4_initial(api,env['PRIVATE_WORKFLOW_COMMIT'])


if __name__=='__main__':
    try:
        main(os.environ)
    except ProviderStop as exc:
        print('STOP '+str(exc),file=sys.stderr)
        sys.exit(1)
    except Exception:
        print('STOP E4F_CONTROL_UNRESOLVED',file=sys.stderr)
        sys.exit(1)
