"""Opt-in owner-flow callers over existing H1I/P4. No publisher/recovery rewrite.
Owner and live Lab ruleset checks share the same reviewed authority policy.
"""
from pathlib import Path
from uuid import UUID
import json,os,re,subprocess,sys,hashlib
import pipeline as P
ROOT=Path(__file__).resolve().parent
PRIVATE='malin-bg/bgzona-private-intake';LAB='malin-bg/bgzona-github-lab'
OWNER=252877943
RECOVERY_PRODUCER='45208989311'
RECOVERY_RELAY=45209284407
RECOVERY_RELAY_JOB=137813384014
RECOVERY_CODE='9fdff3fba807994a4b1cb787bd2771dc55f2b31b'

def select_profile(policy,enabled_real):
 if not enabled_real:return policy
 return dict(policy,owner_flow=policy['real_owner_flow'],state_ref='refs/heads/bot-publisher-state',
             data_ref='refs/heads/publisher-data',data_root='05cfdeb41ca116dc07614906fbbdee9b3a597745',
             state_root=None,source_root=None)


def config(policy):
 c=policy['owner_flow']
 enabled=(c.get('enabled') is False and os.environ.get('BGZONA_REAL_ENABLED')=='true'
          if real(c) else c.get('enabled') is True)
 P.need(enabled,'OWNER_ROUTE_DISABLED')
 P.need(c['source_ref'] in ('refs/heads/cp2b-private-standard-source-r1','refs/heads/synthetic-source'),'OWNER_SOURCE_REF')
 P.control_authority(policy)
 return c

def real(c):
 return c['source_ref']=='refs/heads/cp2b-private-standard-source-r1'


def state_authority(token):
 import write_ref as W
 main=P.ref(PRIVATE,'refs/heads/main',token)
 raw=P.workflow_bytes(PRIVATE,'tools/rs4m-h1/real-state-authority.json',main,token)
 return W.real_private_state(raw),raw


def uuid_trace(value):
 try:trace=UUID(hex=value)
 except (TypeError,ValueError,AttributeError):raise P.Stop('OWNER_TRACE_FORMAT') from None
 P.need(trace.version==4 and trace.hex==value,'OWNER_TRACE_FORMAT');return value


def public_binding(inputs,c):
 P.need(set(inputs)<={'mode','private_run','record_sha256','source_run','relay_run'} and
        inputs.get('mode')==c['Lab_mode'] and
        not any(inputs.get(k) for k in ('record_sha256','source_run','relay_run')),'OWNER_LAB_INPUTS')
 value=inputs.get('private_run','')
 if real(c):
  P.need(isinstance(value,str) and value.startswith('trace:'),'OWNER_LAB_TRACE_REQUIRED')
  trace=value.removeprefix('trace:')
  suffix=':recover-'+RECOVERY_PRODUCER
  if trace.endswith(suffix):
   return {'trace':uuid_trace(trace.removesuffix(suffix)),'recover_pending':True}
  return {'trace':uuid_trace(trace)}
 P.need(re.fullmatch('[1-9][0-9]*',value),'OWNER_LAB_INPUTS')
 return {'private_run':value}


def activities(repo,token):
 # Activities uses opaque before/after cursors, not numeric page. Never send
 # credentials to a Link-provided host/path or silently deduplicate history.
 from urllib.parse import urlsplit,parse_qsl,urlencode
 from urllib.request import parse_http_list
 P.need(repo==PRIVATE,'ACTIVITY_REPOSITORY')
 stem='/repos/'+repo+'/activity';path=stem+'?per_page=100'
 values=[];ids=set();cursors=set()
 while True:
  try:rows,headers=P.request(path,token,with_headers=True)
  except P.Stop:raise
  except Exception:raise P.Stop('ACTIVITY_API_UNKNOWN') from None
  P.need(isinstance(rows,list) and len(rows)<=100 and isinstance(headers,dict),'ACTIVITY_SHAPE')
  for row in rows:
   P.need(isinstance(row,dict) and type(row.get('id')) is int and row['id']>0,'ACTIVITY_ROW')
   P.need(row['id'] not in ids,'DUPLICATE_HISTORY');ids.add(row['id'])
  P.need(bool(rows) or not values,'ACTIVITY_EMPTY_PAGE')
  values.extend(rows);link=headers.get('Link');links={}
  if link is not None:
   P.need(isinstance(link,str) and bool(link) and not any(ord(c)<32 for c in link),'ACTIVITY_LINK')
   for part in parse_http_list(link):
    match=re.fullmatch(r'<([^<>]+)>;\s*rel="(next|prev|first|last)"',part.strip())
    P.need(match is not None and match[2] not in links,'ACTIVITY_LINK')
    try:url=urlsplit(match[1])
    except ValueError:raise P.Stop('ACTIVITY_LINK') from None
    P.need(url.scheme=='https' and url.netloc=='api.github.com' and url.path in (stem,'/repositories/1429436685/activity') and not url.fragment,'ACTIVITY_LINK_SCOPE')
    try:query=parse_qsl(url.query,keep_blank_values=True,strict_parsing=True)
    except ValueError:raise P.Stop('ACTIVITY_LINK_QUERY') from None
    q=dict(query);cursor=set(q)&{'before','after'}
    P.need(len(q)==len(query) and len(cursor)==1 and set(q)==({'per_page'}|cursor) and q['per_page']=='100','ACTIVITY_LINK_QUERY')
    kind=next(iter(cursor));value=q[kind]
    P.need(bool(value) and not any(ord(c)<33 or ord(c)>126 for c in value),'ACTIVITY_CURSOR')
    links[match[2]]=(kind,value)
  if 'next' not in links:
   P.need(len(rows)<100 or 'prev' in links,'INCOMPLETE_ACTIVITY_HISTORY')
   return values
  P.need(bool(rows),'ACTIVITY_EMPTY_NEXT')
  cursor=links['next'];P.need(cursor not in cursors,'ACTIVITY_CURSOR_REPEATED');cursors.add(cursor)
  path=stem+'?'+urlencode([('per_page','100'),cursor])


def history(repo,path,key,token,*,label='HISTORY',require_total=False):
 if path=='/activity' and key is None:return activities(repo,token)
 from urllib.request import parse_http_list
 stem='/repos/'+repo+path+('&' if '?' in path else '?')+'per_page=100&page='
 values=[];page=1;total=None
 def check(ok,code):
  if not ok:
   count=reported if type(reported) is int else 'unknown'
   print(f'H1_HISTORY_DIAG check={label} page={page} rows_read={len(values)} total_count={count} next={int(has_next)}',flush=True)
   P.need(False,code)
 while True:
  # Use only the relation from Link. The URL always stays on the original route.
  response,headers=P.request(stem+str(page),token,with_headers=True)
  rows=response[key] if key else response;P.need(isinstance(rows,list),'HISTORY_SHAPE')
  link=headers.get('Link')
  links=[re.fullmatch(r'<[^<>]+>;\s*rel="(next|prev|first|last)"',part.strip())
         for part in parse_http_list(link)] if link else []
  P.need(all(links),'HISTORY_LINK')
  has_next=any(match[1]=='next' for match in links)
  reported=response.get('total_count',total) if key else None
  values.extend(rows)
  if require_total:
   check('total_count' in response and type(reported) is int and reported>=0,'INCOMPLETE_HISTORY')
  if key and 'total_count' in response:
   if total is None:total=reported
   check(reported==total,'HISTORY_CHANGED')
  if not has_next:break
  check(bool(rows) and type(total) is int and len(values)<=total,'INCOMPLETE_HISTORY')
  page+=1
 check(total is None or len(values)==total,'INCOMPLETE_HISTORY')
 if values and 'id' in values[0]:check(len({v['id'] for v in values})==len(values),'DUPLICATE_HISTORY')
 return values

def source(policy,run_id,token):
 c=config(policy);run=P.api(PRIVATE,'/actions/runs/'+str(run_id)+'/attempts/1',token)
 P.need(run.get('id')==int(run_id) and run.get('workflow_id')==c['source_workflow_id'] and run.get('event')=='push' and run.get('run_attempt')==1 and run.get('status')=='completed' and run.get('conclusion')=='success' and run.get('head_branch')==c['source_ref'].removeprefix('refs/heads/') and run.get('repository',{}).get('id')==1429436685 and run.get('repository',{}).get('full_name')==PRIVATE and run.get('actor',{}).get('id')==OWNER and run['actor'].get('type')=='User' and run.get('triggering_actor',run['actor']).get('id')==OWNER and P.path_matches(run.get('path'),c['source_workflow'],run['head_branch']),'SOURCE_NATIVE_IDENTITY')
 P.need(P.digest(P.workflow_bytes(PRIVATE,c['source_workflow'],run['head_sha'],token))==c['source_workflow_sha256'],'SOURCE_EXECUTABLE_CODE')
 import source_contract as S
 # Existing validator's configured branch, never a branch supplied by event/payload.
 S.STANDARD_REF=c['source_ref']
 artifacts=history(PRIVATE,'/actions/runs/'+str(run_id)+'/artifacts','artifacts',token,label='SOURCE_ARTIFACTS')
 a=S.select(artifacts,int(run_id),1,run['head_sha'],ref=c['source_ref'])
 raw=P.request('/repos/'+PRIVATE+'/actions/artifacts/'+str(a['id'])+'/zip',token,binary=True)
 P.need(len(raw)==a['size_in_bytes'],'SOURCE_ARCHIVE_SIZE');packet=S.decode(raw,a)
 commit=P.api(PRIVATE,'/git/commits/'+run['head_sha'],token)
 activity=history(PRIVATE,'/activity',None,token)
 S.verify(packet,int(run_id),1,run['head_sha'],run,activity,[p['sha'] for p in commit['parents']],ref=c['source_ref'])
 return {'run':str(run_id),'after':run['head_sha'],'before':packet['before'],'packet_sha256':P.digest(S.data(packet))}

def check_private_real_activity(token,current,may_overlap):
 # Removing the status filter keeps completed runs in the inventory.
 rows=history(PRIVATE,'/actions/runs','workflow_runs',token,
              label='PRIVATE_ACTIVE_RUNS',require_total=True)
 created=P.control_time(current['created_at'])
 P.need(any(isinstance(row,dict) and row.get('id')==current['id'] and
            row.get('status')=='in_progress' and row.get('run_attempt')==1
            for row in rows),'CONCURRENT_OR_UNKNOWN')
 for row in rows:
  P.need(isinstance(row,dict) and type(row.get('id')) is int and row['id']>0 and
         row.get('status') in ('in_progress','queued','waiting','requested','pending','completed'),
         'CONCURRENT_OR_UNKNOWN')
  if row['status']=='completed':
   P.need(isinstance(row.get('conclusion'),str) and bool(row['conclusion']),'CONCURRENT_OR_UNKNOWN')
   if P.control_time(row.get('updated_at'))<created:continue
  P.need(may_overlap(row),'CONCURRENT_OR_UNKNOWN')


def context(policy,stage):
 c=config(policy);env=os.environ;private=stage.startswith('private-');repo=PRIVATE if private else LAB
 P.need(env.get('GITHUB_REPOSITORY')==repo and env.get('GITHUB_REPOSITORY_ID')==str(1429436685 if private else 1426765395) and env.get('GITHUB_REF')==('refs/heads/main' if private else policy['control_ref']) and env.get('GITHUB_ACTOR_ID')==str(OWNER) and env.get('GITHUB_RUN_ATTEMPT')=='1','OWNER_EXECUTION_CONTEXT')
 event=P.strict(Path(env['GITHUB_EVENT_PATH']).read_bytes());token=env['GH_TOKEN'];workflow_sha=env.get('H1_WORKFLOW_SHA');P.need(workflow_sha==env.get('GITHUB_SHA') and re.fullmatch('[a-f0-9]{40}',workflow_sha or ''),'OWNER_WORKFLOW_SHA')
 code=subprocess.run(['git','--no-optional-locks','-C',str(ROOT.parents[1]),'rev-parse','HEAD'],capture_output=True);P.need(code.returncode==0,'EXECUTABLE_CHECKOUT');code=code.stdout.decode().strip()
 P.need(P.ref(LAB,policy['control_ref'],token)==P.ref(LAB,'refs/heads/main',token)==code,'TRUSTED_EXECUTABLE_PIN')
 current=P.api(repo,'/actions/runs/'+env['GITHUB_RUN_ID'],token)
 P.need(current.get('head_sha')==workflow_sha and current.get('run_attempt')==1 and str(current.get('actor',{}).get('id'))==str(OWNER) and current.get('repository',{}).get('full_name')==repo and current.get('head_branch')==env['GITHUB_REF'].removeprefix('refs/heads/'),'LIVE_RUN_BINDING')
 P.need(current.get('workflow_id')==(c['private_workflow_id'] if private else 408732936) and P.path_matches(current.get('path'),policy['private_workflow' if private else 'shared_workflow'],current['head_branch']) and env.get('GITHUB_JOB')=={'private-prepare':'prepare','private-write':'writer','public-prepare':'h1-export','public-write':'h1-write','consumer':'h1-consumer','dev-dispatch':'h1-dispatch-dev'}[stage],'LIVE_JOB_WORKFLOW')
 if private:
  upstream=event.get('workflow_run',{});P.need(env.get('GITHUB_EVENT_NAME')==current.get('event')=='workflow_run' and upstream.get('workflow_id')==c['source_workflow_id'] and upstream.get('head_branch')==c['source_ref'].removeprefix('refs/heads/'),'SOURCE_WORKFLOW_RUN_BOUNDARY')
  P.need(P.digest(P.workflow_bytes(PRIVATE,policy['private_workflow'],workflow_sha,token))==policy['private_workflow_sha256'],'PRIVATE_WORKFLOW_PIN')
  binding=source(policy,str(upstream['id']),token);P.need(upstream.get('head_sha')==binding['after'],'SOURCE_EVENT_BINDING')
 else:
  P.need(env.get('GITHUB_EVENT_NAME')==current.get('event')=='workflow_dispatch' and env.get('GITHUB_REF_PROTECTED')=='true' and workflow_sha==code,'PROTECTED_LAB_CONTEXT')
  binding=public_binding(event.get('inputs',{}),c)
 P.control_protection(policy,token,current['created_at'])
 rows=history(repo,'/actions/workflows/'+str(c['private_workflow_id'] if private else 408732936)+'/runs','workflow_runs',token,
              label='PRIVATE_PRODUCER_RUNS' if private else 'LAB_WORKFLOW_RUNS')
 if private:expected=('BGZONA B real source ' if real(c) else 'BGZONA B source ')+binding['run']
 elif real(c):expected='BGZONA B real'
 else:expected='BGZONA B producer '+binding['private_run']
 same=[r for r in rows if r.get('display_title')==expected and
       (str(r['id'])==env['GITHUB_RUN_ID'] or private or not real(c))]
 P.need(len(same)==1 and str(same[0]['id'])==env['GITHUB_RUN_ID'],'DUPLICATE_OWNER_EVENT')
 def same_or_legacy_route_check(row):
  if str(row['id'])==env['GITHUB_RUN_ID']:return True
  if not (private and real(c) and row.get('head_sha')==workflow_sha and
          row.get('workflow_id')==c['relay_workflow_id'] and row.get('event')=='workflow_run' and
          row.get('display_title')=='BGZONA relay '+binding['run'] and
          row.get('actor',{}).get('id')==OWNER and row.get('run_attempt')==1):
   return False
  return P.digest(P.workflow_bytes(PRIVATE,c['relay_workflow'],workflow_sha,token))==c['relay_workflow_sha256']
 if private and real(c):
  check_private_real_activity(token,current,same_or_legacy_route_check)
 else:
  for status in ('in_progress','queued','waiting','requested','pending'):
   active=history(repo,'/actions/runs?status='+status,'workflow_runs',token,label='ACTIVE_'+status.upper())
   P.need(all(same_or_legacy_route_check(row) for row in active),'CONCURRENT_OR_UNKNOWN')
 return repo,code,event,binding

def inert(repo,rev,path):
 q=subprocess.run(['git','--no-optional-locks','--git-dir='+str(repo),'show',rev+':'+path],capture_output=True);P.need(q.returncode==0,'DURABLE_STATE_UNKNOWN');return P.strict(q.stdout)

def handoff_context(kind,code,folder,binding,mode):
 value={'schema':1,'kind':kind,'mode':mode,'repository':os.environ['GITHUB_REPOSITORY'],'run':os.environ['GITHUB_RUN_ID'],'attempt':1,'workflow_sha':os.environ['H1_WORKFLOW_SHA'],'code_commit':code,'proposal_sha':P.digest((folder/'proposal.json').read_bytes())}
 if kind=='private':value['source']=binding
 else:value.update(binding)
 if mode=='CANDIDATE':value['bundle_sha']=P.digest((folder/'candidate.bundle').read_bytes())
 P.dump(folder/'context.json',value)
 for k in ('proposal_sha','bundle_sha'):
  if k in value:P.out(k,value[k])
 P.out('handoff_mode',mode)

def prepare(policy,kind,code,event,binding,work,deps):
 c=config(policy);work.mkdir();token=os.environ['GH_TOKEN'];app=os.environ.get('PRIVATE_READ_TOKEN')
 state_root=policy['state_root'];source_root=policy['source_root']
 if real(c):
  state_root,authority=state_authority(token if kind=='private' else app)
  source_root=P.strict(authority)['source_root']
 if kind=='private':
  P.acquire(PRIVATE,policy['state_ref'],state_root,work/'private.git',work,deps,token)
  G,H=P.libraries(deps);url='https://github.com/'+PRIVATE+'.git';ch=H.Channel(H.Endpoint(url,'source-read',c['source_ref'],source_root),H.Identity('source-read',url,'x-access-token',token));ch.acquire_source(work/'private.git')
  P.need(P.ref(PRIVATE,c['source_ref'],token)==binding['after'],'SOURCE_MOVED_BEFORE_ACQUISITION')
 else:
  head=P.acquire(PRIVATE,policy['state_ref'],state_root,work/'private.git',work,deps,app)
  if real(c):
   private_rec=inert(work/'private.git',head,'recovery.json')
   P.need(private_rec.get('schema')==2 and private_rec.get('trace')==binding['trace']
          and type(private_rec.get('private_run_id')) is int and private_rec['private_run_id']>0,
          'DURABLE_TRACE_BINDING')
   producer=str(private_rec['private_run_id'])
  else:producer=binding['private_run']
  if binding.get('recover_pending'):
   P.need(producer==RECOVERY_PRODUCER,'RECOVERY_PRODUCER')
   completed_relay(policy,producer,app,recovery=True)
   receipt=private_receipt(policy,producer,RECOVERY_CODE,app)
   P.need(receipt['status']=='CONFIRMED' and receipt['git_sends']==1 and
          receipt['public_target']!=receipt['public_expected'],'RECOVERY_PRIVATE_WRITE')
  else:
   relay_run=completed_relay(policy,producer,app)
   if real(c):
    prove_relay_dispatch(relay_run,app,binding,code,private_rec['target'])
   receipt=private_receipt(policy,producer,code,app)
  if real(c):P.need(private_rec['target']==receipt['public_target'] and
                   private_rec['expected']==receipt['public_expected'],'TRACE_TARGET_BINDING')
  P.need(head==receipt['commit'],'PRIVATE_STATE_MOVED')
 P.acquire(LAB,policy['data_ref'],policy['data_root'],work/'public.git',work,deps)
 private_head=P.ref(PRIVATE,policy['state_ref'],token if kind=='private' else app);rec=inert(work/'private.git',private_head,'recovery.json');public_head=P.ref(LAB,policy['data_ref'],token)
 if kind=='private' and public_head!=rec['target']:
  P.need(public_head==rec['expected'],'PUBLIC_CONFLICT');state=inert(work/'private.git',private_head,'state.json');P.need(state['source_cursor']==binding['after'],'PENDING_PREVIOUS_EXPORT_NEW_SOURCE_RETAINED');folder=work/'private-handoff';folder.mkdir();P.dump(folder/'proposal.json',{'expected':private_head,'commit':private_head,'status':'READBACK'});handoff_context(kind,code,folder,binding,'PENDING');return
 q=subprocess.run([sys.executable,'-I','-B',str(ROOT/'prepare_phase.py'),kind,'--workspace',str(work),'--deps',str(Path(deps).resolve()),'--source-ref',c['source_ref']]+(['--producer-id',os.environ['GITHUB_RUN_ID']] if real(c) and kind=='private' else []),capture_output=True)
 if q.returncode:
  marker=re.fullmatch(rb'PREPARATION_STOP:([A-Z0-9_]+)',q.stdout.strip())
  reason=marker[1].decode('ascii') if marker else 'CREDENTIAL_FREE_PREPARATION_STOP'
  advice={'DUPLICATE_ORIGINAL':'This original already belongs to a publication in the same collection. Edit the existing ID.',
          'REPLACEMENT_ID_NOT_EDITABLE':'Use the five-digit number of an existing published item.',
          'MOVE_ALREADY_COMPLETED':'Remove the copied incoming bundle and edit its library folder.',
          'CLEANUP_BINDING':'Check the accepted publication ID and the original bytes.',
           'CLEANUP_NOT_ACCEPTED':'Delete only already accepted incoming files with unchanged bytes.',
           'EDIT_CLEANED_INCOMING_BY_ID':'Use the published ID under incoming/replace to edit a partially cleaned bundle.',
          'MOVE_BUNDLE_EXACT_BYTES':'Move the original unmodified. Use incoming/replace for a new revision.',
          'AMBIGUOUS_OR_INCOMPLETE_BUNDLE':'Keep one original, one optional TXT and one optional OG.'}
  if reason.startswith('PRIVACY_'): print('Privacy Gate rejected a file. Export a clean image in the original format and push the correction.')
  elif reason in advice: print(advice[reason])
  raise P.Stop(reason)
 folder=work/(kind+'-handoff');out=P.strict((folder/'result.json').read_bytes());mode='CANDIDATE'
 if out['status'] in ('NO_CHANGE','CONFIRMED'):
  head=private_head if kind=='private' else public_head;P.dump(folder/'proposal.json',{'expected':head,'commit':head,'status':'READBACK'});mode='NO_CHANGE' if kind=='private' else 'CONFIRMED'
 else:P.need(out['status']==('PRIVATE_PREPARED' if kind=='private' else 'PUBLIC_PREPARED'),'PREPARATION_RESULT')
 if kind=='public':
  proposal=P.strict((folder/'proposal.json').read_bytes())
  P.need(proposal['commit']==receipt['public_target'] and (mode!='CANDIDATE' or proposal['expected']==receipt['public_expected']),'DURABLE_EXPORT_TARGET')
 handoff_context(kind,code,folder,binding,mode)

def write(policy,kind,code,binding,work,deps):
 c=config(policy);mode=os.environ['H1_HANDOFF_MODE'];P.need(mode in (('CANDIDATE','NO_CHANGE','PENDING') if kind=='private' else ('CANDIDATE','CONFIRMED')),'HANDOFF_MODE');inventory={'proposal.json','context.json'}|({'candidate.bundle'} if mode=='CANDIDATE' else set());repo=PRIVATE if kind=='private' else LAB
 files=P.artifact_files(repo,os.environ['GITHUB_RUN_ID'],'h1-'+kind+'-candidate',inventory,os.environ['GH_TOKEN']);folder=Path(os.environ['RUNNER_TEMP'])/'b-handoff';P.need(not folder.exists(),'HANDOFF_EXISTS');folder.mkdir()
 for name,raw in files.items():(folder/name).write_bytes(raw)
 ctx=P.strict(files['context.json']);expected={'schema':1,'kind':kind,'mode':mode,'repository':repo,'run':os.environ['GITHUB_RUN_ID'],'attempt':1,'workflow_sha':os.environ['H1_WORKFLOW_SHA'],'code_commit':code,'proposal_sha':P.digest(files['proposal.json'])}
 if kind=='private':expected['source']=binding
 else:expected.update(binding)
 if mode=='CANDIDATE':expected['bundle_sha']=P.digest(files['candidate.bundle'])
 P.need(ctx==expected and ctx['proposal_sha']==os.environ['H1_PROPOSAL_SHA'] and (mode!='CANDIDATE' or ctx['bundle_sha']==os.environ['H1_BUNDLE_SHA']),'SAME_RUN_CODE_ARTIFACT_BINDING')
 proposal=P.strict(files['proposal.json']);G,H=P.libraries(deps);G.ROOT=work;work.mkdir();client=G.init(work/'writer.git');reference=policy['state_ref' if kind=='private' else 'data_ref'];root=policy['state_root' if kind=='private' else 'data_root']
 import write_ref as W
 authority=None
 if kind=='private' and real(c):
  root,authority=state_authority(os.environ['GH_TOKEN'])
 write_policy=W.Policy(repo,reference,root,authority)
 if mode=='CANDIDATE':
  if real(c):
   live=P.api(repo,'/actions/runs/'+os.environ['GITHUB_RUN_ID'],os.environ['GH_TOKEN'])
   P.control_protection(policy,os.environ['GH_TOKEN'],live['created_at'])
  listing=G.run(client,'bundle','list-heads',str(folder/'candidate.bundle')).stdout.decode().splitlines();P.need(listing==[proposal['commit']+' refs/heads/s1-handoff-candidate'],'BUNDLE_IDENTITY');G.run(client,'bundle','verify',str(folder/'candidate.bundle'));G.run(client,'fetch','--no-tags',str(folder/'candidate.bundle'),'refs/heads/s1-handoff-candidate');result=W.write_once(client,proposal,write_policy,W.HTTPSWriter(write_policy,os.environ['GH_TOKEN']))
  P.need(result['status']=='CONFIRMED' and result['sends'] in (0,1),'WRITE_UNKNOWN_NO_RETRY')
 else:
  P.need(set(proposal)=={'expected','commit','status'} and proposal['status']=='READBACK' and proposal['expected']==proposal['commit'],'READBACK_PROPOSAL');P.need(P.acquire(repo,reference,root,work/'readback.git',work,deps,os.environ['GH_TOKEN'])==proposal['commit'],'READBACK_REF_MOVED');client=work/'readback.git';result={'status':'CONFIRMED','sends':0}
 P.need(P.ref(repo,reference,os.environ['GH_TOKEN'])==proposal['commit'],'INDEPENDENT_REST_READBACK')
 receipt={'schema':2,'repository':repo,'run':os.environ['GITHUB_RUN_ID'],'attempt':1,'workflow_sha':os.environ['H1_WORKFLOW_SHA'],'code_commit':code,'commit':proposal['commit'],'expected':proposal['expected'],'git_sends':result['sends'],'readback':'CONFIRMED','status':'NO_CHANGE' if mode=='NO_CHANGE' else 'CONFIRMED'}
 if kind=='private':
  rec=inert(client,proposal['commit'],'recovery.json');state=inert(client,proposal['commit'],'state.json');P.need(mode=='PENDING' or state['source_cursor']==binding['after'],'DURABLE_SOURCE_CURSOR');receipt.update(source=binding,public_target=rec['target'],public_expected=rec['expected'])
 else:
  P.out('data_commit',proposal['commit'])
  P.out('public_sends',result['sends'])
 dest=work/'receipt';dest.mkdir();P.dump(dest/'receipt.json',receipt)

def private_receipt(policy,run_id,code,token):
 c=config(policy);run=P.api(PRIVATE,'/actions/runs/'+run_id,token)
 P.need(run.get('repository',{}).get('id')==1429436685 and run.get('workflow_id')==c['private_workflow_id'] and run.get('event')=='workflow_run' and run.get('head_branch')=='main' and run.get('run_attempt')==1 and run.get('actor',{}).get('id')==OWNER and run.get('status')=='completed' and run.get('conclusion')=='success','PRODUCER_COMPLETION')
 P.need(P.digest(P.workflow_bytes(PRIVATE,policy['private_workflow'],run['head_sha'],token))==policy['private_workflow_sha256'],'PRODUCER_WORKFLOW_PIN')
 jobs=history(PRIVATE,'/actions/runs/'+run_id+'/attempts/1/jobs','jobs',token,label='PRIVATE_PRODUCER_JOBS');P.need(len(jobs)==2 and {j['name'] for j in jobs}=={'H1 PRIVATE prepare','H1 PRIVATE writer'} and all(j.get('status')=='completed' and j.get('conclusion')=='success' for j in jobs),'PRODUCER_WRITER_JOBS')
 for label,step in [('H1 PRIVATE prepare','Prepare PRIVATE candidate'),('H1 PRIVATE writer','Commit PRIVATE checkpoint')]:
  job=next(j for j in jobs if j['name']==label);hits=[x for x in job.get('steps',[]) if x.get('name')==step];P.need(len(hits)==1 and hits[0].get('conclusion')=='success','PRODUCER_REQUIRED_STEP')
 files=P.artifact_files(PRIVATE,run_id,'h1-private-receipt',{'receipt.json'},token);r=P.strict(files['receipt.json']);P.need(set(r)=={'schema','repository','run','attempt','workflow_sha','code_commit','commit','expected','git_sends','readback','status','source','public_target','public_expected'} and r['schema']==2 and r['run']==run_id and r['repository']==PRIVATE and r['attempt']==1 and r['workflow_sha']==run['head_sha'] and r['code_commit']==code and r['readback']=='CONFIRMED' and r['git_sends'] in (0,1) and r['status'] in ('CONFIRMED','NO_CHANGE'),'PRODUCER_RECEIPT')
 P.need(source(policy,r['source']['run'],token)==r['source'],'PRODUCER_SOURCE_BINDING');return r

def prove_relay_dispatch(relay_run,token,binding,code,target):
 files=P.artifact_files(PRIVATE,str(relay_run['id']),'h1-real-relay-dispatch',
                        {'h1-real-relay-dispatch.json'},token)
 receipt=P.strict(files['h1-real-relay-dispatch.json'])
 expected={'schema':1,'lab_run':int(os.environ['GITHUB_RUN_ID']),
           'trace':binding['trace'],'code_commit':code,'target':target}
 P.need(receipt==expected,'RELAY_DISPATCH_RUN_BINDING')


def dispatch_run_id(response,repo):
 P.need(response.status==200,'DISPATCH_RESPONSE')
 raw=response.read(8193)
 P.need(len(raw)<=8192,'DISPATCH_RESPONSE')
 row=P.strict(raw);run=row.get('workflow_run_id')
 P.need(type(run) is int and run>0 and
        row.get('run_url')==f'https://api.github.com/repos/{repo}/actions/runs/{run}',
        'DISPATCH_RUN_ID')
 return run



def relay(policy,env,event):
 c=config(policy)
 P.need(env.get('GITHUB_ACTIONS')=='true' and env.get('GITHUB_JOB')=='b-relay' and env.get('GITHUB_REPOSITORY_ID')=='1429436685' and env.get('GITHUB_WORKFLOW_REF')==PRIVATE+'/'+c['relay_workflow']+'@refs/heads/main' and env.get('GITHUB_REPOSITORY')==PRIVATE and env.get('GITHUB_REF')=='refs/heads/main' and env.get('GITHUB_ACTOR_ID')==str(OWNER) and env.get('GITHUB_RUN_ATTEMPT')=='1' and env.get('GITHUB_EVENT_NAME')=='workflow_run','RELAY_CONTEXT')
 P.need(event.get('action')=='completed' and event.get('repository',{}).get('id')==1429436685 and event.get('repository',{}).get('full_name')==PRIVATE and event.get('repository',{}).get('private') is True,'RELAY_EVENT_REPOSITORY')
 up=event.get('workflow_run',{});P.need(up.get('workflow_id')==c['private_workflow_id'] and up.get('event')=='workflow_run' and up.get('conclusion')=='success' and up.get('head_branch')=='main','RELAY_PRODUCER_TRIGGER')
 P.need(type(up.get('id')) is int and up['id']>0 and re.fullmatch(r'BGZONA B '+('real ' if real(c) else '')+r'source [1-9][0-9]*',up.get('name','')) is not None and P.path_matches(up.get('path'),policy['private_workflow'],'main') and up.get('status')=='completed' and up.get('run_attempt')==1 and up.get('actor',{}).get('id')==OWNER and up.get('actor',{}).get('type')=='User' and up.get('triggering_actor',up['actor']).get('id')==OWNER,'RELAY_PRODUCER_ATTEMPT_ACTOR')
 for key in ('repository','head_repository'):
  P.need(up.get(key,{}).get('id')==1429436685 and up.get(key,{}).get('full_name')==PRIVATE,'RELAY_PRODUCER_REPOSITORY')
 code=P.ref(LAB,policy['control_ref'],env['GH_TOKEN'])
 P.need(P.ref(LAB,'refs/heads/main',env['GH_TOKEN'])==code,'RELAY_CODE_REF')
 actual=subprocess.run(['git','--no-optional-locks','-C',str(ROOT.parents[1]),'rev-parse','HEAD'],capture_output=True);P.need(actual.returncode==0 and actual.stdout.decode().strip()==code,'RELAY_EXECUTABLE_CODE')
 current=P.api(PRIVATE,'/actions/runs/'+env['GITHUB_RUN_ID'],env['GH_TOKEN'])
 P.need(current.get('workflow_id')==c['relay_workflow_id'] and current.get('event')=='workflow_run' and current.get('run_attempt')==1 and current.get('actor',{}).get('id')==OWNER and current.get('head_branch')=='main' and current.get('head_sha')==env.get('H1_WORKFLOW_SHA') and current.get('repository',{}).get('id')==1429436685,'RELAY_NATIVE_IDENTITY')
 P.need(str(current.get('id'))==env['GITHUB_RUN_ID'] and current.get('repository',{}).get('full_name')==PRIVATE and current.get('head_repository',{}).get('id')==1429436685 and current.get('head_repository',{}).get('full_name')==PRIVATE and current.get('status')=='in_progress' and current.get('conclusion') is None and current.get('display_title')=='BGZONA B relay '+str(up['id']) and P.path_matches(current.get('path'),c['relay_workflow'],'main') and current.get('triggering_actor',current['actor']).get('id')==OWNER,'RELAY_NATIVE_CONTEXT')
 P.need(P.digest(P.workflow_bytes(PRIVATE,c['relay_workflow'],current['head_sha'],env['GH_TOKEN']))==c['relay_workflow_sha256'],'RELAY_WORKFLOW_PIN')
 P.control_protection(policy,env['GH_TOKEN'],current['created_at'])
 r=private_receipt(policy,str(up['id']),code,env['GH_TOKEN'])
 P.need(r['workflow_sha']==up.get('head_sha'),'RELAY_PRODUCER_SOURCE_PIN')
 P.need(up.get('name')==('BGZONA B real source ' if real(c) else 'BGZONA B source ')+r['source']['run'],'RELAY_PRODUCER_NAME_BINDING')
 rows=history(PRIVATE,'/actions/workflows/'+str(c['relay_workflow_id'])+'/runs','workflow_runs',env['GH_TOKEN'],label='PRIVATE_RELAY_RUNS');same=[x for x in rows if x.get('display_title')=='BGZONA B relay '+str(up['id'])];P.need(len(same)==1 and str(same[0]['id'])==env['GITHUB_RUN_ID'],'DUPLICATE_RELAY')
 P.need(all(x.get('status')=='completed' or (str(x.get('id'))==env['GITHUB_RUN_ID'] and x.get('run_attempt')==1 and x.get('status')=='in_progress') for x in rows),'RELAY_CONCURRENT_OR_UNKNOWN')
 # Exactly one POST. No token in URL/arguments and no response-dependent second send.
 value=str(up['id'])
 if real(c):
  if r['status']=='NO_CHANGE' or r['public_target']==r['public_expected']:
   print('B_NO_CHANGE');return
  token=env['GH_TOKEN']
  P.need(P.ref(PRIVATE,policy['state_ref'],token)==r['commit'],'PRIVATE_STATE_MOVED')
  rec=P.strict(P.workflow_bytes(PRIVATE,'recovery.json',r['commit'],token))
  P.need(rec.get('schema')==2 and rec.get('private_run_id')==up['id'] and
         rec.get('target')==r['public_target'] and rec.get('expected')==r['public_expected'],'TRACE_TARGET_BINDING')
  value='trace:'+uuid_trace(rec.get('trace'))
 payload={'ref':'automation-control','inputs':{'mode':c['Lab_mode'],'private_run':value}}
 from urllib.request import Request,build_opener
 headers={'Authorization':'Bearer '+env['LAB_DISPATCH_PAT'],'Accept':'application/vnd.github+json',
          'Content-Type':'application/json','User-Agent':'BGZONA-owner-relay'}
 if real(c):headers['X-GitHub-Api-Version']='2026-03-10'
 req=Request('https://api.github.com/repos/'+LAB+'/actions/workflows/408732936/dispatches',
             data=json.dumps(payload).encode(),method='POST',headers=headers)
 try:
  with build_opener(P.NoRedirect).open(req,timeout=30) as response:
   if real(c):lab_run=dispatch_run_id(response,LAB)
   else:P.need(response.status==204,'RELAY_RESPONSE')
 except Exception:raise P.Stop('RELAY_UNKNOWN_READBACK_NO_RETRY') from None
 if real(c):
  P.dump(Path(env['RUNNER_TEMP'])/'h1-real-relay-dispatch.json',
         {'schema':1,'lab_run':lab_run,'trace':rec['trace'],'code_commit':code,'target':rec['target']})
  P.out('lab_run',lab_run)
 print('B_RELAY_DISPATCHED_ONCE')

def main(policy,args):
 event=P.strict(Path(os.environ['GITHUB_EVENT_PATH']).read_bytes())
 use_real=(event.get('workflow_run',{}).get('head_branch')=='cp2b-private-standard-source-r1'
           if args.stage.startswith('private-') else
           str(event.get('inputs',{}).get('private_run','')).startswith('trace:'))
 policy=select_profile(policy,use_real)
 repo,code,event,binding=context(policy,args.stage)
 if args.guard_only:return print('B_CONTEXT_VERIFIED_ADMIN_PREFLIGHT_STILL_REQUIRED')
 work=Path(os.environ['RUNNER_TEMP'])/'h1-work'
 if args.stage.endswith('prepare'):prepare(policy,args.stage.split('-')[0],code,event,binding,work,args.deps)
 elif args.stage.endswith('write'):write(policy,args.stage.split('-')[0],code,binding,work,args.deps)
 elif args.stage=='dev-dispatch':dispatch_dev(policy,binding)
 else:
  P.consumer(policy,event,work,args.deps)
  from dev_build import native
  receipt=native(policy,Path(os.environ['GITHUB_WORKSPACE'])/'dev-site',work/'public.git',os.environ['H1_DATA_COMMIT'],work/'dev-build',Path(os.environ['RUNNER_TEMP'])/'bgz-hugo/hugo',args.deps)
  if use_real:
   P.need(receipt.get('status')=='LOCAL_BUILD_ACCEPTANCE_SEAL_PASS' and receipt.get('role')=='dev' and
          receipt.get('deployed') is False,'DEV_BUILD_RECEIPT')
   val=dev_proof(binding['trace'],receipt['data_revision'],receipt['snapshot'],receipt['public_tree_sha256'])
   P.out('dev_proof',val)
 print('B_STAGE_VERIFIED')



def dev_proof(trace,commit,sid,seal):
 uuid_trace(trace)
 for value,length in ((commit,40),(sid,64),(seal,64)):
  P.need(isinstance(value,str) and re.fullmatch(f'[a-f0-9]{{{length}}}',value),'PUBLIC_DEV_PROOF')
 return f'real-dev-{trace}-{commit}-{sid}-{seal}'


def dispatch_dev(policy,binding):
 P.need(real(config(policy)) and set(binding) in ({'trace'},{'trace','recover_pending'}) and
        os.environ.get('H1_PUBLIC_SENDS')=='1','DEV_DISPATCH_SCOPE')
 raw=os.environ.get('H1_DEV_PROOF','')
 m=re.fullmatch(r'real-dev-([a-f0-9]{32})-([a-f0-9]{40})-([a-f0-9]{64})-([a-f0-9]{64})',raw)
 P.need(m and dev_proof(*m.groups())==raw and m[1]==binding['trace'] and
        m[2]==P.ref(LAB,policy['data_ref'],os.environ['GH_TOKEN']),'DEV_DISPATCH_BINDING')
 run=os.environ['GITHUB_RUN_ID'];P.need(re.fullmatch('[1-9][0-9]*',run),'LAB_RUN_ID')
 from urllib.request import Request,build_opener
 payload={'ref':'main','inputs':{'input_trust':f'rs4n-real:{run}:{raw}','accepted_cid':m[3]}}
 req=Request('https://api.github.com/repos/malin-bg/bgzona-win-dev/actions/workflows/232539432/dispatches',
              data=json.dumps(payload).encode(),method='POST',headers={'Authorization':'Bearer '+os.environ['DEV_DISPATCH_TOKEN'],
              'Accept':'application/vnd.github+json','Content-Type':'application/json',
              'X-GitHub-Api-Version':'2026-03-10','User-Agent':'BGZONA-DEV-dispatch'})
 try:
  with build_opener(P.NoRedirect).open(req,timeout=30) as response:
   dev_run=dispatch_run_id(response,'malin-bg/bgzona-win-dev')
 except Exception:raise P.Stop('DEV_DISPATCH_UNKNOWN_READBACK_NO_RETRY') from None
 P.dump(Path(os.environ['RUNNER_TEMP'])/'h1-real-dev-dispatch.json',{'schema':1,'dev_run':dev_run})
 P.out('dev_run',dev_run)
 print('DEV_DISPATCH_SENT_ONCE')

def prove_pre_dispatch_stop(run,token):
 # The pinned relay checks REAL in config() before any dispatch is possible.
 P.need(run.get('id')==RECOVERY_RELAY and run.get('conclusion')=='failure','RECOVERY_RELAY')
 run_path='/actions/runs/'+str(RECOVERY_RELAY)
 jobs=history(PRIVATE,run_path+'/attempts/1/jobs','jobs',token,label='RECOVERY_RELAY_JOBS',require_total=True)
 matching=[j for j in jobs if j.get('id')==RECOVERY_RELAY_JOB]
 P.need(len(matching)==1,'RECOVERY_RELAY_JOB')
 job=matching[0]
 steps=[s for s in job.get('steps',[]) if s.get('name')=='Verify producer and wake B Lab once']
 P.need(job.get('name')=='b-relay' and job.get('status')=='completed' and
        job.get('conclusion')=='failure' and len(steps)==1 and
        steps[0].get('conclusion')=='failure','RECOVERY_RELAY_JOB')
 artifacts=P.api(PRIVATE,run_path+'/artifacts?per_page=1',token)
 P.need(artifacts.get('total_count')==0 and artifacts.get('artifacts')==[],'RECOVERY_RELAY_ARTIFACT')
 raw=P.request('/repos/'+PRIVATE+'/actions/jobs/'+str(RECOVERY_RELAY_JOB)+'/logs',token,binary=True)
 lines=[re.sub(r'^\S+Z[ \t]+','',re.sub(r'\x1b\[[0-9;]*m','',line)).strip()
        for line in raw.decode('utf-8-sig').splitlines()]
 # Match runner output only, not shell source lines in the same log.
 stop='B_STOP_OWNER_ROUTE_DISABLED'
 flag='BGZONA_REAL_ENABLED: false'
 error='##[error]Process completed with exit code 2.'
 P.need(all(lines.count(x)==1 for x in (flag,stop,error)) and
        lines.index(flag)<lines.index(stop)<lines.index(error) and
        'B_RELAY_DISPATCHED_ONCE' not in lines,'RECOVERY_RELAY_STOP_PROOF')


def completed_relay(policy,producer_id,token,*,recovery=False):
 import time
 c=config(policy);deadline=time.monotonic()+45
 while True:
  rows=history(PRIVATE,'/actions/workflows/'+str(c['relay_workflow_id'])+'/runs','workflow_runs',token,label='PRIVATE_RELAY_RUNS')
  hits=[r for r in rows if r.get('display_title')=='BGZONA B relay '+producer_id]
  P.need(len(hits)==1,'RELAY_HISTORY_UNKNOWN');r=hits[0]
  P.need(r.get('event')=='workflow_run' and r.get('run_attempt')==1 and r.get('actor',{}).get('id')==OWNER and r.get('head_branch')=='main','RELAY_IDENTITY')
  if r.get('status')=='completed':
   if not recovery:P.need(r.get('conclusion')=='success','RELAY_FAILED')
   P.need(P.digest(P.workflow_bytes(PRIVATE,c['relay_workflow'],r['head_sha'],token))==c['relay_workflow_sha256'],'RELAY_WORKFLOW_PIN')
   if recovery:prove_pre_dispatch_stop(r,token)
   return r
  P.need(time.monotonic()<deadline,'RELAY_OUTCOME_UNKNOWN');time.sleep(2)

if __name__=='__main__':
 try:
  policy=P.verified_code();event=P.strict(Path(os.environ['GITHUB_EVENT_PATH']).read_bytes())
  real_run=str(event.get('workflow_run',{}).get('name','')).startswith('BGZONA B real source ')
  relay(select_profile(policy,real_run),os.environ,event)
 except Exception as e:
  source_stop=getattr(sys.modules.get('source_contract'),'Stop',P.Stop)
  category=str(e) if isinstance(e,(P.Stop,source_stop)) and re.fullmatch('[A-Z][A-Z0-9_]+',str(e)) else 'UNKNOWN'
  print('B_STOP_'+category);sys.exit(2)
