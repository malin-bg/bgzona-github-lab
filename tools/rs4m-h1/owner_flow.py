"""Opt-in owner-flow callers over existing H1I/P4. No publisher/recovery rewrite.
The existing Lab OWNER/admin-preflight contract is deliberately unchanged.
"""
from pathlib import Path
import json,os,re,subprocess,sys,hashlib
import pipeline as P
ROOT=Path(__file__).resolve().parent
PRIVATE='malin-bg/bgzona-private-intake';LAB='malin-bg/bgzona-github-lab'
OWNER=252877943

def config(policy):
 c=policy['owner_flow'];P.need(c.get('enabled') is True,'OWNER_ROUTE_DISABLED')
 P.need(c['source_ref'] in ('refs/heads/cp2b-private-standard-source-r1','refs/heads/synthetic-source'),'OWNER_SOURCE_REF')
 P.need(policy['control_protection_authority']=={'mode':'owner-admin-preflight-and-live-effective','expected_bypass_actors':[],'no_ruleset_update_since_run_creation':True},'LAB_AUTHORITY_REVIEW_REQUIRED')
 return c

def history(repo,path,key,token):
 values=[];page=1;total=None
 while True:
  response=P.api(repo,path+('&' if '?' in path else '?')+'per_page=100&page='+str(page),token)
  rows=response[key] if key else response;P.need(isinstance(rows,list),'HISTORY_SHAPE')
  if key and 'total_count' in response:
   if total is None:total=response['total_count']
   P.need(response['total_count']==total,'HISTORY_CHANGED')
  values.extend(rows)
  if len(rows)<100:break
  page+=1
 P.need(total is None or len(values)==total,'INCOMPLETE_HISTORY')
 if values and 'id' in values[0]:P.need(len({v['id'] for v in values})==len(values),'DUPLICATE_HISTORY')
 return values

def source(policy,run_id,token):
 c=config(policy);run=P.api(PRIVATE,'/actions/runs/'+str(run_id)+'/attempts/1',token)
 P.need(run.get('id')==int(run_id) and run.get('workflow_id')==c['source_workflow_id'] and run.get('event')=='push' and run.get('run_attempt')==1 and run.get('status')=='completed' and run.get('conclusion')=='success' and run.get('head_branch')==c['source_ref'].removeprefix('refs/heads/') and run.get('repository',{}).get('id')==1429436685 and run.get('repository',{}).get('full_name')==PRIVATE and run.get('actor',{}).get('id')==OWNER and run['actor'].get('type')=='User' and run.get('triggering_actor',run['actor']).get('id')==OWNER and P.path_matches(run.get('path'),c['source_workflow'],run['head_branch']),'SOURCE_NATIVE_IDENTITY')
 P.need(P.digest(P.workflow_bytes(PRIVATE,c['source_workflow'],run['head_sha'],token))==c['source_workflow_sha256'],'SOURCE_EXECUTABLE_CODE')
 import source_contract as S
 # Existing validator's configured branch, never a branch supplied by event/payload.
 S.STANDARD_REF=c['source_ref']
 artifacts=history(PRIVATE,'/actions/runs/'+str(run_id)+'/artifacts','artifacts',token)
 a=S.select(artifacts,int(run_id),1,run['head_sha'],ref=c['source_ref'])
 raw=P.request('/repos/'+PRIVATE+'/actions/artifacts/'+str(a['id'])+'/zip',token,binary=True)
 P.need(len(raw)==a['size_in_bytes'],'SOURCE_ARCHIVE_SIZE');packet=S.decode(raw,a)
 commit=P.api(PRIVATE,'/git/commits/'+run['head_sha'],token)
 activity=history(PRIVATE,'/activity',None,token)
 S.verify(packet,int(run_id),1,run['head_sha'],run,activity,[p['sha'] for p in commit['parents']],ref=c['source_ref'])
 return {'run':str(run_id),'after':run['head_sha'],'before':packet['before'],'packet_sha256':P.digest(S.data(packet))}

def context(policy,stage):
 c=config(policy);env=os.environ;private=stage.startswith('private-');repo=PRIVATE if private else LAB
 P.need(env.get('GITHUB_REPOSITORY')==repo and env.get('GITHUB_REPOSITORY_ID')==str(1429436685 if private else 1426765395) and env.get('GITHUB_REF')==('refs/heads/main' if private else policy['control_ref']) and env.get('GITHUB_ACTOR_ID')==str(OWNER) and env.get('GITHUB_RUN_ATTEMPT')=='1','OWNER_EXECUTION_CONTEXT')
 event=P.strict(Path(env['GITHUB_EVENT_PATH']).read_bytes());token=env['GH_TOKEN'];workflow_sha=env.get('H1_WORKFLOW_SHA');P.need(workflow_sha==env.get('GITHUB_SHA') and re.fullmatch('[a-f0-9]{40}',workflow_sha or ''),'OWNER_WORKFLOW_SHA')
 code=subprocess.run(['git','--no-optional-locks','-C',str(ROOT.parents[1]),'rev-parse','HEAD'],capture_output=True);P.need(code.returncode==0,'EXECUTABLE_CHECKOUT');code=code.stdout.decode().strip()
 P.need(P.ref(LAB,policy['control_ref'],token)==P.ref(LAB,'refs/heads/main',token)==code,'TRUSTED_EXECUTABLE_PIN')
 current=P.api(repo,'/actions/runs/'+env['GITHUB_RUN_ID'],token)
 P.need(current.get('head_sha')==workflow_sha and current.get('run_attempt')==1 and str(current.get('actor',{}).get('id'))==str(OWNER) and current.get('repository',{}).get('full_name')==repo and current.get('head_branch')==env['GITHUB_REF'].removeprefix('refs/heads/'),'LIVE_RUN_BINDING')
 P.need(current.get('workflow_id')==(c['private_workflow_id'] if private else 408732936) and P.path_matches(current.get('path'),policy['private_workflow' if private else 'shared_workflow'],current['head_branch']) and env.get('GITHUB_JOB')=={'private-prepare':'prepare','private-write':'writer','public-prepare':'h1-export','public-write':'h1-write','consumer':'h1-consumer'}[stage],'LIVE_JOB_WORKFLOW')
 if private:
  upstream=event.get('workflow_run',{});P.need(env.get('GITHUB_EVENT_NAME')==current.get('event')=='workflow_run' and upstream.get('workflow_id')==c['source_workflow_id'] and upstream.get('head_branch')==c['source_ref'].removeprefix('refs/heads/'),'SOURCE_WORKFLOW_RUN_BOUNDARY')
  P.need(P.digest(P.workflow_bytes(PRIVATE,policy['private_workflow'],workflow_sha,token))==policy['private_workflow_sha256'],'PRIVATE_WORKFLOW_PIN')
  binding=source(policy,str(upstream['id']),token);P.need(upstream.get('head_sha')==binding['after'],'SOURCE_EVENT_BINDING')
 else:
  P.need(env.get('GITHUB_EVENT_NAME')==current.get('event')=='workflow_dispatch' and env.get('GITHUB_REF_PROTECTED')=='true' and workflow_sha==code,'PROTECTED_LAB_CONTEXT')
  v=event.get('inputs',{});P.need(set(v)<={'mode','private_run','record_sha256','source_run','relay_run'} and v.get('mode')==c['Lab_mode'] and re.fullmatch('[1-9][0-9]*',v.get('private_run','')) and not any(v.get(k) for k in ('record_sha256','source_run','relay_run')),'OWNER_LAB_INPUTS');binding={'private_run':v['private_run']}
 P.control_protection(policy,token,current['created_at']) # unchanged actual guard; no invented bypass PASS
 rows=history(repo,'/actions/workflows/'+str(c['private_workflow_id'] if private else 408732936)+'/runs','workflow_runs',token)
 expected='BGZONA B source '+binding['run'] if private else 'BGZONA B producer '+binding['private_run']
 same=[r for r in rows if r.get('display_title')==expected];P.need(len(same)==1 and str(same[0]['id'])==env['GITHUB_RUN_ID'],'DUPLICATE_OWNER_EVENT')
 for status in ('in_progress','queued','waiting','requested','pending'):
  active=history(repo,'/actions/runs?status='+status,'workflow_runs',token);P.need(all(str(r['id'])==env['GITHUB_RUN_ID'] for r in active),'CONCURRENT_OR_UNKNOWN')
 return repo,code,event,binding

def inert(repo,rev,path):
 q=subprocess.run(['git','--no-optional-locks','--git-dir='+str(repo),'show',rev+':'+path],capture_output=True);P.need(q.returncode==0,'DURABLE_STATE_UNKNOWN');return P.strict(q.stdout)

def handoff_context(kind,code,folder,binding,mode):
 value={'schema':1,'kind':kind,'mode':mode,'repository':os.environ['GITHUB_REPOSITORY'],'run':os.environ['GITHUB_RUN_ID'],'attempt':1,'workflow_sha':os.environ['H1_WORKFLOW_SHA'],'code_commit':code,'proposal_sha':P.digest((folder/'proposal.json').read_bytes())}
 if kind=='private':value['source']=binding
 else:value['private_run']=binding['private_run']
 if mode=='CANDIDATE':value['bundle_sha']=P.digest((folder/'candidate.bundle').read_bytes())
 P.dump(folder/'context.json',value)
 for k in ('proposal_sha','bundle_sha'):
  if k in value:P.out(k,value[k])
 P.out('handoff_mode',mode)

def prepare(policy,kind,code,event,binding,work,deps):
 c=config(policy);work.mkdir();token=os.environ['GH_TOKEN'];app=os.environ.get('PRIVATE_READ_TOKEN')
 if kind=='private':
  P.acquire(PRIVATE,policy['state_ref'],policy['state_root'],work/'private.git',work,deps,token)
  G,H=P.libraries(deps);url='https://github.com/'+PRIVATE+'.git';ch=H.Channel(H.Endpoint(url,'source-read',c['source_ref'],policy['source_root']),H.Identity('source-read',url,'x-access-token',token));ch.acquire_source(work/'private.git')
  P.need(P.ref(PRIVATE,c['source_ref'],token)==binding['after'],'SOURCE_MOVED_BEFORE_ACQUISITION')
 else:
  completed_relay(policy,binding['private_run'],app)
  receipt=private_receipt(policy,binding['private_run'],code,app)
  head=P.acquire(PRIVATE,policy['state_ref'],policy['state_root'],work/'private.git',work,deps,app);P.need(head==receipt['commit'],'PRIVATE_STATE_MOVED')
 P.acquire(LAB,policy['data_ref'],policy['data_root'],work/'public.git',work,deps)
 private_head=P.ref(PRIVATE,policy['state_ref'],token if kind=='private' else app);rec=inert(work/'private.git',private_head,'recovery.json');public_head=P.ref(LAB,policy['data_ref'],token)
 if kind=='private' and public_head!=rec['target']:
  P.need(public_head==rec['expected'],'PUBLIC_CONFLICT');state=inert(work/'private.git',private_head,'state.json');P.need(state['source_cursor']==binding['after'],'PENDING_PREVIOUS_EXPORT_NEW_SOURCE_RETAINED');folder=work/'private-handoff';folder.mkdir();P.dump(folder/'proposal.json',{'expected':private_head,'commit':private_head,'status':'READBACK'});handoff_context(kind,code,folder,binding,'PENDING');return
 q=subprocess.run([sys.executable,'-I','-B',str(ROOT/'prepare_phase.py'),kind,'--workspace',str(work),'--deps',str(Path(deps).resolve()),'--source-ref',c['source_ref']],capture_output=True)
 P.need(q.returncode==0,'CREDENTIAL_FREE_PREPARATION_STOP');folder=work/(kind+'-handoff');out=P.strict((folder/'result.json').read_bytes());mode='CANDIDATE'
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
 else:expected['private_run']=binding['private_run']
 if mode=='CANDIDATE':expected['bundle_sha']=P.digest(files['candidate.bundle'])
 P.need(ctx==expected and ctx['proposal_sha']==os.environ['H1_PROPOSAL_SHA'] and (mode!='CANDIDATE' or ctx['bundle_sha']==os.environ['H1_BUNDLE_SHA']),'SAME_RUN_CODE_ARTIFACT_BINDING')
 proposal=P.strict(files['proposal.json']);G,H=P.libraries(deps);G.ROOT=work;work.mkdir();client=G.init(work/'writer.git');reference=policy['state_ref' if kind=='private' else 'data_ref'];root=policy['state_root' if kind=='private' else 'data_root']
 import write_ref as W
 if mode=='CANDIDATE':
  listing=G.run(client,'bundle','list-heads',str(folder/'candidate.bundle')).stdout.decode().splitlines();P.need(listing==[proposal['commit']+' refs/heads/s1-handoff-candidate'],'BUNDLE_IDENTITY');G.run(client,'bundle','verify',str(folder/'candidate.bundle'));G.run(client,'fetch','--no-tags',str(folder/'candidate.bundle'),'refs/heads/s1-handoff-candidate');result=W.write_once(client,proposal,W.Policy(repo,reference,root),W.HTTPSWriter(W.Policy(repo,reference,root),os.environ['GH_TOKEN']))
  P.need(result['status']=='CONFIRMED' and result['sends'] in (0,1),'WRITE_UNKNOWN_NO_RETRY')
 else:
  P.need(set(proposal)=={'expected','commit','status'} and proposal['status']=='READBACK' and proposal['expected']==proposal['commit'],'READBACK_PROPOSAL');P.need(P.acquire(repo,reference,root,work/'readback.git',work,deps,os.environ['GH_TOKEN'])==proposal['commit'],'READBACK_REF_MOVED');client=work/'readback.git';result={'status':'CONFIRMED','sends':0}
 P.need(P.ref(repo,reference,os.environ['GH_TOKEN'])==proposal['commit'],'INDEPENDENT_REST_READBACK')
 receipt={'schema':2,'repository':repo,'run':os.environ['GITHUB_RUN_ID'],'attempt':1,'workflow_sha':os.environ['H1_WORKFLOW_SHA'],'code_commit':code,'commit':proposal['commit'],'expected':proposal['expected'],'git_sends':result['sends'],'readback':'CONFIRMED','status':'NO_CHANGE' if mode=='NO_CHANGE' else 'CONFIRMED'}
 if kind=='private':
  rec=inert(client,proposal['commit'],'recovery.json');state=inert(client,proposal['commit'],'state.json');P.need(mode=='PENDING' or state['source_cursor']==binding['after'],'DURABLE_SOURCE_CURSOR');receipt.update(source=binding,public_target=rec['target'],public_expected=rec['expected'])
 else:P.out('data_commit',proposal['commit'])
 dest=work/'receipt';dest.mkdir();P.dump(dest/'receipt.json',receipt)

def private_receipt(policy,run_id,code,token):
 c=config(policy);run=P.api(PRIVATE,'/actions/runs/'+run_id,token)
 P.need(run.get('repository',{}).get('id')==1429436685 and run.get('workflow_id')==c['private_workflow_id'] and run.get('event')=='workflow_run' and run.get('head_branch')=='main' and run.get('run_attempt')==1 and run.get('actor',{}).get('id')==OWNER and run.get('status')=='completed' and run.get('conclusion')=='success','PRODUCER_COMPLETION')
 P.need(P.digest(P.workflow_bytes(PRIVATE,policy['private_workflow'],run['head_sha'],token))==policy['private_workflow_sha256'],'PRODUCER_WORKFLOW_PIN')
 jobs=history(PRIVATE,'/actions/runs/'+run_id+'/attempts/1/jobs','jobs',token);P.need(len(jobs)==2 and {j['name'] for j in jobs}=={'H1 PRIVATE prepare','H1 PRIVATE writer'} and all(j.get('status')=='completed' and j.get('conclusion')=='success' for j in jobs),'PRODUCER_WRITER_JOBS')
 for label,step in [('H1 PRIVATE prepare','Prepare PRIVATE candidate'),('H1 PRIVATE writer','Commit PRIVATE checkpoint')]:
  job=next(j for j in jobs if j['name']==label);hits=[x for x in job.get('steps',[]) if x.get('name')==step];P.need(len(hits)==1 and hits[0].get('conclusion')=='success','PRODUCER_REQUIRED_STEP')
 files=P.artifact_files(PRIVATE,run_id,'h1-private-receipt',{'receipt.json'},token);r=P.strict(files['receipt.json']);P.need(set(r)=={'schema','repository','run','attempt','workflow_sha','code_commit','commit','expected','git_sends','readback','status','source','public_target','public_expected'} and r['schema']==2 and r['run']==run_id and r['repository']==PRIVATE and r['attempt']==1 and r['workflow_sha']==run['head_sha'] and r['code_commit']==code and r['readback']=='CONFIRMED' and r['git_sends'] in (0,1) and r['status'] in ('CONFIRMED','NO_CHANGE'),'PRODUCER_RECEIPT')
 P.need(source(policy,r['source']['run'],token)==r['source'],'PRODUCER_SOURCE_BINDING');return r

def relay(policy,env,event):
 c=config(policy);P.need(c['source_ref']=='refs/heads/synthetic-source','RELAY_SYNTHETIC_ONLY')
 P.need(env.get('GITHUB_ACTIONS')=='true' and env.get('GITHUB_JOB')=='b-relay' and env.get('GITHUB_REPOSITORY_ID')=='1429436685' and env.get('GITHUB_WORKFLOW_REF')==PRIVATE+'/'+c['relay_workflow']+'@refs/heads/main' and env.get('GITHUB_REPOSITORY')==PRIVATE and env.get('GITHUB_REF')=='refs/heads/main' and env.get('GITHUB_ACTOR_ID')==str(OWNER) and env.get('GITHUB_RUN_ATTEMPT')=='1' and env.get('GITHUB_EVENT_NAME')=='workflow_run','RELAY_CONTEXT')
 P.need(event.get('action')=='completed' and event.get('repository',{}).get('id')==1429436685 and event.get('repository',{}).get('full_name')==PRIVATE and event.get('repository',{}).get('private') is True,'RELAY_EVENT_REPOSITORY')
 up=event.get('workflow_run',{});P.need(up.get('workflow_id')==c['private_workflow_id'] and up.get('event')=='workflow_run' and up.get('conclusion')=='success' and up.get('head_branch')=='main','RELAY_PRODUCER_TRIGGER')
 P.need(type(up.get('id')) is int and up['id']>0 and up.get('name')=='BGZONA H1 synthetic PRIVATE checkpoint' and up.get('status')=='completed' and up.get('run_attempt')==1 and up.get('actor',{}).get('id')==OWNER and up.get('actor',{}).get('type')=='User' and up.get('triggering_actor',up['actor']).get('id')==OWNER,'RELAY_PRODUCER_ATTEMPT_ACTOR')
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
 rows=history(PRIVATE,'/actions/workflows/'+str(c['relay_workflow_id'])+'/runs','workflow_runs',env['GH_TOKEN']);same=[x for x in rows if x.get('display_title')=='BGZONA B relay '+str(up['id'])];P.need(len(same)==1 and str(same[0]['id'])==env['GITHUB_RUN_ID'],'DUPLICATE_RELAY')
 P.need(all(x.get('status')=='completed' or (str(x.get('id'))==env['GITHUB_RUN_ID'] and x.get('run_attempt')==1 and x.get('status')=='in_progress') for x in rows),'RELAY_CONCURRENT_OR_UNKNOWN')
 # Exactly one POST. No token in URL/arguments and no response-dependent second send.
 payload={'ref':'automation-control','inputs':{'mode':c['Lab_mode'],'private_run':str(up['id'])}}
 from urllib.request import Request,build_opener
 req=Request('https://api.github.com/repos/'+LAB+'/actions/workflows/408732936/dispatches',data=json.dumps(payload).encode(),method='POST',headers={'Authorization':'Bearer '+env['LAB_DISPATCH_PAT'],'Accept':'application/vnd.github+json','User-Agent':'BGZONA-owner-relay'})
 try:
  with build_opener(P.NoRedirect).open(req,timeout=30) as response:P.need(response.status==204,'RELAY_RESPONSE')
 except Exception:raise P.Stop('RELAY_UNKNOWN_READBACK_NO_RETRY') from None
 print('B_RELAY_DISPATCHED_ONCE')

def main(policy,args):
 repo,code,event,binding=context(policy,args.stage)
 if args.guard_only:return print('B_CONTEXT_VERIFIED_ADMIN_PREFLIGHT_STILL_REQUIRED')
 work=Path(os.environ['RUNNER_TEMP'])/'h1-work'
 if args.stage.endswith('prepare'):prepare(policy,args.stage.split('-')[0],code,event,binding,work,args.deps)
 elif args.stage.endswith('write'):write(policy,args.stage.split('-')[0],code,binding,work,args.deps)
 else:
  P.consumer(policy,event,work,args.deps)
  from dev_build import native
  native(policy,Path(os.environ['GITHUB_WORKSPACE'])/'dev-site',work/'public.git',os.environ['H1_DATA_COMMIT'],work/'dev-build',Path(os.environ['RUNNER_TEMP'])/'bgz-hugo/hugo',args.deps)
 print('B_STAGE_VERIFIED')


def completed_relay(policy,producer_id,token):
 import time
 c=config(policy);deadline=time.monotonic()+45
 while True:
  rows=history(PRIVATE,'/actions/workflows/'+str(c['relay_workflow_id'])+'/runs','workflow_runs',token)
  hits=[r for r in rows if r.get('display_title')=='BGZONA B relay '+producer_id]
  P.need(len(hits)==1,'RELAY_HISTORY_UNKNOWN');r=hits[0]
  P.need(r.get('event')=='workflow_run' and r.get('run_attempt')==1 and r.get('actor',{}).get('id')==OWNER and r.get('head_branch')=='main','RELAY_IDENTITY')
  if r.get('status')=='completed':
   P.need(r.get('conclusion')=='success','RELAY_FAILED')
   P.need(P.digest(P.workflow_bytes(PRIVATE,c['relay_workflow'],r['head_sha'],token))==c['relay_workflow_sha256'],'RELAY_WORKFLOW_PIN');return r
  P.need(time.monotonic()<deadline,'RELAY_OUTCOME_UNKNOWN');time.sleep(2)

if __name__=='__main__':
 try:
  policy=P.verified_code();relay(policy,os.environ,P.strict(Path(os.environ['GITHUB_EVENT_PATH']).read_bytes()))
 except Exception as e:
  category=str(e) if isinstance(e,P.Stop) and re.fullmatch('[A-Z][A-Z0-9_]+',str(e)) else 'UNKNOWN'
  print('B_STOP_'+category);sys.exit(2)
