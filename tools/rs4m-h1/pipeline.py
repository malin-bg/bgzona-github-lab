"""H1-only glue over S1/C1. GET-only API, two exact Git sends in separate jobs.
No dispatch, generic orchestration, new publisher, issuer or deployment code.
"""
from pathlib import Path
from io import BytesIO
from datetime import datetime
import argparse,base64,hashlib,json,os,re,subprocess,sys,urllib.request,urllib.error,zipfile
ROOT=Path(__file__).resolve().parent
class Stop(ValueError):pass
def need(ok,code):
 if not ok:raise Stop(code)
def strict(raw):
 def unique(rows):
  value={}
  for k,v in rows:need(k not in value,'DUPLICATE_FIELD');value[k]=v
  return value
 return json.loads(raw,object_pairs_hook=unique)
def digest(raw):return hashlib.sha256(raw).hexdigest()
def dump(path,obj):Path(path).write_text(json.dumps(obj,sort_keys=True,separators=(',',':'))+'\n','utf8')
def out(key,value):
 need(re.fullmatch('[a-z_]+',key) and re.fullmatch('[a-zA-Z0-9_-]+',str(value)),'OUTPUT_SHAPE')
 with open(os.environ['GITHUB_OUTPUT'],'a',encoding='utf8') as f:f.write(key+'='+str(value)+'\n')
def verified_code():
 rows=(ROOT/'CODE-SHA256SUMS').read_text('utf8').splitlines();seen=set()
 for row in rows:
  h,name=row.split('  ',1);p=ROOT/name
  need(re.fullmatch('[a-f0-9]{64}',h) and not p.is_symlink() and p.resolve().is_relative_to(ROOT) and name not in seen,'CODE_PATH')
  need(digest(p.read_bytes())==h,'CODE_HASH');seen.add(name)
 actual={p.relative_to(ROOT).as_posix() for p in ROOT.rglob('*') if p.is_file() and '__pycache__' not in p.parts and p.name!='CODE-SHA256SUMS'}
 need(actual==seen,'CODE_FILE_SET');return strict((ROOT/'policy.json').read_bytes())
class NoRedirect(urllib.request.HTTPRedirectHandler):
 def redirect_request(self,*args):return None
def request(path,token=None,binary=False,with_headers=False):
 need(not (binary and with_headers),'API_RESPONSE_MODE')
 need(path.startswith('/repos/malin-bg/') and '..' not in path,'API_SCOPE')
 need(isinstance(token,str) and bool(token),'API_AUTH_REQUIRED')
 headers={'Accept':'application/vnd.github+json','X-GitHub-Api-Version':'2022-11-28','User-Agent':'BGZONA-H1-bounded-proof'}
 if token:headers['Authorization']='Bearer '+token
 req=urllib.request.Request('https://api.github.com'+path,headers=headers,method='GET')
 try:
  with urllib.request.build_opener(NoRedirect).open(req,timeout=30) as r:raw=r.read();status=r.status;link=r.headers.get('Link')
 except urllib.error.HTTPError as e:
  # Never forward authorization to signed artifact storage; never expose its URL.
  if binary and e.code==302:
   from urllib.parse import urlsplit
   location=e.headers.get('Location','');u=urlsplit(location)
   need(u.scheme=='https' and not u.username and not u.password and not u.fragment and u.port in (None,443) and isinstance(u.hostname,str) and (u.hostname.endswith('.blob.core.windows.net') or u.hostname.endswith('.actions.githubusercontent.com')),'ARTIFACT_REDIRECT')
   with urllib.request.build_opener(NoRedirect).open(urllib.request.Request(location,method='GET'),timeout=30) as r:raw=r.read()
  else:raise Stop('API_HTTP_'+str(e.code)) from None
 except Exception:raise Stop('API_UNKNOWN') from None
 if with_headers:
  need(status==200,'API_HTTP_'+str(status))
  return strict(raw),{'Link':link}
 return raw if binary else strict(raw)
def api(repo,path,token=None):return request('/repos/'+repo+path,token)
def ref(repo,name,token=None):
 r=api(repo,'/git/ref/'+name.removeprefix('refs/'),token)
 need(r.get('ref')==name and r.get('object',{}).get('type')=='commit' and re.fullmatch('[a-f0-9]{40}',r['object'].get('sha','')),'REST_REF')
 return r['object']['sha']
def path_matches(value,path,branch):return value in (path,path+'@'+branch)

def workflow_bytes(repo,path,sha,token=None):
 row=api(repo,'/contents/'+path+'?ref='+sha,token)
 need(row.get('type')=='file' and row.get('encoding')=='base64','WORKFLOW_CONTENT')
 return base64.b64decode(row['content'],validate=False)
def stage_context(policy,stage):
 private=stage in ('private-prepare','private-write');repo=policy['private_repo'] if private else policy['lab_repo']
 need(os.environ.get('GITHUB_REPOSITORY')==repo and os.environ.get('GITHUB_REPOSITORY_ID')==policy['private_id' if private else 'lab_id'],'REPOSITORY_CONTEXT')
 need(os.environ.get('GITHUB_ACTOR_ID')==policy['owner_actor'] and os.environ.get('GITHUB_RUN_ATTEMPT')=='1' and os.environ.get('GITHUB_EVENT_NAME')=='workflow_dispatch','ACTOR_ATTEMPT_EVENT')
 need(os.environ.get('GITHUB_JOB')=={'private-prepare':'prepare','private-write':'writer','public-prepare':'h1-export','public-write':'h1-write','consumer':'h1-consumer'}[stage],'JOB_CONTEXT')
 expected_ref='refs/heads/main' if private else policy['control_ref'];need(os.environ.get('GITHUB_REF')==expected_ref,'EXECUTION_REF')
 event=strict(Path(os.environ['GITHUB_EVENT_PATH']).read_bytes());inputs=event.get('inputs',{})
 if private:
  need(set(inputs)=={'code_commit'},'PRIVATE_INPUTS');code=inputs['code_commit']
 else:
  need(set(inputs)<={'mode','private_run','record_sha256','source_run','relay_run'} and inputs.get('mode')=='h1-synthetic' and re.fullmatch('[1-9][0-9]*',inputs.get('private_run','')) and not any(inputs.get(k) for k in ('record_sha256','source_run','relay_run')),'LAB_INPUTS')
  need(os.environ.get('GITHUB_REF_PROTECTED')=='true','PROTECTED_CONTEXT');code=os.environ['GITHUB_SHA']
 need(re.fullmatch('[a-f0-9]{40}',code) is not None,'CODE_COMMIT')
 # The payload is a selector; actual protected refs/settings establish its authority.
 need(ref(policy['lab_repo'],policy['control_ref'],os.environ['GH_TOKEN'])==code and ref(policy['lab_repo'],'refs/heads/main',os.environ['GH_TOKEN'])==code,'TRUSTED_CODE_REF_MOVED')
 checkout=subprocess.run(['git','--no-optional-locks','-C',str(ROOT.parents[1]),'rev-parse','HEAD'],capture_output=True)
 need(checkout.returncode==0 and checkout.stdout.decode().strip()==code,'EXECUTABLE_CODE_PIN')
 workflow_sha=os.environ.get('H1_WORKFLOW_SHA');need(re.fullmatch('[a-f0-9]{40}',workflow_sha or '') and workflow_sha==os.environ.get('GITHUB_SHA'),'WORKFLOW_CODE_CONTEXT')
 current=api(repo,'/actions/runs/'+os.environ['GITHUB_RUN_ID'],os.environ['GH_TOKEN'])
 need(str(current.get('id'))==os.environ['GITHUB_RUN_ID'] and current.get('run_attempt')==1 and str(current.get('actor',{}).get('id'))==policy['owner_actor'] and current.get('event')=='workflow_dispatch' and current.get('repository',{}).get('full_name')==repo and str(current.get('repository',{}).get('id'))==policy['private_id' if private else 'lab_id'] and current.get('head_sha')==workflow_sha and current.get('head_branch')==expected_ref.removeprefix('refs/heads/') and path_matches(current.get('path'),policy['private_workflow' if private else 'shared_workflow'],expected_ref.removeprefix('refs/heads/')),'LIVE_RUN_CONTEXT')
 control_protection(policy,os.environ['GH_TOKEN'],current.get('created_at'))
 if private:
  raw=workflow_bytes(repo,policy['private_workflow'],workflow_sha,os.environ['GH_TOKEN']);need(digest(raw)==policy['private_workflow_sha256'],'PRIVATE_WORKFLOW_PIN')
 else:need(workflow_sha==code,'LAB_WORKFLOW_PIN')
 # Concurrency plus strict history: this one-off test cannot be replayed as a second attempt.
 wf=policy['private_workflow'].split('/')[-1] if private else policy['shared_workflow'].split('/')[-1]
 history=api(repo,'/actions/workflows/'+wf+'/runs?per_page=100',os.environ['GH_TOKEN'])
 need(history.get('total_count')==len(history.get('workflow_runs',[])),'INCOMPLETE_WORKFLOW_HISTORY')
 relevant=[x for x in history['workflow_runs'] if private or x.get('display_title','').startswith('BGZONA H1 synthetic')]
 if private:
  private_history(policy,current,os.environ['GH_TOKEN'],history)
  for key in ('source','state','data'):
   target=policy['lab_repo'] if key=='data' else policy['private_repo']
   need(ref(target,policy[key+'_ref'],os.environ['GH_TOKEN'])==policy['h1g_seed_tips'][key],'H1G_RUNTIME_REF_MOVED')
 else:need(len(relevant)==1 and str(relevant[0]['id'])==os.environ['GITHUB_RUN_ID'],'REPLAY_OR_OTHER_H1_RUN')
 for status in ('in_progress','queued','waiting','requested','pending'):
  active=api(repo,'/actions/runs?status='+status+'&per_page=100',os.environ['GH_TOKEN']);rows=active.get('workflow_runs',[])
  need(active.get('total_count')==len(rows) and all(str(x['id'])==os.environ['GITHUB_RUN_ID'] for x in rows),'CONCURRENT_OR_UNKNOWN')
 return repo,code,event

def control_time(value):
 need(isinstance(value,str),'CONTROL_TIMESTAMP')
 try:parsed=datetime.fromisoformat(value.replace('Z','+00:00'))
 except ValueError:raise Stop('CONTROL_TIMESTAMP') from None
 need(parsed.tzinfo is not None,'CONTROL_TIMESTAMP');return parsed


def owner_control_preflight(policy,code,run_created_at):
 # This variable is supplied only from GitHub Actions settings, never from inputs.
 try:record=strict(os.environ.get('BGZONA_CONTROL_PREFLIGHT','').encode())
 except (ValueError,UnicodeError):raise Stop('CONTROL_OWNER_PREFLIGHT_REQUIRED') from None
 need(isinstance(record,dict) and set(record)=={'repository','ref','ruleset_id','node_id',
      'updated_at','bypass_actors','code_commit'} and record['repository']==policy['lab_repo']
      and record['ref']==policy['control_ref'] and record['ruleset_id']==policy['ruleset_id']
      and type(record['ruleset_id']) is int and isinstance(record['node_id'],str)
      and record['node_id'] and record['bypass_actors']==[]
      and re.fullmatch('[0-9a-f]{40}',code) and record['code_commit']==code,
      'CONTROL_OWNER_PREFLIGHT_SCOPE')
 need(control_time(record['updated_at'])<run_created_at,'CONTROL_OWNER_PREFLIGHT_STALE')
 return record


def control_protection(policy,token,run_created_at,*,real_code=None):
 # Missing bypass metadata is not an empty list. The separately authorized OWNER
 # preflight proves that hidden setting; this read-only job proves live rules.
 authority=policy.get('control_protection_authority')
 need(authority=={'mode':'owner-admin-preflight-and-live-effective','expected_bypass_actors':[],'no_ruleset_update_since_run_creation':True},'CONTROL_AUTHORITY')
 created=control_time(run_created_at);expected_types=['deletion','non_fast_forward','update']
 proof=owner_control_preflight(policy,real_code,created) if real_code is not None else None
 def view(row):
  need(isinstance(row,dict) and row.get('id')==policy['ruleset_id'] and row.get('target')=='branch' and row.get('source_type')=='Repository' and row.get('source')==policy['lab_repo'] and row.get('enforcement')=='active' and row.get('conditions')==policy['ruleset_conditions'],'CONTROL_PROTECTION')
  rules=row.get('rules');need(isinstance(rules,list) and len(rules)==3 and all(isinstance(r,dict) and set(r)=={'type'} for r in rules) and sorted(r['type'] for r in rules)==expected_types,'CONTROL_PROTECTION')
  if 'bypass_actors' in row:need(row['bypass_actors']==[],'CONTROL_BYPASS_PRESENT')
  updated=control_time(row.get('updated_at'));need(updated<created,'CONTROL_CHANGED_SINCE_DISPATCH')
  if proof is not None:
   need(row.get('node_id')==proof['node_id'] and updated==control_time(proof['updated_at']),
        'CONTROL_CHANGED_SINCE_OWNER_PREFLIGHT')
  return (updated,'LIVE_EMPTY' if 'bypass_actors' in row else 'OWNER_PREFLIGHT_ONLY')
 path='/rulesets/'+str(policy['ruleset_id']);before=view(api(policy['lab_repo'],path,token))
 branch=policy['control_ref'].removeprefix('refs/heads/');need(branch=='automation-control','CONTROL_REF')
 effective=api(policy['lab_repo'],'/rules/branches/'+branch+'?per_page=100&page=1',token)
 need(isinstance(effective,list) and len(effective)==3,'CONTROL_EFFECTIVE_RULES')
 for row in effective:
  need(isinstance(row,dict) and set(row)=={'type','ruleset_source_type','ruleset_source','ruleset_id'} and row.get('ruleset_id')==policy['ruleset_id'] and row.get('ruleset_source_type')=='Repository' and row.get('ruleset_source')==policy['lab_repo'],'CONTROL_EFFECTIVE_RULES')
 need(sorted(row['type'] for row in effective)==expected_types,'CONTROL_EFFECTIVE_RULES')
 need(view(api(policy['lab_repo'],path,token))==before,'CONTROL_CHANGED_DURING_CHECK')
 return {'live_rules':'VERIFIED','bypass_authority':before[1]}

def private_history(policy,current,token,history=None):
 # Exactly the two audited pre-publisher failures plus one separately authorized run.
 priors=policy['prior_private_failures'];repo=policy['private_repo'];run_id=str(current.get('id'))
 need(len(priors)==2 and {p['run'] for p in priors}=={43722678769,43727037807},'PRIOR_FAILURE_SCOPE')
 prior_ids={str(p['run']) for p in priors}
 need(run_id not in prior_ids and current.get('workflow_id')==419182651 and current.get('run_attempt')==1,'PRIVATE_SUCCESSOR_IDENTITY')
 if history is None:history=api(repo,'/actions/workflows/'+policy['private_workflow'].split('/')[-1]+'/runs?per_page=100',token)
 rows=history.get('workflow_runs',[])
 need(history.get('total_count')==len(rows)==3 and {str(r.get('id')) for r in rows}==prior_ids|{run_id},'REPLAY_OR_OTHER_H1_RUN')
 for prior in priors:
  old=api(repo,'/actions/runs/'+str(prior['run']),token)
  need(old.get('id')==prior['run'] and old.get('workflow_id')==419182651 and old.get('run_attempt')==1 and old.get('status')=='completed' and old.get('conclusion')=='failure' and old.get('event')=='workflow_dispatch' and old.get('head_branch')=='main' and old.get('repository',{}).get('full_name')==repo and str(old.get('repository',{}).get('id'))==policy['private_id'] and str(old.get('actor',{}).get('id'))==policy['owner_actor'] and path_matches(old.get('path'),policy['private_workflow'],'main') and re.fullmatch('[a-f0-9]{40}',old.get('head_sha','')),'PRIOR_FAILURE_IDENTITY')
  row=next(r for r in rows if r['id']==prior['run'])
  need(all(row.get(k)==old.get(k) for k in ('id','workflow_id','run_attempt','status','conclusion','event','head_branch','head_sha','path','created_at')) and row.get('actor',{}).get('id')==old.get('actor',{}).get('id'),'PRIVATE_HISTORY_CONTRADICTION')
  need(all(re.fullmatch(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z',v or '') for v in (old.get('updated_at'),current.get('created_at'))) and old['updated_at']<current['created_at'],'PRIVATE_SUCCESSOR_ORDER')
  need(digest(workflow_bytes(repo,policy['private_workflow'],old['head_sha'],token))==prior['workflow_sha256'],'PRIOR_WORKFLOW_PIN')
  jobs=api(repo,'/actions/runs/'+str(prior['run'])+'/attempts/1/jobs?per_page=100',token);js=jobs.get('jobs',[])
  need(jobs.get('total_count')==len(js)==2 and {j.get('id') for j in js}=={prior['prepare_job'],prior['writer_job']},'PRIOR_FAILURE_JOBS')
  for j in js:need(j.get('run_id')==prior['run'] and j.get('run_attempt')==1 and j.get('head_sha')==old['head_sha'] and j.get('status')=='completed','PRIOR_JOB_IDENTITY')
  prepare=next(j for j in js if j['id']==prior['prepare_job']);writer=next(j for j in js if j['id']==prior['writer_job'])
  need(prepare.get('name')=='H1 PRIVATE prepare' and prepare.get('conclusion')=='failure' and writer.get('name')=='H1 PRIVATE writer' and writer.get('conclusion')=='skipped' and writer.get('steps')==[],'PRIOR_FAILURE_BOUNDARY')
  failure=prior['failure_step'];need(failure in ('Acquire exact protected public code','Verify execution and immutable code'),'PRIOR_FAILURE_SCOPE')
  expected=[('Set up job','success'),('Acquire exact protected public code','failure' if failure=='Acquire exact protected public code' else 'success'),('Verify execution and immutable code','failure' if failure=='Verify execution and immutable code' else 'skipped'),('Pinned isolated Pillow runtime','skipped'),('Prepare PRIVATE candidate','skipped'),('Run actions/upload-artifact@ea165f8d65b6e75b540449e92b4886f43607fa02','skipped'),('Complete job','success')]
  steps=prepare.get('steps',[]);need([(s.get('name'),s.get('conclusion')) for s in steps]==expected and all(s.get('status')=='completed' for s in steps),'PRIOR_FAILURE_STEPS')
  artifacts=api(repo,'/actions/runs/'+str(prior['run'])+'/artifacts?per_page=100',token)
  need(artifacts.get('total_count')==0 and artifacts.get('artifacts')==[],'PRIOR_FAILURE_ARTIFACTS')
 row=next(r for r in rows if str(r['id'])==run_id)
 need(all(row.get(k)==current.get(k) for k in ('id','workflow_id','run_attempt','status','conclusion','event','head_branch','head_sha','path','created_at')) and row.get('actor',{}).get('id')==current.get('actor',{}).get('id'),'PRIVATE_HISTORY_CONTRADICTION')

def libraries(deps):
 sys.path[:0]=[str(ROOT),str(ROOT/'runtime'),str(Path(deps).resolve())]
 import git_publisher as G
 import https_git as H
 return G,H

def acquire(repo,reference,root,local,work,deps,token=None):
 G,H=libraries(deps);G.ROOT=work;G.init(local)
 url='https://github.com/'+repo+'.git'
 if token:
  ch=H.Channel(H.Endpoint(url,'source-read',reference,root),H.Identity('source-read',url,'x-access-token',token));return ch.acquire_source(local)
 # Public data only: ordinary anonymous Git with the same process/TLS isolation, no credential supplied.
 need(repo=='malin-bg/bgzona-github-lab' and (reference,root) in (
  ('refs/heads/rs4m-synthetic-data','564c405279c89e990c9345ccf7c54e2bf38c4470'),
  ('refs/heads/publisher-data','05cfdeb41ca116dc07614906fbbdee9b3a597745')),'ANONYMOUS_SCOPE')
 def run(*args):
  q=subprocess.run([H.GIT,'--no-optional-locks','-C',str(local),'-c','credential.helper=','-c','http.extraHeader=','-c','http.sslVerify=true','-c','http.followRedirects=false','-c','http.proxy=','-c','http.maxRetries=0','-c','core.hooksPath='+str(ROOT/'no-hooks'),*args],env=H.clean_env(),capture_output=True)
  need(q.returncode==0,'PUBLIC_GIT_UNKNOWN');return q.stdout
 before=run('ls-remote','--exit-code','--refs',url,reference);run('fetch','--no-tags',url,reference)
 actual=run('rev-parse','FETCH_HEAD').decode().strip();after=run('ls-remote','--exit-code','--refs',url,reference)
 need(before==after and before.decode().split()==[actual,reference] and run('rev-list','--max-parents=0',actual).decode().splitlines()==[root],'PUBLIC_REF_ROOT_MOVED')
 G.run(local,'update-ref',reference,actual,G.ZERO);return actual

def owners(policy,token):return {r:ref(policy['private_repo'],r,token) for r in ('refs/heads/main','refs/heads/cp2b-private-standard-source-r1')}
def context(policy,kind,code,folder,owner_refs=None):
 value={'schema':1,'kind':kind,'repository':os.environ['GITHUB_REPOSITORY'],'run':os.environ['GITHUB_RUN_ID'],'attempt':1,'workflow_sha':os.environ['H1_WORKFLOW_SHA'],'code_commit':code,'bundle_sha':digest((folder/'candidate.bundle').read_bytes()),'proposal_sha':digest((folder/'proposal.json').read_bytes())}
 if owner_refs is not None:value['owner_refs']=owner_refs
 dump(folder/'context.json',value)
 for k in ('bundle_sha','proposal_sha'):out(k,value[k])

def prepare(policy,kind,code,event,work,deps):
 work.mkdir();token=os.environ['GH_TOKEN'];app=os.environ.get('PRIVATE_READ_TOKEN')
 if kind=='private':
  expected_owners=owners(policy,token)
  acquire(policy['private_repo'],policy['state_ref'],policy['state_root'],work/'private.git',work,deps,token)
  G,H=libraries(deps)
  # Fetch only this isolated synthetic source, never the real owner/source branch.
  channel=H.Channel(H.Endpoint('https://github.com/'+policy['private_repo']+'.git','source-read',policy['source_ref'],policy['source_root']),H.Identity('source-read','https://github.com/'+policy['private_repo']+'.git','x-access-token',token));channel.acquire_source(work/'private.git')
 else:
  receipt=private_receipt(policy,event['inputs']['private_run'],code,app)
  actual=acquire(policy['private_repo'],policy['state_ref'],policy['state_root'],work/'private.git',work,deps,app)
  need(actual==receipt['commit'] and owners(policy,app)==receipt['owner_refs'],'PRIVATE_CHECKPOINT_OR_OWNER_MOVED');expected_owners=None
 acquire(policy['lab_repo'],policy['data_ref'],policy['data_root'],work/'public.git',work,deps)
 q=subprocess.run([sys.executable,'-I','-B',str(ROOT/'prepare_phase.py'),kind,'--workspace',str(work),'--deps',str(Path(deps).resolve())],capture_output=True)
 need(q.returncode==0,'CREDENTIAL_FREE_PREPARATION_STOP');folder=work/(kind+'-handoff')
 need(strict((folder/'result.json').read_bytes())['status']==('PRIVATE_PREPARED' if kind=='private' else 'PUBLIC_PREPARED'),'NO_NEW_H1_CANDIDATE')
 if kind=='private':need(owners(policy,token)==expected_owners,'OWNER_MOVED_BEFORE_CHECKPOINT')
 else:
  proposal=strict((folder/'proposal.json').read_bytes());need(proposal['commit']==receipt['public_target'] and proposal['expected']==receipt['public_expected'],'DURABLE_PUBLIC_TARGET')
 context(policy,kind,code,folder,expected_owners)


def write(policy,kind,code,work,deps):
 folder=Path(os.environ['RUNNER_TEMP'])/'h1-handoff';need(not folder.exists(),'HANDOFF_EXISTS');folder.mkdir()
 files=artifact_files(os.environ['GITHUB_REPOSITORY'],os.environ['GITHUB_RUN_ID'],'h1-'+kind+'-candidate',{'candidate.bundle','proposal.json','context.json'},os.environ['GH_TOKEN'])
 for name,raw in files.items():(folder/name).write_bytes(raw)
 ctx=strict((folder/'context.json').read_bytes());expected={'schema':1,'kind':kind,'repository':os.environ['GITHUB_REPOSITORY'],'run':os.environ['GITHUB_RUN_ID'],'attempt':1,'workflow_sha':os.environ['H1_WORKFLOW_SHA'],'code_commit':code}
 need(set(ctx)==set(expected)|{'bundle_sha','proposal_sha'}|({'owner_refs'} if kind=='private' else set()) and all(ctx[k]==v for k,v in expected.items()),'ARTIFACT_RUN_CODE_BINDING')
 for k,name in [('bundle_sha','candidate.bundle'),('proposal_sha','proposal.json')]:need(ctx[k]==os.environ['H1_'+k.upper()]==digest((folder/name).read_bytes()),'PRODUCER_OUTPUT_HASH')
 proposal=strict((folder/'proposal.json').read_bytes());G,H=libraries(deps);G.ROOT=work;work.mkdir();client=G.init(work/'writer.git')
 listing=G.run(client,'bundle','list-heads',str(folder/'candidate.bundle')).stdout.decode().splitlines()
 need(listing==[proposal['commit']+' refs/heads/s1-handoff-candidate'],'BUNDLE_REF');G.run(client,'bundle','verify',str(folder/'candidate.bundle'))
 G.run(client,'fetch','--no-tags',str(folder/'candidate.bundle'),'refs/heads/s1-handoff-candidate')
 import write_ref as W
 repo=policy['private_repo' if kind=='private' else 'lab_repo'];reference=policy['state_ref' if kind=='private' else 'data_ref'];root=policy['state_root' if kind=='private' else 'data_root']
 if kind=='private':need(owners(policy,os.environ['GH_TOKEN'])==ctx['owner_refs'],'OWNER_MOVED_BEFORE_WRITE')
 result=W.write_once(client,proposal,W.Policy(repo,reference,root),W.HTTPSWriter(W.Policy(repo,reference,root),os.environ['GH_TOKEN']))
 need(result['status']=='CONFIRMED' and result['sends']==1,'WRITE_NOT_CONFIRMED_NO_RETRY')
 need(ref(repo,reference,os.environ['GH_TOKEN'])==proposal['commit'],'INDEPENDENT_REST_READBACK')
 receipt={'schema':1,'repository':repo,'run':os.environ['GITHUB_RUN_ID'],'attempt':1,'workflow_sha':os.environ['H1_WORKFLOW_SHA'],'code_commit':code,'commit':proposal['commit'],'expected':proposal['expected'],'git_sends':1,'readback':'CONFIRMED'}
 if kind=='private':
  need(owners(policy,os.environ['GH_TOKEN'])==ctx['owner_refs'],'OWNER_MOVED_AFTER_WRITE');receipt['owner_refs']=ctx['owner_refs']
  recovery=strict(G.run(client,'show',proposal['commit']+':recovery.json').stdout);receipt.update(public_target=recovery['target'],public_expected=recovery['expected'])
 else:out('data_commit',proposal['commit'])
 dest=work/'receipt';dest.mkdir();dump(dest/'receipt.json',receipt)

def artifact_files(repo,run_id,name,inventory,token):
 artifacts=api(repo,'/actions/runs/'+run_id+'/artifacts?per_page=100',token);items=artifacts.get('artifacts',[])
 need(artifacts.get('total_count')==len(items),'ARTIFACT_HISTORY')
 matched=[x for x in items if x.get('name')==name];need(len(matched)==1 and not matched[0].get('expired'),'PRODUCER_ARTIFACT');a=matched[0]
 repo_id={'malin-bg/bgzona-private-intake':1429436685,'malin-bg/bgzona-github-lab':1426765395}[repo]
 need(a.get('workflow_run',{}).get('repository_id')==repo_id and a.get('workflow_run',{}).get('head_repository_id')==repo_id,'ARTIFACT_REPOSITORY')
 need(a.get('workflow_run',{}).get('id')==int(run_id) and isinstance(a.get('digest'),str) and re.fullmatch('sha256:[a-f0-9]{64}',a['digest']),'ARTIFACT_RUN_DIGEST')
 raw=request('/repos/'+repo+'/actions/artifacts/'+str(a['id'])+'/zip',token,binary=True)
 need(len(raw)==a['size_in_bytes'] and 'sha256:'+digest(raw)==a['digest'],'ARTIFACT_TRANSFER')
 with zipfile.ZipFile(BytesIO(raw)) as z:
  need(len(z.namelist())==len(inventory) and set(z.namelist())==inventory and z.testzip() is None,'ARTIFACT_INVENTORY')
  return {name:z.read(name) for name in inventory}

def private_receipt(policy,run_id,code,token):
 need(re.fullmatch('[1-9][0-9]*',run_id) is not None and token,'PRIVATE_PRODUCER_INPUT')
 repo=policy['private_repo'];run=api(repo,'/actions/runs/'+run_id,token)
 need(str(run.get('id'))==run_id and run.get('event')=='workflow_dispatch' and run.get('repository',{}).get('full_name')==repo and str(run.get('repository',{}).get('id'))==policy['private_id'] and run.get('head_branch')=='main' and run.get('run_attempt')==1 and str(run.get('actor',{}).get('id'))==policy['owner_actor'] and run.get('status')=='completed' and run.get('conclusion')=='success' and path_matches(run.get('path'),policy['private_workflow'],'main'),'PRIVATE_PRODUCER_RUN')
 wf=api(repo,'/actions/workflows/'+policy['private_workflow'].split('/')[-1],token);need(run['workflow_id']==wf['id'],'PRIVATE_WORKFLOW_ID')
 need(digest(workflow_bytes(repo,policy['private_workflow'],run['head_sha'],token))==policy['private_workflow_sha256'],'PRIVATE_PRODUCER_CODE')
 private_history(policy,run,token)
 jobs=api(repo,'/actions/runs/'+run_id+'/attempts/1/jobs?per_page=100',token);rows=jobs.get('jobs',[])
 need(jobs.get('total_count')==2 and len(rows)==2 and {x['name'] for x in rows}=={'H1 PRIVATE prepare','H1 PRIVATE writer'} and all(x.get('conclusion')=='success' and x.get('status')=='completed' for x in rows),'PRIVATE_PRODUCER_JOBS')
 for label,step in [('H1 PRIVATE prepare','Prepare PRIVATE candidate'),('H1 PRIVATE writer','Commit PRIVATE checkpoint')]:
  row=next(j for j in rows if j['name']==label);matches=[x for x in row['steps'] if x['name']==step];need(len(matches)==1 and matches[0].get('conclusion')=='success','PRIVATE_PRODUCER_STEPS')
 files=artifact_files(repo,run_id,'h1-private-receipt',{'receipt.json'},token);r=strict(files['receipt.json'])
 need(set(r)=={'schema','repository','run','attempt','workflow_sha','code_commit','commit','expected','git_sends','readback','owner_refs','public_target','public_expected'} and r['schema']==1 and r['repository']==repo and r['run']==run_id and r['attempt']==1 and r['workflow_sha']==run['head_sha'] and r['code_commit']==code and r['git_sends']==1 and r['readback']=='CONFIRMED','PRIVATE_RECEIPT_BINDING')
 return r

def consumer(policy,event,work,deps):
 work.mkdir();commit=os.environ['H1_DATA_COMMIT'];need(re.fullmatch('[a-f0-9]{40}',commit),'DATA_COMMIT')
 seen=acquire(policy['lab_repo'],policy['data_ref'],policy['data_root'],work/'public.git',work,deps)
 need(seen==commit,'PUBLIC_COMMIT_MOVED')
 env={k:v for k,v in os.environ.items() if k.upper() in ('PATH','SYSTEMROOT','WINDIR','TEMP','TMP','COMSPEC','PATHEXT')}
 env.update(PYTHONDONTWRITEBYTECODE='1',GIT_CONFIG_NOSYSTEM='1',GIT_CONFIG_GLOBAL=os.devnull,GIT_CONFIG_SYSTEM=os.devnull,GIT_ALLOW_PROTOCOL='file')
 q=subprocess.run([sys.executable,'-I','-B',str(ROOT/'consumer_check.py'),str(work),commit,str(Path(deps).resolve()),policy['data_ref']],capture_output=True,env=env);need(q.returncode==0,'TRUSTED_DEV_CONSUMER_STOP')

def main():
 p=argparse.ArgumentParser();p.add_argument('stage',choices=['private-prepare','private-write','public-prepare','public-write','consumer','dev-dispatch']);p.add_argument('--deps',required=True);p.add_argument('--guard-only',action='store_true');p.add_argument('--owner-flow',action='store_true');args=p.parse_args()
 policy=verified_code()
 if args.owner_flow:
  # Only verified sibling modules; -I omits the script directory.
  sys.path.insert(0,str(ROOT))
  if __name__=='__main__':sys.modules['pipeline']=sys.modules[__name__]
  import owner_flow
  return owner_flow.main(policy,args)
 repo,code,event=stage_context(policy,args.stage)
 if args.guard_only:return print('H1_CONTEXT_VERIFIED')
 work=Path(os.environ['RUNNER_TEMP'])/'h1-work'
 if args.stage.endswith('prepare'):prepare(policy,args.stage.split('-')[0],code,event,work,args.deps)
 elif args.stage.endswith('write'):write(policy,args.stage.split('-')[0],code,work,args.deps)
 else:consumer(policy,event,work,args.deps)
 print('H1_STAGE_VERIFIED')
if __name__=='__main__':
 try:main()
 except Exception as e:
  source_stop=getattr(sys.modules.get('source_contract'),'Stop',Stop)
  category=str(e) if isinstance(e,(Stop,source_stop)) and re.fullmatch('[A-Z][A-Z0-9_]+',str(e)) else 'UNKNOWN'
  print('H1_STOP_'+category);sys.exit(2)
