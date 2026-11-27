"""Local two-ref recovery experiment over P2 publisher. No network/credentials/workflow code."""
from pathlib import Path
from tempfile import TemporaryDirectory
import json,os,sys,time
from uuid import UUID, uuid4
ROOT=Path(__file__).resolve().parent
sys.path[:0]=[str(ROOT/'runtime'),str(ROOT.parent/'r6c-d2-local/runtime/site-packages')]
import git_publisher as G
import trusted_adapter as A
P=G.P;ZERO=G.ZERO;PUBLIC_REF='refs/heads/publisher-data';OWNER_REF='refs/heads/synthetic-source'

def head(repo,ref):
 q=G.run(repo,'for-each-ref','--format=%(refname) %(objectname)',ref,check=False)
 P.need(q.returncode==0,'REF_UNKNOWN');rows=q.stdout.decode().splitlines()
 if not rows:return ZERO
 P.need(len(rows)==1 and rows[0].split()[0]==ref,'REF_UNKNOWN');return G.oid(rows[0].split()[1])

def preparation_context():
 # Preparation may parse source/media; it must run in a read-only job, never the writer job.
 # This defensive check is not a substitute for job permissions/process separation.
 P.need(not any(os.environ.get(k) for k in ('GH_TOKEN','GITHUB_TOKEN','BGZONA_AUTH_SECRET','RS4K_TOKEN','LAB_DISPATCH_PAT','ACTIONS_RUNTIME_TOKEN','ACTIONS_ID_TOKEN_REQUEST_TOKEN')),'PREPARATION_CREDENTIAL_PRESENT')


def verify_export_images(payload):
 """Existing Gate on actual bytes, including historical/OG/derivative paths.
 Never silently rewrite a public candidate: upstream must already have sanitized it.
 Gate evidence contains source fingerprints and must not be exported.
 """
 preparation_context();checked=set();paths=0
 for path,raw in payload.items():
  ext=Path(path).suffix.lower()
  if ext not in P.EXTS:continue
  paths+=1;role='og' if '/og/' in path else 'original';key=(P.digest(raw),ext,role)
  if key in checked:continue
  result=P.privacy_gate.prepare(raw,ext,role)
  P.need(result.evidence.get('status')=='SANITIZED_AND_VERIFIED' and result.output==raw,'PUBLIC_IMAGE_NOT_ALREADY_VERIFIED')
  checked.add(key)
 return {'image_paths':paths,'unique_image_bytes':len(checked),'policy':P.privacy_gate.POLICY}


def public_tree(folder):
 """Explicit export from already validated PRIVATE canonical snapshots; no Git graph copy."""
 preparation_context();state=G.validate(folder);payload={};mapping={}
 for sid in {state['snapshot'],state['public_snapshot'],state['pending_accepted']['snapshot']}:
  snap=folder/'snapshots'/sid;m=P.json.loads((snap/'manifest.json').read_bytes())
  clean={'schema':1,'files':m['files']};public_sid=P.digest(P.data(clean));mapping[sid]=public_sid
  for name,digest in clean['files'].items():
   raw=(snap/name).read_bytes();P.need(P.digest(raw)==digest,'EXPORT_BYTES_CHANGED');payload['snapshots/'+public_sid+'/'+name]=raw
  payload['snapshots/'+public_sid+'/manifest.json']=P.data(clean)
 public={'schema':1,'baseline_complete':True,'snapshot':mapping[state['snapshot']],'public_snapshot':mapping[state['public_snapshot']],'pending_accepted':{'already_accepted':True,'snapshot':mapping[state['pending_accepted']['snapshot']]}}
 payload['state.json']=P.data(public);validate_public(payload);return payload,public

def validate_public(payload):
 s=A.strict(payload['state.json']);P.need(set(s)=={'schema','baseline_complete','snapshot','public_snapshot','pending_accepted'} and s['schema']==1 and s['baseline_complete'] is True,'PUBLIC_STATE')
 pending=s['pending_accepted'];snapshots={s['snapshot'],s['public_snapshot']}
 if pending is None:
  P.need(s['snapshot']==s['public_snapshot'],'PUBLIC_BASELINE_SELECTION')
 else:
  P.need(set(pending)=={'already_accepted','snapshot'} and pending['already_accepted'] is True,'PUBLIC_PENDING')
  snapshots.add(pending['snapshot'])
 expected={'state.json'}
 for sid in snapshots:
  prefix='snapshots/'+sid+'/';files={p[len(prefix):]:raw for p,raw in payload.items() if p.startswith(prefix)};m=A.strict(files['manifest.json'])
  P.need(set(m)=={'schema','files'} and P.digest(P.data(m))==sid,'PUBLIC_SID')
  # Existing strict media/ID/URL/revision/OG/path/hash validator, with PRIVATE source binding removed only in isolated PUBLIC variant.
  A.validate({'data_commit':ZERO,'selection':'CURRENT','snapshot':sid},{},files)
  expected.update(prefix+p for p in files)
 P.need(set(payload)==expected,'PUBLIC_EXTRA_FILES');return s

def commit_tree(repo,payload,parent,stamp):
 preparation_context();validate_public(payload);verify_export_images(payload)
 with TemporaryDirectory(dir=ROOT,prefix='public-index-') as t:
  env={'GIT_INDEX_FILE':str(Path(t)/'index')};G.run(repo,'read-tree','--empty',env=env);entries=[]
  for path,raw in sorted(payload.items()):
   P.safe(path);blob=G.run(repo,'hash-object','-w','--stdin',input=raw).stdout.strip();entries.append(b'100644 '+blob+b'\t'+path.encode()+b'\0')
  G.run(repo,'update-index','-z','--index-info',input=b''.join(entries),env=env);tree=G.run(repo,'write-tree',env=env).stdout.decode().strip()
 if parent!=ZERO and G.run(repo,'rev-parse',parent+'^{tree}').stdout.decode().strip()==tree:return parent
 args=['commit-tree',tree]+([] if parent==ZERO else ['-p',parent])
 # Public commit metadata is deliberately independent of source author/message/timestamp.
 env={'GIT_AUTHOR_NAME':'BGZONA Publisher','GIT_COMMITTER_NAME':'BGZONA Publisher','GIT_AUTHOR_EMAIL':'publisher@invalid','GIT_COMMITTER_EMAIL':'publisher@invalid','GIT_AUTHOR_DATE':str(stamp)+' +0000','GIT_COMMITTER_DATE':str(stamp)+' +0000'}
 return G.run(repo,*args,input=b'Sanitized public snapshot\n',env=env).stdout.decode().strip()

def writer_for(public,work):
 writer=G.init(work/'public-writer.git');seen=head(public,PUBLIC_REF)
 if seen!=ZERO:
  G.run(writer,'fetch','--no-tags',str(public),PUBLIC_REF);P.need(G.run(writer,'rev-parse','FETCH_HEAD').stdout.decode().strip()==seen,'PUBLIC_MOVED')
 return writer,seen


def bind_new_trace(private_recovery, private_run_id):
    """Call only for a new PRIVATE checkpoint, after the producer run is verified."""
    if type(private_run_id) is not int or private_run_id <= 0:
        raise ValueError('PRIVATE_TRACE_BINDING')
    return private_recovery | {
        'schema': 2,
        'trace': uuid4().hex,
        'private_run_id': private_run_id,
    }

def resolve_private_trace(trace, private_recovery, trusted_target):
    """The caller reads this record from the independently verified PRIVATE ref."""
    try:
        value = UUID(hex=trace)
    except (AttributeError, TypeError, ValueError):
        raise ValueError('PRIVATE_TRACE_BINDING') from None
    run_id = private_recovery.get('private_run_id')
    if (
        value.version != 4 or trace != value.hex
        or private_recovery.get('trace') != trace
        or private_recovery.get('target') != trusted_target
        or type(run_id) is not int or run_id <= 0
    ):
        raise ValueError('PRIVATE_TRACE_BINDING')
    return run_id



def descriptor(folder):
 f=folder/'recovery.json'
 if not f.exists():return None
 r=A.strict(f.read_bytes());keys={'schema','expected','target','stamp','snapshot'}
 P.need(type(r.get('schema')) is int and type(r.get('stamp')) is int,'RECOVERY_SCHEMA')
 if r['schema']==2:
  P.need(set(r)==keys|{'trace','private_run_id'},'RECOVERY_SCHEMA')
  resolve_private_trace(r['trace'],r,r['target'])
 else:
  P.need(r['schema']==1 and set(r)==keys,'RECOVERY_SCHEMA')
 for name in ['expected','target']:G.oid(r[name])
 return r

def require_legacy_release(folder, private_state, public_state):
 pending=private_state['pending_accepted']
 manifest=P.json.loads((folder/'snapshots'/pending['snapshot']/'manifest.json').read_bytes())
 expected=P.digest(P.data({'schema':1,'files':manifest['files']}))
 marker={'already_accepted':True,'snapshot':expected}
 P.need(pending['legacy_D3']==13 and pending['already_accepted'] is True and
        public_state['pending_accepted']==marker and
        (private_state['snapshot']!=pending['snapshot'] or public_state['snapshot']==expected),
        'LEGACY_PENDING13_UNRESOLVED')


def prepare(private,public,work,stop_before_private=False,checkpoint=True,producer_id=None):
 """One PRIVATE ref update, only after a reproducible sanitized candidate exists."""
 preparation_context();old=head(private,G.REF)
 with TemporaryDirectory(dir=work,prefix='prepare-') as t:
  folder=Path(t);before=G.checkout(private,old,folder);rec=descriptor(folder)
  if rec:
   seen=head(public,PUBLIC_REF)
   P.need(seen==rec['target'],'PENDING_OR_CONFLICT_NO_NEW_INTAKE')
  source=head(private,OWNER_REF)
  real=OWNER_REF=='refs/heads/cp2b-private-standard-source-r1'
  if real:
   P.need(type(producer_id) is int and producer_id>0,'REAL_PRODUCER_REQUIRED')
   if source!=before['source_cursor']:
    P.need(rec is not None,'REAL_CHECKPOINT_REQUIRED')
    public_state=A.strict(G.run(public,'show',seen+':state.json').stdout)
    require_legacy_release(folder,before,public_state)
  else:
   P.need(producer_id is None,'SYNTHETIC_TRACE')
  result=P.publish(private,folder,target=source, final_tree=real)
  if rec and not result['commits']:return {'status':'NO_CHANGE','private':old,'public':rec['target'],'pushes':0,'private_writes':0}
  payload,view=public_tree(folder)
  with TemporaryDirectory(dir=work,prefix='candidate-') as scratch:
   writer,seen=writer_for(public,Path(scratch));stamp=int(time.time());target=commit_tree(writer,payload,seen,stamp)
  rec={'schema':1,'expected':seen,'target':target,'stamp':stamp,'snapshot':view['snapshot']}
  if real:rec=bind_new_trace(rec,producer_id)
  (folder/'recovery.json').write_bytes(P.data(rec))
  if stop_before_private:return {'status':'STOP_BEFORE_PRIVATE','private':old,'public':seen,'private_writes':0,'pushes':0}
  P.need(head(private,OWNER_REF)==source,'SOURCE_MOVED_BEFORE_CHECKPOINT');proposal=G.version(private,folder,old)
  if not checkpoint:return {'status':'PRIVATE_PREPARED','proposal':proposal,'target':target,'snapshot':view['snapshot'],'private_writes':0,'pushes':0}
  G.finish(private,proposal)
  P.need(head(private,G.REF)==proposal['commit'],'PRIVATE_WRITE_UNKNOWN')
  return {'status':'PRIVATE_CHECKPOINTED','private':proposal['commit'],'public':seen,'target':target,'snapshot':view['snapshot'],'private_writes':1,'pushes':0}

def resume(private,public,work,deny_hook=None,unknown_after_send=False,stop_before_send=False):
 """Rehydrate ONLY from durable PRIVATE Git; no reliance on prepare's return value."""
 checkpoint=head(private,G.REF)
 with TemporaryDirectory(dir=work,prefix='resume-') as t:
  folder=Path(t);G.checkout(private,checkpoint,folder);rec=descriptor(folder);P.need(rec is not None,'NO_DURABLE_RECOVERY')
  seen=head(public,PUBLIC_REF)
  if seen==rec['target']:return {'status':'CONFIRMED','pushes':0,'private_writes':0,'public':seen}
  P.need(seen==rec['expected'],'PUBLIC_CONFLICT_NO_RETRY')
  if stop_before_send:return {'status':'NOT_STARTED','pushes':0,'private_writes':0}
  payload,view=public_tree(folder);P.need(view['snapshot']==rec['snapshot'],'RECOVERY_SNAPSHOT')
  with TemporaryDirectory(dir=work,prefix='send-') as scratch:
   writer,actual=writer_for(public,Path(scratch));P.need(actual==seen,'PUBLIC_MOVED_BEFORE_SEND')
   target=commit_tree(writer,payload,seen,rec['stamp']);P.need(target==rec['target'],'RECOVERY_COMMIT_IDENTITY')
   P.need(head(private,G.REF)==checkpoint,'PRIVATE_CHECKPOINT_MOVED')
   # Exactly one non-force push. A test hook can make the real Git receiver reject it.
   args=['-c','push.followTags=false','push','--porcelain']+(['--receive-pack=git -c core.hooksPath='+Path(deny_hook).as_posix()+' receive-pack'] if deny_hook else [])+[str(public),target+':'+PUBLIC_REF]
   q=G.run(writer,*args,check=False)
  if unknown_after_send:return {'status':'UNKNOWN','pushes':1,'private_writes':0,'retry':False,'response':'LOST_BY_FAILURE_INJECTION'}
  after=head(public,PUBLIC_REF)
  if after==target:return {'status':'CONFIRMED','pushes':1,'private_writes':0,'public':after}
  return {'status':'NOT_CONFIRMED' if after==seen else 'CONFLICT','pushes':1,'private_writes':0,'git_exit':q.returncode,'retry':False}

def read_public(repo):
 commit=head(repo,PUBLIC_REF);payload={}
 for row in G.run(repo,'ls-tree','-r','-z',commit).stdout.split(b'\0'):
  if not row:continue
  meta,path=row.split(b'\t',1);mode,kind,blob=meta.decode().split();P.need(mode=='100644' and kind=='blob','PUBLIC_LINK');payload[path.decode()]=G.run(repo,'cat-file','blob',blob).stdout
 return payload,validate_public(payload)


def export_candidate(private,public,work):
 """Read durable PRIVATE checkpoint; prepare only. No remote write/credentials."""
 preparation_context();checkpoint=head(private,G.REF)
 with TemporaryDirectory(dir=work,prefix='export-') as t:
  folder=Path(t);G.checkout(private,checkpoint,folder);rec=descriptor(folder)
  P.need(rec is not None,'NO_DURABLE_RECOVERY');seen=head(public,PUBLIC_REF)
  if seen==rec['target']:return {'status':'CONFIRMED','pushes':0}
  P.need(seen==rec['expected'],'PUBLIC_CONFLICT_NO_RETRY')
  payload,view=public_tree(folder)
  P.need(view['snapshot']==rec['snapshot'],'RECOVERY_SNAPSHOT')
  writer,actual=writer_for(public,work)
  P.need(actual==seen,'PUBLIC_MOVED_BEFORE_SEND')
  target=commit_tree(writer,payload,seen,rec['stamp'])
  P.need(target==rec['target'] and head(private,G.REF)==checkpoint,'RECOVERY_COMMIT_IDENTITY')
  return {'status':'PUBLIC_PREPARED','client':str(writer),'proposal':{'expected':seen,'commit':target,'status':'PREPARED'},'snapshot':view['snapshot'],'pushes':0}
