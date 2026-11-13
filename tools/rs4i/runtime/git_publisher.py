"""Local-only Git persistence for the existing RS3C publisher. No network or deploy."""
from pathlib import Path
from tempfile import TemporaryDirectory
import json, os, re, subprocess, sys
from io import BytesIO
ROOT=Path(__file__).resolve().parent
sys.path.insert(0,str(ROOT))
import publisher as P
REF='refs/heads/publisher-data'
ZERO='0'*40

def run(repo,*args,input=None,env=None,check=True):
 e=dict(os.environ,GIT_TERMINAL_PROMPT='0',GIT_CONFIG_NOSYSTEM='1',GIT_NO_LAZY_FETCH='1',GIT_ALLOW_PROTOCOL='file')
 if env:e.update(env)
 r=subprocess.run(['git','--no-optional-locks','-C',str(repo),'-c','core.autocrlf=false','-c','core.hooksPath='+str(ROOT/'no-hooks'),'-c','user.name=RS4D Local','-c','user.email=rs4a@invalid',*args],input=input,capture_output=True,env=e)
 if check:P.need(r.returncode==0,'DATA_GIT_FAILED')
 return r

def oid(value):
 P.need(isinstance(value,str) and re.fullmatch('[0-9a-f]{40}',value),'GIT_ID');return value

def location(repo):
 repo=Path(repo).resolve();P.need(repo.is_relative_to(ROOT) and repo!=ROOT,'ISOLATED_DATA_REPOSITORY_ONLY');return repo

def init(repo):
 repo=location(repo);P.need(not repo.exists(),'DATA_REPOSITORY_EXISTS');repo.mkdir(parents=True)
 run(repo,'init','--bare');return repo

def current(repo):
 repo=location(repo);P.need(run(repo,'rev-parse','--is-bare-repository').stdout.strip()==b'true','BARE_REQUIRED')
 P.need(not run(repo,'remote').stdout.strip(),'NO_REMOTE_IN_RS4A')
 r=run(repo,'for-each-ref','--format=%(refname) %(objectname)',REF)
 rows=r.stdout.decode().splitlines()
 if not rows:return ZERO
 P.need(len(rows)==1 and rows[0].split()[0]==REF,'REF_UNKNOWN');return oid(rows[0].split()[1])

def validate(folder):
 state=json.loads((folder/'state.json').read_bytes())
 P.need('source_inputs' not in state and isinstance(state.get('source_bindings'),list),'PUBLIC_STATE_REQUIRED')
 P.need(state.get('baseline_complete') is True,'INCOMPLETE_BASELINE');oid(state['source_cursor']);oid(state['source_genesis'])
 pending=state['pending_accepted'];P.need(pending.get('already_accepted') is True and pending['legacy_D3']==13,'PENDING_IDENTITY')
 for sid in {state['public_snapshot'],state['snapshot'],pending['snapshot']}:
  P.verify_snapshot(folder,sid)
 manifest=json.loads((folder/'snapshots'/pending['snapshot']/'manifest.json').read_bytes())
 P.need(manifest['source_commit']==pending['source_commit'],'PENDING_SOURCE_CHANGED')
 P.need(set(state)=={'schema','baseline_complete','source_cursor','source_genesis','snapshot','public_snapshot','pending_accepted','source_bindings'},'PUBLIC_STATE_FIELDS')
 for b in state['source_bindings']:P.need(set(b)=={'source','slot','item'} and type(b['slot']) is int and b['slot']>=0 and P.re.fullmatch('[a-f0-9]{40}',b['source']) is not None,'PUBLIC_BINDING_FIELDS')
 from public_paths import image_path
 for sid in {state['public_snapshot'],state['snapshot'],pending['snapshot']}:
  snap=folder/'snapshots'/sid;m=json.loads((snap/'manifest.json').read_bytes());reg=json.loads((snap/'registry.json').read_bytes())
  P.need(set(reg)=={'schema','seq','counters','previous','items'},'PUBLIC_REGISTRY_FIELDS')
  for rel in m['files']:
   if rel in ('registry.json','expected-files.json','artifact/catalog.json'):continue
   P.need(rel.startswith(('artifact/','historical/')),'PUBLIC_SNAPSHOT_NAMESPACE');image_path(rel.split('/',1)[1],set(reg['items']))
  for key,item in reg['items'].items():
   P.need(set(item)=={'id','collection','number','page_url','state','slideshow','description','desired_revision','revisions','og'},'PUBLIC_ITEM_FIELDS')
   images=[(v['public_path'],v) for v in item['revisions']]
   for v in item['revisions']:P.need(set(v)=={'number','public_path','ext','format','width','height','frames','durations_ms','loop','sha256','bytes','blob'},'PUBLIC_REVISION_FIELDS')
   if item['og']:
    v=item['og'];P.need(set(v)=={'sha256','ext','blob','public_path'},'PUBLIC_OG_FIELDS');images.append((v['public_path'],v))
   for path,v in images:
    rel=path.lstrip('/');h=m['files'].get('artifact/'+rel,m['files'].get('historical/'+rel));P.need(v['sha256']==h and v['blob']=='objects/'+h+v['ext'],'PUBLIC_BYTES_IDENTITY')
 for f in (folder/'reports').glob('*.json'):
  report=json.loads(f.read_bytes());P.need(set(report)=={'source','status','accepted','skipped','items','snapshot','sanitized_images'},'PUBLIC_REPORT_FIELDS')
  for row in report['items']:P.need(set(row)=={'item','status','reason'} and (row['item'] is None or P.re.fullmatch('bgzona-[pm]-[0-9]{5}',row['item']) is not None) and P.re.fullmatch('[A-Z0-9_]+',row['reason']) is not None,'PUBLIC_REPORT_ITEM')
 return state

def allowed(path):
 P.safe(path)
 P.need(path=='state.json' or re.fullmatch(r'reports/[0-9a-f]{40}\.json',path) or re.fullmatch(r'snapshots/[0-9a-f]{64}/.+',path),'UNEXPECTED_DATA_PATH')

def checkout(repo,commit,folder):
 oid(commit);entries=[]
 for row in run(repo,'ls-tree','-r','-z',commit).stdout.split(b'\0'):
  if not row:continue
  meta,name=row.split(b'\t',1);mode,kind,blob=meta.decode().split();name=name.decode('utf-8');allowed(name)
  P.need(mode=='100644' and kind=='blob','DATA_LINK_OR_EXECUTABLE');entries.append((name,blob))
 stream=BytesIO(run(repo,'cat-file','--batch',input=''.join(o+'\n' for _,o in entries).encode()).stdout)
 for name,blob in entries:
  header=stream.readline().decode().split();P.need(len(header)==3 and header[:2]==[blob,'blob'],'BLOB_HEADER');raw=stream.read(int(header[2]));P.need(len(raw)==int(header[2]) and stream.read(1)==b'\n','BLOB_TRUNCATED')
  dest=folder/name;dest.parent.mkdir(parents=True,exist_ok=True);dest.write_bytes(raw)
 P.need(stream.read()==b'','BATCH_TRAILING')
 return validate(folder)

def version(repo,folder,expected):
 # Temporary Git index; cursor, reports and public bytes are one immutable tree.
 validate(folder);expected=oid(expected)
 with TemporaryDirectory(prefix='index-',dir=ROOT) as t:
  env={'GIT_INDEX_FILE':str(Path(t)/'index')};run(repo,'read-tree','--empty',env=env);entries=[]
  paths=[]
  for f in sorted(folder.rglob('*')):
   P.need(not f.is_symlink() and not f.is_junction(),'DATA_LINK')
   if not f.is_file():continue
   rel=f.relative_to(folder).as_posix()
   if rel=='writer.lock':continue
   allowed(rel);paths.append((rel,f))
  request=''.join(json.dumps(f.resolve().as_posix(),ensure_ascii=False)+'\n' for _,f in paths).encode('utf8')
  blobs=run(repo,*(['-c','core.longpaths=true'] if os.name=='nt' else []),'hash-object','-w','--stdin-paths','--no-filters',input=request).stdout.splitlines();P.need(len(blobs)==len(paths),'BLOB_WRITE_COUNT')
  for (rel,_),blob in zip(paths,blobs):entries.append(b'100644 '+blob+b'\t'+rel.encode('utf-8')+b'\0')
  run(repo,'update-index','-z','--index-info',input=b''.join(entries),env=env)
  tree=run(repo,'write-tree',env=env).stdout.decode().strip()
 if expected!=ZERO and run(repo,'rev-parse',expected+'^{tree}').stdout.decode().strip()==tree:return dict(expected=expected,commit=expected,status='NO_CHANGE')
 args=['commit-tree',tree]
 if expected!=ZERO:args+=['-p',expected]
 commit=run(repo,*args,input=b'RS4D public snapshot\n').stdout.decode().strip()
 return dict(expected=expected,commit=commit,status='PREPARED')

def bootstrap(repo,migration):
 P.need(current(repo)==ZERO,'ALREADY_INITIALIZED')
 # Read the existing sanitized migration, never rewrite it.
 return version(repo,Path(migration),ZERO)

def prepare(repo,source,target=None):
 expected=current(repo);P.need(expected!=ZERO,'NOT_INITIALIZED')
 with TemporaryDirectory(prefix='prepare-',dir=ROOT) as t:
  folder=Path(t);before=checkout(repo,expected,folder)
  result=P.publish(Path(source),folder,target=target)
  state=json.loads((folder/'state.json').read_bytes())
  P.need('source_inputs' not in state and isinstance(state.get('source_bindings'),list),'PUBLIC_STATE_REQUIRED')
  P.need(state['public_snapshot']==before['public_snapshot'] and state['pending_accepted']==before['pending_accepted'],'BASELINE_OR_PENDING_CHANGED')
  for entry in result['commits']:
   report=json.loads((folder/'reports'/f"{entry['source']}.json").read_bytes());P.need(report==entry,'REPORT_NOT_DURABLE')
  proposal=version(repo,folder,expected);proposal['result']=result;return proposal

def finish(repo,proposal,simulate=None):
 expected=oid(proposal['expected']);commit=oid(proposal['commit']);head=current(repo)
 if head==commit:return dict(status='NO_CHANGE' if commit==expected else 'COMMITTED',commit=commit,reconciled=True)
 P.need(head==expected,'CONFLICT_NO_WRITE')
 parents=run(repo,'show','-s','--format=%P',commit).stdout.decode().split()
 P.need(parents==([] if expected==ZERO else [expected]),'NON_FAST_FORWARD')
 if simulate=='before_update':return dict(status='INTERRUPTED_BEFORE_REF',commit=commit)
 r=run(repo,'update-ref',REF,commit,expected,check=False)
 # Never repeat update-ref after an uncertain response. Independent readback only.
 if simulate=='lost_response':r=None
 head=current(repo)
 if head==commit:return dict(status='COMMITTED',commit=commit,reconciled=r is None or r.returncode!=0)
 P.need(head==expected,'CONFLICT_NO_RETRY')
 raise P.Stop('WRITE_UNCONFIRMED_NO_RETRY')
