"""RS2 LOCAL prototype: one source Git history -> per-item transactions -> immutable snapshot.
No network, D3, witness, App, dispatch, workflow, deployment or legacy operator imports.
"""
from pathlib import Path,PurePosixPath
from contextlib import contextmanager
from copy import deepcopy
from hashlib import sha256
import argparse,json,os,re,subprocess,sys,unicodedata
ROOT=Path(__file__).resolve().parent
from runtime_bootstrap import load
DEPENDENCIES=load()
import privacy_gate
from importer import data,digest,text,probe,atomic,COLS,EXTS,Reject

class Stop(ValueError):pass

def need(ok,code):
 if not ok:raise Stop(code)

def git(source,*args):
 p=subprocess.run(['git','--no-optional-locks','-C',str(source),'-c','core.autocrlf=false',*args],capture_output=True,env=dict(os.environ,GIT_TERMINAL_PROMPT='0',GIT_NO_LAZY_FETCH='1',GIT_ALLOW_PROTOCOL='file'))
 need(p.returncode==0,'SOURCE_GIT_FAILED');return p.stdout

def safe(p):
 q=PurePosixPath(p);need(not q.is_absolute() and q.as_posix()==p and '..' not in q.parts and p==unicodedata.normalize('NFC',p) and not any(ord(c)<32 or c in '\\:<>"|?*' for c in p),'UNSAFE_SOURCE_PATH')
 for part in q.parts:need(not part.endswith((' ','.')) and part.split('.')[0].upper() not in {'CON','PRN','AUX','NUL',*(f'COM{i}' for i in range(1,10)),*(f'LPT{i}' for i in range(1,10))},'UNSAFE_SOURCE_PATH')
 return q

def source_files(source,commit):
 files={};folded=set()
 for raw in git(source,'ls-tree','-r','-z',commit).split(b'\0'):
  if not raw:continue
  meta,path=raw.split(b'\t',1);mode,typ,oid=meta.decode().split();path=path.decode('utf8','strict')
  if path.split('/',1)[0] not in ('incoming','library'):continue  # Data outside media is never read/executed.
  safe(path)
  need(mode=='100644' and typ=='blob' and path.startswith(('incoming/','library/')),'UNTRUSTED_SOURCE_ENTRY')
  need(PurePosixPath(path).suffix.lower() in EXTS or PurePosixPath(path).suffix.lower()=='.txt','UNKNOWN_MEDIA_FILE')
  need(path.casefold() not in folded,'SOURCE_CASE_COLLISION');folded.add(path.casefold());files[path]=git(source,'cat-file','blob',oid)
 return files

@contextmanager
def lock(root):
 with (root/'writer.lock').open('a+b') as f:
  f.seek(0,2)
  if not f.tell():f.write(b'0');f.flush()
  f.seek(0)
  try:
   if os.name=='nt':
    import msvcrt;msvcrt.locking(f.fileno(),msvcrt.LK_NBLCK,1)
   else:
    import fcntl;fcntl.flock(f.fileno(),fcntl.LOCK_EX|fcntl.LOCK_NB)
  except OSError:raise Stop('CONCURRENT_WRITER') from None
  try:yield
  finally:
   f.seek(0)
   if os.name=='nt':msvcrt.locking(f.fileno(),msvcrt.LK_UNLCK,1)
   else:fcntl.flock(f.fileno(),fcntl.LOCK_UN)

def clean(raw,ext,role,evidence):
 result=privacy_gate.prepare(raw,ext,role)
 if result.evidence['status']!='SANITIZED_AND_VERIFIED':raise Reject('PRIVACY_'+result.evidence['status']+':'+result.evidence['reason'])
 need(result.output is not None,'PRIVACY_MISSING_OUTPUT');evidence.append(result.evidence);return result.output,deepcopy(result.evidence['output'])  # Gate already validated and decoded this output.

def units(files,registry):
 result={}
 for path,raw in sorted(files.items()):
  parts=PurePosixPath(path).parts
  if len(parts)==5 and parts[0]=='library' and parts[1] in COLS and parts[2]=='published':
   key=parts[3];need(re.fullmatch('bgzona-'+COLS[parts[1]]+'-[0-9]{5}',key) is not None,'INVALID_MANAGED_ID');unit=key;col=parts[1]
  elif len(parts)==5 and parts[:2]==('incoming','replace') and parts[2] in COLS:
   key='bgzona-'+COLS[parts[2]]+'-'+parts[3];need(key in registry['items'],'UNKNOWN_REPLACEMENT_ID');unit=key;col=parts[2]
  elif len(parts)==3 and parts[0]=='incoming' and parts[1] in COLS:
   stem=PurePosixPath(path).stem
   if stem.casefold().endswith('.og'):stem=stem[:-3]
   unit='new:'+parts[1]+':'+stem.casefold();key=None;col=parts[1]
  else:raise Stop('INVALID_MEDIA_NAMESPACE')
  row=result.setdefault(unit,dict(id=key,unit=unit,collection=col,files={}))
  row['files'][path]=raw
 return result

def bundle(row):
 files=row['files'];managed=[p for p in files if p.startswith('library/')];replace=[p for p in files if p.startswith('incoming/replace/')];image=[];txt=[];og=[]
 for p in managed or [p for p in files if p not in replace]:
  name=PurePosixPath(p).name;ext=PurePosixPath(p).suffix.lower()
  if ext=='.txt':txt.append(p)
  elif name.casefold().startswith('og.') or PurePosixPath(p).stem.casefold().endswith('.og'):og.append(p)
  elif ext in EXTS:image.append(p)
  else:raise Reject('UNSUPPORTED_FILE')
 if len(image)!=1 or len(txt)>1 or len(og)>1 or len(replace)>1:raise Reject('AMBIGUOUS_OR_INCOMPLETE_BUNDLE')
 if replace and PurePosixPath(replace[0]).suffix.lower() not in EXTS:raise Reject('UNSUPPORTED_REPLACEMENT')
 return image[0],txt[0] if txt else None,og[0] if og else None,replace[0] if replace else None

def og_path(col,key,own):
 return own.get('public_path',f'{col}/og/{key}-{own["sha256"][:12]}{own["ext"]}').lstrip('/')

def apply(row,registry,inputs,buffers,evidence):
 """All changes are in detached dictionaries until the ENTIRE item succeeds."""
 files=row['files'];identity=row['id'] or row['unit'];old_input=inputs.get(identity,{});key=row['id'] or old_input.get('item');col=row['collection'];image,txt,og,replacement=bundle(row)
 hashes={p:digest(v) for p,v in sorted(files.items())};stamp=digest(data(hashes))
 if old_input.get('unit')==stamp:return key,'UNCHANGED'
 old=deepcopy(registry['items'].get(key));description=text(files[txt]) if txt else ''
 image_path=replacement or image;source_sha=hashes[image_path];changed=old is None or (replacement and old_input.get('replacement')!=source_sha) or (not replacement and old_input.get('managed_image')!=hashes[image])
 if changed:
  original,meta=clean(files[image_path],PurePosixPath(image_path).suffix.lower(),'original',evidence)
 else:
  meta={k:v for k,v in old['revisions'][-1].items() if k not in ('number','blob','public_path')};original=buffers[old['revisions'][-1]['public_path'].lstrip('/')]
 own=None;og_raw=None
 if og and old is not None and old_input.get('og_input')==hashes[og] and old['og']:
  own=deepcopy(old['og']);og_raw=buffers[og_path(col,key,own)]
 elif og:
  og_raw,om=clean(files[og],PurePosixPath(og).suffix.lower(),'og',evidence);own=dict(sha256=om['sha256'],blob='objects/'+om['sha256']+om['ext'],ext=om['ext'])
 if old is None:
  n=registry['counters'][col]+1;key=f'bgzona-{COLS[col]}-{n:05d}';need(key not in registry['items'],'ID_COLLISION')
  registry['counters'][col]=n;old=dict(id=key,collection=col,number=n,page_url=f'/{col}/'+('kolaj-' if col=='kolaji' else 'p-')+f'{n:05d}/',state='published',slideshow=col=='kolaji',revisions=[])
 if not old['revisions'] or changed and meta['sha256']!=old['revisions'][-1]['sha256']:
  number=len(old['revisions'])+1;suffix='' if number==1 else f'-r{number:04d}-{meta["sha256"][:12]}'
  rev=meta|dict(number=number,blob='objects/'+meta['sha256']+meta['ext'],public_path=f'/{col}/full/{key}{suffix}{meta["ext"]}');old['revisions'].append(rev)
 old.update(description=description,og=own,desired_revision=len(old['revisions']))
 buffers[old['revisions'][-1]['public_path'].lstrip('/')]=original
 if own:buffers[og_path(col,key,own)]=og_raw
 registry['items'][key]=old;inputs[identity]=dict(item=key,unit=stamp,managed_image=hashes[image],replacement=source_sha if replacement else None,og_input=hashes[og] if og else None)
 return key,'ACCEPTED'

def snapshot(root,registry,buffers,source):
 catalog=[];payload={};hashes={p:digest(raw) for p,raw in buffers.items()};file_hashes={}
 for key,item in sorted(registry['items'].items()):
  if item['state']!='published':continue
  rev=item['revisions'][-1];path=rev['public_path'].lstrip('/')
  if path not in buffers:continue  # Incomplete baseline remains explicitly blocked by migration report.
  og=item['og'];ogpath=og_path(item['collection'],key,og) if og else None
  if ogpath is not None and ogpath not in buffers:continue
  row={k:item[k] for k in ('id','collection','page_url','state')}
  row.update(revision=rev['number'],original=rev['public_path'],sha256=hashes[path],description=item['description'],slideshow=item['slideshow'])
  payload['artifact/'+path]=buffers[path];file_hashes['artifact/'+path]=hashes[path]
  if ogpath is not None:
   row['og']='/'+ogpath;payload['artifact/'+ogpath]=buffers[ogpath];file_hashes['artifact/'+ogpath]=hashes[ogpath]
  catalog.append(row)
 current={p.removeprefix('artifact/') for p in payload}
 for path,raw in buffers.items():
  if path not in current and Path(path).suffix.lower() in EXTS:
   payload['historical/'+path]=raw;file_hashes['historical/'+path]=hashes[path]
 payload['artifact/catalog.json']=data(dict(schema=1,items=catalog));file_hashes['artifact/catalog.json']=digest(payload['artifact/catalog.json'])
 payload['expected-files.json']=data({p.removeprefix('artifact/'):v for p,v in file_hashes.items() if p.startswith('artifact/')});file_hashes['expected-files.json']=digest(payload['expected-files.json'])
 payload['registry.json']=data(registry);file_hashes['registry.json']=digest(payload['registry.json'])
 manifest=dict(schema=1,source_commit=source,files=file_hashes);sid=digest(data(manifest));dest=root/'snapshots'/sid
 if dest.exists():verify_snapshot(root,sid)
 else:
  dest.mkdir(parents=True)
  for p,raw in payload.items():atomic(dest/p,raw)
  atomic(dest/'manifest.json',data(manifest))
 return sid


def verify_snapshot(root,sid):
 need(re.fullmatch('[a-f0-9]{64}',sid) is not None,'SNAPSHOT_ID');folder=root/'snapshots'/sid;manifest=json.loads((folder/'manifest.json').read_bytes());need(digest(data(manifest))==sid,'MANIFEST_CHANGED')
 actual={}
 for p in folder.rglob('*'):
  need(not p.is_symlink() and not (hasattr(p,'is_junction') and p.is_junction()),'SNAPSHOT_LINK');rel=p.relative_to(folder).as_posix();safe(rel)
  if p.is_file() and rel!='manifest.json':actual[rel]=digest(p.read_bytes())
 need(actual==manifest['files'],'SNAPSHOT_CHANGED')
 return folder,json.loads((folder/'registry.json').read_bytes())

def initialize(*args,**kwargs):
 raise Stop('USE_PUBLIC_EXPORT_BOOTSTRAP')

def publish(source,root,target=None):
 with lock(root):
  state=json.loads((root/'state.json').read_bytes());need(state.get('baseline_complete',True),'MIGRATION_BASELINE_INCOMPLETE');folder,registry=verify_snapshot(root,state['snapshot']);head=target or git(source,'rev-parse','HEAD').decode().strip()
  need(re.fullmatch('[a-f0-9]{40}',head) is not None,'SOURCE_COMMIT');need(git(source,'rev-list','--max-parents=0',head).decode().strip()==state['source_genesis'],'SOURCE_REPOSITORY_CHANGED');git(source,'merge-base','--is-ancestor',state['source_cursor'],head)
  from reconstruction import rebuild
  source_inputs,bindings=rebuild(source,state,registry)
  commits=git(source,'rev-list','--reverse',state['source_cursor']+'..'+head).decode().splitlines();results=[]
  for commit in commits:
   parents=git(source,'show','-s','--format=%P',commit).decode().split();need(parents==[state['source_cursor']],'NONLINEAR_SOURCE_HISTORY')
   files=source_files(source,commit);inputs=deepcopy(source_inputs);buffers={p.split('/',1)[1]: (folder/p).read_bytes() for p in json.loads((folder/'manifest.json').read_bytes())['files'] if p.startswith(('artifact/','historical/'))};reports=[];evidence=[];before=digest(data(registry))
   for slot,(unit,row) in enumerate(units(files,registry).items()):
    next_reg,next_inputs,next_buffers=deepcopy(registry),deepcopy(inputs),dict(buffers);proof=[]
    try:
     key,status=apply(row,next_reg,next_inputs,next_buffers,proof);registry,inputs,buffers=next_reg,next_inputs,next_buffers;evidence.extend(proof)
     if status=='ACCEPTED':bindings[unit]=dict(source=commit,slot=slot,item=key)
     reports.append(dict(item=key,status=status,reason=status))
    except Reject as error:
     reason=str(error).split(':',1)[0];reason=reason if re.fullmatch('[A-Z0-9_]+',reason) else 'ITEM_REJECTED'
     reports.append(dict(item=row['id'],status='SKIPPED',reason=reason))
   accepted=sum(r['status']=='ACCEPTED' for r in reports);skipped=sum(r['status']=='SKIPPED' for r in reports)
   if digest(data(registry))!=before:
    registry['seq']+=1;registry['previous']=before;state['snapshot']=snapshot(root,registry,buffers,commit)
   source_inputs=inputs;state.update(source_cursor=commit,source_bindings=sorted(bindings.values(),key=lambda x:(x['item'],x['source'],x['slot'])));atomic(root/'state.json',data(state));folder=root/'snapshots'/state['snapshot']  # Same-process atomic output; verify once at the next read/consumer boundary.
   result=dict(source=commit,status='PARTIAL' if skipped and accepted else 'REJECTED' if skipped else 'SUCCESS',accepted=accepted,skipped=skipped,items=reports,snapshot=state['snapshot'],sanitized_images=len(evidence));atomic(root/'reports'/f'{commit}.json',data(result));results.append(result)
  return dict(status='PROCESSED' if commits else 'NO_CHANGE',source_cursor=state['source_cursor'],snapshot=state['snapshot'],commits=results)

if __name__=='__main__':
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--source',type=Path,required=True);p.add_argument('--state',type=Path,required=True);a=p.parse_args();print(json.dumps(publish(a.source,a.state),ensure_ascii=False,indent=2))
