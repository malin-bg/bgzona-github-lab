"""RS2 LOCAL prototype: one source Git history -> per-item transactions -> immutable snapshot.
No network, D3, witness, App, dispatch, workflow, deployment or legacy operator imports.
"""
from pathlib import Path,PurePosixPath
from contextlib import contextmanager
from copy import deepcopy
from hashlib import sha256
import argparse,json,os,re,subprocess,sys,time,unicodedata
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
  library_state=None
  if len(parts)==5 and parts[0]=='library' and parts[1] in COLS and parts[2] in ('published','archived'):
   library_state=parts[2]
   key=parts[3];need(re.fullmatch('bgzona-'+COLS[parts[1]]+'-[0-9]{5}',key) is not None,'INVALID_MANAGED_ID');unit=key;col=parts[1]
  elif len(parts)==5 and parts[:2]==('incoming','replace') and parts[2] in COLS:
   key='bgzona-'+COLS[parts[2]]+'-'+parts[3];need(key in registry['items'],'UNKNOWN_REPLACEMENT_ID');unit=key;col=parts[2]
  elif len(parts)==3 and parts[0]=='incoming' and parts[1] in COLS:
   stem=PurePosixPath(path).stem
   if stem.casefold().endswith('.og'):stem=stem[:-3]
   unit='new:'+parts[1]+':'+stem.casefold();key=None;col=parts[1]
  else:raise Stop('INVALID_MEDIA_NAMESPACE')
  row=result.setdefault(unit,dict(id=key,unit=unit,collection=col,files={}))
  if library_state is not None:
   need(row.get('library_state',library_state)==library_state,'DUPLICATE_LIBRARY_STATE')
   row['library_state']=library_state
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

def request_role(item, path):
    name = PurePosixPath(path).name.casefold()
    stem, ext = PurePosixPath(name).stem, PurePosixPath(name).suffix
    if name == item + ".txt":
        return "txt"
    if name == "og.txt":
        return "clear_og"
    if stem in ("og", item + ".og") or stem.endswith(".og"):
        if stem in ("og", item + ".og") and ext in (".jpg", ".png"):
            return "og"
        raise Stop("INVALID_OG_SIDECAR")
    if ext in EXTS:
        return "replacement"
    raise Stop("INVALID_REPLACEMENT_FORMAT")


def request_files(row):
 return {p:raw for p,raw in row['files'].items() if p.startswith('incoming/replace/')}


def request_parts(item, files):
 parts={}
 for path,raw in files.items():
  role=request_role(item,path)
  need(role not in parts,'AMBIGUOUS_REPLACEMENT_REQUEST')
  parts[role]=(path,raw)
 need(not {'og','clear_og'}<=parts.keys(),'AMBIGUOUS_REPLACEMENT_REQUEST')
 return parts


def verify_cleanup(previous, current, inputs):
 # Accept only exact bytes reconstructed from accepted PRIVATE source bindings.
 accepted={}
 for prior in inputs.values():
  for path,checksum in prior['files'].items():
   if path.startswith('incoming/'):
    accepted.setdefault(path,set()).add(checksum)
 for unit,row in previous.items():
  old=row['files'] if row['id'] is None else request_files(row)
  new=current.get(unit,{'files':{}})
  files=new['files'] if row['id'] is None else request_files(new)
  removed=old.keys()-files.keys()
  need(all(digest(old[p]) in accepted.get(p,()) for p in removed),'CLEANUP_NOT_ACCEPTED')
  if removed and row['id'] is None:
   need(all(old.get(p)==raw for p,raw in files.items()),'CLEANUP_WITH_INCOMING_EDIT')


def apply_request(item, files, previous, registry, buffers, evidence):
 current=request_parts(item,files)
 prior=request_parts(item,previous)
 delta={role:v for role,v in current.items() if prior.get(role)!=v}
 if not delta:return 'UNCHANGED'
 row=registry['items'][item]
 if row['state']!='published':raise Reject('ARCHIVED_ITEM')
 if 'replacement' in delta:
  path,raw=delta['replacement'];original,meta=clean(raw,PurePosixPath(path).suffix.lower(),'original',evidence)
  append_revision(row,meta)
  buffers[row['revisions'][-1]['public_path'].lstrip('/')]=original
 if 'txt' in delta:row['description']=text(delta['txt'][1])
 if 'og' in delta:
  path,raw=delta['og'];cleaned,meta=clean(raw,PurePosixPath(path).suffix.lower(),'og',evidence)
  og=dict(sha256=meta['sha256'],blob='objects/'+meta['sha256']+meta['ext'],ext=meta['ext'])
  public=og_path(row['collection'],item,og)
  row['og']=og|{'public_path':'/'+public};buffers[public]=cleaned
 if 'clear_og' in delta:
  if text(delta['clear_og'][1]):raise Reject('OG_CLEAR_REQUIRES_EMPTY_FILE')
  row['og']=None
 row['desired_revision']=len(row['revisions'])
 return 'ACCEPTED'


def append_revision(item, meta):
 if item['revisions'] and item['revisions'][-1]['sha256']==meta['sha256']:return
 number=len(item['revisions'])+1
 suffix='' if number==1 else f'-r{number:04d}-{meta["sha256"][:12]}'
 key=item['id'];col=item['collection']
 item['revisions'].append(meta|dict(number=number,blob='objects/'+meta['sha256']+meta['ext'],
                                  public_path=f'/{col}/full/{key}{suffix}{meta["ext"]}'))


def managed_row(row):
 return dict(row,files={p:raw for p,raw in row['files'].items() if p.startswith('library/')})


def existing_input(inputs, unit, row):
 old=inputs.get(unit,{})
 if row['id'] is None:
  return old
 if old:
  return old
 # A complete byte-identical move transfers the editing location, not the ID.
 matches=[prior for key,prior in inputs.items()
          if key.startswith('new:') and prior.get('item')==row['id']]
 if len(matches)!=1:
  raise Reject('CLEANUP_BINDING')
 prior=matches[0]
 image,txt,og,_=bundle(row)
 if digest(row['files'][image])!=prior['managed_image']:
  raise Reject('MOVE_BUNDLE_EXACT_BYTES')
 return prior

def apply(row,registry,inputs,buffers,evidence,*,old_input=None, final_tree=False):
 """All changes are in detached dictionaries until the ENTIRE item succeeds."""
 files=row['files'];identity=row['id'] or row['unit'];old_input=inputs.get(identity,{}) if old_input is None else old_input;key=row['id'] or old_input.get('item');col=row['collection'];image,txt,og,replacement=bundle(row)
 hashes={p:digest(v) for p,v in sorted(files.items())};stamp=digest(data(hashes))
 if old_input.get('unit')==stamp and identity in inputs:return key,'UNCHANGED'
 old=deepcopy(registry['items'].get(key));description=text(files[txt]) if txt else ''
 if row.get('library_state')=='archived':
  if old is None or replacement:raise Reject('ARCHIVE_REQUIRES_EXISTING_ITEM')
  if old['state']=='archived':return key,'UNCHANGED'
  old['state']='archived';registry['items'][key]=old
  inputs[identity]=dict(item=key,unit=stamp,managed_image=hashes[image],replacement=None,og_input=hashes[og] if og else None)
  return key,'ACCEPTED'
 if old is not None and old['state']!='published':raise Reject('ARCHIVED_ITEM')
 if old is not None and old_input and old_input.get('managed_image')!=hashes[image]:
  raise Reject('EXPLICIT_REPLACEMENT_REQUIRED')
 image_path=replacement or image;source_sha=hashes[image_path];changed=old is None or (replacement and old_input.get('replacement')!=source_sha)
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
  if final_tree and any(existing['collection']==col and
                        any(rev['sha256']==meta['sha256'] for rev in existing['revisions'])
                        for existing in registry['items'].values()):
   raise Reject('DUPLICATE_ORIGINAL')
  n=registry['counters'][col]+1;key=f'bgzona-{COLS[col]}-{n:05d}';need(key not in registry['items'],'ID_COLLISION')
  registry['counters'][col]=n;old=dict(id=key,collection=col,number=n,page_url=f'/{col}/'+('kolaj-' if col=='kolaji' else 'p-')+f'{n:05d}/',state='published',slideshow=col=='kolaji',revisions=[])
 if changed:append_revision(old,meta)
 if own:own['public_path']='/'+og_path(col,key,own)
 old.update(description=description,og=own,desired_revision=len(old['revisions']))
 buffers[old['revisions'][-1]['public_path'].lstrip('/')]=original
 if own:buffers[og_path(col,key,own)]=og_raw
 registry['items'][key]=old;inputs[identity]=dict(item=key,unit=stamp,managed_image=hashes[image],replacement=source_sha if replacement else None,og_input=hashes[og] if og else None)
 return key,'ACCEPTED'

def read_activity(item):
 if 'activity' not in item:return {}
 value=item['activity']
 need(type(value) is dict and bool(value) and value.keys()<={'published_at','updated_at'}
      and all(type(v) is int and 0<v<=253402300799 for v in value.values()),'PUBLIC_MEDIA_ACTIVITY')
 if 'published_at' in value and 'updated_at' in value:
  need(value['published_at']<=value['updated_at'],'PUBLIC_MEDIA_ACTIVITY_ORDER')
 return value


def mark_activity(before, after, accepted_at):
 def signature(item):
  og=item['og']
  return item['description'],item['revisions'][-1]['sha256'],og['sha256'] if og else None
 for key,item in after.items():
  if item['state']!='published':continue
  old=before.get(key)
  if old is None:
   item['activity']={'published_at':accepted_at}
  elif old['state']=='published' and signature(old)!=signature(item):
   dates=dict(old.get('activity',{}))
   dates['updated_at']=max(accepted_at,*dates.values()) if dates else accepted_at
   item['activity']=dates


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
  if 'activity' in item:row['activity']=read_activity(item)
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

def publish(source,root,target=None,*,final_tree=False):
 with lock(root):
  state=json.loads((root/'state.json').read_bytes());need(state.get('baseline_complete',True),'MIGRATION_BASELINE_INCOMPLETE');folder,registry=verify_snapshot(root,state['snapshot']);head=target or git(source,'rev-parse','HEAD').decode().strip()
  need(re.fullmatch('[a-f0-9]{40}',head) is not None,'SOURCE_COMMIT');need(git(source,'rev-list','--max-parents=0',head).decode().strip()==state['source_genesis'],'SOURCE_REPOSITORY_CHANGED');git(source,'merge-base','--is-ancestor',state['source_cursor'],head)
  from reconstruction import rebuild
  source_inputs,bindings=rebuild(source,state,registry)
  commits=git(source,'rev-list','--reverse',state['source_cursor']+'..'+head).decode().splitlines();results=[]
  if final_tree and commits:
   parent=state['source_cursor'];need(len(commits)<=512,'SOURCE_HISTORY_LIMIT')
   for commit in commits:
    need(git(source,'show','-s','--format=%P',commit).decode().split()==[parent],'NONLINEAR_SOURCE_HISTORY')
    parent=commit
   commits=[head]
  for commit in commits:
   parents=git(source,'show','-s','--format=%P',commit).decode().split();need(final_tree or parents==[state['source_cursor']],'NONLINEAR_SOURCE_HISTORY')
   files=source_files(source,commit);inputs=deepcopy(source_inputs);buffers={p.split('/',1)[1]: (folder/p).read_bytes() for p in json.loads((folder/'manifest.json').read_bytes())['files'] if p.startswith(('artifact/','historical/'))};reports=[];evidence=[];before=digest(data(registry))
   before_items=registry['items']
   source_rows=units(files,registry)
   previous_rows=units(source_files(source,state['source_cursor']),registry) if final_tree else {}
   if final_tree:verify_cleanup(previous_rows,source_rows,inputs)
   for slot,(unit,row) in enumerate(source_rows.items()):
    next_reg,next_inputs,next_buffers=deepcopy(registry),deepcopy(inputs),dict(buffers);proof=[]
    try:
     key=row['id']
     if final_tree and key is not None:
      previous=previous_rows.get(unit,{'files':{}})
      managed=managed_row(row)
      status='UNCHANGED'
      if managed['files'] and managed_row(previous)['files']!=managed['files']:
       prior=existing_input(inputs,unit,managed)
       key,status=apply(managed,next_reg,next_inputs,next_buffers,proof,old_input=prior,final_tree=True)
      request_status=apply_request(key,request_files(row),request_files(previous),next_reg,next_buffers,proof)
      if request_status=='ACCEPTED':status=request_status
     else:
      prior=existing_input(inputs,unit,row) if final_tree else None
      if final_tree and prior:
       key=prior['item']
       if all(prior['files'].get(p)==digest(raw) for p,raw in row['files'].items()):continue
       need(key not in source_rows or not managed_row(source_rows[key])['files'],'MOVE_ALREADY_COMPLETED')
       need(all(not p.startswith('incoming/'+row['collection']+'/') or p in row['files']
                for p in prior['files']),'EDIT_CLEANED_INCOMING_BY_ID')
      key,status=apply(row,next_reg,next_inputs,next_buffers,proof,old_input=prior,final_tree=final_tree)
     registry,inputs,buffers=next_reg,next_inputs,next_buffers;evidence.extend(proof)
     if status=='ACCEPTED':bindings[unit]=dict(source=commit,slot=slot,item=key)
     reports.append(dict(item=key,status=status,reason=status))
    except Reject as error:
     reason=str(error).split(':',1)[0];reason=reason if re.fullmatch('[A-Z0-9_]+',reason) else 'ITEM_REJECTED'
     if final_tree:raise Stop(reason) from None
     reports.append(dict(item=row['id'],status='SKIPPED',reason=reason))
   accepted=sum(r['status']=='ACCEPTED' for r in reports);skipped=sum(r['status']=='SKIPPED' for r in reports)
   if final_tree:mark_activity(before_items,registry['items'],int(time.time()))
   if digest(data(registry))!=before:
    registry['seq']+=1;registry['previous']=before;state['snapshot']=snapshot(root,registry,buffers,commit)
   source_inputs=inputs;state.update(source_cursor=commit,source_bindings=sorted(bindings.values(),key=lambda x:(x['item'],x['source'],x['slot'])));atomic(root/'state.json',data(state));folder=root/'snapshots'/state['snapshot']  # Same-process atomic output; verify once at the next read/consumer boundary.
   result=dict(source=commit,status='PARTIAL' if skipped and accepted else 'REJECTED' if skipped else 'SUCCESS',accepted=accepted,skipped=skipped,items=reports,snapshot=state['snapshot'],sanitized_images=len(evidence));atomic(root/'reports'/f'{commit}.json',data(result));results.append(result)
  return dict(status='PROCESSED' if commits else 'NO_CHANGE',source_cursor=state['source_cursor'],snapshot=state['snapshot'],commits=results)

if __name__=='__main__':
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--source',type=Path,required=True);p.add_argument('--state',type=Path,required=True);a=p.parse_args();print(json.dumps(publish(a.source,a.state),ensure_ascii=False,indent=2))
