"""RS4B local pinned Git snapshot -> existing Hugo media input. Never publishes."""
from pathlib import Path
from hashlib import sha256
from io import BytesIO
import json,re,subprocess,sys
ROOT=Path(__file__).resolve().parent
REPO=ROOT/'consumer-cache.git'
PIN='aa14561064c3ab9d314696b254d3ab2768110493'
SELECTIONS={'PUBLIC12':'d269fddd90e5111425e1a663e29da0c210c9ab6f1c74ebcc022093d880471de4','PENDING13':'495c6eeda127de7f1bfb60d704d64cf3c3bbc555c95cc16cd03fe476e1ddda34'}
from public_paths import safe_path

def need(v,c):
 if not v:raise ValueError(c)
def data(v):return json.dumps(v,ensure_ascii=False,sort_keys=True,separators=(',',':')).encode('utf8')
def digest(v):return sha256(v).hexdigest()
def strict(raw):
 def unique(pairs):
  d={}
  for k,v in pairs:need(k not in d,'DUPLICATE_JSON');d[k]=v
  return d
 return json.loads(raw.decode('utf8'),object_pairs_hook=unique)
def git(*args,input=None):
 q=subprocess.run(['git','--no-optional-locks','-c','protocol.allow=never','-c','protocol.file.allow=always','--git-dir='+str(REPO),*args],input=input,capture_output=True)
 need(q.returncode==0,'GIT_READ_FAILED');return q.stdout

def image_path(path,ids):
 safe_path(path)
 m=re.fullmatch(r'(?:bgz-media/)?(kolaji|poslaniya)/(full|og)/(bgzona-([pm])-[0-9]{5})(?:-r[0-9]{4}-[a-f0-9]{12}|-[a-f0-9]{12})?(?:_hu_[a-f0-9]+)?\.(?:jpg|jpeg|png|gif|webp)',path)
 need(m is not None and m[3] in ids and m[4]=={'kolaji':'p','poslaniya':'m'}[m[1]],'PUBLIC_NAMESPACE')
 return path

def read(binding):
 need(set(binding)=={'data_commit','selection','snapshot'},'BINDING_FIELDS')
 import transport as T
 global REPO
 REPO,pin,observed_head=T.trusted_pin(binding['data_commit']);name=binding['selection'];need(name in (*SELECTIONS,'CURRENT'),'SNAPSHOT_SELECTION')
 state=strict(git('show',pin+':state.json'));sid=state['snapshot'] if name=='CURRENT' else state['public_snapshot'] if name=='PUBLIC12' else state['pending_accepted']['snapshot']
 need(binding['snapshot']==sid,'SNAPSHOT_NOT_AUTHORIZED_BY_TRUSTED_STATE')
 need(state.get('baseline_complete') is True,'BASELINE_INCOMPLETE')
 need(state['public_snapshot']==SELECTIONS['PUBLIC12'] and state['pending_accepted']['snapshot']==SELECTIONS['PENDING13'] and state['pending_accepted']['already_accepted'] is True,'PUBLIC_PENDING_CONTRADICTION')
 prefix='snapshots/'+sid+'/';entries=[]
 for row in git('ls-tree','-r','-z',pin,'--',prefix).split(b'\0'):
  if not row:continue
  meta,path=row.split(b'\t',1);mode,kind,oid=meta.decode().split();path=path.decode('utf8');need(path.startswith(prefix),'TREE_PREFIX');rel=path[len(prefix):];safe_path(rel)
  need(mode=='100644' and kind=='blob','TREE_LINK_OR_EXECUTABLE');entries.append((rel,oid))
 # One Git process / one integrity pass over this selected snapshot at the input boundary.
 stream=BytesIO(git('cat-file','--batch',input=''.join(o+'\n' for _,o in entries).encode()));files={}
 for rel,oid in entries:
  header=stream.readline().decode().split();need(len(header)==3 and header[:2]==[oid,'blob'],'GIT_BLOB_HEADER');raw=stream.read(int(header[2]));need(len(raw)==int(header[2]) and stream.read(1)==b'\n','GIT_BLOB_TRUNCATED');need(rel not in files,'DUPLICATE_GIT_PATH');files[rel]=raw
 need(stream.read()==b'','GIT_BATCH_TRAILING')
 value=validate(binding,state,files);value['proof'].update(authority_root=T.ANCHOR,trusted_ref=T.REF,observed_remote_head=observed_head);return value

def validate(binding,state,files):
 sid=binding['snapshot'];manifest=strict(files['manifest.json']);need(digest(data(manifest))==sid,'MANIFEST_IDENTITY')
 need(set(files)==set(manifest['files'])|{'manifest.json'},'SNAPSHOT_FILE_SET')
 seen=set()
 for path,expected in manifest['files'].items():
  safe_path(path);need(path.casefold() not in seen,'CASE_COLLISION');seen.add(path.casefold())
  need(re.fullmatch('[a-f0-9]{64}',expected) is not None and digest(files[path])==expected,'SNAPSHOT_BYTES_CHANGED')
 reg=strict(files['registry.json']);cat=strict(files['artifact/catalog.json']);current=strict(files['expected-files.json'])
 need(set(cat)=={'schema','items'} and cat['schema']==1,'CATALOG_SCHEMA');need(isinstance(cat['items'],list),'CATALOG_ITEMS')
 ids=set(reg['items']);rows=[];used=set();public={};metadata={}
 for path,raw in files.items():
  if path.startswith(('artifact/','historical/')) and path!='artifact/catalog.json':
   rel=path.split('/',1)[1];image_path(rel,ids);need(rel.casefold() not in {k.casefold() for k in public},'PUBLIC_COLLISION');public[rel]=raw
  else:need(path in {'manifest.json','registry.json','expected-files.json','artifact/catalog.json'},'PRIVATE_INPUT_FILE')
 need(current=={p.removeprefix('artifact/'):h for p,h in manifest['files'].items() if p.startswith('artifact/')},'CURRENT_INPUT_SET')
 for row in cat['items']:
  key=row['id'];need(key in ids and key not in used,'CATALOG_ID');used.add(key);item=reg['items'][key];rev=item['revisions'][-1];col=item['collection'];n=item['number']
  url='/'+col+'/'+('kolaj-' if col=='kolaji' else 'p-')+f'{n:05d}/'
  need(row['state']==item['state']=='published' and key==item['id']==f"bgzona-{'p' if col=='kolaji' else 'm'}-{n:05d}" and row['collection']==col and row['page_url']==item['page_url']==url,'ID_URL_STATE')
  need(row['revision']==rev['number'] and row['original']==rev['public_path'] and row['description']==item['description'] and row['slideshow']==item['slideshow'],'CATALOG_REGISTRY')
  rel=row['original'].removeprefix('/');need('artifact/'+rel in files and current[rel]==row['sha256'],'CURRENT_ORIGINAL_BINDING')
  alpha=False
  if rev['format']!='JPEG':
   # Only non-JPEG alpha ambiguity requires decoding. Current real originals are all JPEG.
   from PIL import Image
   with Image.open(BytesIO(public[rel])) as im:alpha=im.convert('RGBA').getextrema()[3][0]<255
  target=dict(collection=col,stem=key,number=n,revision=row['revision'],sha256=row['sha256'],ext=rev['ext'],source=rel,publicPath=rel,alpha=alpha,slideshow=row['slideshow'],txt=f'txt/{key}.txt')
  need(Path(rel).suffix==rev['ext'],'ORIGINAL_FORMAT_PATH')
  if item['og']:
   og=item['og'];expected=og.get('public_path',f"/{col}/og/{key}-{og['sha256'][:12]}{og['ext']}");need(row.get('og')==expected and 'artifact/'+expected[1:] in files,'OWN_OG_BINDING');target.update(og=expected[1:],ogPublicPath=expected[1:])
  else:need('og' not in row,'UNEXPECTED_OG')
  rows.append(target);metadata[key]={k:rev[k] for k in ('format','width','height','frames','loop','durations_ms')}
 need(used==ids,'CATALOG_INCOMPLETE')
 for item in reg['items'].values():
  for rev in item['revisions']:need(rev['public_path'].removeprefix('/') in public,'MISSING_HISTORICAL_REVISION')
 if binding['selection']=='CURRENT':need(re.fullmatch('[a-f0-9]{40}',manifest['source_commit']) is not None,'SOURCE_BINDING')
 else:need(manifest['source_commit']==(state['pending_accepted']['source_commit'] if binding['selection']=='PENDING13' else strict(git('show',binding['data_commit']+':snapshots/'+SELECTIONS['PUBLIC12']+'/manifest.json'))['source_commit']),'SOURCE_BINDING')
 proof=dict(binding,baseline_complete=True,ids=len(ids),revisions=sum(len(x['revisions']) for x in reg['items'].values()),public_files={k:manifest['files'][('artifact/' if 'artifact/'+k in files else 'historical/')+k] for k in public},image_metadata=metadata,deployed=False)
 return dict(manifest={'schema':1,'source_catalog_sha256':digest(files['artifact/catalog.json']),'items':sorted(rows,key=lambda x:(x['collection'],x['number']))},states={k:x['state'] for k,x in reg['items'].items()},catalog=cat,public=public,current={p for p in public if 'artifact/'+p in files},proof=proof)

def materialize(artifact,binding,destination):
 # Binding JSON is only a selector; all authority and public bytes come from fixed PIN.
 value=read(binding);dest=Path(destination);need(dest.resolve().is_relative_to(ROOT) and not dest.exists(),'NEW_ISOLATED_DESTINATION')
 dest.mkdir(parents=True);history=dest.parent/'historical';need(not history.exists() and not history.is_symlink() and not history.is_junction(),'HISTORY_DESTINATION')
 for path,raw in value['public'].items():
  target=(dest if path in value['current'] else history)/path;target.parent.mkdir(parents=True,exist_ok=True);target.write_bytes(raw)
 for row in value['catalog']['items']:
  target=dest/'txt'/f"{row['id']}.txt";target.parent.mkdir(parents=True,exist_ok=True);target.write_bytes(row['description'].encode('utf8'))
 (dest/'catalog.json').write_bytes(data(value['manifest']));(dest.parent/'snapshot-input.json').write_bytes(data(value['proof']))
 return value['manifest'],value['states']
