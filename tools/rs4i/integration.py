"""RS4I synthetic-only integration of unchanged P2 runtime; no network operations."""
from pathlib import Path
from tempfile import TemporaryDirectory
from io import BytesIO
import argparse,json,os,platform,shutil,sys

def main():
 parser=argparse.ArgumentParser();parser.add_argument('--report',required=True);args=parser.parse_args()
 report={'status':'FAIL','os':platform.system(),'checks':{},'publication':'NOT_PERFORMED','GitHub_writes':0}
 def check(ok,name):
  if not ok:raise AssertionError(name)
  report['checks'][name]='PASS'
 try:
  with TemporaryDirectory(prefix='rs4i-',dir=os.environ.get('RUNNER_TEMP')) as temp:
   root=Path(temp);shutil.copytree(Path(__file__).parent/'runtime',root/'runtime');sys.path.insert(0,str(root/'runtime'))
   # Scope all Git state/config to this disposable synthetic test process.
   empty=root/'empty-config';empty.write_bytes(b'')
   for key in list(os.environ):
    if key.startswith('GIT_CONFIG_') or key in ('GIT_DIR','GIT_WORK_TREE','GIT_INDEX_FILE','GIT_OBJECT_DIRECTORY','GIT_ALTERNATE_OBJECT_DIRECTORIES'):os.environ.pop(key)
   os.environ.update(GIT_CONFIG_GLOBAL=str(empty),GIT_CONFIG_SYSTEM=str(empty),GIT_CONFIG_NOSYSTEM='1',GIT_ALLOW_PROTOCOL='file',GIT_TERMINAL_PROMPT='0')
   import lab as L
   from PIL import Image,PngImagePlugin,__version__
   G=L.G;P=G.P;T=L.T;A=L.A
   work=root/'work/public';work.mkdir(parents=True);source=root/'work/private/source.git';source.parent.mkdir(parents=True);source.mkdir()
   G.ROOT=work;G.run(source,'init','--bare');ref='refs/heads/synthetic-source'
   tree=G.run(source,'mktree',input=b'').stdout.decode().strip()
   genesis=G.run(source,'commit-tree',tree,input=b'RS4I synthetic genesis\n').stdout.decode().strip()
   cursor=G.run(source,'commit-tree',tree,'-p',genesis,input=b'RS4I synthetic pending state\n').stdout.decode().strip()
   G.run(source,'update-ref',ref,cursor)
   seed=work/'seed';seed.mkdir();registry={'schema':1,'seq':0,'counters':{'poslaniya':0,'kolaji':0},'previous':None,'items':{}}
   public=P.snapshot(seed,registry,{},genesis);pending=P.snapshot(seed,registry,{},cursor)
   state={'schema':1,'baseline_complete':True,'source_genesis':genesis,'source_cursor':cursor,'snapshot':pending,'public_snapshot':public,'pending_accepted':{'legacy_D3':13,'already_accepted':True,'snapshot':pending,'source_commit':cursor},'source_bindings':[]}
   (seed/'state.json').write_bytes(P.data(state));remote=G.init(work/'remote.git');proposal=G.bootstrap(remote,seed);G.finish(remote,proposal);anchor=proposal['commit']
   # Test-fixture trust configuration only. No validation function is replaced.
   # P2 has module constants for its production root/views; synthetic data must
   # use its own locally generated root and separate synthetic views.
   L.ANCHOR=T.ANCHOR=anchor;A.SELECTIONS={'PUBLIC12':public,'PENDING13':pending}
   config=root/'runtime.json';config.write_text(json.dumps({'schema':1,'public_workspace':'work/public','data_remote':'work/public/remote.git','private_source':'work/private/source.git','source_ref':ref,'trusted_root':anchor}),'utf8')
   image=Image.new('RGB',(8,8),(35,110,190));buffer=BytesIO();metadata=PngImagePlugin.PngInfo();metadata.add_text('Comment','RS4I SYNTHETIC metadata to remove');image.save(buffer,format='PNG',pnginfo=metadata)
   original=buffer.getvalue();description='Синтетичен материал 123';stem='incoming/poslaniya/Синтетичен материал 123'
   index=root/'source-index';env={'GIT_INDEX_FILE':str(index)};G.run(source,'read-tree',cursor,env=env);entries=[]
   for path,raw in [(stem+'.png',original),(stem+'.txt',description.encode())]:
    blob=G.run(source,'hash-object','-w','--stdin',input=raw).stdout.strip();entries.append(b'100644 '+blob+b'\t'+path.encode()+b'\0')
   G.run(source,'update-index','-z','--index-info',input=b''.join(entries),env=env);tree=G.run(source,'write-tree',env=env).stdout.decode().strip()
   target=G.run(source,'commit-tree',tree,'-p',cursor,input=b'RS4I synthetic Cyrillic input\n').stdout.decode().strip();G.run(source,'update-ref',ref,target,cursor)
   out=L.execute(config);check(out['status']=='SUCCESS' and out['accepted']==1 and out['sanitized_images']==1 and out['remote_pushes']==1,'real_publisher_privacy_file_push')
   revision=out['data_revision'];check(G.run(remote,'rev-parse',G.REF).stdout.decode().strip()==revision,'remote_persistence')
   # A new independent fetched consumer, with all real trust and byte checks.
   with TemporaryDirectory(dir=work) as readback:
    T.configure(work,remote,Path(readback)/'reader.git');value=A.read({'data_commit':revision,'selection':'CURRENT','snapshot':out['snapshot']})
    check(value['proof']['ids']==1 and value['proof']['revisions']==1,'independent_snapshot_readback')
    row=value['catalog']['items'][0];check(row['id']=='bgzona-m-00001' and row['page_url']=='/poslaniya/p-00001/' and row['description']==description,'ID_URL_Cyrillic')
    clean=value['public'][row['original'].lstrip('/')];decoded=Image.open(BytesIO(clean));decoded.load()
    check(clean!=original and 'Comment' not in decoded.info and decoded.tobytes()==image.tobytes(),'real_metadata_removed_pixels_preserved')
   check(P.git(source,'show',target+':'+stem+'.png')==original,'synthetic_original_unchanged')
   again=L.execute(config);check(again['status']=='NO_CHANGE' and again['remote_pushes']==0 and again['data_revision']==revision,'repeat_NO_CHANGE_no_push')
   negative=L.execute(config,wake={'mode':'publisher-wake','cid':'untrusted'});check(negative['status']=='STOP' and negative['code']=='WAKE_NOT_AUTHORITY' and G.run(remote,'rev-parse',G.REF).stdout.decode().strip()==revision,'payload_authority_rejected_no_write')
   final=json.loads(G.run(remote,'show',revision+':state.json').stdout);check(final['public_snapshot']==public and final['pending_accepted']==state['pending_accepted'],'synthetic_views_not_promoted')
   report.update(status='PASS',python=platform.python_version(),pillow=__version__,synthetic_only=True)
 except Exception as error:
  report['error_type']=type(error).__name__
  if isinstance(error,AssertionError):report['failed_check']=str(error)
 finally:
  path=Path(args.report);path.parent.mkdir(parents=True,exist_ok=True);path.write_text(json.dumps(report,indent=2)+'\n','utf8');print(json.dumps(report))
 return 0 if report['status']=='PASS' else 1
if __name__=='__main__':raise SystemExit(main())
