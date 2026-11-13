"""Local Lab integration: one configured source, one remote data authority. File protocol only."""
from pathlib import Path,PurePosixPath
from tempfile import TemporaryDirectory
import argparse,json,re,sys
try:
 import git_publisher as G
 import transport as T
 import trusted_adapter as A
except Exception as error:
 message=str(error);code=message if re.fullmatch('[A-Z][A-Z0-9_]+',message) else 'DEPENDENCY_UNAVAILABLE'
 print(json.dumps(dict(status='STOP',code=code,publication='NOT_PERFORMED',GitHub_writes=0)));raise SystemExit(2)
P=G.P
ROOT=Path(__file__).resolve().parent
ANCHOR=T.ANCHOR

def strict(raw):
 def unique(pairs):
  value={}
  for k,v in pairs:P.need(k not in value,'DUPLICATE_CONFIG_KEY');value[k]=v
  return value
 return json.loads(raw,object_pairs_hook=unique)

def configuration(path):
 path=Path(path).resolve();c=strict(path.read_bytes())
 P.need(set(c)=={'schema','public_workspace','data_remote','private_source','source_ref','trusted_root'} and type(c['schema']) is int and c['schema']==1,'CONFIG_FIELDS')
 P.need(c['trusted_root']==ANCHOR,'UNTRUSTED_ROOT')
 for key in ('public_workspace','data_remote','private_source'):
  raw=c[key];P.need(isinstance(raw,str) and raw and not PurePosixPath(raw).is_absolute() and ':' not in raw and '\\' not in raw,'CONFIG_RELATIVE_PATH')
  target=path.parent.joinpath(*PurePosixPath(raw).parts)
  P.need(not any(p.is_symlink() or p.is_junction() for p in [target,*target.parents]),'CONFIG_LINK');c[key]=target.resolve()
 work=c['public_workspace'];remote=c['data_remote'];source=c['private_source']
 P.need(work.is_relative_to(ROOT.parent) and work!=ROOT.parent and not work.is_relative_to(ROOT),'ISOLATED_WORKSPACE')
 P.need(remote.is_relative_to(work) and remote!=work,'LOCAL_REMOTE_REQUIRED')
 P.need(not source.is_relative_to(work) and not work.is_relative_to(source),'PRIVATE_PUBLIC_SEPARATION')
 P.need(source.is_dir(),'MISSING_PRIVATE_SOURCE')
 P.need(isinstance(c['source_ref'],str) and re.fullmatch(r'refs/heads/[A-Za-z0-9._/-]+',c['source_ref']) is not None and '..' not in c['source_ref'],'SOURCE_REF_INVALID')
 P.need(remote.is_dir() and not (remote/'objects/info/alternates').exists(),'REMOTE_MISSING_OR_ALTERNATE')
 P.need(G.run(remote,'rev-parse','--is-bare-repository').stdout.strip()==b'true' and not G.run(remote,'remote').stdout.strip(),'LOCAL_BARE_REMOTE')
 return c

def safe_error(error):
 message=str(error)
 return message if re.fullmatch('[A-Z][A-Z0-9_]+',message) else 'RUNTIME_STOP'

def public_result(status,**fields):
 return dict(status=status,publication='NOT_PERFORMED',GitHub_writes=0,**fields)

def confirmed(client,commit,snapshot=None):
 # Separate fetched consumer cache, not the writer checkout or prepared PASS report.
 state=strict(G.run(client,'show',commit+':state.json').stdout)
 if snapshot is None:snapshot=state['snapshot']
 value=A.read(dict(data_commit=commit,selection='CURRENT',snapshot=snapshot))
 P.need(value['proof']['data_commit']==commit and value['proof']['observed_remote_head']==commit,'REMOTE_MOVED_AFTER_WRITE')
 P.need(T.observe(client)==dict(status='KNOWN',head=commit),'REMOTE_MOVED_AFTER_READBACK')
 return value

def execute(config_path,wake=None,send=None):
 try:
  # Wake is a hint only. No caller-selected source, endpoint, CID or PASS is accepted.
  if wake is not None:P.need(wake=={'mode':'publisher-wake'},'WAKE_NOT_AUTHORITY')
  c=configuration(config_path);work=c['public_workspace'];work.mkdir(parents=True,exist_ok=True)
  with TemporaryDirectory(dir=work,prefix='lab-') as scratch:
   scratch=Path(scratch);T.configure(work,c['data_remote'],scratch/'consumer.git')
   client=G.init(scratch/'writer.git');seen=T.fetch_head(client)
   P.need(seen['status']=='KNOWN' and seen['head']!=G.ZERO,'REMOTE_UNKNOWN')
   expected=seen['head'];G.run(client,'update-ref',G.REF,expected,G.ZERO)
   target=P.git(c['private_source'],'rev-parse','--verify',c['source_ref']+'^{commit}').decode().strip();G.oid(target)
   proposal=G.prepare(client,c['private_source'],target)
   P.need(proposal['expected']==expected,'STALE_PREPARATION')
   rows=proposal['result']['commits'];accepted=sum(x['accepted'] for x in rows);skipped=sum(x['skipped'] for x in rows)
   stats=dict(processed_source_commits=len(rows),accepted=accepted,skipped=skipped,items=[item for row in rows for item in row['items'] if item['status']!='UNCHANGED'])
   if proposal['commit']==expected:
    confirmed(client,expected,proposal['result']['snapshot'])
    return public_result('NO_CHANGE',data_confirmed=True,data_revision=expected,snapshot=proposal['result']['snapshot'],remote_pushes=0,independent_consumer_verified=True,**stats)
   # No G.finish: remote ref is the only persistent authority. One send, independent readback.
   outcome=(T.push if send is None else send)(client,proposal['commit'],expected)
   if outcome['status']!='CONFIRMED':
    return public_result('UNKNOWN' if outcome['status']=='UNKNOWN' else 'STOP',code=outcome['status'],data_confirmed=False,candidate=proposal['commit'],expected=expected,remote_pushes=outcome['push_calls'],retry=False,**stats)
   value=confirmed(client,proposal['commit'],proposal['result']['snapshot'])
   status='PARTIAL' if skipped and accepted else 'REJECTED' if skipped else 'SUCCESS'
   return public_result(status,data_confirmed=True,data_revision=proposal['commit'],snapshot=value['proof']['snapshot'],remote_pushes=outcome['push_calls'],independent_consumer_verified=True,sanitized_images=sum(x['sanitized_images'] for x in rows),**stats)
 except Exception as error:return public_result('STOP',code=safe_error(error),retry=False)

def reconcile(config_path,candidate,expected):
 try:
  G.oid(candidate);G.oid(expected);c=configuration(config_path)
  with TemporaryDirectory(dir=c['public_workspace'],prefix='readback-') as scratch:
   scratch=Path(scratch);T.configure(c['public_workspace'],c['data_remote'],scratch/'consumer.git');client=G.init(scratch/'reader.git')
   result=T.reconcile(client,candidate,expected)
   if result['status']=='CONFIRMED':
    P.need(T.fetch_head(client)['head']==candidate,'REMOTE_CHANGED');confirmed(client,candidate)
   return public_result('SUCCESS' if result['status']=='CONFIRMED' else 'UNKNOWN' if result['status']=='UNKNOWN' else 'STOP',reconciliation=result['status'],remote_pushes=0,retry=False,data_confirmed=result['status']=='CONFIRMED')
 except Exception as error:return public_result('STOP',code=safe_error(error),retry=False)

def main():
 parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--config',required=True);parser.add_argument('--wake');parser.add_argument('--reconcile',nargs=2,metavar=('CANDIDATE','EXPECTED'));a=parser.parse_args()
 try:result=reconcile(a.config,*a.reconcile) if a.reconcile else execute(a.config,strict(Path(a.wake).read_bytes()) if a.wake else None)
 except Exception as error:result=public_result('STOP',code=safe_error(error),retry=False)
 print(json.dumps(result,ensure_ascii=True));return 0 if result['status'] in ('SUCCESS','PARTIAL','REJECTED','NO_CHANGE','CONFIRMED','NOT_WRITTEN') else 2
if __name__=='__main__':raise SystemExit(main())
