"""One synthetic GitHub HTTPS push, only on an explicitly authorized future run."""
from pathlib import Path
from tempfile import TemporaryDirectory
import argparse,hashlib,json,os,platform,re,shutil,sys
REPOSITORY='malin-bg/bgzona-github-lab'
CODE_REF='refs/heads/rs4h-isolated-smoke-20261113'
TARGET='refs/heads/rs4j-https-synthetic-20261113'
URL='https://github.com/'+REPOSITORY+'.git'

def need(ok,code):
 if not ok:raise ValueError(code)

def identity(env):
 need(env.get('GITHUB_REPOSITORY')==REPOSITORY,'REPOSITORY_IDENTITY')
 need(env.get('GITHUB_REF')==CODE_REF and env.get('GITHUB_EVENT_NAME')=='push','CODE_REF_EVENT')
 need(env.get('GITHUB_RUN_ATTEMPT')=='1','ATTEMPT_NOT_ONE')
 need(re.fullmatch('[0-9a-f]{40}',env.get('GITHUB_SHA','')) is not None,'CODE_COMMIT')
 need(re.fullmatch('[1-9][0-9]*',env.get('GITHUB_RUN_ID','')) is not None,'RUN_ID')

def absent(channel,repo):
 q=channel.run(repo,'ls-remote','--exit-code','--refs',URL,TARGET,check=False)
 need(q.returncode==2 and q.stdout==b'','TARGET_EXISTS_OR_UNKNOWN')

def transfer(channel,writer,reader,commit,marker,inspect,progress):
 need(channel.endpoint.url==URL and channel.endpoint.ref==TARGET and channel.endpoint.role=='data-write','EXACT_WRITE_SCOPE')
 absent(channel,reader)
 # Durable local one-attempt marker is created before the sole send, never reset.
 with marker.open('x',encoding='utf8') as handle:handle.write('ONE_PUSH_ATTEMPT\n')
 progress.update(push_calls=1,status='UNKNOWN')
 q=channel.run(writer,'-c','push.followTags=false','-c','push.recurseSubmodules=no','push','--porcelain',URL,commit+':'+TARGET,check=False)
 # Unknown/failed response gets read-only reconciliation, never a second send.
 seen=channel.observe(reader)
 if seen!={'status':'KNOWN','head':commit}:
  return {'status':'UNKNOWN' if seen.get('status')=='UNKNOWN' else 'STOP','push_calls':1,'retry':False,'push_exit_code':q.returncode,'push_failure_category':getattr(q,'failure_category','UNKNOWN'),'code':'REMOTE_NOT_CONFIRMED'}
 channel.run(reader,'fetch','--no-tags',URL,TARGET)
 inspect(reader,commit)
 need(channel.observe(reader)==seen,'REMOTE_MOVED_AFTER_READBACK')
 return {'status':'PASS','push_calls':1,'retry':False,'push_exit_code':q.returncode,'push_failure_category':getattr(q,'failure_category','UNKNOWN'),'independent_readback':'PASS','response_reconciled':q.returncode!=0}

def main():
 parser=argparse.ArgumentParser();parser.add_argument('--report',required=True);args=parser.parse_args()
 report={'status':'STOP','scope':'RS4J synthetic HTTPS transport only','push_calls':0,'retry':False}
 try:
  identity(os.environ);need(platform.system()=='Linux','LINUX_REQUIRED')
  secret=os.environ.pop('RS4J_TOKEN','');need(bool(secret),'TOKEN_MISSING')
  # Only public test identities; no PRIVATE input is read.
  payload=(json.dumps({'schema':1,'synthetic':True,'message':'RS4J synthetic HTTPS bytes 123','repository':REPOSITORY,'code_commit':os.environ['GITHUB_SHA'],'run_id':os.environ['GITHUB_RUN_ID']},sort_keys=True)+'\n').encode()
  with TemporaryDirectory(prefix='rs4j-',dir=os.environ['RUNNER_TEMP']) as temp:
   root=Path(temp);adapter=root/'adapter';adapter.mkdir()
   for name in ['https_git.py','credential_helper.py']:shutil.copyfile(Path(__file__).parent/name,adapter/name)
   sys.path[:0]=[str(adapter),str(Path(__file__).parent.parent/'rs4i/runtime')]
   import https_git as H
   G=H.G;G.ROOT=root;G.REF=TARGET
   writer=G.init(root/'writer.git');reader=G.init(root/'reader.git')
   blob=G.run(writer,'hash-object','-w','--stdin',input=payload).stdout.strip()
   tree=G.run(writer,'mktree',input=b'100644 blob '+blob+b'\tprobe.json\n').stdout.decode().strip()
   commit=G.run(writer,'commit-tree',tree,input=b'RS4J synthetic HTTPS transport only\n').stdout.decode().strip()
   # Configure the existing data trust constants only for this synthetic root/ref.
   # Endpoint and Channel validation remain unmodified; no production baseline.
   H.ANCHOR=commit
   channel=H.Channel(H.Endpoint(URL,'data-write',TARGET,commit),H.Identity('data-write',URL,'x-access-token',secret))
   def inspect(repo,expected):
    need(G.run(repo,'rev-parse','FETCH_HEAD').stdout.decode().strip()==expected,'FETCH_COMMIT_MISMATCH')
    need(G.run(repo,'show','-s','--format=%P',expected).stdout.strip()==b'','UNEXPECTED_HISTORY')
    need(G.run(repo,'ls-tree','-r',expected).stdout==b'100644 blob '+blob+b'\tprobe.json\n','TREE_MISMATCH')
    actual=G.run(repo,'show',expected+':probe.json').stdout
    need(actual==payload and secret.encode() not in actual,'BYTES_MISMATCH')
   outcome=transfer(channel,writer,reader,commit,root/'push-attempt',inspect,report)
   report.update(outcome,repository=REPOSITORY,target=TARGET,synthetic_commit=commit,payload_sha256=hashlib.sha256(payload).hexdigest())
 except Exception as error:
  # No raw Git output, token, URL queries, local path or traceback enters the report.
  report.update(status='UNKNOWN' if report['push_calls'] else 'STOP',error_type=type(error).__name__)
  if isinstance(error,ValueError) and re.fullmatch('[A-Z_]+',str(error)):report['code']=str(error)
 finally:
  path=Path(args.report);path.parent.mkdir(parents=True,exist_ok=True);path.write_text(json.dumps(report,indent=2)+'\n','utf8');print(json.dumps(report))
 return 0 if report['status']=='PASS' else 1
if __name__=='__main__':raise SystemExit(main())
