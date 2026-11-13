"""Protected, single PRIVATE Git read. Never exports source metadata or objects."""
from pathlib import Path
from tempfile import TemporaryDirectory
import argparse,json,os,re,shlex,shutil,subprocess,sys,urllib.request
REPO='malin-bg/bgzona-github-lab';PRIVATE='malin-bg/bgzona-private-intake'
URL='https://github.com/'+PRIVATE+'.git'

def need(ok):
 if not ok:raise ValueError('STOP')

def guard(env):
 expected={'GITHUB_REPOSITORY':REPO,'GITHUB_REPOSITORY_ID':'1426765395','GITHUB_ACTOR_ID':'252877943','GITHUB_REF':'refs/heads/automation-control','GITHUB_REF_PROTECTED':'true','GITHUB_EVENT_NAME':'workflow_dispatch','GITHUB_RUN_ATTEMPT':'1','RS4K_MODE':'app-source-read'}
 need(all(env.get(k)==v for k,v in expected.items()))
 need(env.get('GITHUB_WORKFLOW_REF')==REPO+'/.github/workflows/shared-media.yml@refs/heads/automation-control')
 need(re.fullmatch('[a-f0-9]{40}',env.get('GITHUB_SHA','')) is not None and env.get('RS4K_WORKFLOW_SHA')==env['GITHUB_SHA'])

def scope(data):
 need(data.get('total_count')==1 and len(data.get('repositories',[]))==1)
 row=data['repositories'][0];need(row.get('full_name')==PRIVATE and row.get('private') is True)

class NoRedirect(urllib.request.HTTPRedirectHandler):
 def redirect_request(self,*args,**kwargs):return None

def verify(token,workspace):
 request=urllib.request.Request('https://api.github.com/installation/repositories?per_page=100',headers={'Authorization':'Bearer '+token,'Accept':'application/vnd.github+json','X-GitHub-Api-Version':'2022-11-28','User-Agent':'BGZONA-protected-private-read'},method='GET')
 with urllib.request.build_opener(NoRedirect).open(request,timeout=30) as response:
  need(response.status==200);scope(json.load(response))
 # Isolated process environment, matching the existing hardened helper pattern.
 env={k:os.environ[k] for k in ['PATH','SystemRoot','COMSPEC','TEMP','TMP'] if k in os.environ}
 env.update(HOME=str(workspace),USERPROFILE=str(workspace),XDG_CONFIG_HOME=str(workspace),CURL_HOME=str(workspace),GIT_CONFIG_NOSYSTEM='1',GIT_CONFIG_GLOBAL=os.devnull,GIT_CONFIG_SYSTEM=os.devnull,GIT_TERMINAL_PROMPT='0',GIT_NO_LAZY_FETCH='1',GIT_NO_REPLACE_OBJECTS='1',GIT_ALLOW_PROTOCOL='https',LC_ALL='C',BGZONA_AUTH_URL=URL,BGZONA_AUTH_USER='x-access-token',BGZONA_AUTH_SECRET=token)
 helper='!'+shlex.quote(Path(sys.executable).as_posix())+' -I -S -B '+shlex.quote((Path(__file__).parent/'credential_helper.py').as_posix())
 settings={'credential.helper':'','credential.useHttpPath':'true','credential.interactive':'false','http.sslVerify':'true','http.followRedirects':'false','http.proxy':'','http.extraHeader':'','http.maxRetries':'0','protocol.allow':'never','protocol.https.allow':'always'}
 argv=[shutil.which('git'),'--no-optional-locks']
 for key,value in settings.items():argv+=['-c',key+'='+value]
 argv+=['-c','credential.helper='+helper,'ls-remote','--exit-code',URL,'HEAD']
 # Exactly one Git invocation; no fetch, checkout, push or retry.
 result=subprocess.run(argv,cwd=workspace,env=env,capture_output=True,timeout=60)
 valid=result.returncode==0 and re.fullmatch(rb'[a-f0-9]{40}\tHEAD\r?\n',result.stdout) is not None
 result.stdout=b'';result.stderr=b''  # Never export raw response or errors.
 need(valid)
 return {'status':'PASS','repository_scope':'PASS','private_git_read':'PASS'}

def main():
 parser=argparse.ArgumentParser();parser.add_argument('--guard',action='store_true');args=parser.parse_args()
 result={'status':'UNKNOWN','private_git_read':'NOT_CONFIRMED'}
 try:
  guard(os.environ)
  if args.guard:result={'status':'PASS','protected_context':'PASS'}
  else:
   token=os.environ.pop('RS4K_TOKEN','');need(bool(token))
   with TemporaryDirectory(prefix='rs4k-read-',dir=os.environ['RUNNER_TEMP']) as temp:result=verify(token,Path(temp))
 except Exception:pass  # Only fixed categories, never exception/server/source text.
 print(json.dumps(result));return 0 if result['status']=='PASS' else 2
if __name__=='__main__':raise SystemExit(main())
