"""Minimal HTTPS subprocess boundary; no credential storage, issuer or GitHub API."""
from pathlib import Path
from dataclasses import dataclass,field
from urllib.parse import urlsplit
import os,re,shlex,shutil,subprocess,sys
import git_publisher as G
P=G.P
ANCHOR='aa14561064c3ab9d314696b254d3ab2768110493'
GIT=shutil.which('git')

@dataclass(frozen=True)
class Endpoint:
 url:str
 role:str
 ref:str
 root:str
 test_loopback:bool=False
 ca_file:object=None
 def __post_init__(self):
  u=urlsplit(self.url)
  P.need(u.scheme=='https' and not u.username and not u.password and not u.query and not u.fragment and '\\' not in self.url and '%' not in self.url and not any(ord(x)<33 for x in self.url),'HTTPS_ENDPOINT')
  P.need(self.role in ('source-read','data-write'),'HTTPS_ROLE')
  if self.test_loopback:
   P.need(u.hostname=='127.0.0.1' and u.port is not None and re.fullmatch(r'/[a-z0-9-]+\.git',u.path) is not None and u.netloc=='127.0.0.1:'+str(u.port),'TEST_LOOPBACK_ENDPOINT')
  else:P.need(u.netloc=='github.com' and re.fullmatch(r'/[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+\.git',u.path) is not None and self.ca_file is None,'UNSUPPORTED_HTTPS_ORIGIN')
  P.need(re.fullmatch(r'refs/heads/[A-Za-z0-9._/-]+',self.ref) is not None and '..' not in self.ref and not self.ref.endswith(('/', '.lock')),'HTTPS_REF');G.oid(self.root)
  if self.role=='data-write':P.need(self.ref==G.REF and self.root==ANCHOR,'DATA_TRUST_POLICY')
  if self.ca_file is not None:P.need(self.test_loopback and Path(self.ca_file).is_file(),'TEST_CA_REQUIRED')

@dataclass(frozen=True,repr=False)
class Identity:
 role:str
 origin:str
 username:str=field(repr=False)
 secret:str=field(repr=False)
 def __repr__(self):return '<ScopedIdentity redacted>'
 def __post_init__(self):
  P.need(self.role in ('source-read','data-write') and isinstance(self.username,str) and isinstance(self.secret,str) and self.username and self.secret and ':' not in self.username and all(33<=ord(c)<127 for c in self.username+self.secret),'CREDENTIAL_SHAPE')

def clean_env():
 # No inherited Git config/trace/auth/proxy/askpass variables. No global config lookup.
 env={k:v for k,v in os.environ.items() if k.upper() in ('PATH','SYSTEMROOT','WINDIR','COMSPEC','TEMP','TMP','PATHEXT')}
 home=Path(__file__).resolve().parent.parent/'work/empty-home';home.mkdir(parents=True,exist_ok=True)
 env.update(HOME=str(home),USERPROFILE=str(home),XDG_CONFIG_HOME=str(home),CURL_HOME=str(home),GIT_CONFIG_NOSYSTEM='1',GIT_CONFIG_SYSTEM=os.devnull,GIT_CONFIG_GLOBAL=os.devnull,GIT_TERMINAL_PROMPT='0',GIT_NO_LAZY_FETCH='1',GIT_NO_REPLACE_OBJECTS='1',GIT_ALLOW_PROTOCOL='https',LC_ALL='C')
 return env

def safe_client(repo):
 repo=Path(repo).resolve();P.need(repo.is_dir() and not any(p.is_symlink() or p.is_junction() for p in [repo,*repo.parents]),'HTTPS_CLIENT_PATH')
 P.need(not (repo/'objects/info/alternates').exists(),'HTTPS_CLIENT_ALTERNATE')
 q=subprocess.run([GIT,'--no-optional-locks','-C',str(repo),'config','--local','--no-includes','--null','--list'],env=clean_env(),capture_output=True)
 P.need(q.returncode==0,'HTTPS_CLIENT_CONFIG')
 allowed={'core.repositoryformatversion','core.filemode','core.bare','core.logallrefupdates','core.ignorecase','core.symlinks','core.precomposeunicode'}
 for row in q.stdout.split(b'\0'):
  if not row:continue
  key,sep,value=row.partition(b'\n');P.need(sep and key.decode('ascii').lower() in allowed,'UNSAFE_LOCAL_GIT_CONFIG')
 q=subprocess.run([GIT,'--no-optional-locks','-C',str(repo),'rev-parse','--is-bare-repository'],env=clean_env(),capture_output=True)
 P.need(q.returncode==0 and q.stdout.strip()==b'true','HTTPS_BARE_CLIENT')
 return repo

def helper_command():
 return '!'+shlex.quote(Path(sys.executable).as_posix())+' -I -S -B '+shlex.quote((Path(__file__).parent/'credential_helper.py').as_posix())

class Channel:
 def __init__(self,endpoint,identity):
  P.need(isinstance(endpoint,Endpoint) and isinstance(identity,Identity) and identity.role==endpoint.role and identity.origin==endpoint.url,'IDENTITY_SCOPE')
  self.endpoint=endpoint;self.identity=identity
 def run(self,repo,*args,input=None,check=True):
  ep=self.endpoint;url=ep.url;ref=ep.ref
  valid=args in (('ls-remote','--exit-code','--refs',url,ref),('fetch','--no-tags',url,ref))
  prefix=('-c','push.followTags=false','-c','push.recurseSubmodules=no','push','--porcelain',url)
  if args[:len(prefix)]==prefix and len(args)==len(prefix)+1:
   P.need(ep.role=='data-write','SOURCE_READ_ONLY');spec=args[-1];oid,sep,name=spec.partition(':');G.oid(oid);P.need(sep and name==ref,'HTTPS_PUSH_REF');valid=True
  P.need(valid,'HTTPS_COMMAND_SCOPE');repo=safe_client(repo)
  helper=helper_command()
  settings={'credential.helper':'','credential.useHttpPath':'true','credential.interactive':'false','http.sslVerify':'true','http.followRedirects':'false','http.proxy':'','http.extraHeader':'','http.maxRetries':'0','fetch.recurseSubmodules':'false','submodule.recurse':'false','protocol.allow':'never','protocol.https.allow':'always','core.hooksPath':str(Path(__file__).parent/'no-hooks'),'gc.auto':'0'}
  argv=[GIT,'--no-optional-locks','-C',str(repo)]
  for k,v in settings.items():argv+=['-c',k+'='+v]
  argv+=['-c','credential.helper='+helper]
  if ep.ca_file is not None:argv+=['-c','http.sslCAInfo='+Path(ep.ca_file).resolve().as_posix(),'-c','http.schannelUseSSLCAInfo=true']
  argv+=list(args)
  env=clean_env();env.update(BGZONA_AUTH_URL=url,BGZONA_AUTH_USER=self.identity.username,BGZONA_AUTH_SECRET=self.identity.secret)
  try:q=subprocess.run(argv,input=input,capture_output=True,env=env,timeout=180)
  except (OSError,subprocess.TimeoutExpired):
   if check:raise P.Stop('HTTPS_GIT_UNKNOWN') from None
   return subprocess.CompletedProcess(argv,124,b'',b'HTTPS_GIT_UNKNOWN')
  # Raw server errors are not reports/logs and may contain arbitrary text. Never export them.
  q.stderr=b'' if q.returncode==0 else b'HTTPS_GIT_FAILED'
  if check:P.need(q.returncode==0,'HTTPS_GIT_FAILED')
  return q
 def observe(self,repo):
  q=self.run(repo,'ls-remote','--exit-code','--refs',self.endpoint.url,self.endpoint.ref,check=False)
  if q.returncode!=0:return dict(status='UNKNOWN',head=None)
  rows=q.stdout.decode('ascii').splitlines()
  if len(rows)!=1:return dict(status='UNKNOWN',head=None)
  cols=rows[0].split()
  if len(cols)!=2 or cols[1]!=self.endpoint.ref:return dict(status='UNKNOWN',head=None)
  G.oid(cols[0]);return dict(status='KNOWN',head=cols[0])
 def acquire_source(self,repo):
  P.need(self.endpoint.role=='source-read','SOURCE_IDENTITY_REQUIRED');before=self.observe(repo);P.need(before['status']=='KNOWN','SOURCE_HTTPS_UNKNOWN')
  self.run(repo,'fetch','--no-tags',self.endpoint.url,self.endpoint.ref)
  fetched=G.run(repo,'rev-parse','--verify','FETCH_HEAD').stdout.decode().strip();after=self.observe(repo)
  P.need(after==before and fetched==before['head'],'SOURCE_REF_MOVED')
  P.need(G.run(repo,'rev-list','--max-parents=0',fetched).stdout.decode().splitlines()==[self.endpoint.root],'SOURCE_ROOT')
  G.run(repo,'update-ref',self.endpoint.ref,fetched,G.ZERO);return fetched
