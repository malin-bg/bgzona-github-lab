"""Fixed-ref commit handoff. No publisher, Gate, media or source parser imports.
Channel is the existing authenticated RS4J transport or the explicit local file adapter.
Only trusted, same-code producer outputs may call this function in a privileged job.
"""
from dataclasses import dataclass
from pathlib import Path
import os,re,subprocess

class Stop(ValueError):pass

def need(ok,code):
 if not ok:raise Stop(code)

def oid(value):
 need(isinstance(value,str) and re.fullmatch('[a-f0-9]{40}',value) is not None,'COMMIT_ID');return value

@dataclass(frozen=True)
class Policy:
 repository:str
 ref:str
 root:str
 def __post_init__(self):
  need((self.repository,self.ref) in (
   ('malin-bg/bgzona-private-intake','refs/heads/rs4m-synthetic-state'),
   ('malin-bg/bgzona-github-lab','refs/heads/rs4m-synthetic-data')),'FIXED_SYNTHETIC_DESTINATION')
  oid(self.root)
  roots={'malin-bg/bgzona-private-intake':'598ad1022c0d0646dd491575ccac682c39ae2f5d','malin-bg/bgzona-github-lab':'564c405279c89e990c9345ccf7c54e2bf38c4470'}
  need(self.root==roots[self.repository],'FIXED_SYNTHETIC_ROOT')


def local_git(repo,*args):
 # Inspection of inert Git objects only; no checkout, hooks, filters or media decoder.
 env={k:v for k,v in os.environ.items() if k.upper() in ('PATH','SYSTEMROOT','WINDIR','TEMP','TMP','COMSPEC','PATHEXT')}
 env.update(GIT_CONFIG_NOSYSTEM='1',GIT_CONFIG_SYSTEM=os.devnull,GIT_CONFIG_GLOBAL=os.devnull,GIT_TERMINAL_PROMPT='0',GIT_NO_REPLACE_OBJECTS='1',GIT_NO_LAZY_FETCH='1')
 q=subprocess.run(['git','--no-optional-locks','-C',str(repo),'-c','core.hooksPath='+str(Path(__file__).parent/'no-hooks'),*args],capture_output=True,env=env)
 need(q.returncode==0,'COMMIT_INSPECTION_UNKNOWN');return q.stdout


def write_once(client,proposal,policy,channel):
 """One send, readback only afterwards. No force, no blind retry, no bootstrap.
 Artifact provenance is a mandatory external caller precondition, not a PASS input.
 """
 need(type(proposal) is dict and set(proposal)=={'expected','commit','status'} and proposal['status']=='PREPARED','PROPOSAL_FIELDS')
 expected=oid(proposal['expected']);target=oid(proposal['commit'])
 need(expected!='0'*40 and expected!=target,'NO_BOOTSTRAP_OR_DUPLICATE')
 need(channel.repository==policy.repository and channel.ref==policy.ref,'CHANNEL_SCOPE')
 need(local_git(client,'rev-parse','--is-bare-repository').strip()==b'true','BARE_CLIENT')
 need(not (Path(client)/'objects/info/alternates').exists(),'NO_ALTERNATE')
 need(local_git(client,'rev-list','--max-parents=0',target).decode().splitlines()==[policy.root],'UNTRUSTED_ROOT')
 need(local_git(client,'show','-s','--format=%P',target).decode().split()==[expected],'EXACT_EXPECTED_PARENT')
 before=channel.observe(client)
 if before['status']!='KNOWN':return {'status':'UNKNOWN','sends':0,'retry':False}
 if before['head']==target:return {'status':'CONFIRMED','sends':0,'retry':False}
 if before['head']!=expected:return {'status':'CONFLICT','sends':0,'retry':False}
 try:channel.send(client,target)
 except Exception:pass  # A lost response requires readback, never a second send.
 after=channel.observe(client)
 if after['status']!='KNOWN':status='UNKNOWN'
 elif after['head']==target:status='CONFIRMED'
 elif after['head']==expected:status='NOT_CONFIRMED'
 else:status='CONFLICT'
 return {'status':status,'sends':1,'retry':False}


class HTTPSWriter:
 """Existing RS4J Channel, pinned to an approved repository/ref/root policy."""
 def __init__(self,policy,secret):
  import sys
  sys.path.insert(0,str(Path(__file__).parent/'runtime'))
  import https_git as H
  H.G.REF=policy.ref;H.ANCHOR=policy.root
  url='https://github.com/'+policy.repository+'.git'
  ep=H.Endpoint(url,'data-write',policy.ref,policy.root)
  self.channel=H.Channel(ep,H.Identity('data-write',url,'x-access-token',secret))
  self.repository=policy.repository;self.ref=policy.ref
 def observe(self,client):return self.channel.observe(client)
 def send(self,client,target):
  return self.channel.run(client,'-c','push.followTags=false','-c','push.recurseSubmodules=no','push','--porcelain',self.channel.endpoint.url,target+':'+self.ref,check=False)
