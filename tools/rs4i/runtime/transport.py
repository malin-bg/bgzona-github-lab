"""RS4E local file-protocol transport only; reuse RS4D publisher validation, no new persistence."""
from pathlib import Path
from tempfile import TemporaryDirectory
import importlib.util,json,sys
import git_publisher as G
P=G.P;ROOT=None;REMOTE=None;CACHE=None;REF=G.REF;ANCHOR='aa14561064c3ab9d314696b254d3ab2768110493';ZERO=G.ZERO

def configure(workspace,remote,cache):
 global ROOT,REMOTE,CACHE
 ROOT=Path(workspace).resolve();REMOTE=Path(remote).resolve();CACHE=Path(cache).resolve();G.ROOT=ROOT
 P.need(REMOTE.is_relative_to(ROOT) and CACHE.is_relative_to(ROOT),'LOCAL_POLICY')

def local(repo):
 q=Path(repo).resolve();P.need(q.is_relative_to(ROOT) and q!=ROOT,'ISOLATED_REPOSITORY_REQUIRED');return q

def run(repo,*args,input=None,check=True):
 return G.run(local(repo),'-c','protocol.allow=never','-c','protocol.file.allow=always',*args,input=input,check=check,env={'GIT_ALLOW_PROTOCOL':'file'})

def endpoint(value=None):
 value=str(REMOTE) if value is None else str(value)
 P.need('://' not in value and Path(value).resolve()==REMOTE,'UNAUTHORIZED_REMOTE');return value

def observe(client,destination=None):
 target=endpoint(destination);q=run(client,'ls-remote','--exit-code','--refs',target,REF,check=False)
 if q.returncode==2 and not q.stdout:return dict(status='KNOWN',head=ZERO)
 if q.returncode!=0:return dict(status='UNKNOWN',head=None)
 rows=q.stdout.decode().splitlines()
 if len(rows)!=1:return dict(status='UNKNOWN',head=None)
 h,name=rows[0].split();G.oid(h)
 return dict(status='KNOWN',head=h) if name==REF else dict(status='UNKNOWN',head=None)

def fetch_head(client):
 # FETCH_HEAD only; no force, tags, remote-tracking ref or unrelated namespace fetched.
 q=run(client,'fetch','--no-tags',endpoint(),REF,check=False)
 if q.returncode:return dict(status='UNKNOWN',head=None)
 fetched=run(client,'rev-parse','--verify','FETCH_HEAD').stdout.decode().strip();after=observe(client)
 if after['status']!='KNOWN' or after['head']!=fetched:return dict(status='UNKNOWN',head=None)
 roots=run(client,'rev-list','--max-parents=0',fetched).stdout.decode().splitlines();P.need(roots==[ANCHOR],'UNTRUSTED_ROOT')
 return dict(status='KNOWN',head=fetched)

def reconcile(client,commit,expected,simulate_unknown=False):
 G.oid(commit);G.oid(expected)
 seen=dict(status='UNKNOWN',head=None) if simulate_unknown else observe(client)
 if seen['status']!='KNOWN':return dict(status='UNKNOWN',remote_head=None,retry=False)
 if seen['head']==commit:return dict(status='CONFIRMED',remote_head=commit,retry=False)
 if seen['head']==expected:return dict(status='NOT_WRITTEN',remote_head=expected,retry=False)
 actual=fetch_head(client)
 if actual['status']!='KNOWN':return dict(status='UNKNOWN',remote_head=None,retry=False)
 if run(client,'merge-base','--is-ancestor',commit,actual['head'],check=False).returncode==0:return dict(status='CONFIRMED_SUPERSEDED',remote_head=actual['head'],retry=False)
 return dict(status='CONFLICT',remote_head=actual['head'],retry=False)

def push(client,commit,expected,destination=None,simulate=None):
 endpoint(destination);G.oid(commit);G.oid(expected);before=observe(client,destination)
 if before['status']!='KNOWN':return dict(status='UNKNOWN',push_calls=0,retry=False)
 if before['head']==commit:return dict(status='CONFIRMED',push_calls=0,retry=False)
 if before['head']!=expected:return dict(status='CONFLICT',push_calls=0,retry=False)
 roots=run(client,'rev-list','--max-parents=0',commit).stdout.decode().splitlines();P.need(roots==[ANCHOR],'UNTRUSTED_ROOT')
 if expected!=ZERO:P.need(run(client,'merge-base','--is-ancestor',expected,commit,check=False).returncode==0,'NON_FAST_FORWARD')
 with TemporaryDirectory(dir=ROOT,prefix='validate-send-') as t:G.checkout(client,commit,Path(t))
 if simulate=='before_send':return reconcile(client,commit,expected)|dict(push_calls=0)
 # One exact refspec; never --force, +refspec, --mirror, --all or follow-tags.
 q=run(client,'-c','push.followTags=false','-c','push.recurseSubmodules=no','push','--porcelain',endpoint(),commit+':'+REF,check=False)
 result=reconcile(client,commit,expected,simulate_unknown=simulate=='unknown_readback')
 return result|dict(push_calls=1,response='LOST_SIMULATED' if simulate in ('lost_response','unknown_readback') else 'EXIT_'+str(q.returncode))

def trusted_pin(commit):
 G.oid(commit);cache=CACHE
 if not cache.exists():G.init(cache)
 actual=fetch_head(cache);P.need(actual['status']=='KNOWN','CONSUMER_REMOTE_UNKNOWN')
 P.need(run(cache,'merge-base','--is-ancestor',ANCHOR,commit,check=False).returncode==0 and run(cache,'merge-base','--is-ancestor',commit,actual['head'],check=False).returncode==0,'UNAUTHORIZED_CONSUMER_PIN')
 return cache,commit,actual['head']
