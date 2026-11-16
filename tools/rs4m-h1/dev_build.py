"""Call unchanged H2R native DEV delivery under the existing protected Lab authority.
This is a Lab-hosted site build, never a DEV workflow dispatch or deployment.
"""
from pathlib import Path
import json,os,subprocess,sys

def native(policy,site,public_git,data_commit,out,hugo,deps):
 site=Path(site).resolve();public_git=Path(public_git).resolve();out=Path(out).resolve()
 def git(repo,*args):
  q=subprocess.run(['git','--no-optional-locks','-C',str(repo),*args],capture_output=True)
  if q.returncode:raise ValueError('DEV_CODE_OR_DATA_UNKNOWN')
  return q.stdout.decode().strip()
 if git(site,'rev-parse','HEAD')!=policy['owner_flow']['DEV_code_commit']:raise ValueError('DEV_CODE_PIN')
 if git(public_git,'rev-parse',policy['data_ref'])!=data_commit:raise ValueError('PUBLIC_HEAD_MOVED')
 state=json.loads(git(public_git,'show',data_commit+':state.json'));sid=state['snapshot']
 package=site/'tools/media_snapshot/git_snapshot';root=Path(os.path.commonpath([site,public_git,out]));
 if root==Path(root.anchor):raise ValueError('ISOLATED_WORKSPACE_REQUIRED')
 rel=lambda p:os.path.relpath(p,package).replace('\\','/')
 config={'schema':1,'channel':'dev','endpoint':rel(public_git),'workspace':rel(root),'output_workspace':rel(out.parent),'ref':policy['data_ref'],'root':policy['data_root']}
 (package/'site-input-policy.json').write_text(json.dumps(config,indent=2)+'\n','utf8')
 request=out.parent/'dev-request.json';request.write_text(json.dumps({'data_commit':data_commit,'selection':'CURRENT','snapshot':sid})+'\n','utf8')
 env={k:v for k,v in os.environ.items() if k.upper() in ('PATH','SYSTEMROOT','WINDIR','TEMP','TMP','COMSPEC','PATHEXT')}
 env.update(PYTHONDONTWRITEBYTECODE='1',PYTHONPATH=str(Path(deps).resolve()),GIT_CONFIG_NOSYSTEM='1',GIT_CONFIG_GLOBAL=os.devnull,GIT_CONFIG_SYSTEM=os.devnull,GIT_ALLOW_PROTOCOL='file',GIT_TERMINAL_PROMPT='0')
 q=subprocess.run([sys.executable,'-B',str(site/'tools/media_snapshot/snapshot_delivery.py'),'--site',str(site),'--request',str(request),'--work',str(out),'--hugo',str(Path(hugo).resolve())],env=env,capture_output=True)
 if q.returncode:raise ValueError('NATIVE_DEV_BUILD_ACCEPTANCE_SEAL_STOP')
 receipt=json.loads((out/'receipt.json').read_bytes())
 if receipt['data_revision']!=data_commit or receipt['snapshot']!=sid or receipt['deployed'] is not False:raise ValueError('DEV_RECEIPT_BINDING')
 return receipt
