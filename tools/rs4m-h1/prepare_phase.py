"""Credential-free process entrypoint for the existing split P4 preparation.
Inputs are acquired LOCAL bare repositories, not URLs or workflow PASS assertions.
Does not issue a token, fetch HTTPS, push or dispatch.
"""
from pathlib import Path
import argparse,json,os,re,subprocess,sys
D=Path(__file__).resolve().parent

def main():
 ap=argparse.ArgumentParser();ap.add_argument('phase',choices=['private','public']);ap.add_argument('--workspace',required=True);ap.add_argument('--isolated-child',action='store_true');ap.add_argument('--source-ref',choices=['refs/heads/synthetic-source','refs/heads/cp2b-private-standard-source-r1'],default='refs/heads/synthetic-source');ap.add_argument('--deps',required=True);ap.add_argument('--producer-id',type=int);a=ap.parse_args()
 work=Path(a.workspace).resolve()
 if not a.isolated_child:
  # No GitHub file commands/runtime tokens/auth/proxy/trace config reach source/media code.
  env={k:v for k,v in os.environ.items() if k.upper() in ('PATH','SYSTEMROOT','WINDIR','COMSPEC','TEMP','TMP','PATHEXT')}
  env.update(PYTHONDONTWRITEBYTECODE='1',GIT_CONFIG_GLOBAL=os.devnull,GIT_CONFIG_SYSTEM=os.devnull,GIT_CONFIG_NOSYSTEM='1',GIT_ALLOW_PROTOCOL='file',GIT_TERMINAL_PROMPT='0')
  q=subprocess.run([sys.executable,'-I','-B',str(Path(__file__).resolve()),a.phase,'--workspace',str(work),'--isolated-child','--source-ref',a.source_ref,'--deps',a.deps]+(['--producer-id',str(a.producer_id)] if a.producer_id else []),env=env,capture_output=True)
  # Failure output might contain source-derived paths. Export only fixed categories.
  if q.returncode:
   marker=re.fullmatch(rb'PREPARATION_STOP:[A-Z0-9_]+',q.stdout.strip())
   print(marker[0].decode('ascii') if marker else 'PREPARATION_STOP');return 2
  print('PREPARATION_COMPLETE');return 0
 sys.path[:0]=[str(D),str(Path(a.deps).resolve())]
 import boundary as B
 G=B.G;P=B.P;G.ROOT=work;G.REF,B.PUBLIC_REF={
  'refs/heads/synthetic-source':('refs/heads/rs4m-synthetic-state','refs/heads/rs4m-synthetic-data'),
  'refs/heads/cp2b-private-standard-source-r1':('refs/heads/bot-publisher-state','refs/heads/publisher-data'),
 }[a.source_ref]
 B.OWNER_REF=a.source_ref
 B.preparation_context()
 P.need(work.is_dir() and (work/'private.git').is_dir() and (work/'public.git').is_dir(),'ACQUIRED_REPOSITORIES_REQUIRED')
 if a.phase=='private':
  try:
   result=B.prepare(work/'private.git',work/'public.git',work,checkpoint=False,producer_id=a.producer_id)
  except P.Stop as exc:
   reason=str(exc)
   print('PREPARATION_STOP:'+reason if re.fullmatch('[A-Z0-9_]+',reason) else 'PREPARATION_STOP')
   return 2
  client=work/'private.git'
 else:
  result=B.export_candidate(work/'private.git',work/'public.git',work)
  client=Path(result.get('client',work/'public.git'))
 out=work/(a.phase+'-handoff');P.need(not out.exists(),'HANDOFF_EXISTS_NO_OVERWRITE');out.mkdir()
 if result['status'] in ('NO_CHANGE','CONFIRMED'):
  (out/'result.json').write_bytes(P.data({'status':result['status'],'writes':0}));return 0
 proposal=result['proposal'];commit=G.oid(proposal['commit']);ref='refs/heads/s1-handoff-candidate'
 P.need(B.head(client,ref)==G.ZERO,'CANDIDATE_REF_EXISTS');G.run(client,'update-ref',ref,commit,G.ZERO)
 G.run(client,'bundle','create',str(out/'candidate.bundle'),ref)
 (out/'proposal.json').write_bytes(P.data(proposal))
 # PRIVATE artifact must remain in PRIVATE. Public handoff has only public objects/proposal.
 (out/'result.json').write_bytes(P.data({'status':result['status'],'artifact_visibility':'PRIVATE' if a.phase=='private' else 'PUBLIC_SAFE','writes':0}))
 return 0
if __name__=='__main__':
 try:sys.exit(main())
 except Exception:print('PREPARATION_STOP');sys.exit(2)
