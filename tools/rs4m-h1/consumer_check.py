"""Existing C1 trusted DEV reader, in a credential-free process. No deployment."""
from pathlib import Path
import json,sys
ROOT=Path(__file__).resolve().parent;work=Path(sys.argv[1]).resolve();commit=sys.argv[2]
sys.path[:0]=[str(ROOT/'consumer'),str(ROOT/'runtime'),sys.argv[3]]
import trusted_adapter as A
import transport as T
reference=sys.argv[4] if len(sys.argv)==5 else 'refs/heads/rs4m-synthetic-data'
if reference=='refs/heads/publisher-data':
 T.ANCHOR='05cfdeb41ca116dc07614906fbbdee9b3a597745'
 A.SELECTIONS={'PUBLIC12':'317a99d4d0f13416e2a001b94352870d95f63cc4a057b6957ebaf22a68edc55f',
               'PENDING13':'f61fc13e43557f75df88eed7f39e4ab661c5097f60f8d3f9131fab22d9b36f81'}
elif reference!='refs/heads/rs4m-synthetic-data':
 raise ValueError('PUBLIC_CONSUMER_REF')
T.REF=T.G.REF=reference;T.configure(work,work/'public.git',work/'consumer-dev.git');A.ROOT=work
value=A.read({'data_commit':commit,'selection':'CURRENT'});proof=value['proof']
state=A.strict(A.git('show',commit+':state.json'))
assert proof['data_commit']==commit and proof['authority_root']==T.ANCHOR and proof['trusted_ref']==reference and proof['observed_remote_head']==commit and proof['snapshot']==state['snapshot']
# Source-free native reader already validates registry/catalog/URLs/history/bytes; do not invent source bindings.
receipt={'status':'TRUSTED_DEV_READER_VERIFIED','data_commit':commit,'snapshot':proof['snapshot'],'authority_root':proof['authority_root'],'ids':proof['ids'],'revisions':proof['revisions'],'image_paths':len(proof['public_files']),'native_build':'NOT RUN','deployment':'NOT RUN'}
(work/'consumer-receipt').mkdir();(work/'consumer-receipt/receipt.json').write_text(json.dumps(receipt,indent=2)+'\n','utf8')
