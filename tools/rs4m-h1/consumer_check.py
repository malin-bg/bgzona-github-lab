"""Existing C1 trusted DEV reader, in a credential-free process. No deployment."""
from pathlib import Path
import json,sys
ROOT=Path(__file__).resolve().parent;work=Path(sys.argv[1]).resolve();commit=sys.argv[2]
sys.path[:0]=[str(ROOT/'consumer'),str(ROOT/'runtime'),sys.argv[3]]
import trusted_adapter as A
import transport as T
T.REF=T.G.REF='refs/heads/rs4m-synthetic-data';T.configure(work,work/'public.git',work/'consumer-dev.git');A.ROOT=work
value=A.read({'data_commit':commit,'selection':'CURRENT'});proof=value['proof']
assert proof['data_commit']==commit and proof['authority_root']=='564c405279c89e990c9345ccf7c54e2bf38c4470' and proof['snapshot']=='8e48b14dd9f6c7e9d26d07913b9b1386b63ccd12bfdb25647e777587cb1db117'
assert proof['ids']==1 and proof['revisions']==3 and len(proof['public_files'])==4
# Source-free native reader already validates registry/catalog/URLs/history/bytes; do not invent source bindings.
receipt={'status':'TRUSTED_DEV_READER_VERIFIED','data_commit':commit,'snapshot':proof['snapshot'],'authority_root':proof['authority_root'],'ids':proof['ids'],'revisions':proof['revisions'],'image_paths':len(proof['public_files']),'native_build':'NOT RUN','deployment':'NOT RUN'}
(work/'consumer-receipt').mkdir();(work/'consumer-receipt/receipt.json').write_text(json.dumps(receipt,indent=2)+'\n','utf8')
