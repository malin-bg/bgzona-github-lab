from pathlib import PurePosixPath
import re
RESERVED={'CON','PRN','AUX','NUL',*(f'COM{i}' for i in range(1,10)),*(f'LPT{i}' for i in range(1,10))}
class Rejected(ValueError):pass
def require(ok, message):
    if not ok:
        raise Rejected(message)
def safe_path(value):
    require(isinstance(value, str) and bool(value), 'UNSAFE_PATH')
    require(not any(ord(c) < 32 or ord(c) == 127 or c in '\\:<>"|?*' for c in value),
            'UNSAFE_PATH: ' + repr(value))
    for part in value.split('/'):
        require(part not in ('', '.', '..') and not part.endswith((' ', '.')) and
                part.split('.')[0].upper() not in RESERVED, 'UNSAFE_PATH: ' + repr(value))
    value.encode('utf8', 'strict')

def need(v,c):
 if not v:raise Rejected(c)
def image_path(path,ids):
 safe_path(path)
 m=re.fullmatch(r'(?:bgz-media/)?(kolaji|poslaniya)/(full|og)/(bgzona-([pm])-[0-9]{5})(?:-r[0-9]{4}-[a-f0-9]{12}|-[a-f0-9]{12})?(?:_hu_[a-f0-9]+)?\.(?:jpg|jpeg|png|gif|webp)',path)
 need(m is not None and m[3] in ids and m[4]=={'kolaji':'p','poslaniya':'m'}[m[1]],'PUBLIC_NAMESPACE')
 return path
