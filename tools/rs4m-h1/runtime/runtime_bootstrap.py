"""Dependency discovery only; no installation, network, system or environment changes."""
from pathlib import Path,PurePosixPath
from hashlib import sha256
import json,sys
ROOT=Path(__file__).resolve().parent

def load():
 d=json.loads((ROOT/'dependencies.json').read_bytes())
 if sys.version_info[:2]<tuple(d['minimum_python']):raise RuntimeError('PYTHON_VERSION')
 for row in d['worker'].values():
  p=PurePosixPath(row['path'])
  if p.is_absolute() or '..' in p.parts or '\\' in row['path']:raise RuntimeError('DEPENDENCY_PATH')
  actual=ROOT.joinpath(*p.parts)
  if actual.is_symlink() or not actual.resolve().is_relative_to(ROOT) or sha256(actual.read_bytes()).hexdigest()!=row['sha256']:raise RuntimeError('DEPENDENCY_CHANGED')
 sys.path.insert(0,str(ROOT/'workers'))
 try:
  from PIL import __version__,Image,ImageCms,features
 except ImportError:raise RuntimeError('PILLOW_UNAVAILABLE') from None
 if __version__!=d['pillow_version']:raise RuntimeError('PILLOW_VERSION')
 if not features.check('littlecms2'):raise RuntimeError('IMAGECMS_UNAVAILABLE')
 return d
