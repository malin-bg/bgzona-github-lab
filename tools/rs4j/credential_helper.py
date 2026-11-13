"""One-process exact HTTPS credential context. Never stores credentials."""
import os,sys
from urllib.parse import urlsplit

def main():
 if len(sys.argv)!=2 or sys.argv[1]!='get':return
 fields={}
 for line in sys.stdin:
  line=line.rstrip('\n')
  if not line:break
  if '=' not in line:return
  key,value=line.split('=',1)
  # Git protocol arrays are informational; never credential scope fields.
  if key in ('capability[]','wwwauth[]','state[]'):continue
  if key in fields:return
  fields[key]=value
 url=urlsplit(os.environ.get('BGZONA_AUTH_URL',''))
 ok=(fields.get('protocol')=='https' and fields.get('host')==url.netloc and fields.get('path')==url.path.lstrip('/') and fields.get('username',os.environ.get('BGZONA_AUTH_USER'))==os.environ.get('BGZONA_AUTH_USER'))
 if not ok:print('quit=true\n');return
 user=os.environ.get('BGZONA_AUTH_USER','');secret=os.environ.get('BGZONA_AUTH_SECRET','')
 if not user or not secret:print('quit=true\n');return
 print('username='+user+'\npassword='+secret+'\n')
if __name__=='__main__':main()
