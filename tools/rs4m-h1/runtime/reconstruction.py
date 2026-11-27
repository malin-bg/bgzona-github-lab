"""Rebuild ephemeral comparisons from immutable PRIVATE Git; no private state persistence."""
import publisher as P

def comparison(row,item):
 files=row['files'];requests=P.request_files(row)
 ordinary=dict(row,files={p:raw for p,raw in files.items() if p not in requests})
 hashes={p:P.digest(raw) for p,raw in sorted(files.items())}
 parts=P.request_parts(item,requests)
 image,og=None,None
 if ordinary['files']:
  image,_,og,_=P.bundle(ordinary)
 return dict(item=item,unit=P.digest(P.data(hashes)),managed_image=hashes.get(image),
             replacement=hashes[parts['replacement'][0]] if 'replacement' in parts else None,
             og_input=hashes.get(og),files=hashes)

def rebuild(source,state,registry):
 P.need('source_inputs' not in state,'PRIVATE_STATE_IN_PUBLIC_INPUT');inputs={};bindings={};groups={}
 for b in state['source_bindings']:
  P.need(set(b)=={'source','slot','item'} and type(b['slot']) is int and b['slot']>=0 and b['item'] in registry['items'],'BINDING_SCHEMA')
  P.need(P.re.fullmatch('[a-f0-9]{40}',b['source']) is not None,'BINDING_SOURCE');groups.setdefault(b['source'],[]).append(b)
 for commit,group in groups.items():
  P.git(source,'merge-base','--is-ancestor',commit,state['source_cursor']);rows=list(P.units(P.source_files(source,commit),registry).items())
  for binding in group:
   P.need(binding['slot']<len(rows),'BINDING_SLOT');unit,row=rows[binding['slot']]
   P.need(unit not in bindings and (row['id'] is None or row['id']==binding['item']) and row['collection']==registry['items'][binding['item']]['collection'],'AMBIGUOUS_BINDING')
   if row['id'] is None:
    replacement=dict(rows).get(binding['item'])
    if replacement and all(path.startswith('incoming/replace/') for path in replacement['files']):
     row=dict(row,files=row['files']|replacement['files'])
   inputs[unit]=comparison(row,binding['item']);bindings[unit]=dict(binding)
  del rows  # Retain only comparisons, not copies of original bytes for every historical commit.
 return inputs,bindings
