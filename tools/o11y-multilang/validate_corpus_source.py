#!/usr/bin/env python3
import argparse,hashlib,json
from pathlib import Path
p=argparse.ArgumentParser();p.add_argument('--snapshot',type=Path,required=True);p.add_argument('--corpus',type=Path,required=True);p.add_argument('--output',type=Path,required=True);a=p.parse_args();source=[x.strip() for x in a.snapshot.read_text().splitlines() if x.strip()];c=json.loads(a.corpus.read_text());queries=c['queries']
if len(source)!=len(queries):raise SystemExit(f'occurrence count mismatch: {len(source)} != {len(queries)}')
for i,(line,q) in enumerate(zip(source,queries)):
 expected=q['promql']
 if line!=expected:raise SystemExit(f'occurrence {i} mismatch')
out={'snapshot':a.snapshot.name,'source_sha256':hashlib.sha256(a.snapshot.read_bytes()).hexdigest(),'occurrences':[{'occurrence_index':i,'id':q['id'],'expression':q['promql']} for i,q in enumerate(queries)]};a.output.write_text(json.dumps(out,indent=2)+'\n')
