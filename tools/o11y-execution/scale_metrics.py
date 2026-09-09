#!/usr/bin/env python3
"""Deterministically scale the versioned o11y-bench metric trace for systems tests."""
import argparse, hashlib, json, re
from collections import defaultdict
from pathlib import Path

SAMPLE=re.compile(r'([a-zA-Z_:][a-zA-Z0-9_:]*)(\{.*\})?\s+(\S+)\s+(\d+(?:\.\d+)?)$')
COUNTER_SUFFIXES=("_total","_sum","_count","_bucket")

def add_replica(lbls, replica):
    tag=f'replica="r{replica:03d}"'
    return "{"+tag+(","+lbls[1:-1] if lbls else "")+"}"

def main():
    ap=argparse.ArgumentParser(); ap.add_argument('--input',type=Path,required=True); ap.add_argument('--output',type=Path,required=True)
    ap.add_argument('--interval-seconds',type=int,default=30); ap.add_argument('--days',type=int,default=1); ap.add_argument('--replicas',type=int,default=1); a=ap.parse_args()
    headers=[]; groups=defaultdict(list)
    for line in a.input.read_text().splitlines():
        if line.startswith('#'):
            if line != '# EOF': headers.append(line)
            continue
        m=SAMPLE.fullmatch(line); metric,lbls,value,ts=m.groups(); groups[int(float(ts)*1000)].append((metric,lbls or '',float(value)))
    times=sorted(groups); source_step=times[1]-times[0]; target_step=a.interval_seconds*1000
    if source_step%target_step: raise SystemExit('target interval must divide source interval')
    first,last=times[0],times[-1]; span=last-first+source_step
    by_key_first={}; by_key_last={}
    for metric,lbls,value in groups[first]: by_key_first[(metric,lbls)]=value
    for metric,lbls,value in groups[last]: by_key_last[(metric,lbls)]=value
    a.output.parent.mkdir(parents=True,exist_ok=True)
    rows=0
    with a.output.open('w',buffering=1024*1024) as out:
        out.write('\n'.join(headers)+'\n')
        for day in range(a.days):
            day_offset=(day - (a.days - 1))*span
            for i,t in enumerate(times):
                nxt=times[i+1] if i+1<len(times) else None
                next_map={(m,l):v for m,l,v in groups[nxt]} if nxt is not None else {}
                for sub in range(0,source_step,target_step):
                    if nxt is None and sub: continue
                    frac=sub/source_step
                    out_t=t+day_offset+sub
                    for metric,lbls,value in groups[t]:
                        key=(metric,lbls); v=value
                        if nxt is not None and key in next_map: v=value+(next_map[key]-value)*frac
                        if metric.endswith(COUNTER_SUFFIXES):
                            v += day*(by_key_last.get(key,value)-by_key_first.get(key,value))
                        for replica in range(a.replicas):
                            out.write(f'{metric}{add_replica(lbls,replica) if a.replicas>1 else lbls} {v:.12g} {out_t/1000:.3f}\n')
                            rows+=1
        out.write('# EOF\n')
    meta={'source':str(a.input),'source_sha256':hashlib.sha256(a.input.read_bytes()).hexdigest(),'output':str(a.output),'output_sha256':hashlib.sha256(a.output.read_bytes()).hexdigest(),'interval_seconds':a.interval_seconds,'days':a.days,'replicas':a.replicas,'samples':rows,'start_ms':first-(a.days-1)*span,'end_ms':last}
    a.output.with_suffix('.metadata.json').write_text(json.dumps(meta,indent=2)+'\n')
    print(json.dumps(meta))
if __name__=='__main__': main()
