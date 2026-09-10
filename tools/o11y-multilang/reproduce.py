#!/usr/bin/env python3
"""Regenerate frontend, terminal-stage, fresh-runtime, and comparison artifacts."""
import argparse,os,subprocess
from pathlib import Path
HERE=Path(__file__).resolve().parent
def run(*cmd,**kw):subprocess.run(cmd,check=True,text=True,**kw)
def main():
 p=argparse.ArgumentParser();p.add_argument('--backend-source',type=Path,required=True);p.add_argument('--binary',type=Path,required=True);p.add_argument('--binary-source',type=Path,required=True);p.add_argument('--output-dir',type=Path,required=True);p.add_argument('--trials',type=int,default=1);p.add_argument('--repetitions',type=int,default=3);p.add_argument('--seed',type=int,default=20260910);a=p.parse_args();a.output_dir.mkdir(parents=True,exist_ok=True);corpus=HERE/'corpus.json'
 with (HERE/'frontend-planner-coverage.json').open('w') as f:run('cargo','run','-q','-p','control_plane','--example','audit_multilang_corpus','--',str(corpus),cwd=a.backend_source,stdout=f)
 with (HERE/'metricsql-production-compile.json').open('w') as f:run('cargo','run','-q','-p','control_plane','--example','audit_metricsql_compile','--',str(corpus),cwd=a.backend_source,stdout=f)
 run('python3',str(HERE/'build_stage_report.py'));run('python3',str(HERE/'run_fresh_fallback_trials.py'),'--trials',str(a.trials),'--repetitions',str(a.repetitions),'--seed',str(a.seed),'--output-dir',str(a.output_dir),'--binary',str(a.binary),'--backend-source',str(a.backend_source),'--binary-source',str(a.binary_source))
 for t in range(a.trials):
  run('python3',str(HERE/'compare_structured.py'),str(a.output_dir/f'trial-{t}-raw.json'),'--output',str(a.output_dir/f'trial-{t}-comparisons.json'))
  run('python3',str(HERE/'summarize_latency.py'),str(a.output_dir/f'trial-{t}-raw.json'),'--output',str(a.output_dir/f'trial-{t}-latency-summary.json'))
 run('python3',str(HERE/'finalize_stage_report.py'),'--stage',str(HERE/'stage-coverage.json'),'--raw',str(a.output_dir/'trial-0-raw.json'))
if __name__=='__main__':main()
