#!/usr/bin/env python3
"""Regenerate all stage, runtime, comparison, and summary artifacts."""
import argparse,os,shutil,subprocess
from pathlib import Path
HERE=Path(__file__).resolve().parent
def run(*cmd,**kw):subprocess.run(cmd,check=True,text=True,**kw)
def main():
 p=argparse.ArgumentParser();p.add_argument('--backend-source',type=Path,required=True);p.add_argument('--output-dir',type=Path,required=True);p.add_argument('--trials',type=int,default=1);p.add_argument('--repetitions',type=int,default=3);p.add_argument('--seed',type=int,default=20260910);a=p.parse_args()
 dirty=subprocess.check_output(['git','-C',str(a.backend_source),'status','--porcelain'],text=True).strip();
 if dirty:raise SystemExit('backend source must be clean before reproduction')
 target=Path(os.environ.get('CARGO_TARGET_DIR',a.backend_source/'target'));run('cargo','build','-q','-p','data_plane','--bin','data_plane',cwd=a.backend_source);binary=target/'debug/data_plane'
 if a.output_dir.exists():shutil.rmtree(a.output_dir)
 a.output_dir.mkdir(parents=True);corpus=HERE/'corpus.json';run('python3',str(HERE/'validate_corpus_source.py'),'--snapshot',str(HERE/'evaluation_query_datasets.snapshot.yaml'),'--corpus',str(corpus),'--output',str(a.output_dir/'source-provenance.json'));front=a.output_dir/'frontend-planner.json';production=a.output_dir/'metricsql-production.json';stage=a.output_dir/'stage-pre-runtime.json'
 with front.open('w') as f:run('cargo','run','-q','-p','control_plane','--example','audit_multilang_corpus','--',str(corpus),cwd=a.backend_source,stdout=f)
 with production.open('w') as f:run('cargo','run','-q','-p','control_plane','--example','audit_metricsql_compile','--',str(corpus),cwd=a.backend_source,stdout=f)
 run('python3',str(HERE/'build_stage_report.py'),'--corpus',str(corpus),'--frontend',str(front),'--production',str(production),'--output',str(stage))
 run('python3',str(HERE/'run_fresh_fallback_trials.py'),'--trials',str(a.trials),'--repetitions',str(a.repetitions),'--seed',str(a.seed),'--output-dir',str(a.output_dir),'--binary',str(binary),'--backend-source',str(a.backend_source))
 for t in range(a.trials):
  raw=a.output_dir/f'trial-{t}-raw.json';run('python3',str(HERE/'compare_structured.py'),str(raw),'--output',str(a.output_dir/f'trial-{t}-comparisons.json'));run('python3',str(HERE/'summarize_latency.py'),str(raw),'--output',str(a.output_dir/f'trial-{t}-latency-summary.json'))
 run('python3',str(HERE/'finalize_stage_report.py'),'--stage',str(stage),'--raw',str(a.output_dir/'trial-0-raw.json'),'--output',str(a.output_dir/'stage-coverage.json'))
if __name__=='__main__':main()
