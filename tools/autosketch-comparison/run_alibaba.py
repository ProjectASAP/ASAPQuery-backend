#!/usr/bin/env python3
"""Checkpoint real Alibaba experiments and publish only validated measurements.

Run in the dedicated evaluation worktree. Publication is opt-in, requires the
full real-data geometry, and stops on failed commands or changed source state.
No test/fixture results are eligible for publication.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import shutil
import subprocess
import sys
import time

METHODS=['exact-pane','exact-scan','analytical','auto','erp-no-sharing','erp']
WORKLOADS=['service','edge','latency']


def digest(path):
    sha=hashlib.sha256()
    with Path(path).open('rb') as source:
        for block in iter(lambda:source.read(8*1024**2),b''):
            sha.update(block)
    return sha.hexdigest()


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--directory',type=Path,required=True)
    parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--calibration-files',type=int,default=120)
    parser.add_argument('--total-files',type=int,default=240)
    parser.add_argument('--calibration-events',type=int,default=10_000_000)
    parser.add_argument('--trials',type=int,default=3)
    parser.add_argument('--profile-trials',type=int,default=3)
    parser.add_argument('--stage-timeout-seconds',type=int,default=21600)
    parser.add_argument('--data-wait-timeout-seconds',type=int,default=43200)
    parser.add_argument('--publish-pr',action='store_true')
    args=parser.parse_args()
    root=Path.cwd()
    binary=(root/'target/release/examples/alibaba_dashboard_comparison').resolve()
    args.directory=args.directory.resolve();args.output=args.output.resolve()
    args.output.mkdir(parents=True,exist_ok=True)
    progress=args.output/'progress.json'
    branch=subprocess.check_output(['git','branch','--show-current'],text=True).strip()
    revision=subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip()
    if args.publish_pr:
        if branch!='eval/alibaba-dashboard-observations' or (args.calibration_files,args.total_files,args.trials,args.profile_trials)!=(120,240,3,3):
            raise ValueError('publication requires the preregistered real-data branch and full geometry')
        if subprocess.check_output(['git','diff','--name-only'],text=True).strip():
            raise ValueError('commit source changes before starting a publishing run')
        if subprocess.check_output(['git','diff','--cached','--name-only'],text=True).strip():
            raise ValueError('index must be empty before a publishing run')
    provenance={'schema_version':1,'backend_revision':revision,'binary_sha256':digest(binary),
                'sketchlib_revision':subprocess.check_output(['git','-C','../asap_sketchlib','rev-parse','HEAD'],text=True).strip(),
                'planner_revision':'a9651cc','platform':platform.platform(),
                'affinity':sorted(os.sched_getaffinity(0)),
                'arguments':{k:str(v) if isinstance(v,Path) else v for k,v in vars(args).items()},
                'method_order':METHODS,'workload_order':WORKLOADS,'commands':[],
                'timing_scope':'single-process wall and process CPU primitive regions on a shared host; offline oracle IO excluded',
                'trial_scope':'three timing repetitions of one unchanged trace, not independent datasets'}
    manifest=args.output/'manifest.json'
    if manifest.exists():
        old=json.loads(manifest.read_text())
        if any(old[k]!=provenance[k] for k in ['backend_revision','binary_sha256','arguments']):
            raise ValueError('refusing to mix measurements from different source/binary/arguments')
        provenance=old

    def status(stage,**extra):
        progress.write_text(json.dumps({'stage':stage,'time_utc':time.strftime('%Y-%m-%dT%H:%M:%SZ',time.gmtime()),'pid':os.getpid(),**extra},indent=2)+'\n')
        print(json.dumps({'stage':stage,**extra}),flush=True)

    def save_manifest():
        manifest.write_text(json.dumps(provenance,indent=2)+'\n')

    common=[str(binary),'--directory',str(args.directory),'--calibration-files',str(args.calibration_files),
            '--total-files',str(args.total_files),'--calibration-events',str(args.calibration_events),
            '--profile-trials',str(args.profile_trials),'--memory-budget-bytes','16000000000']

    def run(path,extra):
        if path.exists():
            if path.suffix=='.json' and json.loads(path.read_text()).get('status')=='failed':
                raise ValueError(f'previous failure needs diagnosis, not silent reuse: {path}')
            return
        if digest(binary)!=provenance['binary_sha256']:
            raise ValueError('executable changed during evaluation')
        if shutil.disk_usage(args.directory).free < 8*1024**3:
            raise RuntimeError('less than 8 GiB free before experiment stage; preserve data and stop')
        command=[*common,'--output',str(path),*extra]
        provenance['commands'].append(command);save_manifest();status('running',artifact=path.name)
        with (args.output/(path.name+'.log')).open('w') as log:
            subprocess.run(command,stdout=log,stderr=subprocess.STDOUT,check=True,timeout=args.stage_timeout_seconds)

    try:
        save_manifest()
        data_deadline=time.monotonic()+args.data_wait_timeout_seconds
        # Readiness is per-file atomic metadata, never a partially written gzip.
        while not all((args.directory/f'observations_{i}.json').exists() for i in range(args.calibration_files)):
            if time.monotonic()>data_deadline:
                raise TimeoutError('calibration data readiness deadline exceeded')
            count=sum((args.directory/f'observations_{i}.json').exists() for i in range(args.total_files))
            status('waiting_for_calibration_data',prepared_files=count,required_files=args.total_files)
            time.sleep(30)
        calibration=args.directory/f'calibration-{args.calibration_files}-{args.calibration_events}-seed42.bin'
        run(calibration,['--stage','calibration','--seed','42'])
        provenance['calibration']=json.loads(calibration.with_suffix('.metadata.json').read_text())
        metadata=provenance['calibration']
        if (metadata['calibration_files'],metadata['sample_events'],metadata['seed'],metadata['held_out_used'])!=(args.calibration_files,args.calibration_events,42,False):
            raise ValueError('calibration metadata does not match requested geometry')
        provenance['calibration_sha256']=digest(calibration);save_manifest()
        for workload in WORKLOADS:
            run(args.output/f'{workload}-catalog.json',['--stage','profile','--workload',workload,'--calibration',str(calibration),'--seed','42'])
        while not all((args.directory/f'observations_{i}.json').exists() for i in range(args.total_files)):
            if time.monotonic()>data_deadline:
                raise TimeoutError('held-out data readiness deadline exceeded')
            status('waiting_for_held_out_data',prepared_files=sum((args.directory/f'observations_{i}.json').exists() for i in range(args.total_files)))
            time.sleep(30)
        data=[json.loads((args.directory/f'observations_{i}.json').read_text()) for i in range(args.total_files)]
        if any(r['index']!=i or not (args.directory/f'observations_{i}.bin.gz').exists() for i,r in enumerate(data)):
            raise ValueError('noncontiguous or missing real-data replay')
        (args.output/'dataset-manifest.json').write_text(json.dumps({'files':data,'events':sum(r['events'] for r in data)},indent=2)+'\n')
        # Runtime timing starts only after data projection is complete.
        for workload in WORKLOADS:
            oracle=args.directory/f'oracle-{revision[:12]}-{workload}-{args.calibration_files}-{args.total_files}'
            catalog=args.output/f'{workload}-catalog.json'
            for trial in range(args.trials):
                for method in METHODS:
                    plan=args.output/f'{workload}-{method}-trial{trial}-plan.json'
                    run(plan,['--stage','plan','--workload',workload,'--method',method,'--calibration',str(calibration),'--catalog',str(catalog),'--seed',str(42+trial)])
                    result=args.output/f'{workload}-{method}-trial{trial}-run.json'
                    extra=['--stage','run','--workload',workload,'--method',method,'--deployment',str(plan),'--oracle-directory',str(oracle)]
                    if method=='exact-pane' and trial==0:
                        extra.append('--write-oracle')
                    run(result,extra)
        save_manifest();status('validating_results')
        subprocess.run([sys.executable,'tools/autosketch-comparison/summarize_alibaba.py',str(args.output)],check=True)
        if args.publish_pr:
            status('publishing_results')
            if subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip()!=revision:
                raise ValueError('HEAD changed; refusing unattended commit/publication')
            if subprocess.check_output(['git','diff','HEAD','--name-only'],text=True).strip():
                raise ValueError('tracked files changed; refusing unattended publication')
            selected=[str(p.relative_to(root)) for p in args.output.iterdir() if p.suffix in ['.json','.csv','.md','.svg','.png'] and p.name!='progress.json']
            subprocess.run(['git','add','--',*selected],check=True)
            subprocess.run(['git','diff','--cached','--check'],check=True)
            subprocess.run(['git','commit','-m','eval: report full Alibaba dashboard measurements'],check=True)
            subprocess.run(['git','push','-u','origin',branch],check=True)
            body=args.output/'pr-body.md'
            subprocess.run(['gh','pr','create','--base','eval/topk-dashboard-autosketch','--head',branch,
                            '--title','eval: compare AutoSketch and measured ERP on Alibaba dashboards',
                            '--body-file',str(body)],check=True)
        status('complete',published=args.publish_pr)
    except BaseException as error:
        status('failed',error=repr(error),note='No complete-results PR was published; preserve artifacts and diagnose before resuming.')
        raise


if __name__=='__main__':
    main()
