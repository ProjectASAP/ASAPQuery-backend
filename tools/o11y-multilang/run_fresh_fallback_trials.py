#!/usr/bin/env python3
"""Provision isolated exact engines and an ASAP proxy for repeatable trials."""
import argparse, json, os, shutil, subprocess, time, urllib.request
from pathlib import Path

def run(*cmd, **kw): return subprocess.run(cmd,check=True,text=True,**kw)
def wait(url):
 for _ in range(120):
  try:
   urllib.request.urlopen(url,timeout=1).read();return
  except Exception: time.sleep(.25)
 raise RuntimeError('service not ready: '+url)
def proc(pid):
 s=Path(f'/proc/{pid}/stat').read_text().split();return {'pid':pid,'cpu_ticks':int(s[13])+int(s[14]),'rss_pages':int(s[23])}
def cpid(name): return int(subprocess.check_output(['sudo','docker','inspect','-f','{{.State.Pid}}',name],text=True))
def main():
 p=argparse.ArgumentParser();p.add_argument('--trials',type=int,default=3);p.add_argument('--repetitions',type=int,default=5);p.add_argument('--output-dir',type=Path,required=True);p.add_argument('--binary',type=Path,required=True);p.add_argument('--physical-plan',type=Path,required=True);a=p.parse_args();a.output_dir.mkdir(parents=True,exist_ok=True)
 corpus=Path(__file__).with_name('corpus.json'); metrics=Path('/mydata/query-benefit-evaluation/o11y-with-evaluation-aliases.prom'); manifest=[]
 for t in range(a.trials):
  names={x:f'o11y-final-{x}-{t}' for x in ('prom','vm','ch')}; pp, vp, cp, apv, apc=19100+t,18500+t,18200+t,18600+t,18300+t
  tsdb=Path(f'/tmp/o11y-final-prom-{t}')
  if tsdb.exists(): shutil.rmtree(tsdb)
  shutil.copytree('/tmp/o11y-prom-tsdb-first',tsdb)
  (tsdb/'lock').unlink(missing_ok=True)
  try:
   run('sudo','docker','run','-d','--name',names['prom'],'--user',f'{os.getuid()}:{os.getgid()}','-p',f'{pp}:9090','-v','/tmp/o11y-prom.yml:/etc/prometheus/prometheus.yml:ro','-v',f'{tsdb}:/prometheus','prom/prometheus:v2.55.1','--config.file=/etc/prometheus/prometheus.yml','--storage.tsdb.path=/prometheus','--storage.tsdb.retention.time=100000h',stdout=subprocess.DEVNULL)
   run('sudo','docker','run','-d','--name',names['vm'],'-p',f'{vp}:8428','victoriametrics/victoria-metrics:v1.126.0',stdout=subprocess.DEVNULL)
   run('sudo','docker','run','-d','--name',names['ch'],'-p',f'{cp}:8123','--tmpfs','/var/lib/clickhouse:size=3g','-e','CLICKHOUSE_USER=bench','-e','CLICKHOUSE_PASSWORD=bench','clickhouse/clickhouse-server:latest',stdout=subprocess.DEVNULL)
   wait(f'http://127.0.0.1:{pp}/-/ready');wait(f'http://127.0.0.1:{vp}/health');
   for _ in range(120):
    try: urllib.request.urlopen(f'http://127.0.0.1:{cp}/ping?user=bench&password=bench',timeout=1);break
    except Exception:time.sleep(.25)
   run('python3',str(Path(__file__).with_name('load_openmetrics_clickhouse.py')),'--metrics',str(metrics),'--url',f'http://127.0.0.1:{cp}')
   run('curl','-fsS','-X','POST','--data-binary',f'@{metrics}',f'http://127.0.0.1:{vp}/api/v1/import/prometheus',stdout=subprocess.DEVNULL);run('curl','-fsS','-X','POST',f'http://127.0.0.1:{vp}/internal/force_flush',stdout=subprocess.DEVNULL)
   dp=subprocess.Popen([str(a.binary),'--profile','asapquery','--physical-plan',str(a.physical_plan),'--http-port',str(18700+t),'--victoriametrics-http-port',str(apv),'--victoriametrics-url',f'http://127.0.0.1:{vp}','--clickhouse-http-port',str(apc),'--clickhouse-url',f'http://127.0.0.1:{cp}','--clickhouse-user','bench','--clickhouse-password','bench','--forward-unsupported-queries','--prometheus-server',f'http://127.0.0.1:{pp}','--output-dir',f'/tmp/o11y-final-dp-{t}'],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
   wait(f'http://127.0.0.1:{apv}/api/v1/health');before={'dp':proc(dp.pid),**{k:proc(cpid(v)) for k,v in names.items()}};started=time.time()
   raw=a.output_dir/f'trial-{t}-raw.json';run('python3',str(Path(__file__).with_name('run_latency_trials.py')),'--corpus',str(corpus),'--repetitions',str(a.repetitions),'--prometheus',f'http://127.0.0.1:{pp}/api/v1/query','--victoriametrics',f'http://127.0.0.1:{vp}/api/v1/query','--clickhouse',f'http://127.0.0.1:{cp}/','--asap-metricsql',f'http://127.0.0.1:{apv}/api/v1/query','--asap-clickhouse',f'http://127.0.0.1:{apc}/','--output',str(raw),stdout=subprocess.DEVNULL)
   after={'dp':proc(dp.pid),**{k:proc(cpid(v)) for k,v in names.items()}};manifest.append({'trial':t,'raw':str(raw),'wall_seconds':time.time()-started,'before':before,'after':after})
  finally:
   if 'dp' in locals() and dp.poll() is None:dp.terminate();dp.wait(timeout=10)
   for n in names.values():subprocess.run(['sudo','docker','rm','-f',n],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
 images={image:subprocess.check_output(['sudo','docker','image','inspect','-f','{{.Id}}',image],text=True).strip() for image in ('prom/prometheus:v2.55.1','victoriametrics/victoria-metrics:v1.126.0','clickhouse/clickhouse-server:latest')}
 (a.output_dir/'manifest.json').write_text(json.dumps({'trials':manifest,'repetitions':a.repetitions,'dataset_sha256':subprocess.check_output(['sha256sum',str(metrics)],text=True).split()[0],'binary_git_head':'abe19211','images':images},indent=2)+'\n')
if __name__=='__main__':main()
