#!/usr/bin/env python3
"""One-command, isolated reproduction of the full-corpus fallback experiment."""
import argparse,hashlib,json,os,shutil,subprocess,time,urllib.request
from pathlib import Path
HERE=Path(__file__).resolve().parent

def run(*cmd,**kw): return subprocess.run(cmd,check=True,text=True,**kw)
def wait(url):
 for _ in range(120):
  try: urllib.request.urlopen(url,timeout=1).read();return
  except Exception:time.sleep(.25)
 raise RuntimeError('not ready: '+url)
def pid(name):return int(subprocess.check_output(['sudo','docker','inspect','-f','{{.State.Pid}}',name],text=True))
def process(pid):
 s=Path(f'/proc/{pid}/stat').read_text().split();status=dict(x.split(':',1) for x in Path(f'/proc/{pid}/status').read_text().splitlines() if ':' in x)
 return {'pid':pid,'cpu_ticks':int(s[13])+int(s[14]),'rss_pages':int(s[23]),'vm_hwm':status.get('VmHWM','').strip()}
def size(path):return sum(x.stat().st_size for x in Path(path).rglob('*') if x.is_file())
def container_size(name):return int(json.loads(subprocess.check_output(['sudo','docker','inspect','--size',name],text=True))[0].get('SizeRw') or 0)
def snap(dp,names,paths):return {'processes':{'dp':process(dp.pid),**{k:process(pid(v)) for k,v in names.items()}},'storage_bytes':{**{k:size(v) for k,v in paths.items()},**{k:container_size(v) for k,v in names.items() if k not in paths}}}
def main():
 p=argparse.ArgumentParser();p.add_argument('--trials',type=int,default=1);p.add_argument('--repetitions',type=int,default=3);p.add_argument('--seed',type=int,default=20260910);p.add_argument('--output-dir',type=Path,required=True);p.add_argument('--binary',type=Path,required=True);p.add_argument('--backend-source',type=Path,required=True);p.add_argument('--binary-source',type=Path,required=True);a=p.parse_args();a.output_dir=a.output_dir.resolve();a.output_dir.mkdir(parents=True,exist_ok=True)
 fixture=a.output_dir/'fixture.prom';run('python3',str(HERE/'generate_fixture.py'),'--output',str(fixture));config=a.output_dir/'prometheus.yml';config.write_text('global:\n  scrape_interval: 1h\nscrape_configs: []\n')
 plan=a.output_dir/'physical-plan.json'
 with plan.open('w') as f:run('cargo','run','-q','-p','control_plane','--example','emit_benchmark_physical_plan',cwd=a.backend_source,stdout=f)
 manifest=[]
 for t in range(a.trials):
  root=a.output_dir/f'trial-{t}-state'
  if root.exists():shutil.rmtree(root)
  (root/'prom').mkdir(parents=True);(root/'dp').mkdir();names={x:f'o11y-repro-{x}-{os.getpid()}-{t}' for x in ('prom','vm','ch')};pp,vp,cp,apv,apc=19100+t,18500+t,18200+t,18600+t,18300+t
  run('sudo','docker','run','--rm','--user',f'{os.getuid()}:{os.getgid()}','-v',f'{a.output_dir}:/input:ro','-v',f'{root}/prom:/output','--entrypoint','promtool','prom/prometheus:v2.55.1','tsdb','create-blocks-from','openmetrics','/input/fixture.prom','/output',stdout=subprocess.DEVNULL)
  phase={};started=time.monotonic();dp=None
  try:
   run('sudo','docker','run','-d','--name',names['prom'],'--user',f'{os.getuid()}:{os.getgid()}','-p',f'{pp}:9090','-v',f'{config}:/etc/prometheus/prometheus.yml:ro','-v',f'{root}/prom:/prometheus','prom/prometheus:v2.55.1','--config.file=/etc/prometheus/prometheus.yml','--storage.tsdb.path=/prometheus',stdout=subprocess.DEVNULL)
   run('sudo','docker','run','-d','--name',names['vm'],'-p',f'{vp}:8428','victoriametrics/victoria-metrics:v1.126.0','-storageDataPath=/vm-data',stdout=subprocess.DEVNULL)
   run('sudo','docker','run','-d','--name',names['ch'],'-p',f'{cp}:8123','--tmpfs','/var/lib/clickhouse:size=1g','-e','CLICKHOUSE_USER=bench','-e','CLICKHOUSE_PASSWORD=bench','clickhouse/clickhouse-server:latest',stdout=subprocess.DEVNULL)
   wait(f'http://127.0.0.1:{pp}/-/ready');wait(f'http://127.0.0.1:{vp}/health');wait(f'http://127.0.0.1:{cp}/?user=bench&password=bench&query=SELECT%201')
   dp=subprocess.Popen([str(a.binary),'--profile','asapquery','--physical-plan',str(plan),'--http-port',str(18700+t),'--victoriametrics-http-port',str(apv),'--victoriametrics-url',f'http://127.0.0.1:{vp}','--clickhouse-http-port',str(apc),'--clickhouse-url',f'http://127.0.0.1:{cp}','--clickhouse-user','bench','--clickhouse-password','bench','--forward-unsupported-queries','--prometheus-server',f'http://127.0.0.1:{pp}','--output-dir',str(root/'dp')],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL);wait(f'http://127.0.0.1:{apv}/api/v1/health');phase['started']=snap(dp,names,{'prom':root/'prom','dp':root/'dp'})
   ingest=time.monotonic();run('python3',str(HERE/'load_openmetrics_clickhouse.py'),'--metrics',str(fixture),'--url',f'http://127.0.0.1:{cp}');run('curl','-fsS','-X','POST','--data-binary',f'@{fixture}',f'http://127.0.0.1:{vp}/api/v1/import/prometheus',stdout=subprocess.DEVNULL);run('curl','-fsS','-X','POST',f'http://127.0.0.1:{vp}/internal/force_flush',stdout=subprocess.DEVNULL);phase['post_ingest']=snap(dp,names,{'prom':root/'prom','dp':root/'dp'});ingest_s=time.monotonic()-ingest
   raw=a.output_dir/f'trial-{t}-raw.json';query=time.monotonic();run('python3',str(HERE/'run_latency_trials.py'),'--corpus',str(HERE/'corpus.json'),'--repetitions',str(a.repetitions),'--seed',str(a.seed+t*1000),'--prometheus',f'http://127.0.0.1:{pp}/api/v1/query','--victoriametrics',f'http://127.0.0.1:{vp}/api/v1/query','--clickhouse',f'http://127.0.0.1:{cp}/','--asap-metricsql',f'http://127.0.0.1:{apv}/api/v1/query','--asap-clickhouse',f'http://127.0.0.1:{apc}/','--output',str(raw),stdout=subprocess.DEVNULL);phase['post_query']=snap(dp,names,{'prom':root/'prom','dp':root/'dp'});manifest.append({'trial':t,'seed':a.seed+t*1000,'lifecycle_seconds':time.monotonic()-started,'ingest_seconds':ingest_s,'query_seconds':time.monotonic()-query,'raw':raw.name,'phases':phase})
  finally:
   if dp and dp.poll() is None:dp.terminate();dp.wait(timeout=10)
   for n in names.values():subprocess.run(['sudo','docker','rm','-f',n],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
 images={x:subprocess.check_output(['sudo','docker','image','inspect','-f','{{index .RepoDigests 0}}',x],text=True).strip() for x in ('prom/prometheus:v2.55.1','victoriametrics/victoria-metrics:v1.126.0','clickhouse/clickhouse-server:latest')}
 sha=lambda p:hashlib.sha256(Path(p).read_bytes()).hexdigest();(a.output_dir/'manifest.json').write_text(json.dumps({'schema_version':2,'backend_head':subprocess.check_output(['git','-C',str(a.binary_source),'rev-parse','HEAD'],text=True).strip(),'binary_sha256':sha(a.binary),'fixture_sha256':sha(fixture),'physical_plan_sha256':sha(plan),'images':images,'trials':manifest},indent=2)+'\n')
if __name__=='__main__':main()
