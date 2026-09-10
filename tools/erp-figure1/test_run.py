import hashlib, importlib.util, json, pathlib, tempfile, unittest
P=pathlib.Path(__file__).with_name('run.py'); S=importlib.util.spec_from_file_location('runner',P); R=importlib.util.module_from_spec(S); S.loader.exec_module(R)
class ContractTests(unittest.TestCase):
 def manifest(self, root):
  data=root/'data.tsv'; data.write_text('0\t1\n'); sha=hashlib.sha256(data.read_bytes()).hexdigest()
  contract={'candidate_space':[{'family':'cms','width':64,'depth':2}],'memory_budget_bytes':1024,'accuracy':{'metric':'max_normalized_additive_error','upper_bound':0.01},'window':{'pane_seconds':60,'window_panes':[1,5]}}
  arms=[{'name':n,'command':['true']} for n in R.ARM_NAMES]
  return {'schema_version':1,'dataset':{'path':'data.tsv','sha256':sha},'constraints':contract,'arms':arms}
 def test_accepts_one_shared_contract(self):
  with tempfile.TemporaryDirectory() as d: R.validate_manifest(self.manifest(pathlib.Path(d)),pathlib.Path(d))
 def test_rejects_dataset_drift(self):
  with tempfile.TemporaryDirectory() as d:
   root=pathlib.Path(d); m=self.manifest(root); (root/'data.tsv').write_text('changed')
   with self.assertRaisesRegex(ValueError,'checksum mismatch'): R.validate_manifest(m,root)
 def test_rejects_missing_or_reordered_arm(self):
  with tempfile.TemporaryDirectory() as d:
   root=pathlib.Path(d); m=self.manifest(root); m['arms'].reverse()
   with self.assertRaisesRegex(ValueError,'ordered exactly'): R.validate_manifest(m,root)
 def test_rejects_arm_contract_drift(self):
  with tempfile.TemporaryDirectory() as d:
   root=pathlib.Path(d); script=root/'arm.py'; script.write_text("import json; print(json.dumps({'contract':{},'selected_plan':{},'metrics':{'state_bytes':0,'max_error':0}}))")
   with self.assertRaisesRegex(ValueError,'identical evaluation contract'): R.run_arm({'name':'exact','command':['python3',str(script)]},{'x':1},[],root)
 def test_failed_arm_keeps_resources_and_stderr(self):
  with tempfile.TemporaryDirectory() as d:
   row=R.run_arm({'name':'planner_erp','command':['python3','-c','import sys; print("bad", file=sys.stderr); sys.exit(3)']},
                 {'memory_budget_bytes':1,'accuracy':{'metric':'max_error','upper_bound':0}},[],pathlib.Path(d))
   self.assertEqual((row['status'],row['exit_code']),('failed',3))
   self.assertIn('bad',row['stderr']); self.assertIn('peak_rss_kb',row['measured_resources'])
 def test_rejects_out_of_space_over_budget_or_inaccurate_selection(self):
  contract={'memory_budget_bytes':100,'accuracy':{'metric':'max_error','upper_bound':0.01}}
  base={'contract':contract,'selected_plan':{},'selected_candidates':[{'id':'a'}],
        'metrics':{'state_bytes':10,'max_error':0.0}}
  R.validate_result('planner_erp',base,contract,[{'id':'a'}])
  for field,value,pattern in [('candidate',None,'outside'),('state_bytes',101,'memory'),('max_error',0.02,'accuracy')]:
   row=json.loads(json.dumps(base))
   if field=='candidate': row['selected_candidates']=[{'id':'b'}]
   else: row['metrics'][field]=value
   with self.assertRaisesRegex(ValueError,pattern): R.validate_result('planner_erp',row,contract,[{'id':'a'}])
if __name__=='__main__': unittest.main()
