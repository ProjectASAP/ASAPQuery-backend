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
   with self.assertRaisesRegex(ValueError,'identical evaluation contract'): R.run_arm({'name':'exact','command':['python3',str(script)]},{'x':1},root)
if __name__=='__main__': unittest.main()
