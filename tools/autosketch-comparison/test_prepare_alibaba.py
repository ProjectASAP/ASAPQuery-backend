"""Preparation preserves parseable fields and explicitly accounts for bad rows."""
import io
import json
from pathlib import Path
import tarfile
import tempfile
import unittest
import gzip
import struct

import pyarrow.parquet as pq
from prepare_alibaba import COLUMNS, audit, prepare
from project_alibaba import project


class PreparationTests(unittest.TestCase):
    def test_projection_removes_only_full_duplicates_and_preserves_time_and_zero(self):
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder)
            late='200,t,s,0,rpc,MS_1,ui,i,MS_2,di,0.0\n'
            early='100,t,s,0,rpc,MS_1,ui2,i,MS_2,di,4.0\n'
            content=(','.join(COLUMNS)+'\n'+late+early+late).encode()
            with tarfile.open(root/'CallGraph_0.tar.gz','w:gz') as archive:
                member=tarfile.TarInfo('CallGraph_0.csv');member.size=len(content)
                archive.addfile(member,io.BytesIO(content))
            result=project(root,0)
            self.assertEqual(result['exact_duplicate_rows_removed'],1)
            self.assertEqual(result['events'],2)
            self.assertEqual(result['zero_latency_rows'],1)
            with gzip.open(root/'observations_0.bin.gz','rb') as source:
                records=list(struct.iter_unpack('<IIId',source.read()))
            self.assertEqual(records,[(100,1,2,4.0),(200,1,2,0.0)])

    def test_lossless_conversion_and_malformed_accounting(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            csv = (','.join(COLUMNS) + '\n' +
                   '100,t,s,0,rpc,u,ui,i,d,di,2.0\n' +
                   '100,t,s,0,rpc,u,ui,i,d,di,2.0,EXTRA\n').encode()
            with tarfile.open(root / 'CallGraph_0.tar.gz', 'w:gz') as archive:
                member = tarfile.TarInfo('CallGraph_0.csv')
                member.size = len(csv)
                archive.addfile(member, io.BytesIO(csv))
            record = prepare(root, 0)
            self.assertEqual(record['parsed_rows'], 1)
            self.assertEqual(record['malformed_rows']['count'], 1)
            self.assertEqual(pq.read_table(root / 'CallGraph_0.parquet')['rt'].to_pylist(), ['2.0'])
            audit(root, [record])
            result = json.loads((root / 'audit-0-0.json').read_text())
            self.assertEqual(result['summary']['raw_rows'], 1)
            self.assertEqual(result['call_identity']['trace_rpc_pairs'], 1)

    def test_repeated_rpc_identity_is_not_silently_deduplicated(self):
        # Two different observations of an RPC must survive preparation.
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            csv = (','.join(COLUMNS) + '\n' +
                   '100,t,s,0,rpc,u,ui,i,d,di,2.0\n' +
                   '101,t,s,0,rpc,u,ui2,i,d,di,3.0\n').encode()
            with tarfile.open(root / 'CallGraph_0.tar.gz', 'w:gz') as archive:
                member = tarfile.TarInfo('CallGraph_0.csv')
                member.size = len(csv)
                archive.addfile(member, io.BytesIO(csv))
            record = prepare(root, 0)
            audit(root, [record])
            result = json.loads((root / 'audit-0-0.json').read_text())
            self.assertEqual(result['summary']['raw_rows'], 2)
            self.assertEqual(result['call_identity']['repeated_pair_rows'], 1)
            self.assertEqual(result['call_identity']['conflicting_measurements'], 1)
            self.assertEqual(result['call_identity']['conflicting_instances'], 1)


if __name__ == '__main__':
    unittest.main()
