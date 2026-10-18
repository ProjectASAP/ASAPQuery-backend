"""Exercise the shared sync script without accessing GitHub or Cargo registries."""
import os
import shutil
from pathlib import Path
import subprocess
import tempfile
import unittest

REPO = Path(__file__).resolve().parents[2]
SCRIPT = REPO / "scripts" / "sync-asapplanner-main.sh"

# Like `cargo update -p`, the fake cargo exits 101 when a package spec
# (`name` or `name@version`) is absent from the Cargo.lock in its directory.
FAKE_CARGO = """#!/usr/bin/env python3
import os, re, sys
args = sys.argv[1:]
with open('cargo-args', 'w') as out:
    out.write(''.join(arg + '\\n' for arg in args))
lock = open('Cargo.lock').read()
locked = set(re.findall(r'name = "([^"]+)"\\nversion = "([^"]+)"', lock))
for flag, spec in zip(args, args[1:]):
    if flag != '-p':
        continue
    name, _, version = spec.partition('@')
    if not any(n == name and (not version or v == version) for n, v in locked):
        sys.stderr.write(f'error: package ID specification `{spec}` did not match any packages\\n')
        sys.exit(101)
sys.exit(int(os.environ['TEST_CARGO_STATUS']))
"""


class PlannerSyncTests(unittest.TestCase):
    revision = "a" * 40
    manifest = ''.join(
        f'{name} = {{ git = "https://github.com/ProjectASAP/ASAPPlanner", rev = "{"b" * 40}" }}\n'
        for name in ['planner-types', 'asap-frontend-promql', 'asap-logical-asap-optimization',
                     'asap-plan-selection', 'asap-executor']
    )

    def run_sync(self, manifest, revision=None, cargo_status=0):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        root = Path(temp.name)
        (root / 'Cargo.toml').write_text(manifest)
        shutil.copy(REPO / 'Cargo.lock', root / 'Cargo.lock')
        (root / 'git').write_text('#!/bin/sh\nprintf "%s refs/heads/main\\n" "$TEST_REV"\n')
        (root / 'cargo').write_text(FAKE_CARGO)
        for name in ['git', 'cargo']:
            (root / name).chmod(0o755)
        env = dict(os.environ, PATH=f'{root}:{os.environ["PATH"]}',
                   TEST_REV=self.revision if revision is None else revision,
                   TEST_CARGO_STATUS=str(cargo_status))
        result = subprocess.run(['bash', str(SCRIPT)], cwd=root, env=env, capture_output=True, text=True)
        return root, result

    def test_updates_all_dependencies_and_lock_resolution(self):
        """All Planner pins and Cargo's requested revision agree, preserving unrelated pins."""
        other = 'other = { git = "https://example.com/other", rev = "unchanged" }\n'
        root, result = self.run_sync(self.manifest + other)
        self.assertEqual(result.returncode, 0, result.stderr)
        updated = (root / 'Cargo.toml').read_text()
        self.assertEqual(updated.count(f'rev = "{self.revision}"'), 5)
        self.assertTrue(updated.endswith(other))
        self.assertEqual((root / 'cargo-args').read_text().splitlines(), [
            'update', '-p', 'asap-types@0.1.0', '-p', 'asap-frontend-promql',
            '-p', 'asap-logical-asap-optimization', '-p', 'asap-plan-selection',
            '-p', 'asap-executor', '--precise', self.revision])

    def test_repository_manifest_updates_only_locked_packages(self):
        """Every pin in the real Cargo.toml is rewritten and every `-p` names a locked package."""
        manifest = (REPO / 'Cargo.toml').read_text()
        root, result = self.run_sync(manifest)
        self.assertEqual(result.returncode, 0, result.stderr)
        updated = (root / 'Cargo.toml').read_text()
        self.assertNotRegex(updated, r'ASAPPlanner", rev = "(?!' + self.revision + ')')

    def test_invalid_revision_does_not_mutate_manifest(self):
        """An upstream lookup failure cannot produce a malformed pin."""
        root, result = self.run_sync(self.manifest, revision='')
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual((root / 'Cargo.toml').read_text(), self.manifest)
        self.assertFalse((root / 'cargo-args').exists())

    def test_unexpected_layout_does_not_partially_update(self):
        """Removing a dependency requires deliberate updater changes."""
        manifest = self.manifest.split('\n', 1)[1]
        root, result = self.run_sync(manifest)
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual((root / 'Cargo.toml').read_text(), manifest)
        self.assertFalse((root / 'cargo-args').exists())

    def test_cargo_failure_is_reported(self):
        """Lock resolution failure stops the workflow before PR publication."""
        _, result = self.run_sync(self.manifest, cargo_status=1)
        self.assertNotEqual(result.returncode, 0)


if __name__ == '__main__':
    unittest.main()
