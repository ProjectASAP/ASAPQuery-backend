"""Exercise the shared sync script without accessing GitHub or Cargo registries."""
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

SCRIPT = Path(__file__).resolve().parents[1] / "sync-asapplanner-main.sh"


class PlannerSyncTests(unittest.TestCase):
    revision = "a" * 40
    manifest = ''.join(
        f'{name} = {{ git = "https://github.com/ProjectASAP/ASAPPlanner", rev = "{"b" * 40}" }}\n'
        for name in ['planner-types', 'asap-aware-mapping', 'asap-frontend-promql', 'asap-frontend-sql']
    )

    def run_sync(self, manifest, revision=None, cargo_status=0):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        root = Path(temp.name)
        (root / 'Cargo.toml').write_text(manifest)
        (root / 'git').write_text('#!/bin/sh\nprintf "%s refs/heads/main\\n" "$TEST_REV"\n')
        (root / 'cargo').write_text('#!/bin/sh\nprintf "%s\\n" "$@" > cargo-args\nexit "$TEST_CARGO_STATUS"\n')
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
        self.assertEqual(updated.count(f'rev = "{self.revision}"'), 4)
        self.assertTrue(updated.endswith(other))
        self.assertEqual((root / 'cargo-args').read_text().splitlines(), [
            'update', '-p', 'asap-types@0.1.0', '-p', 'asap-aware-mapping',
            '-p', 'asap-frontend-promql', '-p', 'asap-frontend-sql', '--precise', self.revision])

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
