"""Guard atomic Planner revision updates and reject unexpected manifests."""
import unittest

from update_planner import pin_planner


class PlannerPinTests(unittest.TestCase):
    revision = "a" * 40
    manifest = ''.join(
        f'{name} = {{ git = "https://github.com/ProjectASAP/ASAPPlanner", rev = "{"b" * 40}" }}\n'
        for name in ['planner-types', 'asap-aware-mapping', 'asap-frontend-promql', 'asap-frontend-sql']
    )

    def test_atomic_and_idempotent(self):
        """All four dependencies advance together, without touching other Git pins."""
        other = 'other = { git = "https://example.com/other", branch = "main" }\n'
        result = pin_planner(self.manifest + other, self.revision)
        self.assertEqual(result.count(f'rev = "{self.revision}"'), 4)
        self.assertTrue(result.endswith(other))
        self.assertEqual(pin_planner(result, self.revision), result)

    def test_accepts_branch_selector(self):
        """A branch-based Planner manifest can be converted to reproducible pins."""
        result = pin_planner(self.manifest.replace(f'rev = "{"b" * 40}"', 'branch = "main"'), self.revision)
        self.assertEqual(result.count(f'rev = "{self.revision}"'), 4)

    def test_rejects_changed_dependency_layout(self):
        """A removed dependency or missing selector requires a deliberate updater change."""
        for manifest in [self.manifest.split('\n', 1)[1], self.manifest.replace(f', rev = "{"b" * 40}"', '')]:
            with self.subTest(manifest=manifest), self.assertRaises(ValueError):
                pin_planner(manifest, self.revision)

    def test_rejects_invalid_revision(self):
        """Branch names and truncated SHAs cannot become immutable pins."""
        for revision in ['main', 'abc123', 'z' * 40]:
            with self.subTest(revision=revision), self.assertRaises(ValueError):
                pin_planner(self.manifest, revision)


if __name__ == '__main__':
    unittest.main()
