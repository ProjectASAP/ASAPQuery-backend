// Execute the privileged workflow handler with mocked GitHub responses.
const fs = require('node:fs');
const path = require('node:path');
const workflow = fs.readFileSync(path.join(__dirname, '../../.github/workflows/planner-main-merge.yml'), 'utf8');
const script = workflow.split('          script: |\n')[1].split('\n').map(line => line.slice(12)).join('\n');
const AsyncFunction = Object.getPrototypeOf(async function () {}).constructor;
const handler = new AsyncFunction('github', 'context', 'core', script);

async function verifyMergeGuards() {
  for (const scenario of ['valid', 'stale-head', 'foreign-repo', 'draft', 'stale-main', 'extra-file', 'empty-diff', 'no-pr']) {
    let merged = false;
    let failed = false;
    const pr = {
      number: 12,
      head: {repo: {full_name: scenario === 'foreign-repo' ? 'else/repo' : 'o/r'}, sha: scenario === 'stale-head' ? 'old' : 'new'},
      draft: scenario === 'draft',
    };
    const github = {
      rest: {
        pulls: {
          list: async () => ({data: scenario === 'no-pr' ? [] : [pr]}),
          get: async () => ({data: pr}),
          listFiles: () => {},
          merge: async args => {
            if (args.sha !== 'new' || args.pull_number !== 12) throw Error('Wrong merge target');
            merged = true;
          },
        },
        repos: {compareCommitsWithBasehead: async () => ({data: {
          merge_base_commit: {sha: scenario === 'stale-main' ? 'old' : 'base'},
          base_commit: {sha: 'base'},
        }})},
      },
      paginate: async () => scenario === 'empty-diff' ? [] : [{filename: scenario === 'extra-file' ? 'other.rs' : 'Cargo.toml'}],
    };
    await handler(github, {repo: {owner: 'o', repo: 'r'}, payload: {workflow_run: {head_sha: 'new'}}}, {
      info: () => {}, setFailed: () => { failed = true; },
    });
    if (merged !== (scenario === 'valid')) throw Error(`Unexpected merge: ${scenario}`);
    if (failed !== ['extra-file', 'empty-diff'].includes(scenario)) throw Error(`Unexpected failure: ${scenario}`);
  }
}

verifyMergeGuards().catch(error => { console.error(error); process.exitCode = 1; });
