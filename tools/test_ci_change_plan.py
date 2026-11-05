import json
from pathlib import Path
import subprocess
import tempfile
import unittest

import ci_change_plan as plan


class ChangePlanTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.git('init', '-q')
        self.write('README.md', 'before\n')
        self.write('.github/scenarios/catalog.json', json.dumps({'scenarios':[
            {'group':'mon-mgr'}, {'group':'osd'}, {'group':'mds-sessions'}, {'group':'rbd'}]}))
        self.base = self.commit()

    def git(self, *args):
        return subprocess.check_output(['git', '-c', 'user.name=CI test', '-c',
                                        'user.email=ci@example.invalid', '-c', 'commit.gpgsign=false', *args],
                                       cwd=self.root, stderr=subprocess.PIPE).decode().strip()

    def write(self, name, text):
        path = self.root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text)

    def commit(self):
        self.git('add', '.')
        self.git('commit', '-qm', 'Synthetic planner fixture')
        return self.git('rev-parse', 'HEAD')

    def result(self, **kwargs):
        args = dict(event='pull_request', fork=False, base=self.base, head=self.git('rev-parse','HEAD'))
        args.update(kwargs)
        return plan.plan(self.root, **args)

    def test_only_audited_regular_content_modification_can_skip(self):
        self.write('README.md', 'after\n')
        self.commit()
        result = self.result()
        self.assertTrue(result['docs_only'])
        self.assertFalse(any(r['recommended'] for r in result['groups']))
        self.assertIn('not executed', result['reason'])
        for kwargs in ({'fork':True}, {'event':'schedule'}, {'event':'workflow_dispatch'}, {'base':'missing'}, {'head':'f'*40}):
            with self.subTest(kwargs=kwargs):
                result = self.result(**kwargs)
                self.assertFalse(result['docs_only'])
                self.assertTrue(all(r['recommended'] for r in result['groups']))

    def test_consumed_document_contract_and_unknown_path_run_checks(self):
        self.write('README.md', 'after\n')
        self.write('tools/read.py', 'from pathlib import Path\nPath("README.md").read_text()\n')
        consumer_base = self.commit()
        self.write('README.md', 'later\n')
        self.commit()
        result = self.result(base=consumer_base)
        self.assertFalse(result['docs_only'])
        self.assertEqual(result['documentation_inputs'], ['README.md'])
        for path in ('SPEC.md','cephmsgr/MESSAGE_LEDGER.md','new.md'):
            self.write(path, 'content\n')
        self.commit()
        self.assertFalse(self.result()['docs_only'])

    def test_rename_deletion_and_symlink_never_skip(self):
        self.git('mv','README.md','THIRD_PARTY_NOTICES.md')
        self.commit()
        self.assertFalse(self.result()['docs_only'])
        self.git('reset','--hard',self.base)
        (self.root/'README.md').unlink()
        (self.root/'README.md').symlink_to('.github/scenarios/catalog.json')
        self.commit()
        self.assertFalse(self.result()['docs_only'])

    def test_executable_mode_and_root_embed_do_not_skip(self):
        self.write('README.md', 'after\n')
        (self.root / 'README.md').chmod(0o755)
        self.commit()
        self.assertFalse(self.result()['docs_only'])
        self.git('reset', '--hard', self.base)
        self.write('main.go', 'package sample\nimport _ "embed"\n//go:embed *.md\nvar docs string\n')
        embed_base = self.commit()
        self.write('README.md', 'embedded change\n')
        self.commit()
        self.assertFalse(self.result(base=embed_base)['docs_only'])

    def test_recommendations_are_role_specific_but_shared_changes_widen(self):
        self.write('internal/mds/test.go','package mds\n')
        self.commit()
        result=self.result()
        self.assertFalse(result['docs_only'])
        self.assertEqual([r['group'] for r in result['groups'] if r['recommended']], ['mds-sessions'])
        self.write('internal/osd/test.go','package osd\n')
        self.commit()
        self.assertEqual([r['group'] for r in self.result()['groups'] if r['recommended']], ['mds-sessions','osd','rbd'])
        self.write('internal/session/shared.go','package session\n')
        self.commit()
        self.assertTrue(all(r['recommended'] for r in self.result()['groups']))

    def test_test_merge_identity_and_unrelated_checkout(self):
        self.git('checkout','-qb','feature')
        self.write('README.md','feature\n')
        head=self.commit()
        self.git('checkout','-qb','target',self.base)
        self.write('internal/session/main.go','package session\n')
        self.commit()
        self.git('merge','--no-ff','-qm','Synthetic test merge','feature')
        result=self.result(head=head)
        self.assertFalse(result['docs_only'])
        self.assertIn('internal/session/main.go',[e['path'] for e in result['changed']])
        result=self.result(head=self.base)
        self.assertIn('Unproven',result['reason'])
        self.assertTrue(all(r['recommended'] for r in result['groups']))

    def test_empty_diff_runs_and_summary_escapes_catalog_data(self):
        self.assertFalse(self.result()['docs_only'])
        result=self.result()
        result['groups'][0]['group']='@user|<img src=x>'
        output=plan.summary(result)
        self.assertNotIn('@user',output)
        self.assertNotIn('<img',output)
        self.assertIn('not a dependency proof',output)

    def test_workflows_fail_open_and_keep_matrix_check_names(self):
        root=plan.ROOT/'.github/workflows'
        basic=(root/'ci.yml').read_text()
        self.assertEqual(basic.count("if: ${{ !cancelled() && needs.changes.outputs.docs_only != 'true' }}"),2)
        quality=(root/'code.yml').read_text()
        for name, end in (('lint','tooling'),('vulnerabilities',None)):
            section=quality.split('  '+name+':\n',1)[1]
            if end: section=section.split('  '+end+':\n',1)[0]
            job_condition=section.split('    steps:',1)[0]
            self.assertNotIn('docs_only',job_condition)
            self.assertIn('Documentation-only policy',section)
            self.assertIn("if: needs.changes.outputs.docs_only != 'true'",section)
        for file in ('periodic.yml','ci-command.yml','native-scenarios.yml','platform-checks.yml'):
            self.assertNotIn('docs_only',(root/file).read_text())


if __name__=='__main__':
    unittest.main()
