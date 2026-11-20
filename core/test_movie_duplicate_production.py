"""Offline production-plan validation and authorization; never remote connections."""
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from django.core.management import call_command
from django.core.management.base import CommandError
from core import movie_duplicate_production as production
from core import movie_duplicate_cleanup as engine
from core.movie_duplicate_cleanup_plan import POLICY, choose_survivor
from core.remote_movie_duplicate_audit import CONTRACT
from core.test_local_movie_duplicates import movie
from core.tmdb_enrichment_store import file_digest


def make_plan(folder, target):
    folder.mkdir()
    group = {'type': 'movie', 'tmdb_id': 10, **choose_survivor([movie(1, director='Complete'), movie(2), movie(3)]),
             'dependencies': [], 'status': 'planned'}
    engine.publish(folder / 'remote_dependencies.json', {'django': [], 'postgresql': {'blockers': [], 'fk_graph_matches': True}})
    (folder / 'remote_candidates.jsonl').write_text(engine.json_text(group) + '\n', encoding='utf-8')
    rules = {name: file_digest(Path(production.__file__).with_name(name)) for name in
             ('remote_movie_duplicate_audit.py', 'movie_duplicate_cleanup_plan.py', 'movie_duplicate_diagnosis.py', 'models.py')}
    manifest = {'contract': CONTRACT, 'status': 'complete', 'read_only': True, 'apply_enabled': False,
                'mode': 'plan', 'policy': POLICY, 'target': target, 'rules_sha256': rules,
                'schema_blockers': [], 'protected_groups': 0, 'unplannable_duplicate_groups': 0,
                'dependency_estimate_complete': True, 'null_tmdb': 0, 'invalid_tmdb': 0,
                'groups': 1, 'all_duplicate_groups': 1, 'remove_movies': 2, 'extra_duplicate_rows': 2,
                'groups_with_dependencies': 0, 'impact_by_fk_path': [],
                'outputs': {name: file_digest(folder / name) for name in ('remote_candidates.jsonl', 'remote_dependencies.json')}}
    engine.publish(folder / 'remote_manifest.json', manifest)
    (folder / 'remote_manifest.sha256').write_text(file_digest(folder / 'remote_manifest.json'))
    return manifest


class RemotePlanTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.target = {'environment': 'render-production', 'alias': 'reccool_production_audit',
                       'host': 'dpg-synthetic.oregon-postgres.render.com', 'port': 5432, 'name': 'synthetic'}
        self.folder = self.root / 'plan'
        self.manifest = make_plan(self.folder, self.target)

    def republish(self):
        self.manifest['outputs'] = {name: file_digest(self.folder / name) for name in self.manifest['outputs']}
        engine.publish(self.folder / 'remote_manifest.json', self.manifest)
        (self.folder / 'remote_manifest.sha256').write_text(file_digest(self.folder / 'remote_manifest.json'))

    def test_complete_remote_plan_reuses_deterministic_survivor(self):
        _, groups, _, _ = production.load_remote_plan(self.folder, self.target)
        self.assertEqual(groups[0]['keep_id'], 1)
        self.assertEqual(groups[0]['remove_ids'], [2, 3])

    def test_hash_wrong_destination_and_explicit_digest_rejected(self):
        with self.assertRaises(CommandError):
            production.load_remote_plan(self.folder, {**self.target, 'name': 'other'})
        with self.assertRaises(CommandError):
            production.load_remote_plan(self.folder, self.target, 'a' * 64)
        (self.folder / 'remote_candidates.jsonl').write_text('tampered')
        with self.assertRaises(ValueError):
            production.load_remote_plan(self.folder, self.target)

    def test_manifest_rules_and_completeness_rejected(self):
        for field, value in [('groups', 2), ('remove_movies', 4), ('groups_with_dependencies', 9),
                             ('null_tmdb', 1), ('mode', 'diagnose'), ('policy', 'unknown'),
                             ('rules_sha256', {}), ('dependency_estimate_complete', False)]:
            old = self.manifest[field]
            self.manifest[field] = value
            self.republish()
            with self.subTest(field=field), self.assertRaises((CommandError, ValueError)):
                production.load_remote_plan(self.folder, self.target)
            self.manifest[field] = old
        self.republish()

    def test_invalid_ids_duplicate_members_and_unsupported_dependencies(self):
        path = self.folder / 'remote_candidates.jsonl'
        original = json.loads(path.read_text())
        for patching in ({'keep_id': True}, {'remove_ids': [1, 2]}, {'tmdb_id': 0},
                         {'dependencies': [{'movie_id': 2, 'on_delete': 'CASCADE', 'model': 'auth.User', 'path': 'movie', 'rows': 1}]}):
            path.write_text(engine.json_text({**original, **patching}) + '\n')
            self.republish()
            with self.assertRaises(CommandError):
                production.load_remote_plan(self.folder, self.target)

    def test_local_manifest_is_not_a_remote_plan(self):
        self.manifest['contract'] = 'local'
        self.republish()
        with self.assertRaises(ValueError):
            production.load_remote_plan(self.folder, self.target)

    def test_strict_json_and_boolean_counters(self):
        for value in ('{"id":1,"id":2}', '{"score":NaN}', '{"score":Infinity}'):
            with self.assertRaises(ValueError):
                production.strict_json(value)
        self.manifest['groups'] = True
        self.republish()
        with self.assertRaises(CommandError):
            production.load_remote_plan(self.folder, self.target)

    def test_apply_requires_all_approvals_before_connection_or_filesystem(self):
        options = {'plan_dir': 'unused', 'output_dir': 'unused', 'host': self.target['host'], 'name': 'synthetic',
                   'confirm_host': self.target['host'], 'confirm_database': 'synthetic', 'confirm_user': 'fixture',
                   'tls_ca_file': 'unused', 'apply': True}
        command = 'core.management.commands.cleanup_render_movie_duplicates'
        with patch(command + '.production_session') as session:
            with self.assertRaisesRegex(CommandError, 'Apply requires'):
                call_command('cleanup_render_movie_duplicates', **options)
            session.assert_not_called()
