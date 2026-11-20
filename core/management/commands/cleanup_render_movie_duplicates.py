"""Separate, explicitly authorized production maintenance using the shared engine."""
import hashlib
from pathlib import Path
import uuid

from django.conf import settings
from django import get_version
from django.core.management.base import BaseCommand, CommandError
from core import movie_duplicate_cleanup as engine
from core.movie_duplicate_production import load_remote_plan, production_session
from core.movie_duplicate_resume import load_review, failure_report
from core.remote_movie_duplicate_audit import ALIAS, connection_config
from core.tmdb_enrichment_store import file_digest, run_lock
from core.tmdb_importer import read_json


def maintenance_rules():
    root = Path(__file__).resolve().parents[2]
    names = ('movie_duplicate_cleanup.py', 'movie_duplicate_preferences.py', 'movie_duplicate_resume.py',
             'movie_duplicate_execution_scope.py', 'movie_duplicate_production.py', 'services.py', 'signals.py', 'models.py')
    names = (*names, 'movie_duplicate_weekly.py', 'feed_pool.py')
    return engine.digest({**{name: file_digest(root / name) for name in names},
                          'command': file_digest(Path(__file__)), 'django_version': get_version()})


class Command(BaseCommand):
    help = 'Production duplicate maintenance: READ ONLY dry-run by default; separate explicit apply.'
    requires_system_checks = []
    requires_migrations_checks = False

    def add_arguments(self, parser):
        for name in ('plan-dir', 'output-dir', 'host', 'name', 'confirm-host', 'confirm-database', 'confirm-user', 'tls-ca-file'):
            parser.add_argument('--' + name, required=True)
        parser.add_argument('--port', type=int, default=5432)
        mode = parser.add_mutually_exclusive_group()
        mode.add_argument('--dry-run', action='store_true')
        mode.add_argument('--apply', action='store_true')
        parser.add_argument('--confirm-maintenance', action='store_true')
        parser.add_argument('--confirm-plan-sha256')
        parser.add_argument('--confirm-review-sha256')
        parser.add_argument('--confirm-backup-id', help='Operator-verified restorable backup identifier, never credentials')
        parser.add_argument('--review-dir')
        parser.add_argument('--limit-groups', type=int)
        parser.add_argument('--resume', action='store_true')

    def handle(self, *args, **o):
        apply = o['apply']
        if apply and (not o['confirm_maintenance'] or not o['confirm_plan_sha256'] or not o['confirm_review_sha256']
                or not o['confirm_backup_id'] or not o['confirm_backup_id'].strip() or not o['review_dir']
                or type(o['limit_groups']) is not int or o['limit_groups'] < 1):
            raise CommandError('Apply requires maintenance, exact plan/review digests, reviewed dry-run, verified backup ID and positive group limit.')
        if not apply and any(o[key] for key in ('confirm_maintenance', 'confirm_plan_sha256', 'confirm_review_sha256',
                                               'confirm_backup_id', 'review_dir', 'resume')) or (not apply and o['limit_groups'] is not None):
            raise CommandError('Write approval, limit and resume arguments require --apply.')
        source, output = Path(o['plan_dir']).resolve(strict=True), Path(o['output_dir']).resolve()
        repo = Path(settings.BASE_DIR).resolve()
        if (output == repo or repo in output.parents or any((p / '.git').exists() for p in (output, *output.parents))
                or output == source or source in output.parents or output in source.parents):
            raise CommandError('Output must be outside repositories and independent of the source plan.')
        target = {'environment': 'render-production', 'alias': ALIAS, 'host': o['host'], 'port': o['port'], 'name': o['name']}
        progress, execution = {}, {}
        try:
            manifest, groups, schema, source_digest = load_remote_plan(source, target, o['confirm_plan_sha256'])
            config = connection_config(o['host'], o['port'], o['name'], o['tls_ca_file'],
                                       o['confirm_host'], o['confirm_database'], o['confirm_user'])
            execution = {'target': target, 'mode': 'apply' if apply else 'dry-run',
                         'plan_sha256': manifest['outputs']['remote_candidates.jsonl'],
                         'source_manifest_sha256': source_digest, 'maintenance_rules_sha256': maintenance_rules(),
                         'connection_user_sha256': hashlib.sha256(config['USER'].encode()).hexdigest()}
            reviewed = None
            if apply:
                review = Path(o['review_dir']).resolve(strict=True)
                if review == output or review in output.parents or output in review.parents:
                    raise CommandError('Review and apply directories must be independent.')
                reviewed, review_digest = load_review(review, execution)
                report = read_json(review / 'execution_summary.json')
                if (review_digest != o['confirm_review_sha256']
                        or report.get('maintenance_rules_sha256') != execution['maintenance_rules_sha256']
                        or report.get('connection_user_sha256') != execution['connection_user_sha256']):
                    raise CommandError('Explicit review digest or maintenance rules differ.')
                execution['review_sha256'] = review_digest
                # Hash the operator reference; never record a possible secret-bearing string.
                execution['backup_reference_sha256'] = hashlib.sha256(o['confirm_backup_id'].encode()).hexdigest()
            if not o['resume']:
                output.mkdir(parents=True, exist_ok=False)
            elif not output.is_dir():
                raise CommandError('Resume requires the original apply directory.')
            with run_lock(output):
                config_path = output / 'execution_config.json'
                if o['resume']:
                    if read_json(config_path) != execution:
                        raise CommandError('Resume source, destination, review, backup or rules changed.')
                else:
                    engine.publish(config_path, execution)
                previous = read_json(output / 'execution_summary.json') if (output / 'execution_summary.json').exists() else None
                attempt = output / ('attempt-' + uuid.uuid4().hex + '.json')
                engine.publish(attempt, {**execution, 'state': 'started', 'previous_summary': previous})
                try:
                    with production_session(config, target, apply) as connection:
                        summary = engine.execute_cleanup(connection, target, schema, groups, output, apply=apply,
                            limit=o['limit_groups'], resume=o['resume'], reviewed=reviewed, progress=progress)
                except BaseException as exc:
                    failure = failure_report(execution, progress, exc)
                    failure['recovery'] = 'Reconcile the same remote plan, review and receipts using this command with --resume; never use the local verifier.'
                    engine.publish(attempt, {**failure, 'previous_summary': previous})
                    engine.publish(output / 'execution_summary.json', failure)
                    raise
                engine.publish(attempt, {**execution, **summary, 'previous_summary': previous})
                engine.publish(output / 'execution_summary.json', {**execution, **summary})
        except BaseException as exc:
            if isinstance(exc, KeyboardInterrupt):
                raise CommandError('Maintenance interrupted. Reconcile checkpoints before resuming.') from None
            if isinstance(exc, CommandError):
                raise CommandError(str(exc)) from None
            raise CommandError('Production maintenance stopped; inspect reports. No connection secrets are included. Prior apply groups may have committed.') from None
        self.stdout.write(f"Maintenance {summary['state']}; groups={summary['groups']}; committed={summary['committed_groups']}; reports={output}")
