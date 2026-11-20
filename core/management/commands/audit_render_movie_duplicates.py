"""Explicit remote READ ONLY diagnostic; deliberately has no apply path."""
from django.core.management.base import BaseCommand, CommandError
from core.remote_movie_duplicate_audit import connection_config, run_audit


class Command(BaseCommand):
    help = 'Diagnose or plan Render duplicates using a separate read-only TLS connection. Never applies changes.'
    requires_system_checks = []
    requires_migrations_checks = False

    def add_arguments(self, parser):
        parser.add_argument('--host', required=True)
        parser.add_argument('--port', type=int, default=5432)
        parser.add_argument('--name', required=True)
        parser.add_argument('--confirm-user', required=True)
        parser.add_argument('--confirm-host', required=True)
        parser.add_argument('--confirm-database', required=True)
        parser.add_argument('--tls-ca-file', required=True)
        parser.add_argument('--output-dir', required=True)
        parser.add_argument('--plan', action='store_true', help='Produce a production-only review plan; never apply')

    def handle(self, *args, **options):
        try:
            config = connection_config(options['host'], options['port'], options['name'], options['tls_ca_file'],
                                       options['confirm_host'], options['confirm_database'], options['confirm_user'])
        except (OSError, ValueError):
            raise CommandError('Invalid remote connection configuration or TLS CA file.') from None
        target = {'environment': 'render-production', 'alias': 'reccool_production_audit',
                  'host': options['host'], 'port': options['port'], 'name': options['name']}
        result = run_audit(config, target, options['output_dir'], plan=options['plan'])
        self.stdout.write(f"Read-only audit complete: {result['total_movies']} Movie rows; "
                          f"{result['all_duplicate_groups']} duplicate groups; reports={result['run_directory']}")
