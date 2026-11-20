"""Read-only local pilot preflight; never applies imports or migrations."""
import argparse
import gzip
import importlib
import json
import os
from pathlib import Path
import sys
from unittest.mock import patch


def main():
    parser = argparse.ArgumentParser(__doc__)
    parser.add_argument('--prepared', required=True)
    parser.add_argument('--output-dir', required=True)
    parser.add_argument('--enrichment')
    parser.add_argument('--expect-present', action='store_true',
                        help='Require every prepared identity and its exact imported fields to exist')
    args = parser.parse_args()
    root = Path(__file__).resolve().parent.parent
    sys.path.insert(0, str(root))
    os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')
    from django.conf import settings
    cfg = settings.DATABASES['default']
    if tuple(str(cfg[key]) for key in ('HOST', 'PORT', 'NAME')) != ('127.0.0.1', '5432', 'socialapp'):
        raise RuntimeError('Only the explicitly selected local socialapp endpoint is allowed')
    cfg.setdefault('OPTIONS', {})['options'] = '-c default_transaction_read_only=on'
    import django
    django.setup()
    from django.core.management import call_command
    from django.db import connection, transaction
    from core.tmdb_import_apply import check_schema, check_session, resolve_author
    from core.tmdb_import_ready import prepare_run, rules, verify_ready_run
    from core.tmdb_importer import read_json, evaluate_record
    from core.tmdb_enrichment_store import file_digest
    from core.management.commands.diagnose_local_movie_duplicates import require_local
    from core.models import Movie
    require_local(connection)
    source = Path(args.prepared).resolve(strict=True)
    enrichment = Path(args.enrichment).resolve(strict=True) if args.enrichment else None
    originals = {str(p): file_digest(p) for folder in (source, enrichment) if folder
                 for p in folder.rglob('*') if p.is_file()}
    output = Path(args.output_dir).resolve()
    if (any((p / '.git').exists() for p in (output, *output.parents))
            or any(output == p or p in output.parents for p in (root, source, enrichment) if p)):
        raise RuntimeError('Output must be independent and outside the repository')
    output.mkdir(parents=True, exist_ok=False)
    old = read_json(source / 'preparation_manifest.json')
    for key, filename in (('ready', 'import_ready.jsonl.gz'), ('omitted', 'omitted.jsonl')):
        entry = old['outputs'][key]
        path = source / filename
        if entry['file'] != filename or entry['sha256'] != file_digest(path) or entry['bytes'] != path.stat().st_size:
            raise RuntimeError('Historical preparation output integrity mismatch')
    with gzip.open(source / 'import_ready.jsonl.gz', 'rt', encoding='utf-8') as stream:
        original_records = [json.loads(line) for line in stream]
    differences = {name: {'prepared': old['rules_sha256'].get(name), 'current': value}
                   for name, value in rules().items() if old['rules_sha256'].get(name) != value}
    result = {'original_preparation': str(source), 'rule_differences': differences}
    # Preparation is strictly offline, including an explicit database prohibition.
    with patch.object(connection, 'cursor', side_effect=AssertionError('Preparation must not query PostgreSQL')), \
            patch('socket.socket.connect', side_effect=AssertionError('Preparation must not use the network')):
        if differences:
            if enrichment is None:
                raise RuntimeError('Stale preparation requires its original enrichment')
            prepared = prepare_run(enrichment, output / 'prepared',
                                   convert_legacy=old.get('convert_legacy', False),
                                   allow_partial=old.get('allow_partial', False))
            source = Path(prepared['run_directory'])
        result['preparation'] = verify_ready_run(source)
    result['prepared_directory'] = str(source)
    with gzip.open(source / 'import_ready.jsonl.gz', 'rt', encoding='utf-8') as stream:
        records = [json.loads(line) for line in stream]
    result['prepared_records_unchanged'] = records == original_records
    if not result['prepared_records_unchanged']:
        raise RuntimeError('Regeneration changed prepared records; review before import')
    result['candidates'] = [row['movie_data'] for row in records]
    target = {'alias': 'default', 'host': '127.0.0.1', 'port': '5432', 'name': 'socialapp'}
    with transaction.atomic():
        with connection.cursor() as cursor:
            cursor.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')
            check_session(cursor, target, read_only=True)
            check_schema(cursor, '"core_movie"')
            result['schema_verified'] = True
            cursor.execute('SELECT count(*), count(*) FILTER (WHERE tmdb_id IS NULL) FROM core_movie')
            result['catalog_rows'], result['null_tmdb_ids'] = cursor.fetchone()
            cursor.execute('SELECT count(*), coalesce(sum(n-1),0) FROM '
                           '(SELECT count(*) n FROM core_movie GROUP BY type,tmdb_id HAVING count(*) > 1) d')
            result['duplicate_groups'], extra = cursor.fetchone()
            result['extra_duplicates'] = int(extra)
            constraints = connection.introspection.get_constraints(cursor, 'core_movie')
            result['identity_constraint'] = constraints.get('movie_type_tmdb_id_uniq')
            result['author'] = resolve_author(target, 'Admin')
            cursor.execute("SELECT EXISTS(SELECT 1 FROM django_migrations WHERE app='core' AND name='0079_movie_type_tmdb_id_uniq')")
            result['migration_0079_recorded'] = cursor.fetchone()[0]
            cursor.execute("SELECT pg_get_triggerdef(t.oid), pg_get_functiondef(t.tgfoid) "
                           "FROM pg_trigger t WHERE t.tgrelid='public.core_movie'::regclass "
                           "AND t.tgname='core_movie_search_vector_update_trigger'")
            definition, function = cursor.fetchone()
            expression = importlib.import_module('core.migrations.0040_movie_full_text_search').SEARCH_VECTOR_EXPRESSION
            result['search_trigger'] = {'definition': definition,
                'expected_expression_present': ' '.join(expression.split()) in ' '.join(function.split())}
            if not result['search_trigger']['expected_expression_present']:
                raise RuntimeError('Search-vector function differs from the expected expression; review required')
        result['candidate_status'] = []
        for record in records:
            movie = Movie.objects.filter(type=record['type'], tmdb_id=record['tmdb_id']).first()
            status = {'type': record['type'], 'tmdb_id': record['tmdb_id'],
                      'movie_id': movie.pk if movie else None}
            if movie:
                candidate = {**record['candidate'], '_ready_contract': 1}
                expected = evaluate_record(candidate, None)['proposed_fields']
                status['mismatched_fields'] = [name for name, value in expected.items()
                    if (str(getattr(movie, name)) if name == 'external_rating' and getattr(movie, name) is not None
                        else getattr(movie, name)) != value]
                status['author_matches'] = movie.author_id == result['author']['id']
                status['search_vector_present'] = bool(movie.search_vector)
            result['candidate_status'].append(status)
        if args.expect_present and any(not s['movie_id'] or s['mismatched_fields']
                or not s['author_matches'] or not s['search_vector_present'] for s in result['candidate_status']):
            raise RuntimeError('Post-import verification failed: missing identity, fields, author or search vector')
    connection.close()
    call_command('import_tmdb_productions', run_dir=str(source), output_dir=str(output / 'dry-run'),
                 dry_run=True, author_username='Admin', limit=10, batch_size=10)
    result['dry_run'] = read_json(next((output / 'dry-run').glob('*/import_manifest.json')))
    result['originals_unchanged'] = all(file_digest(Path(name)) == digest for name, digest in originals.items())
    if not result['originals_unchanged']:
        raise RuntimeError('Original artifacts changed')
    result['original_hashes'] = originals
    result['database_writes'] = False
    report = output / 'pilot_validation.json'
    report.write_text(json.dumps(result, ensure_ascii=False, indent=2, default=str) + '\n', encoding='utf-8')
    print(json.dumps({'report': str(report), 'prepared': str(source), 'changed_rules': list(differences),
                      'catalog_rows': result['catalog_rows'], 'duplicates': result['duplicate_groups'],
                      'author': result['author'], 'counts': result['dry_run']['counts']}, ensure_ascii=False))


if __name__ == '__main__':
    main()
