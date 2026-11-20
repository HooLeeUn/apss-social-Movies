"""Exact, reviewed weekly-parent deltas for production duplicate deletion."""
from copy import deepcopy
from django.core.management.base import CommandError
from django.db.models import F
from core.models import WeeklyRecommendationItem, WeeklyRecommendationSnapshot

KEY = '__weekly_review__'


def capture(removed_ids):
    parents = sorted(set(WeeklyRecommendationItem.objects.filter(movie_id__in=removed_ids)
                         .values_list('snapshot_id', flat=True)))
    result = {}
    for pk in parents:
        row = WeeklyRecommendationSnapshot.objects.filter(pk=pk).values().get()
        items = list(WeeklyRecommendationItem.objects.filter(snapshot_id=pk).order_by('pk').values())
        if row['items_count'] != len(items):
            raise CommandError('Weekly snapshot count is historically inconsistent; separate review required.')
        result[str(pk)] = {'snapshot': row, 'items': items}
    # Canonical JSON values, matching the review written to disk.
    from core.management.commands.diagnose_local_movie_duplicates import json_text
    import json
    return json.loads(json_text(result))


def assert_state(review, removed_ids):
    removed = set(removed_ids)
    from core.management.commands.diagnose_local_movie_duplicates import json_text
    import json
    for key, state in review.items():
        wanted = deepcopy(state)
        wanted['items'] = [row for row in wanted['items'] if row['movie_id'] not in removed]
        wanted['snapshot']['items_count'] = len(wanted['items'])
        actual = {'snapshot': WeeklyRecommendationSnapshot.objects.filter(pk=int(key)).values().get(),
                  'items': list(WeeklyRecommendationItem.objects.filter(snapshot_id=int(key)).order_by('pk').values())}
        if json.loads(json_text(actual)) != wanted:
            raise CommandError('Weekly parent or item changed outside the reviewed deletion delta.')


def decrement(review, group_ids):
    for key, state in review.items():
        count = sum(row['movie_id'] in group_ids for row in state['items'])
        if count:
            WeeklyRecommendationSnapshot.objects.filter(pk=int(key)).update(items_count=F('items_count') - count)
