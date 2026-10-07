from django.contrib.auth import get_user_model
from django.db import connection, transaction
from .models import MovieRecommendationItem, MovieRecommendationHistory


@transaction.atomic
def set_recommendation(user, movie_id, active):
    # Serialize adds/removes even when there is no current item to lock.
    get_user_model().objects.select_for_update().get(pk=user.pk)
    items = MovieRecommendationItem.objects.filter(user=user, movie_id=movie_id)
    if active:
        item, created = items.get_or_create(user=user, movie_id=movie_id)
        return created
    deleted, _ = items.delete()
    return bool(deleted)


def backfill_history():
    """Bounded, retryable backfill; synchronize on the same lock as API writes."""
    count = 0
    user_ids = MovieRecommendationItem.objects.order_by().values_list("user_id", flat=True).distinct()
    for user_id in user_ids.iterator(chunk_size=1000):
        with transaction.atomic():
            if not get_user_model().objects.select_for_update().filter(pk=user_id).exists():
                continue
            # One INSERT SELECT per user, bounded transactions and no per-item N+1.
            with connection.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO core_movierecommendationhistory (user_id, movie_id, started_at, ended_at)
                    SELECT item.user_id, item.movie_id, item.created_at, NULL
                    FROM core_movierecommendationitem item
                    WHERE item.user_id = %s
                    AND NOT EXISTS (
                        SELECT 1 FROM core_movierecommendationhistory history
                        WHERE history.user_id = item.user_id AND history.movie_id = item.movie_id
                        AND history.ended_at IS NULL
                    )
                    ON CONFLICT DO NOTHING
                """, [user_id])
                count += cursor.rowcount
    return count
