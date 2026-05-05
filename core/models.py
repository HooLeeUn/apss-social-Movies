import re

from django.conf import settings
from django.db import models
from django.contrib.auth.models import User
from django.core.cache import cache
from django.core.exceptions import ValidationError
from django.core.validators import MinValueValidator, MaxValueValidator
from django.db.models import Avg, Count, OuterRef, Subquery, IntegerField, FloatField, Case, When, F, Value, CharField, Exists
from django.db.models.functions import Cast
from django.db.models import Q
from django.db.models.functions import Coalesce


def build_genre_key(value):
    if value is None:
        return None

    genres = sorted({part.strip() for part in str(value).split(",") if part and part.strip()})
    if not genres:
        return None
    return "|".join(genres)

class PostQuerySet(models.QuerySet):

    def feed_following(self, user):
        following_users = User.objects.filter(
            followers__follower=user
        )
        return (
            self.filter(Q(author=user) | Q(author__in=following_users))
            .order_by("-created_at")
        )

    def feed_discover(self):
        return (
            self.with_rating_stats()
            .with_comment_stats()
            .filter(ratings_count__gt=0)
            .order_by('-avg_rating')
        )
    
    def with_my_rating(self, user):
        from .models import Rating  

        if not user or not user.is_authenticated:
            return self.annotate(my_rating=models.Value(None, output_field=IntegerField()))

        return self.annotate(
            my_rating=Subquery(
                Rating.objects.filter(
                    post_id=OuterRef("pk"),
                    user_id=user.id,
                ).values("score")[:1]
            )
        )
        
    def with_rating_stats(self):
        return self.annotate(
            avg_rating=Avg("ratings__score"),
            ratings_count=Count("ratings", distinct=True),
        )
    
    def with_comment_stats(self):
        return self.annotate(
            comments_count=Count("comments", distinct=True),
        )
    
class Post(models.Model):
    author = models.ForeignKey(User, on_delete=models.CASCADE, related_name="posts")
    text = models.CharField(max_length=300)
    image = models.ImageField(upload_to='posts/', null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    objects = PostQuerySet.as_manager()

    def average_rating(self):
        ratings = self.ratings.all()
        if ratings.exists():
            return sum(r.score for r in ratings) / ratings.count()
        return 0

    def __str__(self):
        return f"{self.author.username} - {self.text[:30]}"


class MovieQuerySet(models.QuerySet):
    RANKING_CONFIDENCE_THRESHOLD = 5000.0
    DISPLAY_RATING_THRESHOLD = 100.0

    def with_rating_stats(self):
        return self.annotate(
            real_ratings_count=Count("movie_ratings", distinct=True),
            real_ratings_avg=Avg("movie_ratings__score"),
        )

    def with_rating_signals(self):
        required_annotations = {
            "real_ratings_count",
            "real_ratings_avg",
            "_external_rating_float",
            "_real_ratings_avg_float",
            "_external_rating_for_mix",
            "_real_ratings_count_float",
            "_external_votes_float",
        }
        if required_annotations.issubset(self.query.annotations):
            return self

        qs = self
        if {"real_ratings_count", "real_ratings_avg"}.difference(self.query.annotations):
            qs = qs.with_rating_stats()

        return qs.annotate(
            _external_rating_float=Cast("external_rating", FloatField()),
            _real_ratings_avg_float=Coalesce(F("real_ratings_avg"), Value(0.0), output_field=FloatField()),
            _external_rating_for_mix=Coalesce(Cast("external_rating", FloatField()), Value(0.0), output_field=FloatField()),
            _real_ratings_count_float=Cast(F("real_ratings_count"), FloatField()),
            _external_votes_float=Cast(F("external_votes"), FloatField()),
        )

    def with_my_rating(self, user):
        if not user or not user.is_authenticated:
            return self.annotate(my_rating=Value(None, output_field=IntegerField()))

        return self.annotate(
            my_rating=Subquery(
                MovieRating.objects.filter(
                    movie_id=OuterRef("pk"),
                    user_id=user.id,
                ).values("score")[:1]
            )
        )

    def with_in_my_list(self, user):
        if not user or not user.is_authenticated:
            return self.annotate(is_in_my_list=Value(False))

        return self.annotate(
            is_in_my_list=Exists(
                MovieListItem.objects.filter(
                    user_id=user.id,
                    movie_id=OuterRef("pk"),
                )
            )
        )

    def with_in_my_recommendations(self, user):
        if not user or not user.is_authenticated:
            return self.annotate(is_in_my_recommendations=Value(False))

        return self.annotate(
            is_in_my_recommendations=Exists(
                MovieRecommendationItem.objects.filter(
                    user_id=user.id,
                    movie_id=OuterRef("pk"),
                )
            )
        )

    def with_following_rating_stats(self, user):
        if not user or not user.is_authenticated:
            return self.annotate(
                following_avg_rating=Value(None, output_field=FloatField()),
                following_ratings_count=Value(0, output_field=IntegerField()),
            )
        return self.with_following_rating_stats_for_user_id(user.id)

    def with_following_rating_stats_for_user_id(self, user_id):
        if not user_id:
            return self.annotate(
                following_avg_rating=Value(None, output_field=FloatField()),
                following_ratings_count=Value(0, output_field=IntegerField()),
            )

        followed_user_ids = Follow.objects.filter(
            follower_id=user_id,
        ).exclude(
            following_id=user_id,
        ).values("following_id")

        following_ratings = MovieRating.objects.filter(
            movie_id=OuterRef("pk"),
            user_id__in=followed_user_ids,
        ).values("movie_id")

        following_avg_subquery = following_ratings.annotate(
            avg_score=Avg("score"),
        ).values("avg_score")[:1]
        following_count_subquery = following_ratings.annotate(
            total=Count("id"),
        ).values("total")[:1]

        return self.annotate(
            following_avg_rating=Subquery(following_avg_subquery, output_field=FloatField()),
            following_ratings_count=Coalesce(
                Subquery(following_count_subquery, output_field=IntegerField()),
                Value(0),
            ),
        )

    def with_display_rating(self):
        return self.with_rating_signals().annotate(
            display_rating=Case(
                When(real_ratings_count=0, then=F("_external_rating_float")),
                When(real_ratings_count__gte=self.DISPLAY_RATING_THRESHOLD, then=F("_real_ratings_avg_float")),
                default=(
                    (
                        F("_external_rating_for_mix")
                        * (Value(self.DISPLAY_RATING_THRESHOLD) - F("_real_ratings_count_float"))
                    ) + (
                        F("_real_ratings_avg_float") * F("_real_ratings_count_float")
                    )
                ) / Value(self.DISPLAY_RATING_THRESHOLD),
                output_field=FloatField(),
            )
        )

    def with_comment_stats(self):
        comments_count_subquery = (
            Comment.objects.filter(movie_id=OuterRef("pk"))
            .values("movie_id")
            .annotate(total=Count("id"))
            .values("total")[:1]
        )
        return self.annotate(
            comments_count=Coalesce(Subquery(comments_count_subquery, output_field=IntegerField()), Value(0)),
        )

    def with_ranking_scores(self):
        threshold = self.RANKING_CONFIDENCE_THRESHOLD
        return self.with_rating_signals().annotate(
            _ranking_primary_quality=Case(
                When(real_ratings_count__gte=threshold, then=F("_real_ratings_avg_float")),
                When(external_votes__gte=threshold, then=Coalesce(F("_external_rating_float"), F("_real_ratings_avg_float"))),
                When(real_ratings_count__gte=F("external_votes"), then=Coalesce(F("_real_ratings_avg_float"), F("_external_rating_float"), Value(0.0))),
                default=Coalesce(F("_external_rating_float"), F("_real_ratings_avg_float"), Value(0.0)),
                output_field=FloatField(),
            ),
            _ranking_primary_votes=Case(
                When(real_ratings_count__gte=threshold, then=F("_real_ratings_count_float")),
                When(external_votes__gte=threshold, then=F("_external_votes_float")),
                When(real_ratings_count__gte=F("external_votes"), then=F("_real_ratings_count_float")),
                default=F("_external_votes_float"),
                output_field=FloatField(),
            ),
        ).annotate(
            ranking_confidence_score=Case(
                When(_ranking_primary_votes__gte=threshold, then=Value(1.0)),
                default=F("_ranking_primary_votes") / Value(threshold * 2.0),
                output_field=FloatField(),
            ),
            ranking_quality_score=F("_ranking_primary_quality") * Case(
                When(_ranking_primary_votes__gte=threshold, then=Value(1.0)),
                default=F("_ranking_primary_votes") / Value(threshold * 2.0),
                output_field=FloatField(),
            ),
        )

    def feed_for_user(self, user, include_recommendation_score=True, include_my_rating=True):
        qs = self.with_display_rating().with_ranking_scores().annotate(
            recency_score=Coalesce(Cast("release_year", FloatField()), Value(0.0)) / Value(1000.0),
        )
        if include_my_rating:
            qs = qs.with_my_rating(user)
        if not include_recommendation_score:
            return qs.annotate(
                recommendation_score=(
                    F("ranking_quality_score") * Value(1.20)
                    + F("ranking_confidence_score") * Value(0.80)
                    + F("recency_score") * Value(0.05)
                )
            )

        genre_score_subquery = UserGenrePreference.objects.filter(
            user_id=user.id,
            genre=OuterRef("genre_key"),
        ).values("score")[:1]
        genre_count_subquery = UserGenrePreference.objects.filter(
            user_id=user.id,
            genre=OuterRef("genre_key"),
        ).values("ratings_count")[:1]
        director_score_subquery = UserDirectorPreference.objects.filter(
            user_id=user.id,
            director=OuterRef("director"),
        ).values("score")[:1]
        director_count_subquery = UserDirectorPreference.objects.filter(
            user_id=user.id,
            director=OuterRef("director"),
        ).values("ratings_count")[:1]
        type_score_subquery = UserTypePreference.objects.filter(
            user_id=user.id,
            content_type=OuterRef("type"),
        ).values("score")[:1]
        type_count_subquery = UserTypePreference.objects.filter(
            user_id=user.id,
            content_type=OuterRef("type"),
        ).values("ratings_count")[:1]

        return qs.annotate(
            genre_combo_score=Coalesce(Cast(Subquery(genre_score_subquery), FloatField()), Value(0.0)),
            director_score=Coalesce(Cast(Subquery(director_score_subquery), FloatField()), Value(0.0)),
            type_score=Coalesce(Cast(Subquery(type_score_subquery), FloatField()), Value(0.0)),
            genre_pref_ratings_count=Coalesce(Cast(Subquery(genre_count_subquery), FloatField()), Value(0.0)),
            director_pref_ratings_count=Coalesce(Cast(Subquery(director_count_subquery), FloatField()), Value(0.0)),
            type_pref_ratings_count=Coalesce(Cast(Subquery(type_count_subquery), FloatField()), Value(0.0)),
        ).annotate(
            genre_pref_confidence=Case(
                When(genre_pref_ratings_count__gte=8.0, then=Value(1.0)),
                default=F("genre_pref_ratings_count") / Value(8.0),
                output_field=FloatField(),
            ),
            director_pref_confidence=Case(
                When(director_pref_ratings_count__gte=5.0, then=Value(1.0)),
                default=F("director_pref_ratings_count") / Value(5.0),
                output_field=FloatField(),
            ),
            type_pref_confidence=Case(
                When(type_pref_ratings_count__gte=4.0, then=Value(1.0)),
                default=F("type_pref_ratings_count") / Value(4.0),
                output_field=FloatField(),
            ),
        ).annotate(
            user_affinity_score=(
                F("genre_combo_score") * F("genre_pref_confidence") * Value(0.72)
                + F("type_score") * F("type_pref_confidence") * Value(0.18)
                + F("director_score") * F("director_pref_confidence") * Value(0.10)
            )
        ).annotate(
            recommendation_score=(
                F("user_affinity_score") * Value(0.65)
                + F("ranking_quality_score") * Value(0.45)
                + F("ranking_confidence_score") * Value(0.55)
                + F("recency_score") * Value(0.04)
            )
        )


class Movie(models.Model):
    MOVIE = "movie"
    SERIES = "series"
    TYPE_CHOICES = [
        (MOVIE, "Movie"),
        (SERIES, "Series"),
    ]

    author = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.CASCADE, related_name="movies")
    title_english = models.CharField(max_length=255)
    title_spanish = models.CharField(max_length=255, null=True, blank=True)
    type = models.CharField(max_length=10, choices=TYPE_CHOICES, null=True, blank=True)
    genre = models.CharField(max_length=100, null=True, blank=True)
    genre_key = models.CharField(max_length=100, null=True, blank=True, db_index=True)
    release_year = models.PositiveIntegerField(null=True, blank=True)
    director = models.CharField(max_length=255, null=True, blank=True)
    cast_members = models.TextField(null=True, blank=True)
    synopsis = models.TextField(blank=True, default="")
    external_rating = models.DecimalField(max_digits=3, decimal_places=1, null=True, blank=True)
    external_votes = models.PositiveIntegerField(default=0)
    imdb_id = models.CharField(max_length=20, null=True, blank=True, db_index=True)
    image = models.URLField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    objects = MovieQuerySet.as_manager()

    class Meta:
        ordering = ["-created_at"]
        indexes = [
            models.Index(fields=["title_english", "release_year", "id"], name="movie_title_en_auto_idx"),
            models.Index(fields=["title_spanish", "release_year", "id"], name="movie_title_es_auto_idx"),
            models.Index(fields=["release_year", "id"], name="movie_year_auto_idx"),
        ]

    def save(self, *args, **kwargs):
        self.genre_key = build_genre_key(self.genre)
        super().save(*args, **kwargs)

    def __str__(self):
        if self.release_year:
            return f"{self.title_english} ({self.release_year})"
        return self.title_english

class MovieRating(models.Model):
    user = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.CASCADE, related_name="movie_ratings")
    movie = models.ForeignKey("Movie", on_delete=models.CASCADE, related_name="movie_ratings")
    score = models.PositiveSmallIntegerField(validators=[MinValueValidator(1), MaxValueValidator(10)])
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        constraints = [
            models.UniqueConstraint(fields=["user", "movie"], name="unique_rating_per_user_per_movie")
        ]

    def __str__(self):
        return f"MovieRating(user={self.user_id}, movie={self.movie_id}, score={self.score})"


class MovieListItem(models.Model):
    user = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.CASCADE, related_name="movie_list_items")
    movie = models.ForeignKey("Movie", on_delete=models.CASCADE, related_name="saved_by_users")
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["-created_at", "-id"]
        constraints = [
            models.UniqueConstraint(fields=["user", "movie"], name="unique_movie_list_item_per_user")
        ]

    def __str__(self):
        return f"MovieListItem(user={self.user_id}, movie={self.movie_id})"


class MovieRecommendationItem(models.Model):
    user = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.CASCADE, related_name="movie_recommendation_items")
    movie = models.ForeignKey("Movie", on_delete=models.CASCADE, related_name="recommended_by_users")
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["-created_at", "-id"]
        constraints = [
            models.UniqueConstraint(fields=["user", "movie"], name="unique_movie_recommendation_item_per_user")
        ]

    def __str__(self):
        return f"MovieRecommendationItem(user={self.user_id}, movie={self.movie_id})"


class ProfileFavoriteMovie(models.Model):
    SLOT_1 = 1
    SLOT_2 = 2
    SLOT_3 = 3
    SLOT_CHOICES = [
        (SLOT_1, "Slot 1"),
        (SLOT_2, "Slot 2"),
        (SLOT_3, "Slot 3"),
    ]

    user = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.CASCADE,
        related_name="profile_favorite_movies",
    )
    slot = models.PositiveSmallIntegerField(choices=SLOT_CHOICES)
    movie = models.ForeignKey(
        "Movie",
        on_delete=models.CASCADE,
        related_name="profile_favorite_slots",
    )
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["slot", "id"]
        constraints = [
            models.UniqueConstraint(fields=["user", "slot"], name="unique_profile_favorite_slot_per_user"),
            models.UniqueConstraint(fields=["user", "movie"], name="unique_profile_favorite_movie_per_user"),
        ]

    def __str__(self):
        return f"ProfileFavoriteMovie(user={self.user_id}, slot={self.slot}, movie={self.movie_id})"


class WeeklyRecommendationSnapshot(models.Model):
    week_start = models.DateField()
    week_end = models.DateField()
    items_count = models.PositiveSmallIntegerField(default=0)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["-week_start", "-id"]
        constraints = [
            models.UniqueConstraint(fields=["week_start", "week_end"], name="unique_weekly_recommendation_snapshot_window")
        ]

    def __str__(self):
        return f"WeeklyRecommendationSnapshot({self.week_start} -> {self.week_end})"


class WeeklyRecommendationItem(models.Model):
    snapshot = models.ForeignKey(
        "WeeklyRecommendationSnapshot",
        on_delete=models.CASCADE,
        related_name="items",
    )
    movie = models.ForeignKey("Movie", on_delete=models.CASCADE, related_name="weekly_recommendation_items")
    position = models.PositiveSmallIntegerField()
    genre = models.CharField(max_length=100, null=True, blank=True)
    weekly_score = models.DecimalField(max_digits=6, decimal_places=3)
    week_ratings_count = models.PositiveIntegerField(default=0)
    week_ratings_sum = models.DecimalField(max_digits=8, decimal_places=2, default=0)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["position", "id"]
        constraints = [
            models.UniqueConstraint(fields=["snapshot", "position"], name="unique_weekly_recommendation_position_per_snapshot"),
            models.UniqueConstraint(fields=["snapshot", "movie"], name="unique_weekly_recommendation_movie_per_snapshot"),
        ]

    def __str__(self):
        return f"WeeklyRecommendationItem(snapshot={self.snapshot_id}, movie={self.movie_id}, position={self.position})"


class UserTasteProfile(models.Model):
    user = models.OneToOneField(
        settings.AUTH_USER_MODEL,
        on_delete=models.CASCADE,
        related_name="taste_profile",
    )
    ratings_count = models.PositiveIntegerField(default=0)
    last_updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return f"UserTasteProfile(user={self.user_id}, ratings_count={self.ratings_count})"


class PreferenceDistributionMixin(models.Model):
    count_1 = models.PositiveIntegerField(default=0)
    count_2 = models.PositiveIntegerField(default=0)
    count_3 = models.PositiveIntegerField(default=0)
    count_4 = models.PositiveIntegerField(default=0)
    count_5 = models.PositiveIntegerField(default=0)
    count_6 = models.PositiveIntegerField(default=0)
    count_7 = models.PositiveIntegerField(default=0)
    count_8 = models.PositiveIntegerField(default=0)
    count_9 = models.PositiveIntegerField(default=0)
    count_10 = models.PositiveIntegerField(default=0)
    ratings_count = models.PositiveIntegerField(default=0)
    score = models.DecimalField(max_digits=4, decimal_places=2, default=0)

    class Meta:
        abstract = True

    def get_distribution_counts(self):
        return [
            self.count_1,
            self.count_2,
            self.count_3,
            self.count_4,
            self.count_5,
            self.count_6,
            self.count_7,
            self.count_8,
            self.count_9,
            self.count_10,
        ]

    def recalculate_distribution_metrics(self):
        counts = self.get_distribution_counts()
        total = sum(counts)
        self.ratings_count = total
        if total == 0:
            self.score = 0
            return

        weighted_sum = sum((index + 1) * count for index, count in enumerate(counts))
        self.score = round(weighted_sum / total, 2)

    def save(self, *args, **kwargs):
        self.recalculate_distribution_metrics()
        super().save(*args, **kwargs)


class UserGenrePreference(PreferenceDistributionMixin):
    user = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.CASCADE,
        related_name="genre_preferences",
    )
    genre = models.CharField(max_length=100)

    class Meta:
        constraints = [
            models.UniqueConstraint(fields=["user", "genre"], name="unique_user_genre_preference")
        ]

    def __str__(self):
        return f"UserGenrePreference(user={self.user_id}, genre={self.genre}, score={self.score})"


class UserTypePreference(PreferenceDistributionMixin):
    user = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.CASCADE,
        related_name="type_preferences",
    )
    content_type = models.CharField(max_length=10, choices=Movie.TYPE_CHOICES)

    class Meta:
        constraints = [
            models.UniqueConstraint(fields=["user", "content_type"], name="unique_user_type_preference")
        ]

    def __str__(self):
        return f"UserTypePreference(user={self.user_id}, content_type={self.content_type}, score={self.score})"


class UserDirectorPreference(PreferenceDistributionMixin):
    user = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.CASCADE,
        related_name="director_preferences",
    )
    director = models.CharField(max_length=255)

    class Meta:
        constraints = [
            models.UniqueConstraint(fields=["user", "director"], name="unique_user_director_preference")
        ]

    def __str__(self):
        return f"UserDirectorPreference(user={self.user_id}, director={self.director}, score={self.score})"


class UserDailyFeedPool(models.Model):
    user = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.CASCADE,
        related_name="daily_feed_pools",
    )
    pool_date = models.DateField(db_index=True)
    pool_version = models.CharField(max_length=64, default="v1", db_index=True)
    expires_at = models.DateTimeField()
    rotation_seed = models.PositiveBigIntegerField(default=0)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        constraints = [
            models.UniqueConstraint(fields=["user", "pool_date"], name="unique_user_daily_feed_pool"),
        ]
        indexes = [
            models.Index(fields=["user", "pool_date"]),
            models.Index(fields=["expires_at"]),
        ]

    def __str__(self):
        return f"UserDailyFeedPool(user={self.user_id}, pool_date={self.pool_date})"


class UserDailyFeedCandidate(models.Model):
    pool = models.ForeignKey(
        "UserDailyFeedPool",
        on_delete=models.CASCADE,
        related_name="candidates",
    )
    movie = models.ForeignKey("Movie", on_delete=models.CASCADE, related_name="daily_feed_candidates")
    base_rank = models.PositiveIntegerField(default=0)
    base_score = models.FloatField(default=0.0)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        constraints = [
            models.UniqueConstraint(fields=["pool", "movie"], name="unique_movie_per_daily_pool"),
        ]
        indexes = [
            models.Index(fields=["pool", "base_rank"]),
            models.Index(fields=["pool", "-base_score"]),
            models.Index(fields=["movie"]),
        ]

    def __str__(self):
        return f"UserDailyFeedCandidate(pool={self.pool_id}, movie={self.movie_id}, rank={self.base_rank})"


class Rating(models.Model):
    user = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.CASCADE, related_name="ratings")
    post = models.ForeignKey("Post", on_delete=models.CASCADE, related_name="ratings")
        
    RATING_CHOICES = [
    (1, '1 ⭐'),
    (2, '2 ⭐'),
    (3, '3 ⭐'),
    (4, '4 ⭐'),
    (5, '5 ⭐'),
    (6, '6 ⭐'),
    (7, '7 ⭐'),
    (8, '8 ⭐'),
    (9, '9 ⭐'),
    (10, '10 ⭐'),
    ]
    
    score = models.PositiveSmallIntegerField(choices=RATING_CHOICES)
    created_at = models.DateTimeField(auto_now_add=True)
    
    class Meta:
        constraints = [
            models.UniqueConstraint(fields=["user", "post"], name="unique_rating_per_user_per_post")
        ]

class Follow(models.Model):
    follower = models.ForeignKey(
        User,
        related_name='following',
        on_delete=models.CASCADE
    )
    following = models.ForeignKey(
        User,
        related_name='followers',
        on_delete=models.CASCADE
    )
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        unique_together = ('follower', 'following')
        constraints = [
            models.CheckConstraint(
                condition=~models.Q(follower=models.F("following")),
                name="follow_cannot_follow_self",
            ),
        ]

    def clean(self):
        if self.follower_id and self.following_id and self.follower_id == self.following_id:
            raise ValidationError({"following": "You cannot follow yourself."})

        if self.following_id:
            target_user = self.following if hasattr(self, "following") else User.objects.filter(pk=self.following_id).select_related("profile").first()
            profile = target_user.profile if target_user and hasattr(target_user, "profile") else None
            if profile and profile.visibility == Profile.Visibility.PRIVATE:
                raise ValidationError({"following": "You cannot follow a private profile."})

    def save(self, *args, **kwargs):
        self.full_clean()
        return super().save(*args, **kwargs)

    def __str__(self):
        return f"{self.follower.username} follows {self.following.username}"


class Friendship(models.Model):
    STATUS_PENDING = "pending"
    STATUS_ACCEPTED = "accepted"
    STATUS_REJECTED = "rejected"
    STATUS_CANCELLED = "cancelled"
    STATUS_CHOICES = [
        (STATUS_PENDING, "Pending"),
        (STATUS_ACCEPTED, "Accepted"),
        (STATUS_REJECTED, "Rejected"),
        (STATUS_CANCELLED, "Cancelled"),
    ]

    requester = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.CASCADE,
        related_name="sent_friendship_requests",
    )
    user1 = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.CASCADE,
        related_name="friendships_as_user1",
    )
    user2 = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.CASCADE,
        related_name="friendships_as_user2",
    )
    status = models.CharField(max_length=10, choices=STATUS_CHOICES, default=STATUS_PENDING)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        constraints = [
            models.UniqueConstraint(fields=["user1", "user2"], name="unique_friendship_pair"),
            models.CheckConstraint(
                condition=~models.Q(user1=models.F("user2")),
                name="friendship_users_must_differ",
            ),
        ]
        ordering = ["-updated_at", "-created_at"]

    def clean(self):
        if self.user1_id and self.user2_id and self.user1_id == self.user2_id:
            raise ValidationError("A user cannot be friends with themselves.")

        if self.requester_id not in {self.user1_id, self.user2_id}:
            raise ValidationError({"requester": "Requester must belong to the friendship pair."})

    def save(self, *args, **kwargs):
        if self.user1_id and self.user2_id and self.user1_id > self.user2_id:
            self.user1_id, self.user2_id = self.user2_id, self.user1_id
        self.full_clean()
        return super().save(*args, **kwargs)

    @classmethod
    def between(cls, user_a, user_b):
        if not user_a or not user_b:
            return cls.objects.none()
        user1_id, user2_id = sorted((user_a.id, user_b.id))
        return cls.objects.filter(user1_id=user1_id, user2_id=user2_id)

    def other_user(self, user):
        if user.id == self.user1_id:
            return self.user2
        if user.id == self.user2_id:
            return self.user1
        raise ValidationError("User does not belong to this friendship.")

    @property
    def recipient(self):
        return self.user2 if self.requester_id == self.user1_id else self.user1

    def __str__(self):
        return f"Friendship({self.user1_id}, {self.user2_id}, {self.status})"


class Profile(models.Model):
    class GenderIdentity(models.TextChoices):
        MALE = "male", "Hombre"
        FEMALE = "female", "Mujer"
        NON_BINARY = "non_binary", "No binario"
        PREFER_NOT_TO_SAY = "prefer_not_to_say", "Prefiero no decirlo"

    class Visibility(models.TextChoices):
        PUBLIC = "public", "Public"
        PRIVATE = "private", "Private"

    user = models.OneToOneField(
        settings.AUTH_USER_MODEL,
        on_delete=models.CASCADE,
        related_name="profile"
    )
    bio = models.TextField(blank=True)
    avatar = models.ImageField(upload_to="avatars/", blank=True, null=True)
    birth_date = models.DateField(null=True, blank=True)
    birth_date_locked = models.BooleanField(default=False)
    gender_identity = models.CharField(
        max_length=20,
        choices=GenderIdentity.choices,
        null=True,
        blank=True,
    )
    birth_date_visible = models.BooleanField(default=True)
    gender_identity_visible = models.BooleanField(default=True)
    is_public = models.BooleanField(default=True)
    visibility = models.CharField(
        max_length=10,
        choices=Visibility.choices,
        default=Visibility.PUBLIC,
    )

    def __str__(self):
        return f"Profile({self.user.username})"


class UserVisibilityBlock(models.Model):
    owner = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.CASCADE,
        related_name="visibility_blocks",
    )
    blocked_user = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.CASCADE,
        related_name="blocked_by_visibility_users",
    )
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=["owner", "blocked_user"],
                name="unique_user_visibility_block",
            ),
            models.CheckConstraint(
                condition=~models.Q(owner=models.F("blocked_user")),
                name="user_visibility_block_cannot_block_self",
            ),
        ]
        ordering = ["-created_at", "-id"]

    def clean(self):
        if self.owner_id and self.blocked_user_id and self.owner_id == self.blocked_user_id:
            raise ValidationError({"blocked_user": "You cannot block yourself."})

    def save(self, *args, **kwargs):
        self.full_clean()
        return super().save(*args, **kwargs)



class CommentQuerySet(models.QuerySet):

    def with_reaction_stats(self, user):
        qs = self.annotate(
            likes_count=Count("reactions", filter=Q(reactions__reaction_type=CommentReaction.REACT_LIKE), distinct=True),
            dislikes_count=Count("reactions", filter=Q(reactions__reaction_type=CommentReaction.REACT_DISLIKE), distinct=True),
        )

        if not user or not user.is_authenticated:
            return qs.annotate(my_reaction=Value(None, output_field=CharField()))

        return qs.annotate(
            my_reaction=Subquery(
                CommentReaction.objects.filter(
                    comment_id=OuterRef("pk"),
                    user_id=user.id,
                ).values("reaction_type")[:1]
            )
        )



class Comment(models.Model):
    VISIBILITY_PUBLIC = "public"
    VISIBILITY_MENTIONED = "mentioned"
    VISIBILITY_CHOICES = [
        (VISIBILITY_PUBLIC, "Public"),
        (VISIBILITY_MENTIONED, "Mentioned user only"),
    ]

    author = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.CASCADE, related_name="comments")
    movie = models.ForeignKey("Movie", on_delete=models.CASCADE, related_name="comments")
    target_user = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.SET_NULL,
        related_name="targeted_comments",
        null=True,
        blank=True,
    )
    body = models.TextField()
    visibility = models.CharField(
        max_length=20,
        choices=VISIBILITY_CHOICES,
        default=VISIBILITY_PUBLIC,
    )
    is_read = models.BooleanField(default=False)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    objects = CommentQuerySet.as_manager()
    mention_pattern = re.compile(r"(?<!\w)@(?P<username>[\w.@+-]+)")

    class Meta:
        ordering = ["-created_at"]

    def clean(self):
        super().clean()
        if self.visibility == self.VISIBILITY_MENTIONED and not self.target_user_id:
            raise ValidationError({"target_user": "Target user is required for mentioned comments."})

    def save(self, *args, **kwargs):
        if not self.visibility:
            self.visibility = self.VISIBILITY_PUBLIC
        return super().save(*args, **kwargs)

    def __str__(self):
        return f"Comment({self.author_id} -> {self.movie_id})"

    def has_valid_target_mention(self):
        """
        Valida que el texto incluya una mención explícita al username del target_user.
        Se usa para filtrar registros legacy inconsistentes.
        """
        if self.visibility != self.VISIBILITY_MENTIONED:
            return False
        if not self.target_user_id or not getattr(self, "target_user", None):
            return False
        if not self.body:
            return False

        target_username = self.target_user.username.lower()
        for match in self.mention_pattern.finditer(self.body):
            if match.group("username").lower() == target_username:
                return True
        return False

class CommentReaction(models.Model):
    REACT_LIKE = "like"
    REACT_DISLIKE = "dislike"
    REACTION_CHOICES = [
        (REACT_LIKE, "Like"),
        (REACT_DISLIKE, "Dislike"),
    ]

    comment = models.ForeignKey("Comment", on_delete=models.CASCADE, related_name="reactions")
    user = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.CASCADE, related_name="comment_reactions")
    reaction_type = models.CharField(max_length=10, choices=REACTION_CHOICES)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        constraints = [
            models.UniqueConstraint(fields=["comment", "user"], name="unique_comment_reaction_per_user")
        ]

    def __str__(self):
        return f"CommentReaction(comment={self.comment_id}, user={self.user_id}, reaction={self.reaction_type})"


class UserNotification(models.Model):
    TYPE_PRIVATE_MESSAGE = "private_message"
    TYPE_PUBLIC_COMMENT_REACTION = "public_comment_reaction"
    TYPE_PRIVATE_COMMENT_REACTION = "private_comment_reaction"
    TYPE_CHOICES = [
        (TYPE_PRIVATE_MESSAGE, "Private message"),
        (TYPE_PUBLIC_COMMENT_REACTION, "Public comment reaction"),
        (TYPE_PRIVATE_COMMENT_REACTION, "Private comment reaction"),
    ]

    TARGET_ACTIVITY = "activity"
    TARGET_PRIVATE_INBOX = "private_inbox"
    TARGET_TAB_CHOICES = [
        (TARGET_ACTIVITY, "Activity"),
        (TARGET_PRIVATE_INBOX, "Private inbox"),
    ]

    recipient = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.CASCADE,
        related_name="notifications_received",
    )
    actor = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.CASCADE,
        related_name="notifications_triggered",
        null=True,
        blank=True,
    )
    comment = models.ForeignKey(
        "Comment",
        on_delete=models.CASCADE,
        related_name="notifications",
        null=True,
        blank=True,
    )
    movie = models.ForeignKey(
        "Movie",
        on_delete=models.CASCADE,
        related_name="notifications",
        null=True,
        blank=True,
    )
    type = models.CharField(max_length=40, choices=TYPE_CHOICES)
    target_tab = models.CharField(max_length=20, choices=TARGET_TAB_CHOICES)
    reaction_type = models.CharField(
        max_length=10,
        choices=CommentReaction.REACTION_CHOICES,
        null=True,
        blank=True,
    )
    is_read = models.BooleanField(default=False)
    read_at = models.DateTimeField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["-created_at", "-id"]
        constraints = [
            models.UniqueConstraint(
                fields=["recipient", "actor", "comment", "type"],
                name="unique_user_notification_per_actor_comment_type",
            ),
        ]

    def __str__(self):
        return f"UserNotification({self.type} -> {self.recipient_id})"


class AppBranding(models.Model):
    app_name = models.CharField(max_length=120, default="MiAppSocialMovies")
    default_logo = models.ImageField(upload_to="branding/", blank=True, null=True)
    login_logo = models.ImageField(upload_to="branding/", blank=True, null=True)
    signup_logo = models.ImageField(upload_to="branding/", blank=True, null=True)
    feed_logo = models.ImageField(upload_to="branding/", blank=True, null=True)
    movie_detail_logo = models.ImageField(upload_to="branding/", blank=True, null=True)
    profile_feed_logo = models.ImageField(upload_to="branding/", blank=True, null=True)
    visited_profile_logo = models.ImageField(upload_to="branding/", blank=True, null=True)
    personal_data_logo = models.ImageField(upload_to="branding/", blank=True, null=True)
    privacy_security_logo = models.ImageField(upload_to="branding/", blank=True, null=True)
    updated_at = models.DateTimeField(auto_now=True)
    is_active = models.BooleanField(default=True)

    class Meta:
        verbose_name = "App Branding"
        verbose_name_plural = "App Branding"
        constraints = [
            models.UniqueConstraint(
                fields=["is_active"],
                condition=models.Q(is_active=True),
                name="unique_active_app_branding",
            ),
        ]
        ordering = ["-is_active", "-updated_at", "-id"]

    def __str__(self):
        status = "active" if self.is_active else "inactive"
        return f"AppBranding({self.app_name} - {status})"

    def clean(self):
        super().clean()
        if self.pk is None and AppBranding.objects.exists():
            raise ValidationError("Only one App Branding configuration is allowed.")

    def save(self, *args, **kwargs):
        self.full_clean()
        result = super().save(*args, **kwargs)
        cache.delete("app_branding_active_v1")
        return result
