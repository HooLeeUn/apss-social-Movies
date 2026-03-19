from django.conf import settings
from django.db import models
from django.contrib.auth.models import User
from django.core.validators import MinValueValidator, MaxValueValidator
from django.db.models import Avg, Count, OuterRef, Subquery, IntegerField, FloatField, Case, When, F, Value
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

    def feed_for_user(self, user, include_recommendation_score=True):
        qs = self.with_display_rating().with_ranking_scores().with_my_rating(user)
        if not include_recommendation_score:
            return qs

        genre_score_subquery = UserGenrePreference.objects.filter(
            user_id=user.id,
            genre=OuterRef("genre_key"),
        ).values("score")[:1]
        director_score_subquery = UserDirectorPreference.objects.filter(
            user_id=user.id,
            director=OuterRef("director"),
        ).values("score")[:1]
        type_score_subquery = UserTypePreference.objects.filter(
            user_id=user.id,
            content_type=OuterRef("type"),
        ).values("score")[:1]

        return qs.annotate(
            genre_combo_score=Coalesce(Cast(Subquery(genre_score_subquery), FloatField()), Value(0.0)),
            director_score=Coalesce(Cast(Subquery(director_score_subquery), FloatField()), Value(0.0)),
            type_score=Coalesce(Cast(Subquery(type_score_subquery), FloatField()), Value(0.0)),
            popularity_score=Case(
                When(real_ratings_count__gte=20, then=Value(10.0)),
                When(real_ratings_count__gte=10, then=Value(7.0)),
                When(real_ratings_count__gte=5, then=Value(4.0)),
                default=Value(0.0),
                output_field=FloatField(),
            ),
        ).annotate(
            recommendation_score=(
                F("genre_combo_score") * Value(0.60)
                + F("director_score") * Value(0.20)
                + F("type_score") * Value(0.10)
                + Coalesce(F("display_rating"), Value(0.0), output_field=FloatField()) * Value(0.05)
                + F("popularity_score") * Value(0.05)
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

    def __str__(self):
        return f"{self.follower.username} follows {self.following.username}"
    
class Profile(models.Model):
    user = models.OneToOneField(
        settings.AUTH_USER_MODEL,
        on_delete=models.CASCADE,
        related_name="profile"
    )
    bio = models.TextField(blank=True)
    avatar = models.ImageField(upload_to="avatars/", blank=True, null=True)

    def __str__(self):
        return f"Profile({self.user.username})"


class Comment(models.Model):
    author = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.CASCADE, related_name="comments")
    post = models.ForeignKey("Post", on_delete=models.CASCADE, related_name="comments")
    body = models.TextField()
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["-created_at"]

    def __str__(self):
        return f"Comment({self.author_id} -> {self.post_id})"
