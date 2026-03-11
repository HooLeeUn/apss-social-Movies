from django.conf import settings
from django.db import models
from django.contrib.auth.models import User
from django.core.validators import MinValueValidator, MaxValueValidator
from django.db.models import Avg, Count, OuterRef, Subquery, IntegerField, FloatField, Case, When, F, Value
from django.db.models.functions import Cast
from django.db.models import Q
from django.db.models.functions import Coalesce

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
    def with_rating_stats(self):
        return self.annotate(
            real_ratings_count=Count("movie_ratings", distinct=True),
            real_ratings_avg=Avg("movie_ratings__score"),
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
        return self.with_rating_stats().annotate(
            _external_rating_float=Cast("external_rating", FloatField()),
            _real_ratings_avg_float=Coalesce(F("real_ratings_avg"), Value(0.0), output_field=FloatField()),
            _external_rating_for_mix=Coalesce(Cast("external_rating", FloatField()), Value(0.0), output_field=FloatField()),
        ).annotate(
            display_rating=Case(
                When(real_ratings_count=0, then=F("_external_rating_float")),
                When(real_ratings_count__gte=100, then=F("_real_ratings_avg_float")),
                default=(
                    (
                        F("_external_rating_for_mix") * (Value(100.0) - Cast(F("real_ratings_count"), FloatField()))
                    ) + (
                        F("_real_ratings_avg_float") * Cast(F("real_ratings_count"), FloatField())
                    )
                ) / Value(100.0),
                output_field=FloatField(),
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
    release_year = models.PositiveIntegerField(null=True, blank=True)
    director = models.CharField(max_length=255, null=True, blank=True)
    cast_members = models.TextField(null=True, blank=True)
    external_rating = models.DecimalField(max_digits=3, decimal_places=1, null=True, blank=True)
    image = models.URLField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    objects = MovieQuerySet.as_manager()

    class Meta:
        ordering = ["-created_at"]

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
