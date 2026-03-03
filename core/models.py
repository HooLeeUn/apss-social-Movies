from django.conf import settings
from django.db import models
from django.contrib.auth.models import User
from django.db.models import Avg, Count, OuterRef, Subquery, IntegerField
from django.db.models.functions import Cast
from django.db.models import Q

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
        return self.annotate(
            avg_rating=Avg('ratings__score'),
            ratings_count=Count('ratings', distinct=True)
        ).filter(
            ratings_count__gt=0
        ).order_by(
            '-avg_rating',
            '-ratings_count',
            '-created_at'
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

class Rating(models.Model):
    user = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.CASCADE, related_name="ratings")
    post = models.ForeignKey("Post", on_delete=models.CASCADE, related_name="ratings")
        
    RATING_CHOICES = [
    (1, '1 ⭐'),
    (2, '2 ⭐⭐'),
    (3, '3 ⭐⭐⭐'),
    (4, '4 ⭐⭐⭐⭐'),
    (5, '5 ⭐⭐⭐⭐⭐'),
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
