from django.contrib import admin
from django.db.models import Avg, Count
from .models import Post, Rating, Follow, Comment


@admin.register(Post)
class PostAdmin(admin.ModelAdmin):
    list_display = ("id", "author", "short_text", "avg_rating", "ratings_count", "created_at")
    list_filter = ("created_at", "author")
    search_fields = ("text", "author__username")

    def short_text(self, obj):
        return obj.text[:40]
    short_text.short_description = "Texto"

    def avg_rating(self, obj):
        return round(obj.avg_rating or 0, 2)
    avg_rating.short_description = "Promedio"

    def ratings_count(self, obj):
        return obj.ratings_count
    ratings_count.short_description = "# Ratings"

    def get_queryset(self, request):
        qs = super().get_queryset(request)
        return qs.annotate(
            avg_rating=Avg("ratings__score"),
            ratings_count=Count("ratings")
        )


@admin.register(Rating)
class RatingAdmin(admin.ModelAdmin):
    list_display = ("id", "user", "post", "score", "created_at")
    list_filter = ("score", "created_at")
    search_fields = ("user__username", "post__text")


@admin.register(Follow)
class FollowAdmin(admin.ModelAdmin):
    list_display = ("id", "follower", "following", "created_at")
    search_fields = ("follower__username", "following__username")

@admin.register(Comment)
class CommentAdmin(admin.ModelAdmin):
    list_display = ("id", "post", "author", "created_at")
    search_fields = ("body", "author__username")
    list_filter = ("created_at",)