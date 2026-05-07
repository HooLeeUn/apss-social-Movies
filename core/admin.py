from django.contrib import admin
from django import forms
from django.utils.html import format_html
from django.db.models import Avg, Count
from .models import (
    AppBranding,
    Post,
    Rating,
    Follow,
    Friendship,
    Comment,
    Movie,
    MovieRating,
    Profile,
    UserVisibilityBlock,
    UserTasteProfile,
    UserGenrePreference,
    UserTypePreference,
    UserDirectorPreference,
)


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


@admin.register(Friendship)
class FriendshipAdmin(admin.ModelAdmin):
    list_display = ("id", "requester", "user1", "user2", "status", "created_at", "updated_at")
    list_filter = ("status", "created_at", "updated_at")
    search_fields = ("requester__username", "user1__username", "user2__username")
    list_select_related = ("requester", "user1__profile", "user2__profile")

@admin.register(Comment)
class CommentAdmin(admin.ModelAdmin):
    list_display = ("id", "movie", "author", "visibility", "target_user", "created_at", "updated_at")
    search_fields = ("body", "author__username", "target_user__username", "movie__title_english", "movie__title_spanish")
    list_filter = ("visibility", "created_at")
    autocomplete_fields = ("movie", "author", "target_user")
    list_select_related = ("movie", "author", "target_user")


@admin.register(Movie)
class MovieAdmin(admin.ModelAdmin):
    list_display = ("id", "title_english", "title_spanish", "genre", "release_year", "author", "external_rating", "external_votes", "created_at")
    list_filter = ("genre", "release_year", "created_at", "author")
    search_fields = ("id", "title_english", "title_spanish", "director", "genre", "cast_members")
    readonly_fields = ("created_at", "updated_at")
    fieldsets = (
        (
            "Información principal",
            {
                "fields": (
                    "author",
                    "title_english",
                    "title_spanish",
                    "type",
                    "genre",
                    "release_year",
                    "director",
                    "cast_members",
                    "synopsis",
                )
            },
        ),
        (
            "Datos externos",
            {
                "fields": (
                    "external_rating",
                    "external_votes",
                    "imdb_id",
                    "image",
                )
            },
        ),
        ("Auditoría", {"fields": ("created_at", "updated_at")}),
    )


class MovieRatingAdminForm(forms.ModelForm):
    class Meta:
        model = MovieRating
        fields = "__all__"
        widgets = {
            "score": forms.NumberInput(attrs={"min": 1, "max": 10, "step": 1}),
        }
        help_texts = {
            "score": "Calificación de 1 a 10.",
        }


@admin.register(MovieRating)
class MovieRatingAdmin(admin.ModelAdmin):
    form = MovieRatingAdminForm
    list_display = ("id", "user", "movie", "movie_genre", "score", "created_at", "updated_at")
    list_filter = ("score", "created_at", "updated_at")
    search_fields = ("user__username", "movie__title_english", "movie__title_spanish")
    autocomplete_fields = ("user", "movie")
    list_select_related = ("movie", "user")
    ordering = ("-created_at",)

    @admin.display(description="Género")
    def movie_genre(self, obj):
        return obj.movie.genre or "—"


@admin.register(UserTasteProfile)
class UserTasteProfileAdmin(admin.ModelAdmin):
    list_display = ("id", "user", "ratings_count", "last_updated_at")
    search_fields = ("user__username",)
    list_filter = ("last_updated_at",)
    autocomplete_fields = ("user",)
    list_select_related = ("user",)


class PreferenceDistributionAdmin(admin.ModelAdmin):
    list_display = (
        "id",
        "user",
        "ratings_count",
        "score",
        "count_1",
        "count_10",
    )
    list_filter = ("score", "ratings_count")
    search_fields = ("user__username",)
    autocomplete_fields = ("user",)
    list_select_related = ("user",)


@admin.register(UserGenrePreference)
class UserGenrePreferenceAdmin(PreferenceDistributionAdmin):
    list_display = ("id", "user", "genre", "ratings_count", "score", "count_1", "count_10")
    search_fields = ("user__username", "genre")
    list_filter = ("genre", "score", "ratings_count")


@admin.register(UserTypePreference)
class UserTypePreferenceAdmin(PreferenceDistributionAdmin):
    list_display = ("id", "user", "content_type", "ratings_count", "score", "count_1", "count_10")
    search_fields = ("user__username", "content_type")
    list_filter = ("content_type", "score", "ratings_count")


@admin.register(UserDirectorPreference)
class UserDirectorPreferenceAdmin(PreferenceDistributionAdmin):
    list_display = ("id", "user", "director", "ratings_count", "score", "count_1", "count_10")
    search_fields = ("user__username", "director")
    list_filter = ("director", "score", "ratings_count")


@admin.register(Profile)
class ProfileAdmin(admin.ModelAdmin):
    list_display = ("id", "user", "is_public", "visibility", "friend_requests_restricted")
    search_fields = ("user__username",)
    list_filter = ("is_public", "visibility", "friend_requests_restricted")
    autocomplete_fields = ("user",)


@admin.register(UserVisibilityBlock)
class UserVisibilityBlockAdmin(admin.ModelAdmin):
    list_display = ("id", "owner", "blocked_user", "created_at")
    search_fields = ("owner__username", "blocked_user__username")
    autocomplete_fields = ("owner", "blocked_user")


@admin.register(AppBranding)
class AppBrandingAdmin(admin.ModelAdmin):
    list_display = ("id", "app_name", "is_active", "updated_at")
    list_filter = ("is_active", "updated_at")
    search_fields = ("app_name",)
    readonly_fields = (
        "updated_at",
        "default_logo_preview",
        "login_logo_preview",
        "signup_logo_preview",
        "feed_logo_preview",
        "movie_detail_logo_preview",
        "profile_feed_logo_preview",
        "visited_profile_logo_preview",
        "personal_data_logo_preview",
        "privacy_security_logo_preview",
    )
    fieldsets = (
        ("General", {"fields": ("app_name", "is_active", "updated_at")}),
        ("Default logo", {"fields": ("default_logo", "default_logo_preview")}),
        (
            "Screen logos",
            {
                "fields": (
                    ("login_logo", "login_logo_preview"),
                    ("signup_logo", "signup_logo_preview"),
                    ("feed_logo", "feed_logo_preview"),
                    ("movie_detail_logo", "movie_detail_logo_preview"),
                    ("profile_feed_logo", "profile_feed_logo_preview"),
                    ("visited_profile_logo", "visited_profile_logo_preview"),
                    ("personal_data_logo", "personal_data_logo_preview"),
                    ("privacy_security_logo", "privacy_security_logo_preview"),
                )
            },
        ),
    )


    def has_add_permission(self, request):
        has_permission = super().has_add_permission(request)
        if not has_permission:
            return False
        return not AppBranding.objects.exists()

    def _render_image_preview(self, image_field):
        if not image_field:
            return "—"
        return format_html(
            '<img src="{}" style="max-height: 64px; max-width: 220px; border-radius: 8px;" />',
            image_field.url,
        )

    @admin.display(description="Preview")
    def default_logo_preview(self, obj):
        return self._render_image_preview(obj.default_logo)

    @admin.display(description="Preview")
    def login_logo_preview(self, obj):
        return self._render_image_preview(obj.login_logo)

    @admin.display(description="Preview")
    def signup_logo_preview(self, obj):
        return self._render_image_preview(obj.signup_logo)

    @admin.display(description="Preview")
    def feed_logo_preview(self, obj):
        return self._render_image_preview(obj.feed_logo)

    @admin.display(description="Preview")
    def movie_detail_logo_preview(self, obj):
        return self._render_image_preview(obj.movie_detail_logo)

    @admin.display(description="Preview")
    def profile_feed_logo_preview(self, obj):
        return self._render_image_preview(obj.profile_feed_logo)

    @admin.display(description="Preview")
    def visited_profile_logo_preview(self, obj):
        return self._render_image_preview(obj.visited_profile_logo)

    @admin.display(description="Preview")
    def personal_data_logo_preview(self, obj):
        return self._render_image_preview(obj.personal_data_logo)

    @admin.display(description="Preview")
    def privacy_security_logo_preview(self, obj):
        return self._render_image_preview(obj.privacy_security_logo)
