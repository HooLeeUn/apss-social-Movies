import socket

from django.contrib.auth.models import User
from django.contrib.auth.validators import UnicodeUsernameValidator
from rest_framework import serializers
from django.db.models import Avg, Count
from rest_framework.validators import UniqueValidator
from .models import (
    Comment,
    CommentReaction,
    Friendship,
    Movie,
    Post,
    Rating,
    UserDirectorPreference,
    UserGenrePreference,
    UserTasteProfile,
    UserTypePreference,
    WeeklyRecommendationItem,
)

# Importas tus modelos solo si los necesitas aquí.
# OJO: para esta versión no necesitas Avg ni consultas en serializer,
# porque los stats vienen por annotate() desde la vista.
from .models import Follow, Profile


class UserProfileSerializer(serializers.ModelSerializer):
    bio = serializers.CharField(source="profile.bio", read_only=True)
    avatar = serializers.SerializerMethodField()
    is_public = serializers.BooleanField(source="profile.is_public", read_only=True)

    followers_count = serializers.IntegerField(read_only=True)
    following_count = serializers.IntegerField(read_only=True)
    posts_count = serializers.IntegerField(read_only=True)
    avg_post_rating = serializers.FloatField(read_only=True)
    is_following = serializers.SerializerMethodField()
    friendship_status = serializers.SerializerMethodField()
    can_follow = serializers.SerializerMethodField()
    can_send_friend_request = serializers.SerializerMethodField()

    class Meta:
        model = User
        fields = [
            "id", "username",
            "bio", "avatar", "is_public",
            "followers_count", "following_count",
            "posts_count", "avg_post_rating",
            "is_following", "friendship_status",
            "can_follow", "can_send_friend_request",
        ]
        
    def _get_friendship(self, obj):
        request = self.context.get("request")
        if not request or not request.user.is_authenticated or obj == request.user:
            return None
        return Friendship.between(request.user, obj).first()

    def get_is_following(self, obj):
        request = self.context.get("request")
        if not request or not request.user.is_authenticated:
            return False
        return obj.followers.filter(follower=request.user).exists()

    def get_friendship_status(self, obj):
        request = self.context.get("request")
        if not request or not request.user.is_authenticated:
            return "none"
        if obj == request.user:
            return "self"

        friendship = self._get_friendship(obj)
        if not friendship:
            return "none"
        if friendship.status == Friendship.STATUS_ACCEPTED:
            return Friendship.STATUS_ACCEPTED
        if friendship.status == Friendship.STATUS_PENDING:
            return "pending_sent" if friendship.requester_id == request.user.id else "pending_received"
        return "none"

    def get_can_follow(self, obj):
        request = self.context.get("request")
        if not request or not request.user.is_authenticated or obj == request.user:
            return False
        profile = obj.profile if hasattr(obj, "profile") else None
        if profile and not profile.is_public:
            return False
        return not Follow.objects.filter(follower=request.user, following=obj).exists()

    def get_can_send_friend_request(self, obj):
        request = self.context.get("request")
        if not request or not request.user.is_authenticated or obj == request.user:
            return False

        friendship = self._get_friendship(obj)
        if not friendship:
            return True
        return friendship.status in {Friendship.STATUS_REJECTED, Friendship.STATUS_CANCELLED}

    def get_avatar(self, obj):
        if hasattr(obj, "profile") and obj.profile.avatar:
            request = self.context.get("request")
            url = obj.profile.avatar.url
            return request.build_absolute_uri(url) if request else url
        return None

class MeSerializer(serializers.ModelSerializer):
    # editable (escribe en Profile)
    email = serializers.EmailField(read_only=True)
    bio = serializers.CharField(source="profile.bio", required=False, allow_blank=True)
    avatar = serializers.ImageField(source="profile.avatar", required=False)
    is_public = serializers.BooleanField(source="profile.is_public", required=False)
    
    # read-only stats (vienen anotados en la vista)
    followers_count = serializers.IntegerField(read_only=True)
    following_count = serializers.IntegerField(read_only=True)
    posts_count = serializers.IntegerField(read_only=True)
    avg_post_rating = serializers.FloatField(read_only=True)

    class Meta:
        model = User
        fields = [
            "id", "username","email",
            "bio", "avatar", "is_public",
            "followers_count", "following_count",
            "posts_count", "avg_post_rating",
        ]

    def update(self, instance, validated_data):
        # Solo actualizamos datos del Profile
        profile_data = validated_data.pop("profile", {})

        # Asegura que existe profile (por si acaso)
        profile, _ = Profile.objects.get_or_create(user=instance)

        if "bio" in profile_data:
            profile.bio = profile_data["bio"]

        if "avatar" in profile_data:
            profile.avatar = profile_data["avatar"]

        if "is_public" in profile_data:
            profile.is_public = profile_data["is_public"]

        profile.save()
        return instance

class UserMiniSerializer(serializers.ModelSerializer):
    bio = serializers.CharField(source="profile.bio", read_only=True)
    avatar = serializers.SerializerMethodField()
    
    class Meta:
        model = User
        fields = ["id", "username", "bio", "avatar"]
    
    def get_avatar(self, obj):
        # si tienes Profile con related_name="profile"
        if hasattr(obj, "profile") and obj.profile.avatar:
            request = self.context.get("request")
            url = obj.profile.avatar.url
            return request.build_absolute_uri(url) if request else url
        return None
    
class PostListSerializer(serializers.ModelSerializer):
    author = UserMiniSerializer(read_only=True)
    avg_rating = serializers.FloatField(read_only=True)
    ratings_count = serializers.IntegerField(read_only=True)
    comments_count = serializers.IntegerField(read_only=True)
    my_rating = serializers.IntegerField(read_only=True)

    class Meta:
        model = Post
        fields = ["id", "author", "text", "image", "created_at", "avg_rating", "ratings_count", "comments_count", "my_rating"]

    
class PostCreateSerializer(serializers.ModelSerializer):
    class Meta:
        model = Post
        fields = ["text", "image"]


class PostDetailSerializer(serializers.ModelSerializer):
    author = UserMiniSerializer(read_only=True)
    avg_rating = serializers.FloatField(read_only=True)
    ratings_count = serializers.IntegerField(read_only=True)
    comments_count = serializers.IntegerField(read_only=True)
    my_rating = serializers.IntegerField(read_only=True)

    class Meta:
        model = Post
        fields = ["id", "author", "text", "image", "created_at", "avg_rating", "ratings_count", "comments_count", "my_rating"]

    
class PostWriteSerializer(serializers.ModelSerializer):
    class Meta:
        model = Post
        fields = ["text", "image"]


class RegisterSerializer(serializers.ModelSerializer):
    username = serializers.CharField(
        min_length=8,
        validators=[
            UnicodeUsernameValidator(),
            UniqueValidator(
                queryset=User.objects.all(),
                message="A user with that username already exists.",
            ),
        ],
        help_text="At least 8 characters. Letters, digits and @/./+/-/_ only.",
    )
    email = serializers.EmailField(
        validators=[
            UniqueValidator(
                queryset=User.objects.all(),
                message="A user with that email already exists.",
            )
        ]
    )
    password = serializers.CharField(write_only=True, min_length=6)
    password_confirmation = serializers.CharField(write_only=True, min_length=6)

    class Meta:
        model = User
        fields = ["id", "username", "email", "password", "password_confirmation"]
        read_only_fields = ["id"]

    def validate_email(self, value):
        domain = value.split("@")[-1]
        try:
            socket.getaddrinfo(domain, None)
        except socket.gaierror:
            raise serializers.ValidationError("This email domain does not appear to exist")

        return value

    def validate(self, attrs):
        if attrs.get("password") != attrs.get("password_confirmation"):
            raise serializers.ValidationError({"password": "Passwords do not match."})
        return attrs

    def create(self, validated_data):
        validated_data.pop("password_confirmation", None)
        return User.objects.create_user(**validated_data)

class CommentSerializer(serializers.ModelSerializer):
    author = UserMiniSerializer(read_only=True)
    target_user = serializers.PrimaryKeyRelatedField(read_only=True)
    likes_count = serializers.IntegerField(read_only=True)
    dislikes_count = serializers.IntegerField(read_only=True)
    my_reaction = serializers.CharField(read_only=True, allow_null=True)

    class Meta:
        model = Comment
        fields = [
            "id", "author", "movie", "target_user", "body", "visibility",
            "created_at", "updated_at", "likes_count", "dislikes_count", "my_reaction",
        ]
        read_only_fields = [
            "id", "author", "movie", "target_user", "visibility",
            "created_at", "updated_at", "likes_count", "dislikes_count", "my_reaction",
        ]


class CommentReactionSerializer(serializers.ModelSerializer):
    class Meta:
        model = CommentReaction
        fields = ["reaction_type"]


class MovieListSerializer(serializers.ModelSerializer):
    author = UserMiniSerializer(read_only=True)
    real_ratings_count = serializers.IntegerField(read_only=True)
    real_ratings_avg = serializers.FloatField(read_only=True)
    display_rating = serializers.FloatField(read_only=True)
    my_rating = serializers.IntegerField(read_only=True)
    comments_count = serializers.IntegerField(read_only=True)

    class Meta:
        model = Movie
        fields = [
            "id", "author",
            "title_english", "title_spanish",
            "type", "genre", "release_year",
            "director", "cast_members", "synopsis",
            "image", "external_rating", "external_votes",
            "real_ratings_count", "real_ratings_avg",
            "display_rating", "my_rating", "comments_count",
        ]


class MovieRatingSerializer(serializers.Serializer):
    score = serializers.IntegerField(min_value=1, max_value=10)


class WeeklyRecommendationMovieSerializer(serializers.ModelSerializer):
    class Meta:
        model = Movie
        fields = [
            "id",
            "title_english",
            "title_spanish",
            "type",
            "genre",
            "release_year",
            "director",
            "image",
        ]


class WeeklyRecommendationItemSerializer(serializers.ModelSerializer):
    movie = WeeklyRecommendationMovieSerializer(read_only=True)
    weekly_score = serializers.FloatField(read_only=True)
    general_rating = serializers.FloatField(read_only=True)
    display_rating = serializers.FloatField(read_only=True)
    my_rating = serializers.IntegerField(read_only=True, allow_null=True)
    following_avg_rating = serializers.FloatField(read_only=True, allow_null=True)

    class Meta:
        model = WeeklyRecommendationItem
        fields = [
            "position",
            "movie",
            "weekly_score",
            "general_rating",
            "display_rating",
            "my_rating",
            "following_avg_rating",
        ]


class TastePreferenceBaseSerializer(serializers.ModelSerializer):
    class Meta:
        fields = [
            "score",
            "ratings_count",
            "count_1",
            "count_2",
            "count_3",
            "count_4",
            "count_5",
            "count_6",
            "count_7",
            "count_8",
            "count_9",
            "count_10",
        ]


class UserGenrePreferenceSerializer(TastePreferenceBaseSerializer):
    name = serializers.CharField(source="genre", read_only=True)

    class Meta(TastePreferenceBaseSerializer.Meta):
        model = UserGenrePreference
        fields = ["name", *TastePreferenceBaseSerializer.Meta.fields]


class UserTypePreferenceSerializer(TastePreferenceBaseSerializer):
    name = serializers.CharField(source="content_type", read_only=True)

    class Meta(TastePreferenceBaseSerializer.Meta):
        model = UserTypePreference
        fields = ["name", *TastePreferenceBaseSerializer.Meta.fields]


class UserDirectorPreferenceSerializer(TastePreferenceBaseSerializer):
    name = serializers.CharField(source="director", read_only=True)

    class Meta(TastePreferenceBaseSerializer.Meta):
        model = UserDirectorPreference
        fields = ["name", *TastePreferenceBaseSerializer.Meta.fields]


class UserTasteProfileInspectSerializer(serializers.ModelSerializer):
    genre_preferences = serializers.SerializerMethodField()
    type_preferences = serializers.SerializerMethodField()
    director_preferences = serializers.SerializerMethodField()

    def get_genre_preferences(self, obj):
        preferences = obj.user.genre_preferences.order_by("-score", "-ratings_count", "genre")
        return UserGenrePreferenceSerializer(preferences, many=True).data

    def get_type_preferences(self, obj):
        preferences = obj.user.type_preferences.order_by("-score", "-ratings_count", "content_type")
        return UserTypePreferenceSerializer(preferences, many=True).data

    def get_director_preferences(self, obj):
        preferences = obj.user.director_preferences.order_by("-score", "-ratings_count", "director")
        return UserDirectorPreferenceSerializer(preferences, many=True).data

    class Meta:
        model = UserTasteProfile
        fields = [
            "ratings_count",
            "last_updated_at",
            "genre_preferences",
            "type_preferences",
            "director_preferences",
        ]


class FriendshipUserSerializer(serializers.ModelSerializer):
    bio = serializers.CharField(source="profile.bio", read_only=True)
    avatar = serializers.SerializerMethodField()
    is_public = serializers.BooleanField(source="profile.is_public", read_only=True)

    class Meta:
        model = User
        fields = ["id", "username", "bio", "avatar", "is_public"]

    def get_avatar(self, obj):
        if hasattr(obj, "profile") and obj.profile.avatar:
            request = self.context.get("request")
            url = obj.profile.avatar.url
            return request.build_absolute_uri(url) if request else url
        return None


class FriendshipSerializer(serializers.ModelSerializer):
    requester = serializers.CharField(source="requester.username", read_only=True)
    recipient = serializers.SerializerMethodField()
    user = serializers.SerializerMethodField()

    class Meta:
        model = Friendship
        fields = ["id", "status", "requester", "recipient", "user", "created_at", "updated_at"]

    def get_recipient(self, obj):
        return obj.recipient.username

    def get_user(self, obj):
        request = self.context.get("request")
        if not request or not request.user.is_authenticated:
            return None
        return FriendshipUserSerializer(obj.other_user(request.user), context=self.context).data
