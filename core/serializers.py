from datetime import date

from django.conf import settings
from django.contrib.auth.models import User
from django.contrib.auth.validators import UnicodeUsernameValidator
from rest_framework import serializers
from rest_framework.validators import UniqueValidator
from .models import (
    Comment,
    CommentReaction,
    Friendship,
    Movie,
    Post,
    UserVisibilityBlock,
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


def calculate_age_from_birth_date(birth_date):
    if not birth_date:
        return None

    today = date.today()
    years = today.year - birth_date.year
    if (today.month, today.day) < (birth_date.month, birth_date.day):
        years -= 1
    return years


class UserProfileSerializer(serializers.ModelSerializer):
    first_name = serializers.CharField(read_only=True)
    last_name = serializers.CharField(read_only=True)
    bio = serializers.CharField(source="profile.bio", read_only=True)
    avatar = serializers.SerializerMethodField()
    age = serializers.SerializerMethodField()
    gender_identity = serializers.SerializerMethodField()
    is_public = serializers.BooleanField(source="profile.is_public", read_only=True)
    visibility = serializers.CharField(source="profile.visibility", read_only=True)

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
            "first_name", "last_name",
            "bio", "avatar", "is_public", "visibility",
            "age", "gender_identity",
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
        if profile and profile.visibility == Profile.Visibility.PRIVATE:
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

    def get_age(self, obj):
        profile = getattr(obj, "profile", None)
        if not profile or not profile.birth_date_visible:
            return None
        return calculate_age_from_birth_date(profile.birth_date)

    def get_gender_identity(self, obj):
        profile = getattr(obj, "profile", None)
        if not profile or not profile.gender_identity_visible:
            return None
        return profile.gender_identity

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
            profile.visibility = Profile.Visibility.PUBLIC if profile.is_public else Profile.Visibility.PRIVATE

        profile.save()
        return instance


class PersonalDataSerializer(serializers.ModelSerializer):
    email = serializers.EmailField(required=False)
    first_name = serializers.CharField(required=False, allow_blank=False)
    last_name = serializers.CharField(required=False, allow_blank=False)
    birth_date = serializers.DateField(source="profile.birth_date", required=False, allow_null=True)
    birth_date_locked = serializers.BooleanField(source="profile.birth_date_locked", read_only=True)
    birth_date_visible = serializers.BooleanField(source="profile.birth_date_visible", required=False)
    gender_identity = serializers.ChoiceField(
        source="profile.gender_identity",
        choices=Profile.GenderIdentity.choices,
        required=False,
        allow_null=True,
    )
    gender_identity_visible = serializers.BooleanField(source="profile.gender_identity_visible", required=False)
    avatar = serializers.ImageField(source="profile.avatar", required=False, allow_null=True)
    age = serializers.SerializerMethodField(read_only=True)

    class Meta:
        model = User
        fields = [
            "first_name",
            "last_name",
            "email",
            "birth_date",
            "age",
            "birth_date_locked",
            "birth_date_visible",
            "gender_identity",
            "gender_identity_visible",
            "avatar",
        ]

    def validate_birth_date(self, value):
        if value and value > date.today():
            raise serializers.ValidationError("Birth date cannot be in the future.")
        return value

    def validate(self, attrs):
        profile_data = attrs.get("profile", {})
        birth_date = profile_data.get("birth_date")
        profile = getattr(self.instance, "profile", None) if self.instance else None

        if birth_date is not None and profile and profile.birth_date_locked:
            if profile.birth_date != birth_date:
                raise serializers.ValidationError(
                    {"birth_date": "Birth date is locked and cannot be modified."}
                )
        return attrs

    def get_age(self, obj):
        profile = getattr(obj, "profile", None)
        birth_date = getattr(profile, "birth_date", None)
        return calculate_age_from_birth_date(birth_date)

    def update(self, instance, validated_data):
        profile_data = validated_data.pop("profile", {})
        profile, _ = Profile.objects.get_or_create(user=instance)

        user_fields_to_update = []
        for field in ["first_name", "last_name", "email"]:
            if field in validated_data:
                setattr(instance, field, validated_data[field])
                user_fields_to_update.append(field)
        if user_fields_to_update:
            instance.save(update_fields=user_fields_to_update)

        if "birth_date" in profile_data and not profile.birth_date_locked:
            profile.birth_date = profile_data["birth_date"]
            if profile.birth_date is not None:
                profile.birth_date_locked = True

        for field in ["birth_date_visible", "gender_identity", "gender_identity_visible", "avatar"]:
            if field in profile_data:
                setattr(profile, field, profile_data[field])

        profile.save()
        return instance


class PrivacySettingsSerializer(serializers.ModelSerializer):
    visibility = serializers.ChoiceField(choices=Profile.Visibility.choices)

    class Meta:
        model = Profile
        fields = ["visibility"]

    def update(self, instance, validated_data):
        visibility = validated_data.get("visibility")
        previous_visibility = instance.visibility
        if visibility is not None:
            instance.is_public = visibility == Profile.Visibility.PUBLIC
        instance = super().update(instance, validated_data)

        if (
            visibility == Profile.Visibility.PRIVATE
            and previous_visibility == Profile.Visibility.PUBLIC
        ):
            Follow.objects.filter(following=instance.user).delete()

        return instance


class UserVisibilityBlockSerializer(serializers.ModelSerializer):
    id = serializers.IntegerField(source="blocked_user_id", read_only=True)
    username = serializers.CharField(source="blocked_user.username", read_only=True)

    class Meta:
        model = UserVisibilityBlock
        fields = ["id", "username", "created_at"]


class CreateUserVisibilityBlockSerializer(serializers.Serializer):
    user_id = serializers.IntegerField()

    def validate_user_id(self, value):
        request = self.context["request"]
        if value == request.user.id:
            raise serializers.ValidationError("You cannot block yourself.")
        if not User.objects.filter(id=value).exists():
            raise serializers.ValidationError("User not found.")
        return value

    def create(self, validated_data):
        request = self.context["request"]
        block, _ = UserVisibilityBlock.objects.get_or_create(
            owner=request.user,
            blocked_user_id=validated_data["user_id"],
        )
        return block

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


class UserMiniWithFollowersCountSerializer(UserMiniSerializer):
    followers_count = serializers.IntegerField(read_only=True)

    class Meta(UserMiniSerializer.Meta):
        fields = UserMiniSerializer.Meta.fields + ["followers_count"]


class UserSearchSerializer(serializers.ModelSerializer):
    class Meta:
        model = User
        fields = ["id", "username"]




class SocialActivityActorSerializer(serializers.Serializer):
    id = serializers.IntegerField()
    username = serializers.CharField()
    avatar = serializers.CharField(allow_null=True)


class SocialActivityMovieSerializer(serializers.Serializer):
    id = serializers.IntegerField()
    title_english = serializers.CharField()
    title_spanish = serializers.CharField(allow_null=True)
    release_year = serializers.IntegerField(allow_null=True)
    image = serializers.CharField(allow_null=True)
    type = serializers.CharField(allow_null=True)
    genre = serializers.CharField(allow_null=True)
    display_rating = serializers.FloatField(allow_null=True)
    my_rating = serializers.IntegerField(allow_null=True)
    following_avg_rating = serializers.FloatField(allow_null=True)
    following_ratings_count = serializers.IntegerField()


class SocialActivitySerializer(serializers.Serializer):
    id = serializers.CharField()
    activity_type = serializers.ChoiceField(choices=[
        "rating",
        "public_comment",
        "directed_comment",
        "public_comment_like",
        "public_comment_dislike",
    ])
    type = serializers.SerializerMethodField()
    created_at = serializers.DateTimeField()
    actor = serializers.SerializerMethodField()
    movie = serializers.SerializerMethodField()
    payload = serializers.DictField()
    score = serializers.SerializerMethodField()
    target_user = serializers.SerializerMethodField()
    comment_text = serializers.SerializerMethodField()
    comment_id = serializers.SerializerMethodField()

    def get_type(self, obj):
        mapping = {
            "rating": "rating",
            "public_comment": "comment",
            "directed_comment": "comment",
            "public_comment_like": "like",
            "public_comment_dislike": "dislike",
        }
        return mapping.get(obj.get("activity_type"), obj.get("activity_type"))

    def get_score(self, obj):
        return (obj.get("payload") or {}).get("score")

    def get_target_user(self, obj):
        payload = obj.get("payload") or {}
        return payload.get("target_user") or payload.get("comment_author")

    def get_comment_text(self, obj):
        payload = obj.get("payload") or {}
        return payload.get("content") or payload.get("comment_excerpt")

    def get_comment_id(self, obj):
        return (obj.get("payload") or {}).get("comment_id")

    def get_actor(self, obj):
        actor = obj.get("actor") or {}
        avatar = actor.get("avatar")
        return {
            "id": actor.get("id"),
            "username": actor.get("username"),
            "avatar": self._build_absolute_media_url(avatar),
        }

    def get_movie(self, obj):
        movie = obj.get("movie") or {}
        return {
            "id": movie.get("id"),
            "title_english": movie.get("title_english"),
            "title_spanish": movie.get("title_spanish"),
            "release_year": movie.get("release_year"),
            "image": self._build_absolute_media_url(movie.get("image")),
            "type": movie.get("type"),
            "genre": movie.get("genre"),
            "display_rating": movie.get("display_rating"),
            "my_rating": movie.get("my_rating"),
            "following_avg_rating": movie.get("following_avg_rating"),
            "following_ratings_count": movie.get("following_ratings_count", 0),
        }

    def _build_absolute_media_url(self, value):
        if not value:
            return None
        request = self.context.get("request")
        if request and isinstance(value, str) and value.startswith("/"):
            return request.build_absolute_uri(value)
        return value

class FriendMentionSerializer(serializers.ModelSerializer):
    avatar = serializers.SerializerMethodField()

    class Meta:
        model = User
        fields = ["id", "username", "avatar"]

    def get_avatar(self, obj):
        if hasattr(obj, "profile") and obj.profile.avatar:
            request = self.context.get("request")
            url = obj.profile.avatar.url
            return request.build_absolute_uri(url) if request else url
        return None


class SocialListUserSerializer(serializers.ModelSerializer):
    display_name = serializers.SerializerMethodField()
    avatar_url = serializers.SerializerMethodField()
    followers_count = serializers.IntegerField(read_only=True)

    class Meta:
        model = User
        fields = ["id", "username", "display_name", "avatar_url", "followers_count"]

    def get_display_name(self, obj):
        profile = getattr(obj, "profile", None)
        display_name = getattr(profile, "display_name", None)
        return display_name or obj.username

    def get_avatar_url(self, obj):
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
    first_name = serializers.CharField(required=True, allow_blank=False)
    last_name = serializers.CharField(required=True, allow_blank=False)
    birth_date = serializers.DateField(required=True)

    class Meta:
        model = User
        fields = [
            "id",
            "username",
            "first_name",
            "last_name",
            "email",
            "password",
            "password_confirmation",
            "birth_date",
        ]
        read_only_fields = ["id"]


    def validate(self, attrs):
        birth_date = attrs.get("birth_date")
        if birth_date > date.today():
            raise serializers.ValidationError(
                {"birth_date": "Birth date cannot be in the future."}
            )

        age = calculate_age_from_birth_date(birth_date)
        if age is not None and age < 13:
            raise serializers.ValidationError(
                {"birth_date": "Debes tener al menos 13 años para registrarte."}
            )

        if attrs.get("password") != attrs.get("password_confirmation"):
            raise serializers.ValidationError({"password": "Passwords do not match."})
        return attrs

    def create(self, validated_data):
        birth_date = validated_data.pop("birth_date")
        validated_data.pop("password_confirmation", None)
        user = User.objects.create_user(**validated_data)
        profile, _ = Profile.objects.get_or_create(user=user)
        profile.birth_date = birth_date
        profile.birth_date_locked = True
        profile.save(update_fields=["birth_date", "birth_date_locked"])
        return user

class CommentSerializer(serializers.ModelSerializer):
    author = UserMiniSerializer(read_only=True)
    target_user = serializers.PrimaryKeyRelatedField(read_only=True)
    mentioned_username = serializers.CharField(write_only=True, required=False, allow_blank=True)
    recipient_username = serializers.CharField(write_only=True, required=False, allow_blank=True)
    likes_count = serializers.IntegerField(read_only=True)
    dislikes_count = serializers.IntegerField(read_only=True)
    my_reaction = serializers.CharField(read_only=True, allow_null=True)

    class Meta:
        model = Comment
        fields = [
            "id", "author", "movie", "target_user", "body", "visibility",
            "mentioned_username", "recipient_username",
            "created_at", "updated_at", "likes_count", "dislikes_count", "my_reaction",
        ]
        read_only_fields = [
            "id", "author", "movie", "target_user", "visibility",
            "created_at", "updated_at", "likes_count", "dislikes_count", "my_reaction",
        ]

    def create(self, validated_data):
        validated_data.pop("mentioned_username", None)
        validated_data.pop("recipient_username", None)
        return super().create(validated_data)


class PublicCommentFeedSerializer(CommentSerializer):
    author_followers_count = serializers.IntegerField(read_only=True)
    is_following_author = serializers.BooleanField(read_only=True)
    is_friend_author = serializers.BooleanField(read_only=True)

    class Meta(CommentSerializer.Meta):
        fields = [
            *CommentSerializer.Meta.fields,
            "author_followers_count",
            "is_following_author",
            "is_friend_author",
        ]
        read_only_fields = [
            *CommentSerializer.Meta.read_only_fields,
            "author_followers_count",
            "is_following_author",
            "is_friend_author",
        ]


class CommentReactionSerializer(serializers.Serializer):
    reaction = serializers.ChoiceField(
        choices=[CommentReaction.REACT_LIKE, CommentReaction.REACT_DISLIKE],
    )

    def to_internal_value(self, data):
        payload = dict(data)
        if "reaction" not in payload and "reaction_type" in payload:
            payload["reaction"] = payload["reaction_type"]
        return super().to_internal_value(payload)


class MovieListSerializer(serializers.ModelSerializer):
    author = UserMiniSerializer(read_only=True)
    real_ratings_count = serializers.IntegerField(read_only=True)
    real_ratings_avg = serializers.FloatField(read_only=True)
    display_rating = serializers.FloatField(read_only=True)
    general_rating = serializers.FloatField(read_only=True)
    my_rating = serializers.IntegerField(read_only=True)
    following_avg_rating = serializers.FloatField(read_only=True, allow_null=True)
    following_ratings_count = serializers.IntegerField(read_only=True)
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
            "display_rating", "general_rating", "my_rating", "following_avg_rating", "following_ratings_count", "comments_count",
        ]


class MovieRatingSerializer(serializers.Serializer):
    score = serializers.IntegerField(min_value=1, max_value=10)


class ProfileFavoriteMovieSerializer(serializers.ModelSerializer):
    display_rating = serializers.FloatField(read_only=True)
    general_rating = serializers.FloatField(read_only=True)
    following_avg_rating = serializers.FloatField(read_only=True, allow_null=True)
    following_ratings_count = serializers.IntegerField(read_only=True)
    my_rating = serializers.IntegerField(read_only=True, allow_null=True)

    class Meta:
        model = Movie
        fields = [
            "id",
            "title_english",
            "title_spanish",
            "image",
            "release_year",
            "genre",
            "type",
            "display_rating",
            "general_rating",
            "following_avg_rating",
            "following_ratings_count",
            "my_rating",
        ]


class ProfileFavoriteSlotSerializer(serializers.Serializer):
    slot = serializers.IntegerField(min_value=1, max_value=3)
    movie = ProfileFavoriteMovieSerializer(allow_null=True)


class ProfileFavoriteSlotWriteSerializer(serializers.Serializer):
    movie_id = serializers.PrimaryKeyRelatedField(
        source="movie",
        queryset=Movie.objects.all(),
    )


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
    following_ratings_count = serializers.IntegerField(read_only=True)
    top_user = serializers.SerializerMethodField()

    def _serialize_top_user_avatar(self, avatar_value):
        if not avatar_value:
            return None

        if hasattr(avatar_value, "url"):
            return avatar_value.url

        avatar_path = str(avatar_value).strip()
        if not avatar_path:
            return None
        if avatar_path.startswith(("http://", "https://", "/")):
            return avatar_path

        media_url = settings.MEDIA_URL or "/media/"
        if not media_url.endswith("/"):
            media_url = f"{media_url}/"
        return f"{media_url}{avatar_path.lstrip('/')}"

    def get_top_user(self, obj):
        if getattr(obj, "top_user_id", None) is None:
            return None
        return {
            "id": obj.top_user_id,
            "username": obj.top_user_username,
            "avatar": self._serialize_top_user_avatar(getattr(obj, "top_user_avatar", None)),
            "followers_count": obj.top_user_followers_count or 0,
        }

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
            "following_ratings_count",
            "top_user",
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
