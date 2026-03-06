from django.contrib.auth.models import User
from django.contrib.auth.validators import UnicodeUsernameValidator
from rest_framework import serializers
from rest_framework.validators import UniqueValidator
from django.db.models import Avg, Count
from .models import Post, Rating, Comment

# Importas tus modelos solo si los necesitas aquí.
# OJO: para esta versión no necesitas Avg ni consultas en serializer,
# porque los stats vienen por annotate() desde la vista.
from .models import Profile


class RegisterSerializer(serializers.ModelSerializer):
    username = serializers.CharField(
        min_length=8,
        help_text="Minimum 8 characters. Letters, digits and @/./+/-/_ only.",
        validators=[
            UnicodeUsernameValidator(),
            UniqueValidator(queryset=User.objects.all()),
        ],
    )
    email = serializers.EmailField(
        required=True,
        validators=[UniqueValidator(queryset=User.objects.all())],
    )
    password = serializers.CharField(write_only=True, min_length=6)
    password_confirmation = serializers.CharField(write_only=True, min_length=6)

    class Meta:
        model = User
        fields = ["id", "username", "email", "password", "password_confirmation"]

    def validate(self, attrs):
        if attrs["password"] != attrs["password_confirmation"]:
            raise serializers.ValidationError({"password": "Passwords do not match."})

        domain = attrs["email"].split("@")[-1].lower()
        parts = domain.split(".")
        reserved_domains = {"example.com", "test.com", "invalid", "localhost"}
        if (
            domain in reserved_domains
            or len(parts) < 2
            or not all(part.isalnum() or "-" in part for part in parts)
            or len(parts[-1]) < 2
        ):
            raise serializers.ValidationError(
                {"email": "This email domain does not appear to exist."}
            )

        return attrs

    def create(self, validated_data):
        validated_data.pop("password_confirmation", None)
        return User.objects.create_user(**validated_data)


class UserProfileSerializer(serializers.ModelSerializer):
    bio = serializers.CharField(source="profile.bio", read_only=True)
    avatar = serializers.SerializerMethodField()
        
    followers_count = serializers.IntegerField(read_only=True)
    following_count = serializers.IntegerField(read_only=True)
    posts_count = serializers.IntegerField(read_only=True)
    avg_post_rating = serializers.FloatField(read_only=True)
    is_following = serializers.SerializerMethodField()

    class Meta:
        model = User
        fields = [
            "id", "username",
            "bio", "avatar",
            "followers_count", "following_count",
            "posts_count", "avg_post_rating",
            "is_following",
        ]
        
    def get_is_following(self, obj):
        request = self.context.get("request")
        if not request or not request.user.is_authenticated:
            return False
        return obj.followers.filter(follower=request.user).exists()
    
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
    
    # read-only stats (vienen anotados en la vista)
    followers_count = serializers.IntegerField(read_only=True)
    following_count = serializers.IntegerField(read_only=True)
    posts_count = serializers.IntegerField(read_only=True)
    avg_post_rating = serializers.FloatField(read_only=True)

    class Meta:
        model = User
        fields = [
            "id", "username","email",
            "bio", "avatar",
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
        fields = ["content"]  # ajusta a tus campos editables

class CommentSerializer(serializers.ModelSerializer):
    author = UserMiniSerializer(read_only=True)

    class Meta:
        model = Comment
        fields = ["id", "author", "post", "body", "created_at"]
        read_only_fields = ["id", "author", "post", "created_at"]
