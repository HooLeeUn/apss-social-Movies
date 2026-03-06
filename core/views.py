from django.contrib.auth.models import User
from django.contrib.auth import get_user_model
from django.db.models import Count, Avg
from rest_framework.generics import RetrieveAPIView, ListAPIView
from rest_framework import generics, permissions, status
from rest_framework.authtoken.models import Token
from rest_framework.parsers import MultiPartParser, FormParser, JSONParser
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework.permissions import IsAuthenticated, AllowAny
from .serializers import (
    UserProfileSerializer, MeSerializer, UserMiniSerializer,
    PostListSerializer, PostCreateSerializer, PostDetailSerializer,
    PostWriteSerializer, CommentSerializer,
)
from .models import Post, Rating, Follow, Comment
from .permissions import IsAuthorOrReadOnly, IsCommentAuthorOrReadOnly
from django.shortcuts import get_object_or_404

User = get_user_model()


class RegisterView(APIView):
    permission_classes = [AllowAny]

    def post(self, request):
        username = request.data.get("username")
        email = request.data.get("email")
        password = request.data.get("password")

        errors = {}
        if not username:
            errors["username"] = ["This field is required."]
        if not email:
            errors["email"] = ["This field is required."]
        if not password:
            errors["password"] = ["This field is required."]

        if errors:
            return Response(errors, status=status.HTTP_400_BAD_REQUEST)

        if User.objects.filter(username=username).exists():
            return Response(
                {"username": ["A user with that username already exists."]},
                status=status.HTTP_400_BAD_REQUEST,
            )

        if User.objects.filter(email=email).exists():
            return Response(
                {"email": ["A user with that email already exists."]},
                status=status.HTTP_400_BAD_REQUEST,
            )

        user = User.objects.create_user(
            username=username,
            email=email,
            password=password,
        )
        token, created = Token.objects.get_or_create(user=user)

        return Response(
            {
                "user": {
                    "id": user.id,
                    "username": user.username,
                    "email": user.email,
                },
                "token": token.key,
            },
            status=status.HTTP_201_CREATED,
        )

class UserProfileView(RetrieveAPIView):
    serializer_class = UserProfileSerializer
    lookup_field = "username"
    queryset = (
        User.objects
        .select_related("profile")
        .annotate(
            followers_count=Count("followers", distinct=True),
            following_count=Count("following", distinct=True),
            posts_count=Count("posts", distinct=True),
            avg_post_rating=Avg("posts__ratings__score"),
        )
    )

class MeView(generics.RetrieveUpdateAPIView):
    permission_classes = [permissions.IsAuthenticated]
    serializer_class = MeSerializer
    parser_classes = [JSONParser, MultiPartParser, FormParser]  # para avatar

    def get_object(self):
        # Traemos el User autenticado pero anotado con stats (1 query)
        return (
            User.objects
            .select_related("profile")
            .annotate(
                followers_count=Count("followers", distinct=True),
                following_count=Count("following", distinct=True),
                posts_count=Count("posts", distinct=True),
                avg_post_rating=Avg("posts__ratings__score"), 
            )
            .get(pk=self.request.user.pk)
        )

    def get_queryset(self):
        return User.objects.all()
    
class FollowToggleView(APIView):
    permission_classes = [IsAuthenticated]

    def post(self, request, username):
        """Seguir a username"""
        target = User.objects.filter(username=username).first()
        if not target:
            return Response({"detail": "User not found."}, status=status.HTTP_404_NOT_FOUND)

        if target == request.user:
            return Response({"detail": "You cannot follow yourself."}, status=status.HTTP_400_BAD_REQUEST)

        obj, created = Follow.objects.get_or_create(
            follower=request.user,
            following=target
        )

        # counts actualizados
        return Response({
            "following": True,
            "created": created,
            "followers_count": target.followers.count(),
            "following_count": target.following.count(),
        }, status=status.HTTP_201_CREATED if created else status.HTTP_200_OK)

    def delete(self, request, username):
        """Dejar de seguir a username"""
        target = User.objects.filter(username=username).first()
        if not target:
            return Response({"detail": "User not found."}, status=status.HTTP_404_NOT_FOUND)

        deleted, _ = Follow.objects.filter(
            follower=request.user,
            following=target
        ).delete()

        return Response({
            "following": False,
            "deleted": deleted > 0,
            "followers_count": target.followers.count(),
            "following_count": target.following.count(),
        }, status=status.HTTP_200_OK)

class UserFollowersListView(ListAPIView):
    permission_classes = [AllowAny]
    serializer_class = UserMiniSerializer

    def get_queryset(self):
        username = self.kwargs["username"]
        follower_ids = Follow.objects.filter(
            following__username=username
        ).values_list("follower_id", flat=True)

        return (
            User.objects
            .filter(following__following__username=username)  # users que siguen a <username>
            .select_related("profile")
            .order_by("username")
            .distinct()
        )

class UserFollowingListView(ListAPIView):
    permission_classes = [AllowAny]
    serializer_class = UserMiniSerializer

    def get_queryset(self):
        username = self.kwargs["username"]
        following_ids = Follow.objects.filter(
            follower__username=username
        ).values_list("following_id", flat=True)

        return (
            User.objects
            .filter(followers__follower__username=username)  # users que <username> está siguiendo
            .select_related("profile")
            .order_by("username")
            .distinct()
        )
        
class FeedFollowingView(ListAPIView):
    """
    Posts de gente que sigo (según Follow), ordenados por -created_at.
    """
    permission_classes = [permissions.IsAuthenticated]
    serializer_class = PostListSerializer

    def get_queryset(self):
        qs = (
            Post.objects.feed_following(self.request.user)
            .with_rating_stats()
            .with_comment_stats()
            .select_related("author", "author__profile")
            .order_by("-created_at")
        )
        return qs.with_my_rating(self.request.user)

class DiscoverView(ListAPIView):
    """
    Posts globales ordenados por mejor promedio.
    """
    permission_classes = [AllowAny]
    serializer_class = PostListSerializer

    def get_queryset(self):
        qs = (
            Post.objects.feed_discover()
            .select_related("author", "author__profile")
        )
        return qs.with_my_rating(self.request.user)
    
class PostListCreateView(generics.ListCreateAPIView):
    permission_classes = [permissions.IsAuthenticatedOrReadOnly]
    parser_classes = [JSONParser, MultiPartParser, FormParser]

    def get_queryset(self):
        qs = (
            Post.objects.all()
            .with_rating_stats()
            .with_comment_stats()
            .select_related("author", "author__profile")
            .order_by("-created_at")
        )
        return qs.with_my_rating(self.request.user)

    def get_serializer_class(self):
        return PostCreateSerializer if self.request.method == "POST" else PostListSerializer
    def perform_create(self, serializer):
        serializer.save(author=self.request.user)


class PostDetailView(generics.RetrieveUpdateDestroyAPIView):
    permission_classes = [permissions.IsAuthenticatedOrReadOnly, IsAuthorOrReadOnly]
    parser_classes = [JSONParser, MultiPartParser, FormParser]
    serializer_class = PostDetailSerializer

    def get_queryset(self):
        qs = (
            Post.objects.all()
            .with_rating_stats()
            .with_comment_stats()
            .select_related("author", "author__profile")
        )
        return qs.with_my_rating(self.request.user)
    
    def get_serializer_class(self):
        if self.request.method in ["PATCH", "PUT"]:
            return PostWriteSerializer
        return PostDetailSerializer


class PostRatingView(APIView):
    permission_classes = [permissions.IsAuthenticated]

    def put(self, request, pk):
        post = get_object_or_404(Post, pk=pk)

        score = request.data.get("score")
        try:
            score = int(score)
        except (TypeError, ValueError):
            return Response({"score": "Debe ser un entero 1..10"}, status=status.HTTP_400_BAD_REQUEST)

        if score not in (1, 2, 3, 4, 5, 6, 7, 8, 9, 10):
            return Response({"score": "Debe estar entre 1 y 10"}, status=status.HTTP_400_BAD_REQUEST)

        rating, created = Rating.objects.update_or_create(
            user=request.user,
            post=post,
            defaults={"score": score},
        )

        return Response(
            {"post": post.id, "my_rating": rating.score, "created": created},
            status=status.HTTP_200_OK,
        )

    def delete(self, request, pk):
        post = get_object_or_404(Post, pk=pk)
        Rating.objects.filter(user=request.user, post=post).delete()
        return Response(status=status.HTTP_204_NO_CONTENT)
    


class PostCommentsListCreateView(generics.ListCreateAPIView):
    serializer_class = CommentSerializer

    def get_permissions(self):
        if self.request.method == "POST":
            return [permissions.IsAuthenticated()]
        return [permissions.AllowAny()]

    def get_queryset(self):
        return (
            Comment.objects.filter(post_id=self.kwargs["pk"])
            .select_related("author", "author__profile", "post")
            .order_by("-created_at")
        )

    def perform_create(self, serializer):
        post = get_object_or_404(Post, pk=self.kwargs["pk"])
        serializer.save(author=self.request.user, post=post)


class CommentDetailView(generics.RetrieveUpdateDestroyAPIView):
    permission_classes = [permissions.IsAuthenticatedOrReadOnly, IsCommentAuthorOrReadOnly]
    serializer_class = CommentSerializer
    http_method_names = ["get", "put", "delete", "head", "options"]

    def get_queryset(self):
        return Comment.objects.select_related("author", "author__profile", "post")

class UserPostsListView(generics.ListAPIView):
    permission_classes = [permissions.AllowAny]
    serializer_class = PostListSerializer

    def get_queryset(self):
        user = get_object_or_404(User, username=self.kwargs["username"])
        
        qs = (
            Post.objects.filter(author=user)
            .with_rating_stats()
            .with_comment_stats()
            .select_related("author", "author__profile")
            .order_by("-created_at")
        )
        return qs.with_my_rating(self.request.user)
            
