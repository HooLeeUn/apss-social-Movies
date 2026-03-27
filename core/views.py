import re
from django.contrib.auth.models import User
from django.contrib.auth import get_user_model
from django.db import transaction
from django.db.models import Case, Count, Avg, Exists, F, FloatField, IntegerField, OuterRef, Q, Subquery, Value, When
from rest_framework.generics import RetrieveAPIView, ListAPIView
from rest_framework import generics, permissions, status
from rest_framework.authtoken.models import Token
from rest_framework.parsers import MultiPartParser, FormParser, JSONParser
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework.exceptions import PermissionDenied
from rest_framework.permissions import IsAuthenticated, AllowAny
from .serializers import (
    FriendMentionSerializer, FriendshipSerializer, UserProfileSerializer, MeSerializer, UserMiniSerializer,
    PostListSerializer, PostCreateSerializer, PostDetailSerializer,
    PostWriteSerializer, CommentReactionSerializer, CommentSerializer, PublicCommentFeedSerializer, RegisterSerializer, MovieListSerializer,
    MovieRatingSerializer, UserTasteProfileInspectSerializer, WeeklyRecommendationItemSerializer,
)
from .models import (
    Comment,
    CommentReaction,
    Follow,
    Friendship,
    Movie,
    MovieRating,
    Post,
    Rating,
    UserTasteProfile,
    WeeklyRecommendationItem,
    WeeklyRecommendationSnapshot,
)
from .permissions import IsAuthorOrReadOnly, IsCommentAuthorOrReadOnly
from .weekly_recommendations import get_previous_closed_week_window
from django.core.exceptions import ValidationError as DjangoValidationError
from django.shortcuts import get_object_or_404

User = get_user_model()


def split_search_terms(search):
    return [term for term in re.split(r"\s+", search.strip()) if term]


def apply_movie_search(queryset, search):
    terms = split_search_terms(search)
    if not terms:
        return queryset

    search_fields = [
        "title_english",
        "title_spanish",
        "director",
        "cast_members",
        "genre",
        "synopsis",
    ]

    filters = Q()
    score_expr = Value(0, output_field=IntegerField())
    for term in terms:
        term_match = Q()
        for field in search_fields:
            lookup = {f"{field}__icontains": term}
            term_match |= Q(**lookup)
            score_expr += Case(
                When(**lookup, then=Value(1)),
                default=Value(0),
                output_field=IntegerField(),
            )
        filters &= term_match

    return queryset.filter(filters).annotate(search_relevance=score_expr)


def annotate_comments_for_user(queryset, user):
    return queryset.with_reaction_stats(user)


def can_access_directed_comment_reactions(user, comment):
    if comment.visibility != Comment.VISIBILITY_MENTIONED:
        return True
    if not user or not user.is_authenticated:
        return False
    return user.id in {comment.author_id, comment.target_user_id}



class RegisterView(generics.CreateAPIView):
    queryset = User.objects.all()
    serializer_class = RegisterSerializer
    permission_classes = [AllowAny]

    def create(self, request, *args, **kwargs):
        serializer = self.get_serializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        user = serializer.save()
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
        target = User.objects.filter(username=username).select_related("profile").first()
        if not target:
            return Response({"detail": "User not found."}, status=status.HTTP_404_NOT_FOUND)

        if target == request.user:
            return Response({"detail": "You cannot follow yourself."}, status=status.HTTP_400_BAD_REQUEST)

        profile = target.profile if hasattr(target, "profile") else None
        if profile and not profile.is_public:
            return Response({"detail": "You cannot follow a private profile."}, status=status.HTTP_400_BAD_REQUEST)

        try:
            obj, created = Follow.objects.get_or_create(
                follower=request.user,
                following=target,
            )
        except DjangoValidationError as exc:
            return Response({"detail": exc.message_dict if hasattr(exc, "message_dict") else exc.messages}, status=status.HTTP_400_BAD_REQUEST)

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
        
class FriendshipRequestCreateView(APIView):
    permission_classes = [IsAuthenticated]

    def post(self, request, username):
        target = User.objects.filter(username=username).select_related("profile").first()
        if not target:
            return Response({"detail": "User not found."}, status=status.HTTP_404_NOT_FOUND)
        if target == request.user:
            return Response({"detail": "You cannot send a friendship request to yourself."}, status=status.HTTP_400_BAD_REQUEST)

        friendship = Friendship.between(request.user, target).first()
        if friendship is None:
            friendship = Friendship.objects.create(
                requester=request.user,
                user1=request.user,
                user2=target,
                status=Friendship.STATUS_PENDING,
            )
            serializer = FriendshipSerializer(friendship, context={"request": request})
            return Response(serializer.data, status=status.HTTP_201_CREATED)

        if friendship.status == Friendship.STATUS_ACCEPTED:
            return Response({"detail": "You are already friends."}, status=status.HTTP_400_BAD_REQUEST)

        if friendship.status == Friendship.STATUS_PENDING:
            return Response({"detail": "A friendship request is already pending."}, status=status.HTTP_400_BAD_REQUEST)

        friendship.requester = request.user
        friendship.status = Friendship.STATUS_PENDING
        friendship.save()
        serializer = FriendshipSerializer(friendship, context={"request": request})
        return Response(serializer.data, status=status.HTTP_200_OK)


class FriendshipRequestAcceptView(APIView):
    permission_classes = [IsAuthenticated]

    def post(self, request, pk):
        friendship = get_object_or_404(Friendship.objects.select_related("user1", "user2", "requester"), pk=pk)
        if friendship.status != Friendship.STATUS_PENDING:
            return Response({"detail": "Only pending friendship requests can be accepted."}, status=status.HTTP_400_BAD_REQUEST)
        if friendship.requester_id == request.user.id:
            return Response({"detail": "You cannot accept your own friendship request."}, status=status.HTTP_400_BAD_REQUEST)
        if friendship.other_user(friendship.requester).id != request.user.id:
            return Response({"detail": "You cannot accept this friendship request."}, status=status.HTTP_403_FORBIDDEN)

        friendship.status = Friendship.STATUS_ACCEPTED
        friendship.save()
        return Response(FriendshipSerializer(friendship, context={"request": request}).data, status=status.HTTP_200_OK)


class FriendshipRequestRejectView(APIView):
    permission_classes = [IsAuthenticated]

    def post(self, request, pk):
        friendship = get_object_or_404(Friendship.objects.select_related("user1", "user2", "requester"), pk=pk)
        if friendship.status != Friendship.STATUS_PENDING:
            return Response({"detail": "Only pending friendship requests can be rejected."}, status=status.HTTP_400_BAD_REQUEST)
        if friendship.requester_id == request.user.id:
            return Response({"detail": "You cannot reject your own friendship request."}, status=status.HTTP_400_BAD_REQUEST)
        if friendship.other_user(friendship.requester).id != request.user.id:
            return Response({"detail": "You cannot reject this friendship request."}, status=status.HTTP_403_FORBIDDEN)

        friendship.status = Friendship.STATUS_REJECTED
        friendship.save()
        return Response(FriendshipSerializer(friendship, context={"request": request}).data, status=status.HTTP_200_OK)


class FriendshipRequestCancelView(APIView):
    permission_classes = [IsAuthenticated]

    def post(self, request, pk):
        friendship = get_object_or_404(Friendship.objects.select_related("user1", "user2", "requester"), pk=pk)
        if friendship.status != Friendship.STATUS_PENDING:
            return Response({"detail": "Only pending friendship requests can be cancelled."}, status=status.HTTP_400_BAD_REQUEST)
        if friendship.requester_id != request.user.id:
            return Response({"detail": "You can only cancel requests you sent."}, status=status.HTTP_403_FORBIDDEN)

        friendship.status = Friendship.STATUS_CANCELLED
        friendship.save()
        return Response(FriendshipSerializer(friendship, context={"request": request}).data, status=status.HTTP_200_OK)


class FriendshipDeleteView(APIView):
    permission_classes = [IsAuthenticated]

    def delete(self, request, username):
        target = User.objects.filter(username=username).first()
        if not target:
            return Response({"detail": "User not found."}, status=status.HTTP_404_NOT_FOUND)
        friendship = Friendship.between(request.user, target).first()
        if not friendship or friendship.status != Friendship.STATUS_ACCEPTED:
            return Response({"detail": "Friendship not found."}, status=status.HTTP_404_NOT_FOUND)

        friendship.requester = request.user
        friendship.status = Friendship.STATUS_CANCELLED
        friendship.save()
        return Response(status=status.HTTP_204_NO_CONTENT)


class FriendsListView(ListAPIView):
    permission_classes = [IsAuthenticated]
    serializer_class = FriendshipSerializer

    def get_queryset(self):
        return (
            Friendship.objects
            .filter(status=Friendship.STATUS_ACCEPTED)
            .filter(Q(user1=self.request.user) | Q(user2=self.request.user))
            .select_related("user1", "user2", "user1__profile", "user2__profile", "requester")
        )


class FriendMentionListView(ListAPIView):
    permission_classes = [IsAuthenticated]
    serializer_class = FriendMentionSerializer

    def get_queryset(self):
        friends_queryset = User.objects.filter(
            Q(friendships_as_user1__user2=self.request.user, friendships_as_user1__status=Friendship.STATUS_ACCEPTED)
            | Q(friendships_as_user2__user1=self.request.user, friendships_as_user2__status=Friendship.STATUS_ACCEPTED)
        )
        search = self.request.query_params.get("search")
        if search:
            friends_queryset = friends_queryset.filter(username__icontains=search.strip())

        return friends_queryset.select_related("profile").order_by("username").distinct()


class ReceivedFriendshipRequestsView(ListAPIView):
    permission_classes = [IsAuthenticated]
    serializer_class = FriendshipSerializer

    def get_queryset(self):
        return (
            Friendship.objects
            .filter(status=Friendship.STATUS_PENDING)
            .filter(Q(user1=self.request.user) | Q(user2=self.request.user))
            .exclude(requester=self.request.user)
            .select_related("user1", "user2", "user1__profile", "user2__profile", "requester")
        )


class SentFriendshipRequestsView(ListAPIView):
    permission_classes = [IsAuthenticated]
    serializer_class = FriendshipSerializer

    def get_queryset(self):
        return (
            Friendship.objects
            .filter(status=Friendship.STATUS_PENDING, requester=self.request.user)
            .select_related("user1", "user2", "user1__profile", "user2__profile", "requester")
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
    




class PublicCommentsFeedView(generics.ListAPIView):
    permission_classes = [permissions.IsAuthenticated]
    serializer_class = PublicCommentFeedSerializer

    def get_queryset(self):
        user = self.request.user

        is_following_subquery = Follow.objects.filter(
            follower_id=user.id,
            following_id=OuterRef("author_id"),
        )

        is_friend_subquery = Friendship.objects.filter(
            status=Friendship.STATUS_ACCEPTED,
        ).filter(
            Q(user1_id=user.id, user2_id=OuterRef("author_id"))
            | Q(user2_id=user.id, user1_id=OuterRef("author_id"))
        )

        queryset = (
            Comment.objects.filter(
                visibility=Comment.VISIBILITY_PUBLIC,
                author__profile__is_public=True,
            )
            .select_related("author", "author__profile", "movie", "target_user")
            .annotate(
                author_followers_count=Count("author__followers", distinct=True),
                is_following_author=Exists(is_following_subquery),
                is_friend_author=Exists(is_friend_subquery),
            )
            .annotate(
                feed_category=Case(
                    When(
                        Q(is_following_author=False)
                        & Q(is_friend_author=False)
                        & ~Q(author_id=user.id),
                        then=Value(1),
                    ),
                    When(is_following_author=True, then=Value(2)),
                    When(Q(is_friend_author=True) & Q(is_following_author=False), then=Value(3)),
                    default=Value(4),
                    output_field=IntegerField(),
                )
            )
            .order_by("feed_category", "-author_followers_count", "-created_at", "-id")
        )

        return annotate_comments_for_user(queryset, user)

class MovieCommentsListCreateView(generics.ListCreateAPIView):
    serializer_class = CommentSerializer
    mention_pattern = re.compile(r"(?<!\w)@(?P<username>[\w.@+-]+)")

    def get_permissions(self):
        if self.request.method == "POST":
            return [permissions.IsAuthenticated()]
        return [permissions.AllowAny()]

    def get_queryset(self):
        return annotate_comments_for_user(
            Comment.objects.filter(
                movie_id=self.kwargs["pk"],
                visibility=Comment.VISIBILITY_PUBLIC,
            )
            .select_related("author", "author__profile", "movie", "target_user")
            .order_by("-created_at"),
            self.request.user,
        )

    def _get_mentioned_friend(self, body):
        if not body:
            return None

        match = self.mention_pattern.search(body)
        if not match:
            return None

        username = match.group("username")
        target_user = User.objects.filter(username=username).first()
        if target_user is None or target_user == self.request.user:
            return None

        friendship = Friendship.between(self.request.user, target_user).filter(
            status=Friendship.STATUS_ACCEPTED,
        ).first()
        if friendship is None:
            return None

        return target_user

    def perform_create(self, serializer):
        movie = get_object_or_404(Movie, pk=self.kwargs["pk"])
        target_user = self._get_mentioned_friend(serializer.validated_data.get("body", ""))
        visibility = Comment.VISIBILITY_MENTIONED if target_user else Comment.VISIBILITY_PUBLIC
        serializer.save(
            author=self.request.user,
            movie=movie,
            target_user=target_user,
            visibility=visibility,
        )


class PostCommentsListCreateView(MovieCommentsListCreateView):
    deprecated_warning = '299 - "Deprecated endpoint. Use /api/movies/<pk>/comments/ instead."'

    def finalize_response(self, request, response, *args, **kwargs):
        response = super().finalize_response(request, response, *args, **kwargs)
        response["Warning"] = self.deprecated_warning
        return response


class CommentDetailView(generics.RetrieveUpdateDestroyAPIView):
    permission_classes = [permissions.IsAuthenticatedOrReadOnly, IsCommentAuthorOrReadOnly]
    serializer_class = CommentSerializer
    http_method_names = ["get", "put", "delete", "head", "options"]

    def get_queryset(self):
        queryset = annotate_comments_for_user(
            Comment.objects.select_related("author", "author__profile", "movie", "target_user"),
            self.request.user,
        )

        if self.request.method not in permissions.SAFE_METHODS:
            return queryset.filter(author=self.request.user)

        if not self.request.user.is_authenticated:
            return queryset.filter(visibility=Comment.VISIBILITY_PUBLIC)

        return queryset.filter(
            Q(visibility=Comment.VISIBILITY_PUBLIC)
            | Q(author=self.request.user)
            | Q(target_user=self.request.user)
        )


class ReceivedDirectedCommentsView(generics.ListAPIView):
    permission_classes = [permissions.IsAuthenticated]
    serializer_class = CommentSerializer

    def get_queryset(self):
        return annotate_comments_for_user(
            Comment.objects.filter(
                visibility=Comment.VISIBILITY_MENTIONED,
                target_user=self.request.user,
            )
            .select_related("author", "author__profile", "movie", "target_user")
            .order_by("-created_at"),
            self.request.user,
        )


class SentDirectedCommentsView(generics.ListAPIView):
    permission_classes = [permissions.IsAuthenticated]
    serializer_class = CommentSerializer

    def get_queryset(self):
        return annotate_comments_for_user(
            Comment.objects.filter(
                visibility=Comment.VISIBILITY_MENTIONED,
                author=self.request.user,
            )
            .select_related("author", "author__profile", "movie", "target_user")
            .order_by("-created_at"),
            self.request.user,
        )


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


class MovieListView(generics.ListAPIView):
    permission_classes = [permissions.AllowAny]
    serializer_class = MovieListSerializer

    def get_queryset(self):
        user = self.request.user
        has_preferences = user.is_authenticated and UserTasteProfile.objects.filter(
            user_id=user.id,
            ratings_count__gt=0,
        ).exists()
        if user.is_authenticated:
            qs = Movie.objects.feed_for_user(user, include_recommendation_score=has_preferences)
        else:
            qs = Movie.objects.with_display_rating().with_my_rating(user)

        qs = qs.with_comment_stats().select_related("author", "author__profile").annotate(
            general_rating=F("display_rating"),
        )

        if user.is_authenticated:
            qs = qs.annotate(
                following_avg_rating=Avg(
                    "movie_ratings__score",
                    filter=Q(movie_ratings__user__followers__follower=user),
                )
            )
        else:
            qs = qs.annotate(following_avg_rating=Value(None, output_field=FloatField()))

        if movie_type := self.request.query_params.get("type"):
            qs = qs.filter(type=movie_type)
        if genre := self.request.query_params.get("genre"):
            qs = qs.filter(genre__icontains=genre)
        if release_year := self.request.query_params.get("release_year"):
            qs = qs.filter(release_year=release_year)

        if search := self.request.query_params.get("search"):
            qs = apply_movie_search(qs, search)

        release_year_desc = F("release_year").desc(nulls_last=True)
        if user.is_authenticated:
            search_ordering = ["-search_relevance"] if search else []
            return qs.order_by(
                *search_ordering,
                "-recommendation_score",
                "-ranking_confidence_score",
                "-display_rating",
                release_year_desc,
                "-id",
            )

        search_ordering = ["-search_relevance"] if search else []
        return qs.order_by(*search_ordering, "-display_rating", release_year_desc, "-created_at", "-id")


class FeedMoviesView(generics.ListAPIView):
    permission_classes = [permissions.IsAuthenticated]
    serializer_class = MovieListSerializer

    def get_queryset(self):
        user = self.request.user
        has_preferences = UserTasteProfile.objects.filter(user_id=user.id, ratings_count__gt=0).exists()
        qs = (
            Movie.objects
            .feed_for_user(user, include_recommendation_score=has_preferences)
            .with_comment_stats()
            .select_related("author", "author__profile")
        )

        if self.request.query_params.get("exclude_rated", "true").lower() != "false":
            qs = qs.filter(my_rating__isnull=True)

        if search := self.request.query_params.get("search"):
            qs = apply_movie_search(qs, search)

        if movie_type := self.request.query_params.get("type"):
            qs = qs.filter(type=movie_type)
        if genre := self.request.query_params.get("genre"):
            qs = qs.filter(genre__icontains=genre)

        release_year_desc = F("release_year").desc(nulls_last=True)

        search_ordering = ["-search_relevance"] if self.request.query_params.get("search") else []
        return qs.order_by(
            *search_ordering,
            "-recommendation_score",
            "-ranking_confidence_score",
            "-display_rating",
            release_year_desc,
            "-id",
        )


class WeeklyRecommendationsView(generics.ListAPIView):
    permission_classes = [permissions.AllowAny]
    serializer_class = WeeklyRecommendationItemSerializer
    pagination_class = None

    def get_queryset(self):
        window = get_previous_closed_week_window()
        snapshot = WeeklyRecommendationSnapshot.objects.filter(
            week_start=window.start_date,
            week_end=window.end_date,
        ).first()
        if snapshot is None:
            return WeeklyRecommendationItem.objects.none()

        items = snapshot.items.select_related("movie").order_by("position")
        display_rating_subquery = Movie.objects.with_display_rating().filter(
            pk=OuterRef("movie_id")
        ).values("display_rating")[:1]

        queryset = items.annotate(
            general_rating=Subquery(display_rating_subquery, output_field=FloatField()),
            display_rating=Subquery(display_rating_subquery, output_field=FloatField()),
        )

        user = self.request.user
        if not user or not user.is_authenticated:
            return queryset.annotate(
                my_rating=Value(None, output_field=IntegerField()),
                following_avg_rating=Value(None, output_field=FloatField()),
            )

        return queryset.annotate(
            my_rating=Subquery(
                MovieRating.objects.filter(movie_id=OuterRef("movie_id"), user_id=user.id).values("score")[:1]
            ),
            following_avg_rating=Avg(
                "movie__movie_ratings__score",
                filter=Q(movie__movie_ratings__user__followers__follower=user),
            ),
        )


class MovieRatingView(APIView):
    permission_classes = [permissions.IsAuthenticated]

    def put(self, request, pk):
        movie = get_object_or_404(Movie, pk=pk)
        serializer = MovieRatingSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        new_score = serializer.validated_data["score"]

        with transaction.atomic():
            rating, created = MovieRating.objects.update_or_create(
                user=request.user,
                movie=movie,
                defaults={"score": new_score},
            )

        return Response(
            {"movie": movie.id, "my_rating": rating.score, "created": created},
            status=status.HTTP_200_OK,
        )

    def delete(self, request, pk):
        movie = get_object_or_404(Movie, pk=pk)

        with transaction.atomic():
            rating = MovieRating.objects.select_for_update().filter(user=request.user, movie=movie).first()
            if rating is None:
                return Response({"detail": "Rating not found."}, status=status.HTTP_404_NOT_FOUND)
            rating.delete()

        return Response(status=status.HTTP_204_NO_CONTENT)


class MeTasteProfileView(generics.RetrieveAPIView):
    permission_classes = [permissions.IsAuthenticated]
    serializer_class = UserTasteProfileInspectSerializer

    def get_object(self):
        return UserTasteProfile.objects.get_or_create(user=self.request.user)[0]

class CommentReactionView(APIView):
    permission_classes = [permissions.IsAuthenticated]

    def get_comment(self, pk):
        comment = get_object_or_404(
            Comment.objects.select_related("author", "target_user", "movie"),
            pk=pk,
        )
        if not can_access_directed_comment_reactions(self.request.user, comment):
            raise PermissionDenied("You cannot react to this comment.")
        return comment

    def put(self, request, pk):
        comment = self.get_comment(pk)
        serializer = CommentReactionSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)

        reaction, created = CommentReaction.objects.update_or_create(
            comment=comment,
            user=request.user,
            defaults={"reaction_type": serializer.validated_data["reaction_type"]},
        )

        annotated_comment = annotate_comments_for_user(Comment.objects.filter(pk=comment.pk), request.user).get()
        return Response(
            {
                "comment": comment.id,
                "reaction_type": reaction.reaction_type,
                "my_reaction": annotated_comment.my_reaction,
                "likes_count": annotated_comment.likes_count,
                "dislikes_count": annotated_comment.dislikes_count,
                "created": created,
            },
            status=status.HTTP_200_OK,
        )

    def delete(self, request, pk):
        comment = self.get_comment(pk)
        deleted, _ = CommentReaction.objects.filter(comment=comment, user=request.user).delete()
        if not deleted:
            return Response({"detail": "Reaction not found."}, status=status.HTTP_404_NOT_FOUND)
        return Response(status=status.HTTP_204_NO_CONTENT)
