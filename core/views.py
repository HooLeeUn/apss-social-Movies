import re
import logging
from time import perf_counter
from django.contrib.auth import get_user_model
from django.conf import settings
from django.core.cache import cache
from django.db import transaction
from django.db.models import Case, Count, Avg, Exists, F, FloatField, IntegerField, OuterRef, Q, Subquery, Value, When
from django.db.models.functions import Cast, Coalesce, Mod
from rest_framework.generics import RetrieveAPIView, ListAPIView
from rest_framework import generics, permissions, status
from rest_framework.authtoken.models import Token
from rest_framework.parsers import MultiPartParser, FormParser, JSONParser
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework.exceptions import PermissionDenied, ValidationError
from rest_framework.permissions import IsAuthenticated, AllowAny
from .serializers import (
    FriendMentionSerializer, FriendshipSerializer, UserProfileSerializer, MeSerializer, UserMiniSerializer,
    PostListSerializer, PostCreateSerializer, PostDetailSerializer, SocialActivitySerializer,
    PostWriteSerializer, CommentReactionSerializer, CommentSerializer, PublicCommentFeedSerializer, RegisterSerializer, MovieListSerializer,
    MovieRatingSerializer, ProfileFavoriteSlotSerializer, ProfileFavoriteSlotWriteSerializer,
    ProfileFavoriteMovieSerializer, UserTasteProfileInspectSerializer, WeeklyRecommendationItemSerializer,
    PrivacySettingsSerializer, UserVisibilityBlockSerializer, CreateUserVisibilityBlockSerializer, UserSearchSerializer,
)
from .models import (
    Comment,
    CommentReaction,
    Follow,
    Friendship,
    Movie,
    MovieRating,
    ProfileFavoriteMovie,
    Post,
    Rating,
    UserTasteProfile,
    UserDirectorPreference,
    UserGenrePreference,
    UserTypePreference,
    UserVisibilityBlock,
    WeeklyRecommendationItem,
    WeeklyRecommendationSnapshot,
)
from .permissions import IsAuthorOrReadOnly, IsCommentAuthorOrReadOnly
from .pagination import FeedMoviesPagination
from .social_feed import SocialActivityFeedService
from .weekly_recommendations import (
    get_previous_closed_week_window,
    refresh_weekly_recommendation_snapshot,
)
from .visibility import (
    can_view_user_profile,
    filter_out_authors_who_blocked_viewer,
)
from django.core.exceptions import ValidationError as DjangoValidationError
from django.shortcuts import get_object_or_404
from django.utils import timezone

User = get_user_model()
logger = logging.getLogger(__name__)


def split_search_terms(search):
    return [term for term in re.split(r"\s+", search.strip()) if term]


def apply_movie_search(queryset, search, include_relevance=True):
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

    filtered_queryset = queryset.filter(filters)
    if not include_relevance:
        return filtered_queryset
    return filtered_queryset.annotate(search_relevance=score_expr)


VALID_FEED_GENRES = {
    "Action",
    "Animation",
    "Comedy",
    "Documentary",
    "Drama",
    "Horror",
    "Musical",
    "Sci-Fi",
}

FEED_GENRE_ALIASES = {
    "sci fi": "Sci-Fi",
    "science fiction": "Sci-Fi",
    "ciencia ficcion": "Sci-Fi",
    "ciencia ficción": "Sci-Fi",
}


def normalize_feed_genre_candidate(value):
    candidate = re.sub(r"\s+", " ", value.strip())
    if not candidate:
        return None
    lowered = candidate.lower()
    if lowered in FEED_GENRE_ALIASES:
        return FEED_GENRE_ALIASES[lowered]
    return next((genre for genre in VALID_FEED_GENRES if genre.lower() == lowered), None)


def parse_feed_genre_filters(request):
    raw_values = []
    for key in ("genres", "genre"):
        values = request.query_params.getlist(key)
        for value in values:
            if value:
                raw_values.extend(value.split(","))

    normalized = []
    seen = set()
    for item in raw_values:
        matched_genre = normalize_feed_genre_candidate(item)
        if matched_genre is None or matched_genre in seen:
            continue
        seen.add(matched_genre)
        normalized.append(matched_genre)
    return normalized[:3]


def apply_feed_genre_filters(queryset, genres):
    if not genres:
        return queryset

    for genre in genres:
        normalized_genre = re.escape(genre.lower())
        genre_lookup = rf"(^|,\s*){normalized_genre}(\s*,|$)"
        queryset = queryset.filter(
            Q(genre_key=genre)
            | Q(genre_key__startswith=f"{genre}|")
            | Q(genre_key__endswith=f"|{genre}")
            | Q(genre_key__contains=f"|{genre}|")
            | Q(genre__iregex=genre_lookup)
        )
    return queryset


def annotate_comments_for_user(queryset, user):
    return queryset.with_reaction_stats(user)


def can_access_directed_comment_reactions(user, comment):
    if comment.visibility != Comment.VISIBILITY_MENTIONED:
        return True
    if not user or not user.is_authenticated:
        return False
    return user.id in {comment.author_id, comment.target_user_id}


def filter_comments_visible_to_user(queryset, user):
    queryset = filter_out_authors_who_blocked_viewer(queryset, user, author_field="author")
    if not user or not user.is_authenticated:
        return queryset.filter(visibility=Comment.VISIBILITY_PUBLIC)

    return queryset.filter(
        Q(visibility=Comment.VISIBILITY_PUBLIC)
        | Q(author=user)
        | Q(target_user=user)
    )


def build_profile_favorite_movie_payload_by_id(user, movie_ids):
    if not movie_ids:
        return {}

    movies = (
        Movie.objects.filter(id__in=movie_ids)
        .with_display_rating()
        .with_my_rating(user)
        .with_following_rating_stats(user)
        .annotate(
            general_rating=F("display_rating"),
        )
    )
    serialized_movies = ProfileFavoriteMovieSerializer(movies, many=True)
    return {item["id"]: item for item in serialized_movies.data}



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


class UserSearchView(APIView):
    permission_classes = [IsAuthenticated]
    RESULTS_LIMIT = 20

    def get(self, request):
        query = self._normalize_query(request.query_params.get("q"))
        if not query:
            return Response([], status=status.HTTP_200_OK)

        blocked_user_ids = UserVisibilityBlock.objects.filter(
            owner=request.user,
        ).values_list("blocked_user_id", flat=True)

        queryset = (
            User.objects.filter(username__icontains=query)
            .exclude(id=request.user.id)
            .exclude(id__in=blocked_user_ids)
            .order_by("username")[: self.RESULTS_LIMIT]
        )
        serializer = UserSearchSerializer(queryset, many=True)
        return Response(serializer.data, status=status.HTTP_200_OK)

    @staticmethod
    def _normalize_query(raw_query):
        query = (raw_query or "").strip()
        query = query.lstrip("@").strip()
        return query


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

    def get_object(self):
        obj = super().get_object()
        if not can_view_user_profile(obj, self.request.user):
            raise PermissionDenied("You do not have permission to view this profile.")
        return obj

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
        if profile and profile.visibility == profile.Visibility.PRIVATE:
            return Response({"detail": "You cannot follow a private profile."}, status=status.HTTP_403_FORBIDDEN)

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
        target = get_object_or_404(User.objects.select_related("profile"), username=username)
        if not can_view_user_profile(target, self.request.user):
            raise PermissionDenied("You do not have permission to view this profile.")
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
        target = get_object_or_404(User.objects.select_related("profile"), username=username)
        if not can_view_user_profile(target, self.request.user):
            raise PermissionDenied("You do not have permission to view this profile.")
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

        queryset = filter_out_authors_who_blocked_viewer(queryset, user, author_field="author")
        return annotate_comments_for_user(queryset, user)


class ProfileFeedActivityView(generics.ListAPIView):
    permission_classes = [permissions.IsAuthenticated]
    serializer_class = SocialActivitySerializer

    def get_queryset(self):
        scope = self.request.query_params.get("scope")
        if not SocialActivityFeedService.is_valid_scope(scope):
            raise ValidationError(
                {"scope": "This query param is required and must be one of: following, friends."}
            )

        return SocialActivityFeedService.build_feed(
            user=self.request.user,
            scope=scope,
        )

    def get_serializer_context(self):
        context = super().get_serializer_context()
        context["request"] = self.request
        return context

class MovieCommentsListCreateView(generics.ListCreateAPIView):
    serializer_class = CommentSerializer
    mention_pattern = re.compile(r"(?<!\w)@(?P<username>[\w.@+-]+)")

    def get_permissions(self):
        return [permissions.IsAuthenticated()]

    def get_queryset(self):
        queryset = (
            Comment.objects.filter(
                movie_id=self.kwargs["pk"],
                visibility=Comment.VISIBILITY_PUBLIC,
            )
            .select_related("author", "author__profile", "movie", "target_user")
            .order_by("-created_at")
        )
        queryset = filter_out_authors_who_blocked_viewer(queryset, self.request.user, author_field="author")
        return annotate_comments_for_user(queryset, self.request.user)

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

    def _get_mentioned_friend_from_payload(self, data):
        username = (data.get("mentioned_username") or data.get("recipient_username") or "").strip()
        if not username:
            return None, False

        target_user = User.objects.filter(username=username).first()
        if target_user is None or target_user == self.request.user:
            raise ValidationError({"mentioned_username": "Mentioned user is invalid."})

        friendship = Friendship.between(self.request.user, target_user).filter(
            status=Friendship.STATUS_ACCEPTED,
        ).first()
        if friendship is None:
            raise ValidationError({"mentioned_username": "You can only mention users who are your friends."})

        return target_user, True

    def perform_create(self, serializer):
        movie = get_object_or_404(Movie, pk=self.kwargs["pk"])
        target_user, has_explicit_mention = self._get_mentioned_friend_from_payload(serializer.validated_data)
        if not has_explicit_mention:
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
        queryset = Comment.objects.select_related("author", "author__profile", "movie", "target_user")

        if self.request.method not in permissions.SAFE_METHODS:
            return annotate_comments_for_user(queryset.filter(author=self.request.user), self.request.user)

        return annotate_comments_for_user(
            filter_comments_visible_to_user(queryset, self.request.user),
            self.request.user,
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


class MovieDirectedCommentsListView(MovieCommentsListCreateView):
    permission_classes = [permissions.IsAuthenticated]
    serializer_class = CommentSerializer

    def get_queryset(self):
        return annotate_comments_for_user(
            Comment.objects.filter(
                movie_id=self.kwargs["pk"],
                visibility=Comment.VISIBILITY_MENTIONED,
            )
            .filter(
                Q(author=self.request.user) | Q(target_user=self.request.user)
            )
            .select_related("author", "author__profile", "movie", "target_user")
            .order_by("-created_at"),
            self.request.user,
        )

    def perform_create(self, serializer):
        movie = get_object_or_404(Movie, pk=self.kwargs["pk"])
        target_user, _ = self._get_mentioned_friend_from_payload(serializer.validated_data)
        if target_user is None:
            target_user = self._get_mentioned_friend(serializer.validated_data.get("body", ""))
        if target_user is None:
            raise ValidationError({"mentioned_username": "Directed comments require a valid friend mention."})

        serializer.save(
            author=self.request.user,
            movie=movie,
            target_user=target_user,
            visibility=Comment.VISIBILITY_MENTIONED,
        )


class UserPostsListView(generics.ListAPIView):
    permission_classes = [permissions.AllowAny]
    serializer_class = PostListSerializer

    def get_queryset(self):
        user = get_object_or_404(User, username=self.kwargs["username"])
        if not can_view_user_profile(user, self.request.user):
            raise PermissionDenied("You do not have permission to view this profile.")
        
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
        qs = qs.with_following_rating_stats(user)

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


class MovieDetailView(generics.RetrieveAPIView):
    permission_classes = [permissions.AllowAny]
    serializer_class = MovieListSerializer
    lookup_field = "pk"

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
        return qs.with_following_rating_stats(user)


class ProfileFavoritesView(APIView):
    permission_classes = [permissions.IsAuthenticated]

    def get(self, request):
        favorites = list(
            ProfileFavoriteMovie.objects.filter(user=request.user)
            .select_related("movie")
            .order_by("slot")
        )
        movie_ids = [favorite.movie_id for favorite in favorites]
        movie_payload_by_id = build_profile_favorite_movie_payload_by_id(request.user, movie_ids)
        movie_id_by_slot = {favorite.slot: favorite.movie_id for favorite in favorites}

        payload = [
            {
                "slot": slot,
                "movie": movie_payload_by_id.get(movie_id_by_slot.get(slot)),
            }
            for slot in (1, 2, 3)
        ]
        return Response(ProfileFavoriteSlotSerializer(payload, many=True).data, status=status.HTTP_200_OK)


class ProfilePrivacyView(APIView):
    permission_classes = [permissions.IsAuthenticated]

    def get(self, request):
        serializer = PrivacySettingsSerializer(request.user.profile)
        return Response(serializer.data, status=status.HTTP_200_OK)

    def patch(self, request):
        serializer = PrivacySettingsSerializer(request.user.profile, data=request.data, partial=True)
        serializer.is_valid(raise_exception=True)
        serializer.save()
        return Response(serializer.data, status=status.HTTP_200_OK)


class ProfilePrivacyBlockedUsersView(APIView):
    permission_classes = [permissions.IsAuthenticated]

    def get(self, request):
        blocks = UserVisibilityBlock.objects.filter(owner=request.user).select_related("blocked_user")
        return Response(UserVisibilityBlockSerializer(blocks, many=True).data, status=status.HTTP_200_OK)

    def post(self, request):
        serializer = CreateUserVisibilityBlockSerializer(data=request.data, context={"request": request})
        serializer.is_valid(raise_exception=True)
        block = serializer.save()
        return Response(
            UserVisibilityBlockSerializer(block).data,
            status=status.HTTP_201_CREATED,
        )


class ProfilePrivacyBlockedUserDetailView(APIView):
    permission_classes = [permissions.IsAuthenticated]

    def delete(self, request, user_id):
        deleted, _ = UserVisibilityBlock.objects.filter(
            owner=request.user,
            blocked_user_id=user_id,
        ).delete()
        if not deleted:
            return Response({"detail": "Blocked user not found."}, status=status.HTTP_404_NOT_FOUND)
        return Response(status=status.HTTP_204_NO_CONTENT)


class ProfileFavoriteSlotDetailView(APIView):
    permission_classes = [permissions.IsAuthenticated]

    @staticmethod
    def _validate_slot(slot):
        if slot not in (1, 2, 3):
            raise ValidationError({"slot": "Slot must be 1, 2, or 3."})

    def put(self, request, slot):
        self._validate_slot(slot)
        serializer = ProfileFavoriteSlotWriteSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        movie = serializer.validated_data["movie"]

        duplicated_movie = ProfileFavoriteMovie.objects.filter(
            user=request.user,
            movie=movie,
        ).exclude(slot=slot).exists()
        if duplicated_movie:
            raise ValidationError({"movie_id": "This movie is already assigned to another slot."})

        ProfileFavoriteMovie.objects.update_or_create(
            user=request.user,
            slot=slot,
            defaults={"movie": movie},
        )
        movie_payload = build_profile_favorite_movie_payload_by_id(request.user, [movie.id]).get(movie.id)
        response_payload = {"slot": slot, "movie": movie_payload}
        return Response(ProfileFavoriteSlotSerializer(response_payload).data, status=status.HTTP_200_OK)

    def delete(self, request, slot):
        self._validate_slot(slot)
        ProfileFavoriteMovie.objects.filter(user=request.user, slot=slot).delete()
        return Response(status=status.HTTP_204_NO_CONTENT)


class FeedMoviesView(generics.ListAPIView):
    permission_classes = [permissions.IsAuthenticated]
    serializer_class = MovieListSerializer
    pagination_class = FeedMoviesPagination
    FEED_CANDIDATE_MULTIPLIER = 12
    FEED_CANDIDATE_MIN_POOL = 400
    FEED_CANDIDATE_MAX_POOL = 12000
    FEED_PAGE_CACHE_TTL_SECONDS = 120

    def _is_feed_profiling_enabled(self):
        query_flag = self.request.query_params.get("profile_feed", "").lower() in {"1", "true", "yes"}
        return bool(getattr(settings, "FEED_PROFILING_ENABLED", False) or query_flag)

    def _should_log_explain(self):
        return self.request.query_params.get("profile_explain", "").lower() in {"1", "true", "yes"}

    def _record_profile_timing(self, key, elapsed_seconds):
        if not getattr(self, "_feed_profile_enabled", False):
            return
        self._feed_profile_timings[key] = round(elapsed_seconds, 6)

    def _log_feed_profile_summary(self):
        if not getattr(self, "_feed_profile_enabled", False):
            return
        logger.info(
            "feed.movies.profile user_id=%s params=%s timings=%s",
            getattr(self.request.user, "id", None),
            dict(self.request.query_params),
            self._feed_profile_timings,
        )

    def _log_profile_sql(self, label, queryset):
        if not getattr(self, "_feed_profile_enabled", False):
            return
        try:
            logger.info("feed.movies.profile.sql %s=%s", label, str(queryset.query))
        except Exception as exc:
            logger.warning("feed.movies.profile.sql_failed label=%s error=%s", label, exc)

    def _log_profile_explain(self, label, queryset):
        if not (getattr(self, "_feed_profile_enabled", False) and self._should_log_explain()):
            return
        try:
            explain_start = perf_counter()
            explain_text = queryset.explain(analyze=True, buffers=True)
            self._record_profile_timing(f"{label}_explain_analyze_seconds", perf_counter() - explain_start)
            logger.info("feed.movies.profile.explain %s=\n%s", label, explain_text)
        except Exception as exc:
            logger.warning("feed.movies.profile.explain_failed label=%s error=%s", label, exc)

    def _build_filtered_feed_base_queryset(self, include_search_relevance):
        user = self.request.user
        queryset = Movie.objects.all()

        if self.request.query_params.get("exclude_rated", "true").lower() != "false":
            queryset = queryset.exclude(movie_ratings__user_id=user.id)

        if search := self.request.query_params.get("search"):
            queryset = apply_movie_search(queryset, search, include_relevance=include_search_relevance)

        if movie_type := self.request.query_params.get("type"):
            queryset = queryset.filter(type=movie_type)

        selected_genres = parse_feed_genre_filters(self.request)
        queryset = apply_feed_genre_filters(queryset, selected_genres)
        return queryset

    def get_feed_count_queryset(self):
        if not hasattr(self, "_feed_count_queryset"):
            self._feed_count_queryset = self._build_filtered_feed_base_queryset(include_search_relevance=False)
        return self._feed_count_queryset

    def _resolve_page_size(self):
        paginator = self.paginator
        if not paginator:
            return self.FEED_CANDIDATE_MIN_POOL
        page_size = paginator.get_page_size(self.request)
        if page_size:
            return int(page_size)
        return int(getattr(paginator, "page_size", self.FEED_CANDIDATE_MIN_POOL))

    def _resolve_page_number(self):
        raw_page = self.request.query_params.get("page", 1)
        try:
            page_number = int(raw_page)
        except (TypeError, ValueError):
            return 1
        return max(page_number, 1)

    def _resolve_candidate_pool_size(self):
        page_size = self._resolve_page_size()
        page_number = self._resolve_page_number()
        raw_pool_size = page_size * page_number * self.FEED_CANDIDATE_MULTIPLIER
        return max(self.FEED_CANDIDATE_MIN_POOL, min(raw_pool_size, self.FEED_CANDIDATE_MAX_POOL))

    def _resolve_rotation_bucket(self):
        # Buckets cortos para permitir refresh dinámico, pero orden estable
        # dentro de la misma ventana.
        return int(timezone.now().timestamp() // self.FEED_PAGE_CACHE_TTL_SECONDS)

    def _build_feed_cache_key(self):
        genres = parse_feed_genre_filters(self.request)
        return "|".join(
            [
                "feed_movies_v2",
                f"user:{self.request.user.id}",
                f"page:{self._resolve_page_number()}",
                f"page_size:{self._resolve_page_size()}",
                f"search:{(self.request.query_params.get('search') or '').strip().lower()}",
                f"type:{(self.request.query_params.get('type') or '').strip().lower()}",
                f"genres:{','.join(genres)}",
                f"exclude_rated:{self.request.query_params.get('exclude_rated', 'true').lower()}",
                f"rotation:{self._resolve_rotation_bucket()}",
            ]
        )

    def _build_candidate_ids_queryset(self, filtered_base_queryset, has_preferences):
        user = self.request.user

        candidate_queryset = filtered_base_queryset.annotate(
            candidate_recency_score=Coalesce(F("release_year"), Value(0), output_field=IntegerField()),
        )

        if has_preferences:
            genre_pref_score_subquery = UserGenrePreference.objects.filter(
                user_id=user.id,
                genre=OuterRef("genre_key"),
            ).values("score")[:1]
            type_pref_score_subquery = UserTypePreference.objects.filter(
                user_id=user.id,
                content_type=OuterRef("type"),
            ).values("score")[:1]
            director_pref_score_subquery = UserDirectorPreference.objects.filter(
                user_id=user.id,
                director=OuterRef("director"),
            ).values("score")[:1]

            candidate_queryset = candidate_queryset.annotate(
                candidate_genre_score=Coalesce(Subquery(genre_pref_score_subquery), Value(0.0), output_field=FloatField()),
                candidate_type_score=Coalesce(Subquery(type_pref_score_subquery), Value(0.0), output_field=FloatField()),
                candidate_director_score=Coalesce(Subquery(director_pref_score_subquery), Value(0.0), output_field=FloatField()),
            ).annotate(
                candidate_affinity_score=(
                    F("candidate_genre_score") * Value(0.72)
                    + F("candidate_type_score") * Value(0.18)
                    + F("candidate_director_score") * Value(0.10)
                ),
                candidate_quality_hint=Coalesce(F("external_rating"), Value(0.0), output_field=FloatField()),
                candidate_popularity_hint=Coalesce(F("external_votes"), Value(0), output_field=IntegerField()),
            ).annotate(
                candidate_priority=(
                    F("candidate_affinity_score") * Value(0.75)
                    + F("candidate_quality_hint") * Value(0.23)
                    + Cast(F("candidate_recency_score"), FloatField()) * Value(0.0008)
                    + Cast(F("candidate_popularity_hint"), FloatField()) * Value(0.00001)
                )
            )
        else:
            candidate_queryset = candidate_queryset.annotate(
                candidate_quality_hint=Coalesce(F("external_rating"), Value(0.0), output_field=FloatField()),
                candidate_popularity_hint=Coalesce(F("external_votes"), Value(0), output_field=IntegerField()),
                candidate_priority=(
                    F("candidate_quality_hint") * Value(0.88)
                    + Cast(F("candidate_recency_score"), FloatField()) * Value(0.0010)
                    + Cast(F("candidate_popularity_hint"), FloatField()) * Value(0.00001)
                ),
            )

        search_ordering = ["-search_relevance"] if self.request.query_params.get("search") else []
        candidate_pool_size = self._resolve_candidate_pool_size()
        self._record_profile_timing("candidate_pool_size", float(candidate_pool_size))
        return candidate_queryset.order_by(
            *search_ordering,
            "-candidate_priority",
            "-candidate_recency_score",
            "-id",
        ).values("id")[:candidate_pool_size]

    def get_queryset(self):
        user = self.request.user
        self._feed_profile_enabled = self._is_feed_profiling_enabled()
        if self._feed_profile_enabled and not hasattr(self, "_feed_profile_timings"):
            self._feed_profile_timings = {}

        has_preferences = UserTasteProfile.objects.filter(user_id=user.id, ratings_count__gt=0).exists()
        base_start = perf_counter()
        filtered_base_queryset = self._build_filtered_feed_base_queryset(include_search_relevance=True)
        self._record_profile_timing("base_queryset_build_seconds", perf_counter() - base_start)

        candidates_start = perf_counter()
        candidate_ids_queryset = self._build_candidate_ids_queryset(filtered_base_queryset, has_preferences)
        self._record_profile_timing("candidate_universe_build_seconds", perf_counter() - candidates_start)
        self._log_profile_sql("candidate_ids_sql", candidate_ids_queryset)
        self._log_profile_explain("candidate_ids", candidate_ids_queryset)

        score_start = perf_counter()
        qs = (
            Movie.objects.filter(id__in=Subquery(candidate_ids_queryset))
            .feed_for_user(
                user,
                include_recommendation_score=has_preferences,
                include_my_rating=False,
            )
            .with_following_rating_stats(user)
            .select_related("author", "author__profile")
        )

        rotation_seed = (self._resolve_rotation_bucket() * 31) + user.id
        controlled_jitter_base = Mod(
            F("id") * Value(1103515245) + Value(rotation_seed),
            Value(1000),
        )
        qs = qs.annotate(
            controlled_jitter=Cast(controlled_jitter_base, FloatField()) * Value(0.000001),
            recommendation_final_score=F("recommendation_score") + F("controlled_jitter"),
        )
        self._record_profile_timing("scoring_ranking_build_seconds", perf_counter() - score_start)

        release_year_desc = F("release_year").desc(nulls_last=True)

        search_ordering = ["-search_relevance"] if self.request.query_params.get("search") else []
        order_start = perf_counter()
        ordered_queryset = qs.order_by(
            *search_ordering,
            "-recommendation_final_score",
            "-ranking_confidence_score",
            "-display_rating",
            release_year_desc,
            "-id",
        )
        self._record_profile_timing("final_order_by_build_seconds", perf_counter() - order_start)
        return ordered_queryset

    def _hydrate_page_metrics(self, page_items):
        if not page_items:
            return

        movie_ids = [movie.id for movie in page_items]
        comments_count_by_movie = dict(
            Comment.objects.filter(movie_id__in=movie_ids)
            .values_list("movie_id")
            .annotate(total=Count("id"))
        )
        ratings_by_movie = dict(
            MovieRating.objects.filter(user_id=self.request.user.id, movie_id__in=movie_ids)
            .values_list("movie_id", "score")
        )

        for movie in page_items:
            movie.comments_count = comments_count_by_movie.get(movie.id, 0)
            movie.my_rating = ratings_by_movie.get(movie.id)

    def list(self, request, *args, **kwargs):
        self._feed_profile_enabled = self._is_feed_profiling_enabled()
        if self._feed_profile_enabled:
            self._feed_profile_timings = {}
            total_start = perf_counter()

        cache_key = self._build_feed_cache_key()
        cached_payload = cache.get(cache_key)
        if cached_payload is not None:
            return Response(cached_payload)

        queryset = self.filter_queryset(self.get_queryset())
        page = self.paginate_queryset(queryset)
        if page is not None:
            page_queryset = getattr(page, "object_list", None)
            if page_queryset is not None:
                self._log_profile_sql("page_queryset_sql", page_queryset)
                self._log_profile_explain("page_queryset", page_queryset)
            self._log_profile_sql("count_queryset_sql", self.get_feed_count_queryset())
            self._log_profile_explain("count_queryset", self.get_feed_count_queryset())

            page_fetch_start = perf_counter()
            page_items = list(page)
            self._record_profile_timing("page_results_sql_seconds", perf_counter() - page_fetch_start)

            page_hydration_start = perf_counter()
            self._hydrate_page_metrics(page_items)
            self._record_profile_timing("page_hydration_seconds", perf_counter() - page_hydration_start)

            serializer_start = perf_counter()
            serializer = self.get_serializer(page_items, many=True)
            serialized_data = serializer.data
            self._record_profile_timing("serializer_seconds", perf_counter() - serializer_start)

            response = self.get_paginated_response(serialized_data)
            cache.set(cache_key, response.data, timeout=self.FEED_PAGE_CACHE_TTL_SECONDS)
            if self._feed_profile_enabled:
                self._record_profile_timing("endpoint_total_seconds", perf_counter() - total_start)
                self._log_feed_profile_summary()
            return response

        serializer_start = perf_counter()
        serializer = self.get_serializer(queryset, many=True)
        serialized_data = serializer.data
        self._record_profile_timing("serializer_seconds", perf_counter() - serializer_start)
        response = Response(serialized_data)
        cache.set(cache_key, serialized_data, timeout=self.FEED_PAGE_CACHE_TTL_SECONDS)
        if self._feed_profile_enabled:
            self._record_profile_timing("endpoint_total_seconds", perf_counter() - total_start)
            self._log_feed_profile_summary()
        return response


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
            snapshot = refresh_weekly_recommendation_snapshot(reference_datetime=timezone.now())
            if snapshot.week_start != window.start_date or snapshot.week_end != window.end_date:
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
                following_ratings_count=Value(0, output_field=IntegerField()),
            )

        followed_user_ids = Follow.objects.filter(
            follower_id=user.id,
        ).exclude(
            following_id=user.id,
        ).values("following_id")
        following_ratings = MovieRating.objects.filter(
            movie_id=OuterRef("movie_id"),
            user_id__in=followed_user_ids,
        ).values("movie_id")

        following_avg_subquery = following_ratings.annotate(
            avg_score=Avg("score"),
        ).values("avg_score")[:1]
        following_count_subquery = following_ratings.annotate(
            total=Count("id"),
        ).values("total")[:1]

        return queryset.annotate(
            my_rating=Subquery(
                MovieRating.objects.filter(movie_id=OuterRef("movie_id"), user_id=user.id).values("score")[:1]
            ),
            following_avg_rating=Subquery(following_avg_subquery, output_field=FloatField()),
            following_ratings_count=Coalesce(
                Subquery(following_count_subquery, output_field=IntegerField()),
                Value(0),
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
        base_queryset = Comment.objects.select_related("author", "target_user", "movie")
        comment = get_object_or_404(filter_comments_visible_to_user(base_queryset, self.request.user), pk=pk)
        if not can_access_directed_comment_reactions(self.request.user, comment):
            raise PermissionDenied("You cannot react to this comment.")
        return comment

    def put(self, request, pk):
        comment = self.get_comment(pk)
        serializer = CommentReactionSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)

        CommentReaction.objects.update_or_create(
            comment=comment,
            user=request.user,
            defaults={"reaction_type": serializer.validated_data["reaction"]},
        )

        annotated_comment = annotate_comments_for_user(Comment.objects.filter(pk=comment.pk), request.user).get()
        return Response(
            {
                "comment_id": comment.id,
                "my_reaction": annotated_comment.my_reaction,
                "likes_count": annotated_comment.likes_count,
                "dislikes_count": annotated_comment.dislikes_count,
            },
            status=status.HTTP_200_OK,
        )

    def delete(self, request, pk):
        comment = self.get_comment(pk)
        CommentReaction.objects.filter(comment=comment, user=request.user).delete()
        annotated_comment = annotate_comments_for_user(Comment.objects.filter(pk=comment.pk), request.user).get()
        return Response(
            {
                "comment_id": comment.id,
                "my_reaction": None,
                "likes_count": annotated_comment.likes_count,
                "dislikes_count": annotated_comment.dislikes_count,
            },
            status=status.HTTP_200_OK,
        )
