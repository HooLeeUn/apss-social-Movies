import re
import logging
import random
from datetime import datetime, time
from time import perf_counter
from django.contrib.auth import get_user_model
from django.conf import settings
from django.core.cache import cache
from django.db import connection, transaction
from django.db.models import Case, Count, Avg, Exists, F, FloatField, Func, IntegerField, OuterRef, Q, Subquery, Value, When
from django.db.models.functions import Cast, Coalesce
from django.contrib.postgres.search import SearchQuery, SearchRank
from rest_framework.generics import RetrieveAPIView, ListAPIView
from rest_framework import generics, permissions, status
from rest_framework.authtoken.models import Token
from rest_framework.parsers import MultiPartParser, FormParser, JSONParser
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework.utils.urls import remove_query_param, replace_query_param
from rest_framework.exceptions import NotAuthenticated, PermissionDenied, ValidationError
from rest_framework.permissions import IsAuthenticated, AllowAny
from .serializers import (
    AppBrandingSerializer,
    FriendMentionSerializer, FriendshipSerializer, UserProfileSerializer, MeSerializer, UserMiniSerializer, UserMiniWithFollowersCountSerializer,
    PostListSerializer, PostCreateSerializer, PostDetailSerializer, SocialActivitySerializer,
    PostWriteSerializer, CommentReactionSerializer, CommentSerializer, MeMessageSerializer, PublicCommentFeedSerializer, RegisterSerializer, MovieListSerializer, MovieAutocompleteSerializer, MovieSearchLightSerializer, MovieSearchResultSerializer,
    MyMovieListItemSerializer,
    MyMovieRecommendationItemSerializer,
    UserMovieRecommendationItemSerializer,
    MovieRatingSerializer, ProfileFavoriteSlotSerializer, ProfileFavoriteSlotWriteSerializer,
    ProfileFavoriteMovieSerializer, UserTasteProfileInspectSerializer, WeeklyRecommendationItemSerializer,
    PrivacySettingsSerializer, UserVisibilityBlockSerializer, CreateUserVisibilityBlockSerializer, UserSearchSerializer,
    SocialListUserSerializer, PersonalDataSerializer, DirectedConversationSerializer, DirectedConversationMessageSerializer,
)
from .models import (
    AppBranding,
    Comment,
    CommentReaction,
    Follow,
    Friendship,
    Movie,
    MovieListItem,
    MovieRecommendationItem,
    normalize_movie_search_text,
    MovieRating,
    ProfileFavoriteMovie,
    Post,
    Rating,
    UserTasteProfile,
    UserDirectorPreference,
    UserGenrePreference,
    UserTypePreference,
    UserNotification,
    UserVisibilityBlock,
    WeeklyRecommendationItem,
    WeeklyRecommendationSnapshot,
)
from .permissions import IsAuthorOrReadOnly, IsCommentAuthorOrReadOnly
from .pagination import AutocompletePagination, DefaultPagination, FeedMoviesPagination
from .social_feed import SocialActivityFeedService
from .weekly_recommendations import (
    get_previous_closed_week_window,
    refresh_weekly_recommendation_snapshot,
)
from .feed_pool import DailyFeedPoolService
from .visibility import (
    can_view_user_profile,
    filter_out_authors_who_blocked_viewer,
    is_blocked_from_user_content,
)
from django.core.exceptions import ValidationError as DjangoValidationError
from django.shortcuts import get_object_or_404
from django.utils import timezone

User = get_user_model()
logger = logging.getLogger(__name__)
BRANDING_CACHE_KEY = "app_branding_active_v1"


def split_search_terms(search):
    return [term for term in re.split(r"\s+", search.strip()) if term]


MOVIE_SEARCH_FIELD_WEIGHTS = (
    ("title_spanish", 60),
    ("title_english", 55),
    ("director", 30),
    ("cast_members", 15),
    ("genre", 12),
    ("type", 8),
)
MOVIE_SEARCH_SYNOPSIS_WEIGHT = ("synopsis", 2)
MOVIE_SEARCH_YEAR_WEIGHT = 6
MOVIE_SEARCH_SHORT_TERM_LENGTH = 2
MOVIE_SEARCH_RELEVANCE_GROUPS = (
    (("title_spanish", "title_english"), 500),
    (("director",), 250),
    (("cast_members",), 120),
    (("genre", "type"), 60),
)

MOVIE_AUTOCOMPLETE_SEARCH_FIELD_MAP = {
    "title_spanish": "title_spanish_search",
    "title_english": "title_english_search",
    "director": "director_search",
    "cast_members": "cast_members_search",
}
MOVIE_AUTOCOMPLETE_SEARCH_FIELDS = frozenset(MOVIE_AUTOCOMPLETE_SEARCH_FIELD_MAP)
MOVIE_AUTOCOMPLETE_FAST_FIELDS = (
    "title_spanish",
    "title_english",
    "director",
)
MOVIE_AUTOCOMPLETE_EXTENDED_FIELDS = (
    "cast_members",
)
MOVIE_AUTOCOMPLETE_EXTENDED_MATCH_FIELDS = (
    *MOVIE_AUTOCOMPLETE_FAST_FIELDS,
    *MOVIE_AUTOCOMPLETE_EXTENDED_FIELDS,
)
MOVIE_AUTOCOMPLETE_MIN_TERM_LENGTH = 3
MOVIE_AUTOCOMPLETE_SEARCH_LOOKUP_SUFFIX = "__contains"


def _build_movie_term_match(term, weighted_fields, search_lookup_suffix):
    """Return an OR predicate for one search term across searchable metadata."""
    term_match = Q()
    for field, _weight in weighted_fields:
        term_match |= Q(**{f"{field}{search_lookup_suffix}": term})

    if term.isdigit():
        term_match |= Q(release_year=int(term))

    return term_match


def _build_movie_terms_group_match(terms, fields, search_lookup_suffix):
    """Return an AND predicate requiring every term to match one field in a group."""
    group_match = Q()
    for term in terms:
        term_match = Q()
        for field in fields:
            term_match |= Q(**{f"{field}{search_lookup_suffix}": term})
        group_match &= term_match
    return group_match


def _map_movie_search_field(field, search_field_map):
    if not search_field_map:
        return field
    return search_field_map.get(field, field)


def _map_movie_weighted_fields(weighted_fields, search_field_map, allowed_fields=None):
    return [
        (_map_movie_search_field(field, search_field_map), weight)
        for field, weight in weighted_fields
        if allowed_fields is None or field in allowed_fields
    ]


def _map_movie_relevance_groups(search_field_map, allowed_fields=None):
    mapped_groups = []
    for fields, weight in MOVIE_SEARCH_RELEVANCE_GROUPS:
        mapped_fields = tuple(
            _map_movie_search_field(field, search_field_map)
            for field in fields
            if allowed_fields is None or field in allowed_fields
        )
        if mapped_fields:
            mapped_groups.append((mapped_fields, weight))
    return mapped_groups


def apply_movie_search(
    queryset,
    search,
    include_relevance=True,
    include_synopsis=True,
    use_unaccent=None,
    search_field_map=None,
    normalize_terms=False,
    allowed_fields=None,
    search_lookup_suffix=None,
):
    if normalize_terms:
        search = normalize_movie_search_text(search)

    terms = split_search_terms(search)
    if not terms:
        return queryset

    weighted_fields = list(MOVIE_SEARCH_FIELD_WEIGHTS)
    if include_synopsis:
        weighted_fields.append(MOVIE_SEARCH_SYNOPSIS_WEIGHT)
    weighted_fields = _map_movie_weighted_fields(weighted_fields, search_field_map, allowed_fields)
    relevance_groups = _map_movie_relevance_groups(search_field_map, allowed_fields)

    if search_lookup_suffix is None:
        if use_unaccent is None:
            use_unaccent = connection.vendor == "postgresql"
        search_lookup_suffix = "__unaccent__icontains" if use_unaccent else "__icontains"

    # Main functional rule: every typed term must match at least one metadata
    # field. Inside a single term we use OR across metadata fields; across terms
    # we combine with AND so common words or years cannot make unrelated movies
    # pass the filter by themselves.
    filters = Q()
    for term in terms:
        filters &= _build_movie_term_match(term, weighted_fields, search_lookup_suffix)

    filtered_queryset = queryset.filter(filters)
    if not include_relevance:
        return filtered_queryset

    score_expr = Value(0, output_field=IntegerField())
    relevance_terms = [
        term
        for term in terms
        if term.isdigit() or len(term) > MOVIE_SEARCH_SHORT_TERM_LENGTH
    ]
    if not relevance_terms:
        relevance_terms = terms

    text_relevance_terms = [term for term in relevance_terms if not term.isdigit()]
    for fields, weight in relevance_groups:
        if not text_relevance_terms:
            break
        group_match = _build_movie_terms_group_match(
            text_relevance_terms,
            fields,
            search_lookup_suffix,
        )
        score_expr += Case(
            When(group_match, then=Value(weight)),
            default=Value(0),
            output_field=IntegerField(),
        )

    for term in relevance_terms:
        for field, weight in weighted_fields:
            lookup = {f"{field}{search_lookup_suffix}": term}
            score_expr += Case(
                When(**lookup, then=Value(weight)),
                default=Value(0),
                output_field=IntegerField(),
            )

        if term.isdigit():
            year_lookup = {"release_year": int(term)}
            score_expr += Case(
                When(**year_lookup, then=Value(MOVIE_SEARCH_YEAR_WEIGHT)),
                default=Value(0),
                output_field=IntegerField(),
            )

    return filtered_queryset.annotate(search_relevance=score_expr)


def _is_autocomplete_year_term(term):
    return term.isdigit() and len(term) == 4 and int(term) >= 1000


def _get_autocomplete_terms(search):
    normalized_search = normalize_movie_search_text(search)
    terms = split_search_terms(normalized_search)
    return [
        term
        for term in terms
        if term.isdigit() or len(term) >= MOVIE_AUTOCOMPLETE_MIN_TERM_LENGTH
    ]


def _split_autocomplete_search_terms(search):
    terms = _get_autocomplete_terms(search)
    year_terms = []
    text_terms = []
    for term in terms:
        if _is_autocomplete_year_term(term):
            year = int(term)
            if year not in year_terms:
                year_terms.append(year)
        else:
            text_terms.append(term)
    return text_terms, year_terms


def _apply_autocomplete_year_filters(queryset, year_terms):
    for year in year_terms:
        queryset = queryset.filter(release_year=year)
    return queryset


def _annotate_recency_score(queryset):
    current_year = timezone.now().year
    return queryset.annotate(
        recency_score=Case(
            When(release_year__isnull=True, then=Value(4)),
            When(release_year__gte=current_year - 9, then=Value(0)),
            When(release_year__gte=current_year - 19, then=Value(1)),
            When(release_year__gte=current_year - 29, then=Value(2)),
            default=Value(3),
            output_field=IntegerField(),
        )
    )


def _get_autocomplete_recency_ordering(queryset, year_terms):
    if year_terms:
        return queryset, []
    return _annotate_recency_score(queryset), ["recency_score"]


def _map_autocomplete_fields(fields):
    return tuple(MOVIE_AUTOCOMPLETE_SEARCH_FIELD_MAP[field] for field in fields)


def _build_autocomplete_terms_filter(terms, fields, include_release_year=False):
    filters = Q()
    for term in terms:
        term_match = Q()
        for field in fields:
            term_match |= Q(**{f"{field}{MOVIE_AUTOCOMPLETE_SEARCH_LOOKUP_SUFFIX}": term})
        if include_release_year and term.isdigit():
            term_match |= Q(release_year=int(term))
        filters &= term_match
    return filters


def _build_autocomplete_group_match(terms, fields):
    text_terms = [term for term in terms if not term.isdigit()]
    if not text_terms:
        return Q(pk__isnull=False)
    return _build_autocomplete_terms_filter(text_terms, fields)


def _order_autocomplete_fast_queryset(queryset, terms, year_terms=None):
    year_terms = year_terms or []
    title_fields = _map_autocomplete_fields(("title_spanish", "title_english"))
    director_fields = _map_autocomplete_fields(("director",))
    title_match = _build_autocomplete_group_match(terms, title_fields)
    director_match = _build_autocomplete_group_match(terms, director_fields)
    release_year_desc = F("release_year").desc(nulls_last=True)
    queryset, recency_ordering = _get_autocomplete_recency_ordering(queryset, year_terms)

    return queryset.annotate(
        autocomplete_title_match=Case(
            When(title_match, then=Value(1)),
            default=Value(0),
            output_field=IntegerField(),
        ),
        autocomplete_director_match=Case(
            When(director_match, then=Value(1)),
            default=Value(0),
            output_field=IntegerField(),
        ),
    ).order_by(
        "-autocomplete_title_match",
        "-autocomplete_director_match",
        *recency_ordering,
        release_year_desc,
        "-id",
    )


def build_movie_autocomplete_fast_queryset(queryset, search):
    # Fast lane: query only pre-normalized title/director columns plus direct
    # release_year filters. Four-digit year terms are removed from the textual
    # predicate so PostgreSQL can use the release_year index instead of checking
    # every autocomplete text column with LIKE.
    text_terms, year_terms = _split_autocomplete_search_terms(search)
    if not text_terms and not year_terms:
        return queryset.none()

    fast_fields = _map_autocomplete_fields(MOVIE_AUTOCOMPLETE_FAST_FIELDS)
    filters = _build_autocomplete_terms_filter(text_terms, fast_fields)
    queryset = _apply_autocomplete_year_filters(queryset, year_terms)
    return _order_autocomplete_fast_queryset(queryset.filter(filters), text_terms, year_terms)


def build_movie_autocomplete_extended_queryset(queryset, search, fast_queryset=None):
    # Extended lane: appended only after the title/director fast lane cannot fill
    # the requested page. Each text term may match any autocomplete metadata that
    # is allowed for the endpoint, so cross-field queries like title + cast still
    # keep AND semantics across all terms. Genre/type are intentionally excluded
    # from autocomplete text predicates, and four-digit years stay as release_year
    # filters instead of text LIKE predicates.
    text_terms, year_terms = _split_autocomplete_search_terms(search)
    if not text_terms and not year_terms:
        return queryset.none()

    match_fields = _map_autocomplete_fields(MOVIE_AUTOCOMPLETE_EXTENDED_MATCH_FIELDS)
    filters = _build_autocomplete_terms_filter(text_terms, match_fields)
    extended_queryset = _apply_autocomplete_year_filters(queryset, year_terms).filter(filters)
    if fast_queryset is not None:
        extended_queryset = extended_queryset.exclude(pk__in=fast_queryset.values("pk"))
    release_year_desc = F("release_year").desc(nulls_last=True)
    extended_queryset, recency_ordering = _get_autocomplete_recency_ordering(
        extended_queryset,
        year_terms,
    )
    return extended_queryset.order_by(*recency_ordering, release_year_desc, "-id")


def apply_movie_autocomplete_search(queryset, search):
    # Legacy queryset helper: keep the old broad autocomplete predicate for
    # callers that inspect or reuse it directly, but drop the expensive
    # search_relevance annotation. The API endpoint below uses the explicit
    # two-phase fast/extended lane builders instead.
    return apply_movie_search(
        queryset,
        search,
        include_relevance=False,
        include_synopsis=False,
        use_unaccent=False,
        search_field_map=MOVIE_AUTOCOMPLETE_SEARCH_FIELD_MAP,
        search_lookup_suffix=MOVIE_AUTOCOMPLETE_SEARCH_LOOKUP_SUFFIX,
        normalize_terms=True,
        allowed_fields=MOVIE_AUTOCOMPLETE_SEARCH_FIELDS,
    )


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


def is_valid_directed_comment(comment):
    if comment.visibility != Comment.VISIBILITY_MENTIONED:
        return False
    if not comment.target_user_id:
        return False
    return comment.has_valid_target_mention()


def filter_valid_directed_comments(queryset):
    valid_ids = [comment.id for comment in queryset if is_valid_directed_comment(comment)]
    if not valid_ids:
        return queryset.none()
    return queryset.filter(id__in=valid_ids).order_by("-created_at", "-id")


def get_valid_directed_comment_ids(queryset):
    optimized_queryset = queryset.select_related(
        "author",
        "author__profile",
        "movie",
        "target_user",
        "target_user__profile",
    ).only(
        "id",
        "author",
        "author__username",
        "author__profile",
        "visibility",
        "body",
        "movie",
        "target_user_id",
        "target_user",
        "target_user__username",
        "target_user__profile",
    )
    return [comment.id for comment in optimized_queryset if is_valid_directed_comment(comment)]


def get_unread_private_message_count(user):
    queryset = (
        Comment.objects.filter(
            visibility=Comment.VISIBILITY_MENTIONED,
            target_user=user,
            is_read=False,
        )
        .exclude(author=user)
        .select_related("target_user")
        .order_by("-created_at", "-id")
    )
    valid_ids = get_valid_directed_comment_ids(queryset)
    if not valid_ids:
        return 0
    return Comment.objects.filter(id__in=valid_ids, is_read=False).count()


def build_actor_payload(actor, request):
    if not actor:
        return None
    avatar = None
    if hasattr(actor, "profile") and actor.profile.avatar:
        avatar_url = actor.profile.avatar.url
        avatar = request.build_absolute_uri(avatar_url) if request else avatar_url
    full_name = f"{(actor.first_name or '').strip()} {(actor.last_name or '').strip()}".strip()
    return {
        "id": actor.id,
        "username": actor.username,
        "display_name": full_name or actor.username,
        "avatar": avatar,
    }


def build_notification_message(notification):
    actor_username = notification.actor.username if notification.actor else "Alguien"
    if notification.type == UserNotification.TYPE_PUBLIC_COMMENT_REACTION:
        if notification.reaction_type == CommentReaction.REACT_DISLIKE:
            return f"A {actor_username} no le gustó tu comentario público"
        return f"A {actor_username} le gustó tu comentario público"
    if notification.type == UserNotification.TYPE_PRIVATE_COMMENT_REACTION:
        if notification.reaction_type == CommentReaction.REACT_DISLIKE:
            return f"A {actor_username} no le gustó tu mensaje"
        return f"A {actor_username} le gustó tu mensaje"
    return "Tienes una notificación"


def get_current_reaction_notifications_queryset(user):
    base_queryset = UserNotification.objects.filter(recipient=user)
    reaction_types = {
        UserNotification.TYPE_PUBLIC_COMMENT_REACTION,
        UserNotification.TYPE_PRIVATE_COMMENT_REACTION,
    }
    result_ids = []
    seen_keys = set()
    for notification in base_queryset.order_by("-updated_at", "-id"):
        if notification.type in reaction_types:
            if not notification.comment_id or not notification.actor_id:
                continue
            current_reaction_exists = CommentReaction.objects.filter(
                comment_id=notification.comment_id,
                user_id=notification.actor_id,
                reaction_type=notification.reaction_type,
            ).exists()
            if not current_reaction_exists:
                continue
            key = (
                notification.recipient_id,
                notification.actor_id,
                notification.comment_id,
                notification.type,
            )
            if key in seen_keys:
                continue
            seen_keys.add(key)
        result_ids.append(notification.id)

    if not result_ids:
        return UserNotification.objects.none()
    return UserNotification.objects.filter(id__in=result_ids)


def filter_comments_visible_to_user(queryset, user):
    queryset = filter_out_authors_who_blocked_viewer(queryset, user, author_field="author")
    if not user or not user.is_authenticated:
        return queryset.filter(visibility=Comment.VISIBILITY_PUBLIC)

    return queryset.filter(
        Q(visibility=Comment.VISIBILITY_PUBLIC)
        | Q(author=user)
        | Q(target_user=user)
    )


def build_profile_favorite_movie_payload_by_id(viewer_user, movie_ids, perspective_user=None):
    if not movie_ids:
        return {}

    perspective = perspective_user or viewer_user
    movies = (
        Movie.objects.filter(id__in=movie_ids)
        .with_display_rating()
        .with_my_rating(perspective)
        .with_following_rating_stats_for_user_id(getattr(perspective, "id", None))
        .annotate(
            general_rating=F("display_rating"),
            owner_rating=F("my_rating"),
            owner_following_avg_rating=F("following_avg_rating"),
            owner_following_ratings_count=F("following_ratings_count"),
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
                    "first_name": user.first_name,
                    "last_name": user.last_name,
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


class AppBrandingView(APIView):
    permission_classes = [AllowAny]

    def get(self, request):
        payload = cache.get(BRANDING_CACHE_KEY)
        if payload is not None:
            return Response(payload, status=status.HTTP_200_OK)

        branding = (
            AppBranding.objects.filter(is_active=True).order_by("-updated_at", "-id").first()
            or AppBranding.objects.order_by("-updated_at", "-id").first()
        )
        if not branding:
            payload = {"app_name": "MiAppSocialMovies", "default_logo_url": None}
            cache.set(BRANDING_CACHE_KEY, payload, timeout=300)
            return Response(payload, status=status.HTTP_200_OK)

        payload = AppBrandingSerializer(branding, context={"request": request}).data
        cache.set(BRANDING_CACHE_KEY, payload, timeout=300)
        return Response(payload, status=status.HTTP_200_OK)


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

    def retrieve(self, request, *args, **kwargs):
        obj = self.get_object()
        if is_blocked_from_user_content(obj, request.user):
            raise PermissionDenied("You do not have permission to view this profile.")

        can_view_full_profile = can_view_user_profile(obj, request.user)
        serializer = self.get_serializer(
            obj,
            context={
                **self.get_serializer_context(),
                "can_view_full_profile": can_view_full_profile,
            },
        )
        return Response(serializer.data)

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


class MePersonalDataView(generics.RetrieveUpdateAPIView):
    permission_classes = [permissions.IsAuthenticated]
    serializer_class = PersonalDataSerializer
    parser_classes = [JSONParser, MultiPartParser, FormParser]

    def get_object(self):
        return User.objects.select_related("profile").get(pk=self.request.user.pk)
    
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
    serializer_class = UserMiniWithFollowersCountSerializer

    def get_serializer_class(self):
        username = self.kwargs.get("username", "")
        if username.lower() == "me":
            return SocialListUserSerializer
        return UserMiniWithFollowersCountSerializer

    def get_queryset(self):
        username = self.kwargs["username"]
        if username.lower() == "me":
            if not self.request.user.is_authenticated:
                raise NotAuthenticated("Authentication credentials were not provided.")
            target = self.request.user
        else:
            target = get_object_or_404(User.objects.select_related("profile"), username=username)
            if not can_view_user_profile(target, self.request.user):
                raise PermissionDenied("You do not have permission to view this profile.")

        followers_count_subquery = (
            Follow.objects
            .filter(following_id=OuterRef("pk"))
            .values("following_id")
            .annotate(total=Count("id"))
            .values("total")
        )

        return (
            User.objects
            .filter(followers__follower=target)  # users que <target> está siguiendo
            .select_related("profile")
            .annotate(
                followers_count=Coalesce(
                    Subquery(followers_count_subquery, output_field=IntegerField()),
                    Value(0),
                )
            )
            .order_by("username")
            .distinct()
        )


class UserFriendsListView(ListAPIView):
    permission_classes = [AllowAny]
    serializer_class = SocialListUserSerializer

    def get_queryset(self):
        target = get_object_or_404(
            User.objects.select_related("profile"),
            username=self.kwargs["username"],
        )
        if not can_view_user_profile(target, self.request.user):
            raise PermissionDenied("You do not have permission to view this profile.")

        blocked_by_viewer_ids = UserVisibilityBlock.objects.filter(
            owner=self.request.user,
        ).values_list("blocked_user_id", flat=True) if self.request.user.is_authenticated else []
        blocked_viewer_subquery = UserVisibilityBlock.objects.filter(
            owner_id=OuterRef("id"),
            blocked_user_id=self.request.user.id,
        ) if self.request.user.is_authenticated else UserVisibilityBlock.objects.none()

        queryset = (
            User.objects
            .filter(
                Q(friendships_as_user1__user2=target, friendships_as_user1__status=Friendship.STATUS_ACCEPTED)
                | Q(friendships_as_user2__user1=target, friendships_as_user2__status=Friendship.STATUS_ACCEPTED)
            )
            .exclude(id=target.id)
            .select_related("profile")
            .annotate(followers_count=Count("followers", distinct=True))
            .order_by("username")
            .distinct()
        )
        if self.request.user.is_authenticated:
            queryset = (
                queryset
                .exclude(id__in=blocked_by_viewer_ids)
                .annotate(_blocked_viewer=Exists(blocked_viewer_subquery))
                .filter(_blocked_viewer=False)
            )
        return queryset


class MeFollowingListView(ListAPIView):
    permission_classes = [IsAuthenticated]
    serializer_class = SocialListUserSerializer

    def get_queryset(self):
        followers_count_subquery = (
            Follow.objects
            .filter(following_id=OuterRef("pk"))
            .values("following_id")
            .annotate(total=Count("id"))
            .values("total")
        )
        return (
            User.objects
            .filter(followers__follower=self.request.user)
            .exclude(id=self.request.user.id)
            .select_related("profile")
            .annotate(
                followers_count=Coalesce(
                    Subquery(followers_count_subquery, output_field=IntegerField()),
                    Value(0),
                )
            )
            .order_by("username")
            .distinct()
        )


class SocialFollowingListView(ListAPIView):
    permission_classes = [IsAuthenticated]
    serializer_class = SocialListUserSerializer

    def get_queryset(self):
        blocked_by_me_ids = UserVisibilityBlock.objects.filter(
            owner=self.request.user,
        ).values_list("blocked_user_id", flat=True)
        blocked_me_subquery = UserVisibilityBlock.objects.filter(
            owner_id=OuterRef("id"),
            blocked_user_id=self.request.user.id,
        )
        return (
            User.objects
            .filter(followers__follower=self.request.user)
            .exclude(id=self.request.user.id)
            .exclude(id__in=blocked_by_me_ids)
            .annotate(_blocked_me=Exists(blocked_me_subquery))
            .filter(_blocked_me=False)
            .select_related("profile")
            .annotate(followers_count=Count("followers", distinct=True))
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


class SocialFriendsListView(ListAPIView):
    permission_classes = [IsAuthenticated]
    serializer_class = SocialListUserSerializer

    def get_queryset(self):
        blocked_by_me_ids = UserVisibilityBlock.objects.filter(
            owner=self.request.user,
        ).values_list("blocked_user_id", flat=True)
        blocked_me_subquery = UserVisibilityBlock.objects.filter(
            owner_id=OuterRef("id"),
            blocked_user_id=self.request.user.id,
        )
        friends_queryset = (
            User.objects
            .filter(
                Q(friendships_as_user1__user2=self.request.user, friendships_as_user1__status=Friendship.STATUS_ACCEPTED)
                | Q(friendships_as_user2__user1=self.request.user, friendships_as_user2__status=Friendship.STATUS_ACCEPTED)
            )
            .exclude(id=self.request.user.id)
            .exclude(id__in=blocked_by_me_ids)
            .annotate(_blocked_me=Exists(blocked_me_subquery))
            .filter(_blocked_me=False)
            .select_related("profile")
            .annotate(followers_count=Count("followers", distinct=True))
            .order_by("username")
            .distinct()
        )
        return friends_queryset


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
        scope = SocialActivityFeedService.normalize_scope(
            self.request.query_params.get("scope")
        )

        return SocialActivityFeedService.build_feed(
            user=self.request.user,
            scope=scope,
        )

    def get_serializer_context(self):
        context = super().get_serializer_context()
        context["request"] = self.request
        return context


class UserProfileActivityView(generics.ListAPIView):
    permission_classes = [permissions.IsAuthenticated]
    serializer_class = SocialActivitySerializer

    def get_queryset(self):
        target = get_object_or_404(
            User.objects.select_related("profile"),
            username=self.kwargs["username"],
        )
        if not can_view_user_profile(target, self.request.user):
            raise PermissionDenied("You do not have permission to view this profile.")

        return SocialActivityFeedService.build_feed_for_actor(
            viewer=self.request.user,
            actor=target,
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
            .order_by("-created_at", "-id")
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
        target_user = self._resolve_mentioned_user(username)
        if target_user is None or target_user == self.request.user:
            return None

        friendship = Friendship.between(self.request.user, target_user).filter(
            status=Friendship.STATUS_ACCEPTED,
        ).first()
        if friendship is None:
            return None

        return target_user

    def _resolve_mentioned_user(self, raw_username):
        username = (raw_username or "").strip().lstrip("@")
        if not username:
            return None

        exact_match = User.objects.filter(username=username).first()
        if exact_match is not None:
            return exact_match

        case_insensitive_matches = User.objects.filter(username__iexact=username)
        if case_insensitive_matches.count() == 1:
            return case_insensitive_matches.first()

        return None

    def _get_mentioned_friend_from_payload(self, data):
        username = (data.get("mentioned_username") or data.get("recipient_username") or "").strip()
        if not username:
            return None, False

        target_user = self._resolve_mentioned_user(username)
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
        is_read = visibility != Comment.VISIBILITY_MENTIONED
        serializer.save(
            author=self.request.user,
            movie=movie,
            target_user=target_user,
            visibility=visibility,
            is_read=is_read,
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
    http_method_names = ["get", "put", "patch", "delete", "head", "options"]

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
        movie_id = self.request.query_params.get("movie_id")
        queryset = (
            Comment.objects.filter(
                visibility=Comment.VISIBILITY_MENTIONED,
                target_user=self.request.user,
            )
            .select_related("author", "author__profile", "movie", "target_user")
            .order_by("-created_at", "-id")
        )
        if movie_id:
            queryset = queryset.filter(movie_id=movie_id)
        return annotate_comments_for_user(
            filter_valid_directed_comments(queryset),
            self.request.user,
        )


class SentDirectedCommentsView(generics.ListAPIView):
    permission_classes = [permissions.IsAuthenticated]
    serializer_class = CommentSerializer

    def get_queryset(self):
        movie_id = self.request.query_params.get("movie_id")
        queryset = (
            Comment.objects.filter(
                visibility=Comment.VISIBILITY_MENTIONED,
                author=self.request.user,
            )
            .select_related("author", "author__profile", "movie", "target_user")
            .order_by("-created_at", "-id")
        )
        if movie_id:
            queryset = queryset.filter(movie_id=movie_id)
        return annotate_comments_for_user(
            filter_valid_directed_comments(queryset),
            self.request.user,
        )


class DirectedCommentsListView(generics.ListAPIView):
    permission_classes = [permissions.IsAuthenticated]
    serializer_class = CommentSerializer

    def get_queryset(self):
        movie_id = self.request.query_params.get("movie_id")
        queryset = (
            Comment.objects.filter(
                visibility=Comment.VISIBILITY_MENTIONED,
            )
            .filter(
                Q(author=self.request.user) | Q(target_user=self.request.user)
            )
            .select_related("author", "author__profile", "movie", "target_user")
            .order_by("-created_at", "-id")
        )
        if movie_id:
            queryset = queryset.filter(movie_id=movie_id)
        return annotate_comments_for_user(
            filter_valid_directed_comments(queryset),
            self.request.user,
        )


class MeMessagesView(generics.ListAPIView):
    permission_classes = [permissions.IsAuthenticated]
    serializer_class = MeMessageSerializer

    def list(self, request, *args, **kwargs):
        private_messages_qs = (
            Comment.objects.filter(
                visibility=Comment.VISIBILITY_MENTIONED,
            )
            .filter(Q(target_user=request.user) | Q(author=request.user))
            .select_related("author", "author__profile", "movie", "target_user")
            .order_by("-created_at", "-id")
        )
        private_messages = annotate_comments_for_user(
            filter_valid_directed_comments(private_messages_qs),
            request.user,
        )

        private_reactions_qs = (
            CommentReaction.objects.filter(
                comment__visibility=Comment.VISIBILITY_MENTIONED,
            )
            .filter(
                Q(comment__author=request.user, user__isnull=False)
                | Q(user=request.user, comment__author__isnull=False)
            )
            .exclude(user_id=F("comment__author_id"))
            .select_related("user", "user__profile", "comment", "comment__author", "comment__movie")
            .order_by("-created_at", "-id")
        )
        private_reactions = [item for item in private_reactions_qs if item.comment.has_valid_target_mention()]

        message_items = MeMessageSerializer(
            private_messages,
            many=True,
            context={"request": request},
        ).data
        reaction_items = [
            {
                "id": f"private-reaction-{item.id}",
                "type": UserNotification.TYPE_PRIVATE_COMMENT_REACTION,
                "reaction_type": item.reaction_type,
                "reaction_value": item.reaction_type,
                "created_at": item.created_at.isoformat(),
                "actor": build_actor_payload(item.user, request),
                "movie": (
                    {
                        "id": item.comment.movie.id,
                        "title_english": item.comment.movie.title_english,
                        "title_spanish": item.comment.movie.title_spanish,
                        "type": item.comment.movie.type,
                        "genre": item.comment.movie.genre,
                    }
                    if item.comment and item.comment.movie
                    else None
                ),
                "comment_id": item.comment_id,
                "comment_author": build_actor_payload(item.comment.author, request) if item.comment else None,
                "direction": "received" if item.comment and item.comment.author_id == request.user.id else "sent",
                "is_received_reaction": bool(item.comment and item.comment.author_id == request.user.id),
                "is_given_reaction": bool(item.user_id == request.user.id),
                "target_tab": UserNotification.TARGET_PRIVATE_INBOX,
                "message": (
                    (
                        f"A {item.user.username} no le gustó tu mensaje"
                        if item.reaction_type == CommentReaction.REACT_DISLIKE
                        else f"A {item.user.username} le gustó tu mensaje"
                    )
                    if item.comment and item.comment.author_id == request.user.id
                    else (
                        f"No te gustó el mensaje de {item.comment.author.username}"
                        if item.reaction_type == CommentReaction.REACT_DISLIKE
                        else f"Te gustó el mensaje de {item.comment.author.username}"
                    )
                ),
                "is_read": True,
            }
            for item in private_reactions
        ]

        items = sorted(
            [*message_items, *reaction_items],
            key=lambda payload: payload["created_at"],
            reverse=True,
        )
        return Response(items, status=status.HTTP_200_OK)


class MeMessagesSummaryView(APIView):
    permission_classes = [permissions.IsAuthenticated]

    def get(self, request):
        queryset = (
            Comment.objects.filter(
                visibility=Comment.VISIBILITY_MENTIONED,
                target_user=request.user,
            )
            .exclude(author=request.user)
            .select_related("target_user")
            .order_by("-created_at", "-id")
        )
        valid_ids = get_valid_directed_comment_ids(queryset)
        total_messages = len(valid_ids)
        unread_count = get_unread_private_message_count(request.user)

        return Response(
            {
                "has_unread_messages": unread_count > 0,
                "unread_count": unread_count,
                "total_messages": total_messages,
            }
        )


class MeMessagesMarkAsReadView(APIView):
    permission_classes = [permissions.IsAuthenticated]

    def post(self, request):
        queryset = (
            Comment.objects.filter(
                visibility=Comment.VISIBILITY_MENTIONED,
                target_user=request.user,
                is_read=False,
            )
            .exclude(author=request.user)
            .order_by("-created_at", "-id")
        )
        valid_ids = get_valid_directed_comment_ids(queryset)
        updated = 0
        if valid_ids:
            updated = Comment.objects.filter(id__in=valid_ids, is_read=False).update(is_read=True)
        return Response({"updated": updated})


class MeNotificationsView(APIView):
    permission_classes = [permissions.IsAuthenticated]

    @staticmethod
    def _should_include_read(request):
        raw_include_read = request.query_params.get("include_read")
        if raw_include_read is None:
            return False
        return str(raw_include_read).strip().lower() in {"1", "true", "yes", "y", "on"}

    def get(self, request):
        include_read = self._should_include_read(request)
        unread_private_messages = get_unread_private_message_count(request.user)
        unread_reactions = get_current_reaction_notifications_queryset(request.user).filter(is_read=False).count()

        reaction_notifications = get_current_reaction_notifications_queryset(request.user)
        if not include_read:
            reaction_notifications = reaction_notifications.filter(is_read=False)
        reaction_notifications = reaction_notifications.select_related("actor", "actor__profile", "movie", "comment").order_by("-created_at", "-id")
        reaction_items = [
            {
                "id": item.id,
                "notification_id": item.id,
                "type": item.type,
                "actor": build_actor_payload(item.actor, request),
                "target_tab": item.target_tab,
                "message": build_notification_message(item),
                "text": build_notification_message(item),
                "reaction_type": item.reaction_type,
                "reaction_value": item.reaction_type,
                "is_received_reaction": bool(
                    item.type in {
                        UserNotification.TYPE_PUBLIC_COMMENT_REACTION,
                        UserNotification.TYPE_PRIVATE_COMMENT_REACTION,
                    }
                ),
                "is_given_reaction": False,
                "created_at": item.created_at,
                "is_read": item.is_read,
                "object": {
                    "comment_id": item.comment_id,
                    "comment_author": build_actor_payload(item.comment.author, request) if item.comment else None,
                    "movie": (
                        {
                            "id": item.movie.id,
                            "title_english": item.movie.title_english,
                            "title_spanish": item.movie.title_spanish,
                        }
                        if item.movie
                        else None
                    ),
                },
            }
            for item in reaction_notifications
        ]

        private_queryset = (
            Comment.objects.filter(
                visibility=Comment.VISIBILITY_MENTIONED,
                target_user=request.user,
            )
            .exclude(author=request.user)
            .select_related("author", "author__profile", "movie", "target_user")
            .order_by("-created_at", "-id")
        )
        if not include_read:
            private_queryset = private_queryset.filter(is_read=False)
        valid_private_messages = filter_valid_directed_comments(private_queryset)
        private_items = [
            {
                "id": f"pm-{comment.id}",
                "notification_id": f"pm-{comment.id}",
                "type": UserNotification.TYPE_PRIVATE_MESSAGE,
                "actor": build_actor_payload(comment.author, request),
                "target_tab": UserNotification.TARGET_PRIVATE_INBOX,
                "message": f"Tienes un mensaje privado de {comment.author.username}",
                "created_at": comment.created_at,
                "is_read": comment.is_read,
                "object": {
                    "comment_id": comment.id,
                    "movie": {
                        "id": comment.movie.id,
                        "title_english": comment.movie.title_english,
                        "title_spanish": comment.movie.title_spanish,
                    },
                },
            }
            for comment in valid_private_messages
        ]

        items = sorted(
            [*reaction_items, *private_items],
            key=lambda payload: payload["created_at"],
            reverse=True,
        )
        return Response(
            {
                "total_unread": unread_private_messages + unread_reactions,
                "items": items,
            },
            status=status.HTTP_200_OK,
        )


class MeNotificationsMarkReadView(APIView):
    permission_classes = [permissions.IsAuthenticated]

    @staticmethod
    def _parse_notification_identifier(raw_id):
        if raw_id is None:
            return (None, None)

        if isinstance(raw_id, int):
            return ("notification", raw_id)

        raw_value = str(raw_id).strip()
        if not raw_value:
            return (None, None)

        if raw_value.startswith("pm-"):
            try:
                return ("private_message", int(raw_value.split("pm-", 1)[1]))
            except (TypeError, ValueError):
                return (None, None)

        if ":" in raw_value:
            raw_prefix, raw_pk = raw_value.split(":", 1)
            normalized_prefix = raw_prefix.strip().lower()
            try:
                parsed_pk = int(raw_pk.strip())
            except (TypeError, ValueError):
                return (None, None)
            if normalized_prefix in {"pm", "private_message", "private-message"}:
                return ("private_message", parsed_pk)
            if normalized_prefix in {"notification", "notif", "n"}:
                return ("notification", parsed_pk)
            return (None, None)

        if "-" in raw_value:
            raw_prefix, raw_pk = raw_value.rsplit("-", 1)
            normalized_prefix = raw_prefix.strip().lower()
            try:
                parsed_pk = int(raw_pk.strip())
            except (TypeError, ValueError):
                return (None, None)
            if normalized_prefix in {"pm", "private_message", "private-message"}:
                return ("private_message", parsed_pk)
            if normalized_prefix in {"notification", "notif", "n"}:
                return ("notification", parsed_pk)
            return (None, None)

        try:
            return ("notification", int(raw_value))
        except (TypeError, ValueError):
            return (None, None)

    def post(self, request):
        notification_id = request.data.get("id")
        notification_ids = request.data.get("ids") or []
        raw_notification_ids_provided = bool(notification_ids) or notification_id is not None
        if notification_id is not None:
            notification_ids = [*notification_ids, notification_id]

        normalized_notification_ids = []
        normalized_private_message_ids = []
        for raw_id in notification_ids:
            item_type, parsed_id = self._parse_notification_identifier(raw_id)
            if item_type == "private_message":
                normalized_private_message_ids.append(parsed_id)
                continue
            if item_type == "notification":
                normalized_notification_ids.append(parsed_id)
                continue

        notification_type = request.data.get("type")
        target_tab = request.data.get("target_tab")

        notifications_qs = UserNotification.objects.filter(
            recipient=request.user,
            is_read=False,
        )
        if raw_notification_ids_provided and not normalized_notification_ids:
            notifications_qs = notifications_qs.none()
        elif normalized_notification_ids:
            notifications_qs = notifications_qs.filter(id__in=normalized_notification_ids)
        if notification_type:
            notifications_qs = notifications_qs.filter(type=notification_type)
        if target_tab:
            notifications_qs = notifications_qs.filter(target_tab=target_tab)
        notifications_updated = notifications_qs.update(is_read=True, read_at=timezone.now())

        messages_updated = 0
        private_message_ids = request.data.get("private_message_ids") or []
        raw_private_message_id = request.data.get("private_message_id")
        if raw_private_message_id is not None:
            private_message_ids = [*private_message_ids, raw_private_message_id]

        extra_private_message_ids = []
        for raw_id in private_message_ids:
            try:
                extra_private_message_ids.append(int(raw_id))
            except (TypeError, ValueError):
                continue
        normalized_private_message_ids.extend(extra_private_message_ids)

        should_mark_private_messages = (
            not normalized_notification_ids
            and not notification_type
            and (not target_tab or target_tab == UserNotification.TARGET_PRIVATE_INBOX)
        )
        private_messages_qs = (
            Comment.objects.filter(
                visibility=Comment.VISIBILITY_MENTIONED,
                target_user=request.user,
                is_read=False,
            )
            .exclude(author=request.user)
            .order_by("-created_at", "-id")
        )
        if normalized_private_message_ids:
            private_messages_qs = private_messages_qs.filter(id__in=normalized_private_message_ids)
        if should_mark_private_messages or normalized_private_message_ids:
            messages_updated = private_messages_qs.update(is_read=True)

        updated_total = notifications_updated + messages_updated

        return Response(
            {
                "updated": updated_total,
                "updated_notifications": notifications_updated,
                "updated_private_messages": messages_updated,
            },
            status=status.HTTP_200_OK,
        )


class MeNotificationsMarkAllReadView(APIView):
    permission_classes = [permissions.IsAuthenticated]

    def post(self, request):
        notifications_updated = UserNotification.objects.filter(
            recipient=request.user,
            is_read=False,
        ).update(is_read=True, read_at=timezone.now())

        private_messages_qs = (
            Comment.objects.filter(
                visibility=Comment.VISIBILITY_MENTIONED,
                target_user=request.user,
                is_read=False,
            )
            .exclude(author=request.user)
            .order_by("-created_at", "-id")
        )
        valid_private_message_ids = get_valid_directed_comment_ids(private_messages_qs)
        messages_updated = 0
        if valid_private_message_ids:
            messages_updated = Comment.objects.filter(id__in=valid_private_message_ids, is_read=False).update(is_read=True)

        return Response(
            {
                "updated": notifications_updated + messages_updated,
                "updated_notifications": notifications_updated,
                "updated_private_messages": messages_updated,
            },
            status=status.HTTP_200_OK,
        )


class MeNotificationsMarkReadBatchView(APIView):
    permission_classes = [permissions.IsAuthenticated]

    def post(self, request):
        raw_ids = request.data.get("ids")
        if not isinstance(raw_ids, list):
            return Response(
                {"detail": "ids must be a list of notification ids."},
                status=status.HTTP_400_BAD_REQUEST,
            )

        normalized_notification_ids = []
        normalized_private_message_ids = []
        for raw_id in raw_ids:
            item_type, parsed_id = MeNotificationsMarkReadView._parse_notification_identifier(raw_id)
            if item_type == "notification":
                normalized_notification_ids.append(parsed_id)
            elif item_type == "private_message":
                normalized_private_message_ids.append(parsed_id)

        if not normalized_notification_ids and not normalized_private_message_ids:
            return Response(
                {
                    "updated": 0,
                    "updated_notifications": 0,
                    "updated_private_messages": 0,
                },
                status=status.HTTP_200_OK,
            )

        notifications_updated = 0
        if normalized_notification_ids:
            notifications_updated = UserNotification.objects.filter(
                recipient=request.user,
                is_read=False,
                id__in=normalized_notification_ids,
            ).update(is_read=True, read_at=timezone.now())

        messages_updated = 0
        if normalized_private_message_ids:
            private_messages_qs = (
                Comment.objects.filter(
                    visibility=Comment.VISIBILITY_MENTIONED,
                    target_user=request.user,
                    is_read=False,
                    id__in=normalized_private_message_ids,
                )
                .exclude(author=request.user)
                .order_by("-created_at", "-id")
            )
            valid_private_message_ids = get_valid_directed_comment_ids(private_messages_qs)
            if valid_private_message_ids:
                messages_updated = Comment.objects.filter(
                    id__in=valid_private_message_ids,
                    is_read=False,
                ).update(is_read=True)

        return Response(
            {
                "updated": notifications_updated + messages_updated,
                "updated_notifications": notifications_updated,
                "updated_private_messages": messages_updated,
            },
            status=status.HTTP_200_OK,
        )


class MeNotificationsMarkContextReadView(APIView):
    permission_classes = [permissions.IsAuthenticated]

    def post(self, request):
        context = str(request.data.get("context") or "").strip().lower()
        if context not in {UserNotification.TARGET_PRIVATE_INBOX, UserNotification.TARGET_ACTIVITY}:
            return Response(
                {"detail": "context must be one of: private_inbox, activity."},
                status=status.HTTP_400_BAD_REQUEST,
            )

        notifications_qs = UserNotification.objects.filter(
            recipient=request.user,
            is_read=False,
            target_tab=context,
        )
        if context == UserNotification.TARGET_PRIVATE_INBOX:
            notifications_qs = notifications_qs.filter(
                type__in=[
                    UserNotification.TYPE_PRIVATE_MESSAGE,
                    UserNotification.TYPE_PRIVATE_COMMENT_REACTION,
                ]
            )
        elif context == UserNotification.TARGET_ACTIVITY:
            notifications_qs = notifications_qs.filter(type=UserNotification.TYPE_PUBLIC_COMMENT_REACTION)

        notifications_updated = notifications_qs.update(is_read=True, read_at=timezone.now())

        messages_updated = 0
        if context == UserNotification.TARGET_PRIVATE_INBOX:
            private_messages_qs = (
                Comment.objects.filter(
                    visibility=Comment.VISIBILITY_MENTIONED,
                    target_user=request.user,
                    is_read=False,
                )
                .exclude(author=request.user)
                .order_by("-created_at", "-id")
            )
            valid_private_message_ids = get_valid_directed_comment_ids(private_messages_qs)
            if valid_private_message_ids:
                messages_updated = Comment.objects.filter(
                    id__in=valid_private_message_ids,
                    is_read=False,
                ).update(is_read=True)

        return Response(
            {
                "updated": notifications_updated + messages_updated,
                "updated_notifications": notifications_updated,
                "updated_private_messages": messages_updated,
            },
            status=status.HTTP_200_OK,
        )


class MovieDirectedCommentsListView(MovieCommentsListCreateView):
    permission_classes = [permissions.IsAuthenticated]
    serializer_class = DirectedConversationSerializer
    pagination_class = DefaultPagination

    def get_serializer_class(self):
        if self.request.method == "POST":
            return CommentSerializer
        return DirectedConversationSerializer

    def get_queryset(self):
        queryset = (
            Comment.objects.filter(
                movie_id=self.kwargs["pk"],
                visibility=Comment.VISIBILITY_MENTIONED,
            )
            .filter(
                Q(author=self.request.user) | Q(target_user=self.request.user)
            )
            .select_related("author", "author__profile", "movie", "target_user", "target_user__profile")
            .order_by("-created_at", "-id")
        )
        valid_ids = get_valid_directed_comment_ids(queryset)
        if not valid_ids:
            return []

        directed_comments = list(
            annotate_comments_for_user(
                queryset.filter(id__in=valid_ids),
                self.request.user,
            )
        )

        grouped = {}
        for comment in directed_comments:
            other_user = comment.target_user if comment.author_id == self.request.user.id else comment.author
            if not other_user:
                continue

            direction = "sent" if comment.author_id == self.request.user.id else "received"
            recipient = comment.target_user if comment.target_user_id else None
            conversation = grouped.setdefault(
                other_user.id,
                {
                    "other_user": other_user,
                    "counterpart": other_user,
                    "recipient": recipient,
                    "direction": direction,
                    "last_message_at": comment.created_at,
                    "messages_preview": [],
                },
            )
            if comment.created_at > conversation["last_message_at"]:
                conversation["last_message_at"] = comment.created_at
                conversation["direction"] = direction
                conversation["recipient"] = recipient
            if len(conversation["messages_preview"]) < 1:
                conversation["messages_preview"].append(comment)

        conversations = sorted(
            grouped.values(),
            key=lambda item: (item["last_message_at"], item["other_user"].id),
            reverse=True,
        )
        for item in conversations:
            item["messages_endpoint"] = self.request.build_absolute_uri(
                f"/api/movies/{self.kwargs['pk']}/comments/directed/conversations/{item['other_user'].username}/messages/"
            )
        return conversations

    def list(self, request, *args, **kwargs):
        queryset = self.get_queryset()
        page = self.paginate_queryset(queryset)
        if page is not None:
            serializer = self.get_serializer(page, many=True)
            return self.get_paginated_response(serializer.data)
        serializer = self.get_serializer(queryset, many=True)
        return Response(serializer.data)

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
            is_read=False,
        )


class DirectedConversationMessagesView(generics.ListAPIView):
    permission_classes = [permissions.IsAuthenticated]
    serializer_class = DirectedConversationMessageSerializer

    def get_queryset(self):
        movie_id = self.kwargs["pk"]
        other_user = get_object_or_404(User, username=self.kwargs["username"])
        queryset = (
            Comment.objects.filter(
                movie_id=movie_id,
                visibility=Comment.VISIBILITY_MENTIONED,
            )
            .filter(
                Q(author=self.request.user, target_user=other_user)
                | Q(author=other_user, target_user=self.request.user)
            )
            .select_related("author", "author__profile", "movie", "target_user", "target_user__profile")
            .order_by("-created_at", "-id")
        )
        valid_ids = get_valid_directed_comment_ids(queryset)
        if not valid_ids:
            return Comment.objects.none()
        return annotate_comments_for_user(queryset.filter(id__in=valid_ids), self.request.user)


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



class MovieSearchView(generics.ListAPIView):
    permission_classes = [permissions.AllowAny]
    serializer_class = MovieSearchLightSerializer
    social_serializer_class = MovieSearchResultSerializer
    include_social_truthy_values = {"1", "true", "yes"}

    def include_social_fields(self):
        return self.request.query_params.get("include_social", "").lower() in self.include_social_truthy_values

    def get_serializer_class(self):
        if self.include_social_fields():
            return self.social_serializer_class
        return super().get_serializer_class()

    def _apply_filters(self, qs):
        if movie_type := self.request.query_params.get("type"):
            qs = qs.filter(type=movie_type)
        if genre := self.request.query_params.get("genre"):
            qs = qs.filter(genre__icontains=genre)
        if release_year := self.request.query_params.get("release_year"):
            qs = qs.filter(release_year=release_year)
        return qs

    def _build_title_boost_annotations(self, normalized_query):
        exact_title_filter = Q(title_english_search=normalized_query) | Q(title_spanish_search=normalized_query)
        prefix_title_filter = Q(title_english_search__startswith=normalized_query) | Q(title_spanish_search__startswith=normalized_query)
        year_terms = [int(term) for term in split_search_terms(normalized_query) if term.isdigit()]
        year_filter = Q(release_year__in=year_terms) if year_terms else Q(pk__isnull=True)

        return {
            "exact_title_match": Case(
                When(exact_title_filter, then=Value(1)),
                default=Value(0),
                output_field=IntegerField(),
            ),
            "prefix_title_match": Case(
                When(prefix_title_filter, then=Value(1)),
                default=Value(0),
                output_field=IntegerField(),
            ),
            "year_match": Case(
                When(year_filter, then=Value(1)),
                default=Value(0),
                output_field=IntegerField(),
            ),
        }

    def get_queryset(self):
        raw_query = (self.request.query_params.get("q") or self.request.query_params.get("search") or "").strip()
        if not raw_query:
            raise ValidationError({"q": "This query parameter is required."})

        user = self.request.user
        search_query = SearchQuery(
            Func(Value(raw_query), function="unaccent"),
            config="simple",
            search_type="websearch",
        )
        normalized_query = normalize_movie_search_text(raw_query)

        if self.include_social_fields():
            qs = Movie.objects.with_display_rating().with_my_rating(user)
            qs = qs.with_in_my_list(user).with_in_my_recommendations(user).with_comment_stats().select_related("author", "author__profile").annotate(
                general_rating=F("display_rating"),
            )
            qs = qs.with_following_rating_stats(user)
        else:
            qs = Movie.objects.annotate(
                display_rating=Cast("external_rating", FloatField()),
            ).only(
                "id",
                "image",
                "title_spanish",
                "title_english",
                "type",
                "genre",
                "release_year",
                "director",
                "cast_members",
                "external_rating",
                "external_votes",
            )
        qs = self._apply_filters(qs)
        # SearchVectorField can be queried like an annotated SearchVector;
        # this compiles to PostgreSQL @@ and uses the GIN index.
        qs = qs.filter(search_vector=search_query)
        qs = qs.annotate(
            search_rank=SearchRank(
                F("search_vector"),
                search_query,
                weights=[0.03, 0.12, 0.40, 1.00],
                normalization=32,
            ),
            **self._build_title_boost_annotations(normalized_query),
        )

        release_year_desc = F("release_year").desc(nulls_last=True)
        return qs.order_by(
            "-exact_title_match",
            "-prefix_title_match",
            "-year_match",
            "-search_rank",
            "-display_rating",
            release_year_desc,
            "-id",
        )


class MovieListView(generics.ListAPIView):
    permission_classes = [permissions.AllowAny]
    serializer_class = MovieListSerializer

    autocomplete_truthy_values = {"1", "true", "yes"}

    def is_autocomplete_request(self):
        return self.request.query_params.get("autocomplete", "").lower() in self.autocomplete_truthy_values

    def get_serializer_class(self):
        if self.is_autocomplete_request():
            return MovieAutocompleteSerializer
        return MovieListSerializer

    def get_paginator(self):
        if self.is_autocomplete_request():
            if not hasattr(self, "_autocomplete_paginator"):
                self._autocomplete_paginator = AutocompletePagination()
            return self._autocomplete_paginator
        return super().get_paginator()

    def _apply_common_filters(self, qs):
        if movie_type := self.request.query_params.get("type"):
            qs = qs.filter(type=movie_type)
        if genre := self.request.query_params.get("genre"):
            qs = qs.filter(genre__icontains=genre)
        if release_year := self.request.query_params.get("release_year"):
            qs = qs.filter(release_year=release_year)
        return qs

    def _get_autocomplete_base_queryset(self):
        qs = Movie.objects.only(
            "id",
            "title_english",
            "title_spanish",
            "type",
            "release_year",
            "image",
            "genre",
            "director",
            "cast_members",
        )
        return self._apply_common_filters(qs)

    def _get_autocomplete_page_size(self):
        paginator = self.get_paginator()
        page_size = paginator.get_page_size(self.request) if paginator else None
        return page_size or AutocompletePagination.page_size

    def _get_autocomplete_page_number(self):
        page_number = self.request.query_params.get(self.get_paginator().page_query_param, 1)
        if page_number in self.get_paginator().last_page_strings:
            return 1
        try:
            return max(1, int(page_number))
        except (TypeError, ValueError):
            return 1

    def _build_autocomplete_page_url(self, page_number):
        if page_number < 1:
            return None
        url = self.request.build_absolute_uri()
        if page_number == 1:
            return remove_query_param(url, self.get_paginator().page_query_param)
        return replace_query_param(url, self.get_paginator().page_query_param, page_number)

    def list(self, request, *args, **kwargs):
        if not self.is_autocomplete_request():
            return super().list(request, *args, **kwargs)

        search = request.query_params.get("search") or request.query_params.get("q")
        if not search:
            return super().list(request, *args, **kwargs)

        page_size = self._get_autocomplete_page_size()
        page_number = self._get_autocomplete_page_number()
        offset = (page_number - 1) * page_size
        base_queryset = self._get_autocomplete_base_queryset()

        fast_queryset = build_movie_autocomplete_fast_queryset(base_queryset, search)
        fast_count = fast_queryset.count()

        page_results = []
        if offset < fast_count:
            page_results = list(fast_queryset[offset:offset + page_size])

        total_count = fast_count
        if len(page_results) < page_size:
            extended_queryset = build_movie_autocomplete_extended_queryset(
                base_queryset,
                search,
                fast_queryset=fast_queryset,
            )
            extended_count = extended_queryset.count()
            total_count += extended_count

            extended_offset = max(0, offset - fast_count)
            remaining = page_size - len(page_results)
            page_results.extend(
                extended_queryset[extended_offset:extended_offset + remaining]
            )

        serializer = self.get_serializer(page_results, many=True)
        next_url = None
        if offset + len(page_results) < total_count:
            next_url = self._build_autocomplete_page_url(page_number + 1)
        previous_url = None
        if page_number > 1:
            previous_url = self._build_autocomplete_page_url(page_number - 1)

        return Response({
            "count": total_count,
            "next": next_url,
            "previous": previous_url,
            "results": serializer.data,
        })

    def get_autocomplete_queryset(self):
        qs = self._get_autocomplete_base_queryset()

        search = self.request.query_params.get("search") or self.request.query_params.get("q")
        if search:
            text_terms, year_terms = _split_autocomplete_search_terms(search)
            qs = _apply_autocomplete_year_filters(qs, year_terms)
            if not text_terms:
                release_year_desc = F("release_year").desc(nulls_last=True)
                return qs.order_by("title_english", release_year_desc, "-id")
            return apply_movie_autocomplete_search(qs, " ".join(text_terms))

        release_year_desc = F("release_year").desc(nulls_last=True)
        return qs.order_by("title_english", release_year_desc, "-id")

    def get_queryset(self):
        if self.is_autocomplete_request():
            return self.get_autocomplete_queryset()

        user = self.request.user
        has_preferences = user.is_authenticated and UserTasteProfile.objects.filter(
            user_id=user.id,
            ratings_count__gt=0,
        ).exists()
        if user.is_authenticated:
            qs = Movie.objects.feed_for_user(user, include_recommendation_score=has_preferences)
        else:
            qs = Movie.objects.with_display_rating().with_my_rating(user)

        qs = qs.with_in_my_list(user).with_in_my_recommendations(user).with_comment_stats().select_related("author", "author__profile").annotate(
            general_rating=F("display_rating"),
        )
        qs = qs.with_following_rating_stats(user)
        qs = self._apply_common_filters(qs)

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

        qs = qs.with_in_my_list(user).with_in_my_recommendations(user).with_comment_stats().select_related("author", "author__profile").annotate(
            general_rating=F("display_rating"),
        )
        return qs.with_following_rating_stats(user)


class MyMovieListView(generics.ListAPIView):
    permission_classes = [IsAuthenticated]
    serializer_class = MyMovieListItemSerializer

    def get_queryset(self):
        return MovieListItem.objects.filter(user=self.request.user).select_related("movie").order_by("-created_at", "-id")


class MovieListToggleView(APIView):
    permission_classes = [IsAuthenticated]

    def post(self, request, pk):
        movie = get_object_or_404(Movie, pk=pk)
        _, created = MovieListItem.objects.get_or_create(user=request.user, movie=movie)
        return Response({"movie_id": movie.id, "is_in_my_list": True, "created": created}, status=status.HTTP_200_OK)

    def delete(self, request, pk):
        deleted_count, _ = MovieListItem.objects.filter(user=request.user, movie_id=pk).delete()
        return Response({"movie_id": pk, "is_in_my_list": False, "deleted": deleted_count > 0}, status=status.HTTP_200_OK)


class MyMovieRecommendationsView(generics.ListAPIView):
    permission_classes = [IsAuthenticated]
    serializer_class = MyMovieRecommendationItemSerializer

    def get_queryset(self):
        return MovieRecommendationItem.objects.filter(user=self.request.user).select_related("movie").order_by("-created_at", "-id")


class UserMovieRecommendationsView(generics.ListAPIView):
    permission_classes = [permissions.IsAuthenticated]
    serializer_class = UserMovieRecommendationItemSerializer

    def get_queryset(self):
        target = get_object_or_404(
            User.objects.select_related("profile"),
            username=self.kwargs["username"],
        )
        if not can_view_user_profile(target, self.request.user):
            raise PermissionDenied("You do not have permission to view this profile.")

        return MovieRecommendationItem.objects.filter(user=target).select_related("movie").order_by("-created_at", "-id")


class MovieRecommendationToggleView(APIView):
    permission_classes = [IsAuthenticated]

    def post(self, request, pk):
        movie = get_object_or_404(Movie, pk=pk)
        _, created = MovieRecommendationItem.objects.get_or_create(user=request.user, movie=movie)
        return Response({"movie_id": movie.id, "is_in_my_recommendations": True, "created": created}, status=status.HTTP_200_OK)

    def delete(self, request, pk):
        deleted_count, _ = MovieRecommendationItem.objects.filter(user=request.user, movie_id=pk).delete()
        return Response({"movie_id": pk, "is_in_my_recommendations": False, "deleted": deleted_count > 0}, status=status.HTTP_200_OK)


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


class UserProfileFavoritesView(APIView):
    permission_classes = [permissions.IsAuthenticated]

    def get(self, request, username):
        target = get_object_or_404(
            User.objects.select_related("profile"),
            username=username,
        )
        if not can_view_user_profile(target, request.user):
            raise PermissionDenied("You do not have permission to view this profile.")

        favorites = list(
            ProfileFavoriteMovie.objects.filter(user=target)
            .select_related("movie")
            .order_by("slot")
        )
        movie_ids = [favorite.movie_id for favorite in favorites]
        movie_payload_by_id = build_profile_favorite_movie_payload_by_id(
            request.user,
            movie_ids,
            perspective_user=target,
        )
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
    FEED_PAGE_CACHE_TTL_SECONDS = 120
    FEED_COUNT_CACHE_TTL_SECONDS = 120
    FEED_DEFAULT_PAGE_FALLBACK = 450

    def _is_feed_profiling_enabled(self):
        query_flag = self.request.query_params.get("profile_feed", "").lower() in {"1", "true", "yes"}
        return bool(getattr(settings, "FEED_PROFILING_ENABLED", False) or query_flag)

    def _should_log_explain(self):
        return self.request.query_params.get("profile_explain", "").lower() in {"1", "true", "yes"}

    def _record_profile_timing(self, key, elapsed_seconds):
        if not getattr(self, "_feed_profile_enabled", False):
            return
        self._feed_profile_timings[key] = round(elapsed_seconds, 6)

    def _record_profile_marker(self, key, value):
        if not getattr(self, "_feed_profile_enabled", False):
            return
        self._feed_profile_timings[key] = value

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

    def _build_pool_filtered_queryset(self, include_search_relevance):
        if not hasattr(self, "_feed_pool_filtered_qs"):
            pool_payload = self._get_pool_payload()
            rotated_ids = pool_payload.ordered_ids
            if not rotated_ids:
                self._feed_pool_filtered_qs = Movie.objects.none()
                self._filtered_rotated_ids = []
                return self._feed_pool_filtered_qs

            queryset = Movie.objects.filter(id__in=rotated_ids)
            if self.request.query_params.get("exclude_rated", "true").lower() != "false":
                queryset = queryset.exclude(movie_ratings__user_id=self.request.user.id)
            if search := self.request.query_params.get("search"):
                queryset = apply_movie_search(queryset, search, include_relevance=include_search_relevance)
            if movie_type := self.request.query_params.get("type"):
                queryset = queryset.filter(type=movie_type)

            queryset = apply_feed_genre_filters(queryset, parse_feed_genre_filters(self.request))
            allowed_ids = set(queryset.values_list("id", flat=True))
            self._filtered_rotated_ids = [movie_id for movie_id in rotated_ids if movie_id in allowed_ids]
            self._feed_pool_filtered_qs = queryset
        return self._feed_pool_filtered_qs

    def _build_feed_count_cache_key(self):
        genres = parse_feed_genre_filters(self.request)
        pool_payload = self._get_pool_payload()
        return "|".join(
            [
                "feed_movies_count_v3",
                f"user:{self.request.user.id}",
                f"pool_date:{pool_payload.pool.pool_date.isoformat()}",
                f"pool_version:{pool_payload.pool.pool_version}",
                f"search:{(self.request.query_params.get('search') or '').strip().lower()}",
                f"type:{(self.request.query_params.get('type') or '').strip().lower()}",
                f"genres:{','.join(genres)}",
                f"exclude_rated:{self.request.query_params.get('exclude_rated', 'true').lower()}",
            ]
        )

    def get_feed_total_count(self):
        if hasattr(self, "_feed_total_count"):
            return self._feed_total_count

        count_cache_key = self._build_feed_count_cache_key()
        cached_count = cache.get(count_cache_key)
        if cached_count is not None:
            self._record_profile_marker("count_cache", "hit")
            self._record_profile_marker("count_cache_key", count_cache_key)
            self._feed_total_count = int(cached_count)
            return self._feed_total_count

        self._record_profile_marker("count_cache", "miss")
        start = perf_counter()
        self._build_pool_filtered_queryset(include_search_relevance=False)
        total_count = len(getattr(self, "_filtered_rotated_ids", []))
        self._record_profile_timing("paginated_count_query_seconds", perf_counter() - start)
        cache.set(count_cache_key, total_count, timeout=self.FEED_COUNT_CACHE_TTL_SECONDS)
        self._record_profile_marker("count_cache_key", count_cache_key)
        self._feed_total_count = int(total_count)
        return self._feed_total_count

    def _resolve_page_size(self):
        paginator = self.paginator
        if not paginator:
            return self.FEED_DEFAULT_PAGE_FALLBACK
        page_size = paginator.get_page_size(self.request)
        if page_size:
            return int(page_size)
        return int(getattr(paginator, "page_size", self.FEED_DEFAULT_PAGE_FALLBACK))

    def _resolve_page_number(self):
        raw_page = self.request.query_params.get("page", 1)
        try:
            page_number = int(raw_page)
        except (TypeError, ValueError):
            return 1
        return max(page_number, 1)

    def _resolve_rotation_bucket(self):
        return int(timezone.now().timestamp() // 7200)

    def _get_pool_payload(self):
        if hasattr(self, "_pool_payload"):
            return self._pool_payload
        service = DailyFeedPoolService(user=self.request.user)
        start = perf_counter()
        payload = service.get_rotated_ids(rotation_bucket=self._resolve_rotation_bucket())
        self._record_profile_timing("pool_resolve_seconds", perf_counter() - start)
        self._record_profile_marker("pool_date", payload.pool.pool_date.isoformat())
        self._pool_payload = payload
        return payload

    def _build_feed_cache_key(self):
        genres = parse_feed_genre_filters(self.request)
        pool_payload = self._get_pool_payload()
        taste_profile = UserTasteProfile.objects.filter(user_id=self.request.user.id).values_list("last_updated_at", flat=True).first()
        profile_version = int(taste_profile.timestamp()) if taste_profile else 0
        return "|".join(
            [
                "feed_movies_v3",
                f"user:{self.request.user.id}",
                f"pool_date:{pool_payload.pool.pool_date.isoformat()}",
                f"pool_version:{pool_payload.pool.pool_version}",
                f"page:{self._resolve_page_number()}",
                f"page_size:{self._resolve_page_size()}",
                f"search:{(self.request.query_params.get('search') or '').strip().lower()}",
                f"type:{(self.request.query_params.get('type') or '').strip().lower()}",
                f"genres:{','.join(genres)}",
                f"exclude_rated:{self.request.query_params.get('exclude_rated', 'true').lower()}",
                f"rotation:{self._resolve_rotation_bucket()}",
                f"profile_version:{profile_version}",
            ]
        )

    def get_queryset(self):
        self._feed_profile_enabled = self._is_feed_profiling_enabled()
        if self._feed_profile_enabled and not hasattr(self, "_feed_profile_timings"):
            self._feed_profile_timings = {}

        base_start = perf_counter()
        self._build_pool_filtered_queryset(include_search_relevance=True)
        ordered_ids = getattr(self, "_filtered_rotated_ids", [])
        self._record_profile_timing("pool_filter_build_seconds", perf_counter() - base_start)
        if not ordered_ids:
            return Movie.objects.none()

        ordering_case = Case(
            *[When(id=movie_id, then=position) for position, movie_id in enumerate(ordered_ids)],
            output_field=IntegerField(),
        )
        order_start = perf_counter()
        ordered_queryset = (
            Movie.objects.filter(id__in=ordered_ids)
            .with_display_rating()
            .annotate(general_rating=F("display_rating"))
            .with_in_my_list(self.request.user)
            .with_in_my_recommendations(self.request.user)
            .with_following_rating_stats(self.request.user)
            .select_related("author", "author__profile")
            .order_by(ordering_case)
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
        self._record_profile_marker("page_cache_key", cache_key)
        cached_payload = cache.get(cache_key)
        if cached_payload is not None:
            self._record_profile_marker("page_cache", "hit")
            if self._feed_profile_enabled:
                self._record_profile_timing("endpoint_total_seconds", perf_counter() - total_start)
                self._log_feed_profile_summary()
            return Response(cached_payload)
        self._record_profile_marker("page_cache", "miss")

        queryset = self.filter_queryset(self.get_queryset())
        page = self.paginate_queryset(queryset)
        if page is not None:
            page_queryset = getattr(page, "object_list", None)
            if page_queryset is not None:
                self._log_profile_sql("page_queryset_sql", page_queryset)
                self._log_profile_explain("page_queryset", page_queryset)
            self._log_profile_sql("count_queryset_sql", self._build_pool_filtered_queryset(include_search_relevance=False))
            self._log_profile_explain("count_queryset", self._build_pool_filtered_queryset(include_search_relevance=False))

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
        week_start_at = timezone.make_aware(
            datetime.combine(snapshot.week_start, time.min),
            timezone.get_current_timezone(),
        )
        week_end_at = timezone.make_aware(
            datetime.combine(snapshot.week_end, time.min),
            timezone.get_current_timezone(),
        )
        display_rating_subquery = Movie.objects.with_display_rating().filter(
            pk=OuterRef("movie_id")
        ).values("display_rating")[:1]
        top_user_ratings = MovieRating.objects.filter(
            movie_id=OuterRef("movie_id"),
            created_at__gte=week_start_at,
            created_at__lt=week_end_at,
        ).annotate(
            followers_count=Count("user__followers", distinct=True),
        ).order_by(
            "-followers_count",
            "-created_at",
            "user_id",
        )

        queryset = items.annotate(
            general_rating=Subquery(display_rating_subquery, output_field=FloatField()),
            display_rating=Subquery(display_rating_subquery, output_field=FloatField()),
            top_user_id=Subquery(top_user_ratings.values("user_id")[:1]),
            top_user_username=Subquery(top_user_ratings.values("user__username")[:1]),
            top_user_avatar=Subquery(top_user_ratings.values("user__profile__avatar")[:1]),
            top_user_followers_count=Coalesce(
                Subquery(top_user_ratings.values("followers_count")[:1], output_field=IntegerField()),
                Value(0),
            ),
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
        reaction_type = serializer.validated_data["reaction"]

        CommentReaction.objects.update_or_create(
            comment=comment,
            user=request.user,
            defaults={"reaction_type": reaction_type},
        )
        if request.user.id != comment.author_id:
            notification_type = (
                UserNotification.TYPE_PRIVATE_COMMENT_REACTION
                if comment.visibility == Comment.VISIBILITY_MENTIONED
                else UserNotification.TYPE_PUBLIC_COMMENT_REACTION
            )
            target_tab = (
                UserNotification.TARGET_PRIVATE_INBOX
                if comment.visibility == Comment.VISIBILITY_MENTIONED
                else UserNotification.TARGET_ACTIVITY
            )
            UserNotification.objects.update_or_create(
                recipient=comment.author,
                actor=request.user,
                comment=comment,
                type=notification_type,
                defaults={
                    "movie": comment.movie,
                    "target_tab": target_tab,
                    "reaction_type": reaction_type,
                    "is_read": False,
                    "read_at": None,
                },
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
