from django.contrib.auth import get_user_model
from django.db.models import Case, Exists, F, IntegerField, OuterRef, Q, When

from .models import Friendship, Profile, UserVisibilityBlock


User = get_user_model()


def are_users_restricted(user_a, user_b):
    """Return whether either user has restricted the other.

    The persisted row remains directional so its owner can remove only their
    own restriction.  Visibility, however, is deliberately bilateral.
    """
    if not user_a or not user_b:
        return False
    if not getattr(user_a, "is_authenticated", False) or not getattr(user_b, "is_authenticated", False):
        return False
    if user_a.id == user_b.id:
        return False
    return UserVisibilityBlock.objects.filter(
        Q(owner_id=user_a.id, blocked_user_id=user_b.id)
        | Q(owner_id=user_b.id, blocked_user_id=user_a.id)
    ).exists()


def restricted_user_ids(user):
    """Return IDs connected to ``user`` by a restriction in either direction."""
    if not user or not getattr(user, "is_authenticated", False):
        return User.objects.none().values_list("id", flat=True)
    return (
        UserVisibilityBlock.objects.filter(Q(owner_id=user.id) | Q(blocked_user_id=user.id))
        .annotate(
            restricted_user_id=Case(
                When(owner_id=user.id, then=F("blocked_user_id")),
                default=F("owner_id"),
                output_field=IntegerField(),
            )
        )
        .values_list("restricted_user_id", flat=True)
    )


def has_restricted_viewer(target_user, viewer):
    """Backward-compatible name for the bilateral visibility check."""
    return are_users_restricted(target_user, viewer)


is_blocked_from_user_content = has_restricted_viewer


def annotate_restricted_current_user(user_obj, viewer, attr_name="restricted_current_user"):
    setattr(user_obj, attr_name, has_restricted_viewer(user_obj, viewer))
    return user_obj


def annotate_restricted_current_user_for_users(users, viewer, attr_name="restricted_current_user"):
    users = [user for user in users if user is not None]
    if not viewer or not getattr(viewer, "is_authenticated", False) or not users:
        for user in users:
            setattr(user, attr_name, False)
        return users
    user_ids = {user.id for user in users if getattr(user, "id", None) and user.id != viewer.id}
    restricted_ids = set(restricted_user_ids(viewer).filter(restricted_user_id__in=user_ids))
    for user in users:
        setattr(user, attr_name, user.id in restricted_ids)
    return users


def restricted_profile_response():
    from rest_framework import status
    from rest_framework.response import Response
    return Response(
        {"detail": "This profile is not available.", "code": "restricted_by_user"},
        status=status.HTTP_403_FORBIDDEN,
    )


def filter_out_users_who_restricted_viewer(queryset, viewer):
    if not viewer or not getattr(viewer, "is_authenticated", False):
        return queryset
    blocks = UserVisibilityBlock.objects.filter(
        Q(owner_id=OuterRef("id"), blocked_user_id=viewer.id)
        | Q(owner_id=viewer.id, blocked_user_id=OuterRef("id"))
    )
    return queryset.annotate(_viewer_restricted_by_user=Exists(blocks)).filter(_viewer_restricted_by_user=False)


def filter_out_users_with_any_restriction(queryset, viewer):
    """Exclude users with an active visibility restriction in either direction."""
    if not viewer or not getattr(viewer, "is_authenticated", False):
        return queryset
    return queryset.exclude(id__in=restricted_user_ids(viewer))


users_have_any_restriction = are_users_restricted


def can_view_user_profile(target_user, viewer):
    if target_user is None:
        return False
    profile = getattr(target_user, "profile", None)
    visibility = getattr(profile, "visibility", None)
    if not visibility:
        visibility = Profile.Visibility.PUBLIC if getattr(profile, "is_public", True) else Profile.Visibility.PRIVATE

    # Anonymous visitors have no social relationship to consult.  Public
    # profiles are therefore readable, while every other visibility remains
    # closed without issuing friendship/block queries.
    if not viewer or not getattr(viewer, "is_authenticated", False):
        return visibility == Profile.Visibility.PUBLIC
    if viewer.id == target_user.id:
        return True
    if is_blocked_from_user_content(target_user, viewer):
        return False

    if visibility == Profile.Visibility.PUBLIC:
        return True

    return Friendship.between(target_user, viewer).filter(
        status=Friendship.STATUS_ACCEPTED,
    ).exists()


def filter_out_authors_who_blocked_viewer(queryset, viewer, author_field="author"):
    if not viewer or not getattr(viewer, "is_authenticated", False):
        return queryset

    blocks = UserVisibilityBlock.objects.filter(
        Q(owner_id=OuterRef(f"{author_field}_id"), blocked_user_id=viewer.id)
        | Q(owner_id=viewer.id, blocked_user_id=OuterRef(f"{author_field}_id"))
    )
    return queryset.annotate(_viewer_blocked_by_author=Exists(blocks)).filter(_viewer_blocked_by_author=False)
