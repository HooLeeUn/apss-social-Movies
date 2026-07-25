from django.contrib.auth import get_user_model
from django.db.models import Exists, OuterRef, Q

from .models import Friendship, Profile, UserVisibilityBlock


User = get_user_model()


def has_restricted_viewer(target_user, viewer):
    if target_user is None:
        return False
    if not viewer or not getattr(viewer, "is_authenticated", False):
        return False
    if viewer.id == target_user.id:
        return False
    return UserVisibilityBlock.objects.filter(owner_id=target_user.id, blocked_user_id=viewer.id).exists()


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
    restricted_ids = set(UserVisibilityBlock.objects.filter(owner_id__in=user_ids, blocked_user_id=viewer.id).values_list("owner_id", flat=True))
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
        owner_id=OuterRef("id"),
        blocked_user_id=viewer.id,
    )
    return queryset.annotate(_viewer_restricted_by_user=Exists(blocks)).filter(_viewer_restricted_by_user=False)


def filter_out_users_with_any_restriction(queryset, viewer):
    """Exclude users with an active visibility restriction in either direction."""
    if not viewer or not getattr(viewer, "is_authenticated", False):
        return queryset
    restricted_user_ids = UserVisibilityBlock.objects.filter(
        owner_id=viewer.id,
    ).values_list("blocked_user_id", flat=True)
    restricting_user_ids = UserVisibilityBlock.objects.filter(
        blocked_user_id=viewer.id,
    ).values_list("owner_id", flat=True)
    return queryset.exclude(id__in=restricted_user_ids).exclude(id__in=restricting_user_ids)


def users_have_any_restriction(user_a, user_b):
    if not user_a or not user_b or user_a.id == user_b.id:
        return False
    return UserVisibilityBlock.objects.filter(
        Q(owner_id=user_a.id, blocked_user_id=user_b.id)
        | Q(owner_id=user_b.id, blocked_user_id=user_a.id)
    ).exists()


def can_view_user_profile(target_user, viewer):
    if target_user is None:
        return False
    if not viewer or not getattr(viewer, "is_authenticated", False):
        return False
    if viewer.id == target_user.id:
        return True
    if is_blocked_from_user_content(target_user, viewer):
        return False

    profile = getattr(target_user, "profile", None)
    visibility = getattr(profile, "visibility", None)
    if not visibility:
        visibility = Profile.Visibility.PUBLIC if getattr(profile, "is_public", True) else Profile.Visibility.PRIVATE
    if visibility == Profile.Visibility.PUBLIC:
        return True

    return Friendship.between(target_user, viewer).filter(
        status=Friendship.STATUS_ACCEPTED,
    ).exists()


def filter_out_authors_who_blocked_viewer(queryset, viewer, author_field="author"):
    if not viewer or not getattr(viewer, "is_authenticated", False):
        return queryset

    blocks = UserVisibilityBlock.objects.filter(
        owner_id=OuterRef(f"{author_field}_id"),
        blocked_user_id=viewer.id,
    )
    return queryset.annotate(_viewer_blocked_by_author=Exists(blocks)).filter(_viewer_blocked_by_author=False)
