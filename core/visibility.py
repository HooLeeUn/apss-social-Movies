from django.contrib.auth import get_user_model
from django.db.models import Exists, OuterRef

from .models import Friendship, Profile, UserVisibilityBlock


User = get_user_model()


def is_blocked_from_user_content(owner, viewer):
    if owner is None:
        return False
    if not viewer or not getattr(viewer, "is_authenticated", False):
        return False
    if viewer.id == owner.id:
        return False
    return UserVisibilityBlock.objects.filter(owner_id=owner.id, blocked_user_id=viewer.id).exists()


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
