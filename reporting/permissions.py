def can_view(user):
    return user.is_active and user.is_staff and user.has_perm("reporting.can_view_reports")


def can_export(user):
    return can_view(user) and user.has_perm("reporting.can_export_reports")


def can_view_creator_eligibility(user):
    return bool(user and user.is_superuser and can_view(user))
