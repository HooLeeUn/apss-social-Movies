import logging

from django.conf import settings
from django.contrib.auth import get_user_model
from django.core.mail import send_mail
from django.db import transaction
from django.utils import timezone
from django.utils.translation import get_language

from .models import ACCOUNT_DELETION_TTL, ContactMessage, PendingAccountDeletion


logger = logging.getLogger(__name__)
User = get_user_model()


class AccountDeletionTokenInvalid(Exception):
    pass


def _delete_file_safely(storage, name):
    try:
        # Storage.delete is deliberately called without exists(): S3/R2 deletes are
        # idempotent and this avoids an extra remote request and a race.
        storage.delete(name)
    except Exception:
        logger.exception("Could not delete account-owned media object %s", name)


@transaction.atomic
def delete_user_account(user):
    """Delete an account and every user-owned record; schedule media cleanup on commit."""
    locked_user = User.objects.select_for_update().select_related("profile").get(pk=user.pk)
    files = []

    profile = getattr(locked_user, "profile", None)
    if profile and profile.avatar and profile.avatar.name:
        files.append((profile.avatar.storage, profile.avatar.name))

    for post in locked_user.posts.exclude(image="").only("image"):
        if post.image and post.image.name:
            files.append((post.image.storage, post.image.name))

    for video_comment in locked_user.video_comments.exclude(video="").only("video"):
        if video_comment.video and video_comment.video.name:
            files.append((video_comment.video.storage, video_comment.video.name))

    # ContactMessage intentionally uses PROTECT for ordinary user operations, but
    # contains personal support correspondence and must be erased with the account.
    ContactMessage.objects.filter(user=locked_user).delete()
    locked_user.delete()

    for storage, name in files:
        transaction.on_commit(lambda storage=storage, name=name: _delete_file_safely(storage, name))


def create_account_deletion_request(user):
    token = PendingAccountDeletion.new_token()
    with transaction.atomic():
        locked_user = User.objects.select_for_update().get(pk=user.pk)
        PendingAccountDeletion.objects.filter(user=locked_user, used_at__isnull=True).delete()
        pending = PendingAccountDeletion.objects.create(
            user=locked_user,
            token_hash=PendingAccountDeletion.hash_token(token),
            expires_at=timezone.now() + ACCOUNT_DELETION_TTL,
        )
    return pending, token


def send_account_deletion_confirmation(*, pending, token):
    frontend_base_url = settings.FRONTEND_BASE_URL.rstrip("/")
    confirmation_url = f"{frontend_base_url}/delete-account/confirm/{token}"
    spanish = (get_language() or "").lower().startswith("es")
    subject = "Confirma la eliminación de tu cuenta de RecCool" if spanish else "Confirm your RecCool account deletion"
    message = (
        "Se solicitó eliminar tu cuenta de RecCool. Confirma la eliminación en este enlace:\n"
        f"{confirmation_url}\n\n"
        "El enlace vence en 24 horas. Si no realizaste esta solicitud, ignora este mensaje."
        if spanish
        else "Your RecCool account was requested for deletion. Confirm deletion using this link:\n"
        f"{confirmation_url}\n\n"
        "The link expires in 24 hours. If you did not make this request, ignore this message."
    )
    send_mail(
        subject=subject,
        message=message,
        from_email=getattr(settings, "DEFAULT_FROM_EMAIL", None),
        recipient_list=[pending.user.email],
        fail_silently=False,
    )


@transaction.atomic
def confirm_account_deletion(token):
    pending = (
        PendingAccountDeletion.objects.select_for_update()
        .select_related("user")
        .filter(token_hash=PendingAccountDeletion.hash_token(token))
        .first()
    )
    now = timezone.now()
    if not pending or not pending.is_active:
        raise AccountDeletionTokenInvalid

    pending.used_at = now
    pending.save(update_fields=["used_at"])
    delete_user_account(pending.user)
