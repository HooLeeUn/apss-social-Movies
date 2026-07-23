import hashlib
import logging

from django.conf import settings
from django.contrib.auth import get_user_model
from django.core.mail import send_mail
from django.db import IntegrityError, connection, transaction
from django.utils import timezone

from .models import (
    EMAIL_CONFIRMATION_TTL,
    PendingEmailChange,
    PendingUserRegistration,
    normalize_email_address,
)


logger = logging.getLogger(__name__)
User = get_user_model()


class EmailChangeUnavailable(Exception):
    pass


class EmailChangeInvalid(Exception):
    pass


def _lock_normalized_email(email):
    """Serialize claims for an address on PostgreSQL, including absent rows."""
    if connection.vendor != "postgresql":
        return
    digest = hashlib.sha256(email.encode("utf-8")).digest()
    lock_id = int.from_bytes(digest[:8], byteorder="big", signed=True)
    with connection.cursor() as cursor:
        cursor.execute("SELECT pg_advisory_xact_lock(%s)", [lock_id])


def _email_is_reserved(email, *, excluding_user_id=None):
    users = User.objects.filter(email__iexact=email)
    if excluding_user_id is not None:
        users = users.exclude(pk=excluding_user_id)
    if users.exists():
        return True
    if PendingUserRegistration.objects.filter(
        email__iexact=email,
        expires_at__gt=timezone.now(),
        confirmed_at__isnull=True,
    ).exists():
        return True
    pending_changes = PendingEmailChange.objects.filter(
        new_email__iexact=email,
        expires_at__gt=timezone.now(),
        confirmed_at__isnull=True,
        invalidated_at__isnull=True,
    )
    if excluding_user_id is not None:
        pending_changes = pending_changes.exclude(user_id=excluding_user_id)
    return pending_changes.exists()


def create_email_change(*, user, new_email):
    normalized = normalize_email_address(new_email)
    token = PendingEmailChange.new_token()
    token_hash = PendingEmailChange.hash_token(token)

    with transaction.atomic():
        locked_user = User.objects.select_for_update().get(pk=user.pk)
        _lock_normalized_email(normalized)
        if normalize_email_address(locked_user.email) == normalized:
            return None, None
        if _email_is_reserved(normalized, excluding_user_id=locked_user.pk):
            raise EmailChangeUnavailable
        PendingEmailChange.objects.filter(user=locked_user).delete()
        pending = PendingEmailChange.objects.create(
            user=locked_user,
            new_email=normalized,
            token_hash=token_hash,
            expires_at=timezone.now() + EMAIL_CONFIRMATION_TTL,
        )
    return pending, token


def send_email_change_confirmation(*, request, pending, token):
    backend_base_url = getattr(settings, "BACKEND_BASE_URL", "").rstrip("/")
    path = f"/api/me/confirm-email-change/{token}/"
    confirmation_url = f"{backend_base_url}{path}" if backend_base_url else request.build_absolute_uri(path)
    try:
        sent_count = send_mail(
            subject="Confirma tu nuevo email en Social Movies",
            message=(
                "Hola,\n\n"
                "Se solicitó cambiar el email de tu cuenta. Confirma el nuevo email desde este enlace:\n"
                f"{confirmation_url}\n\n"
                "El enlace vence en 24 horas. Si no solicitaste este cambio, puedes ignorar este correo."
            ),
            from_email=getattr(settings, "DEFAULT_FROM_EMAIL", None),
            recipient_list=[pending.new_email],
            fail_silently=False,
        )
    except Exception:
        PendingEmailChange.objects.filter(pk=pending.pk, token_hash=pending.token_hash).delete()
        logger.exception("Email change delivery failed for user_id=%s request_id=%s", pending.user_id, pending.pk)
        raise
    logger.info(
        "Email change confirmation requested for user_id=%s request_id=%s backend=%s sent_count=%s",
        pending.user_id,
        pending.pk,
        getattr(settings, "EMAIL_BACKEND", ""),
        sent_count,
    )


def confirm_email_change(token):
    token_hash = PendingEmailChange.hash_token(token)
    now = timezone.now()
    failure = None
    confirmed_user = None
    with transaction.atomic():
        pending = (
            PendingEmailChange.objects.select_for_update()
            .select_related("user")
            .filter(token_hash=token_hash)
            .first()
        )
        if not pending or pending.confirmed_at or pending.invalidated_at:
            raise EmailChangeInvalid
        locked_user = User.objects.select_for_update().get(pk=pending.user_id)
        if pending.expires_at <= now:
            pending.invalidated_at = now
            pending.save(update_fields=["invalidated_at"])
            failure = EmailChangeInvalid
        else:
            normalized = normalize_email_address(pending.new_email)
            _lock_normalized_email(normalized)
            if _email_is_reserved(normalized, excluding_user_id=locked_user.pk):
                pending.invalidated_at = now
                pending.save(update_fields=["invalidated_at"])
                failure = EmailChangeUnavailable
            else:
                locked_user.email = normalized
                try:
                    with transaction.atomic():
                        locked_user.save(update_fields=["email"])
                except IntegrityError:
                    failure = EmailChangeUnavailable
                else:
                    pending.confirmed_at = now
                    pending.save(update_fields=["confirmed_at"])
                    confirmed_user = locked_user
        if failure is EmailChangeUnavailable and pending.invalidated_at is None:
            pending.invalidated_at = now
            pending.save(update_fields=["invalidated_at"])
    if failure:
        raise failure
    logger.info("Email change confirmed for user_id=%s request_id=%s", confirmed_user.pk, pending.pk)
    return confirmed_user
