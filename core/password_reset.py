import logging

from django.conf import settings
from django.contrib.auth import get_user_model
from django.contrib.auth.password_validation import validate_password
from django.contrib.auth.tokens import default_token_generator
from django.core.exceptions import ValidationError as DjangoValidationError
from django.core.mail import send_mail
from django.db import transaction
from django.utils.encoding import force_bytes
from django.utils.http import urlsafe_base64_decode, urlsafe_base64_encode
from rest_framework import serializers
from rest_framework.authtoken.models import Token
from rest_framework.permissions import AllowAny
from rest_framework.response import Response
from rest_framework.throttling import SimpleRateThrottle
from rest_framework.views import APIView

from .models import normalize_email_address


GENERIC_RESPONSE = {"detail": "Si el correo está asociado a una cuenta activa de ReCCool, recibirás un enlace para restablecer tu contraseña. El enlace será válido durante 1 hora."}
INVALID_LINK = "El enlace es inválido o ha expirado."
logger = logging.getLogger(__name__)


class ResetRequestSerializer(serializers.Serializer):
    email = serializers.EmailField(max_length=254)


class ResetConfirmSerializer(serializers.Serializer):
    uid = serializers.CharField(max_length=128)
    token = serializers.CharField(max_length=128)
    new_password = serializers.CharField(trim_whitespace=False, max_length=4096)
    confirm_password = serializers.CharField(trim_whitespace=False, max_length=4096)

    def validate(self, attrs):
        if attrs["new_password"] != attrs["confirm_password"]:
            raise serializers.ValidationError({"confirm_password": "Las contraseñas no coinciden."})
        return attrs


class PasswordResetRequestThrottle(SimpleRateThrottle):
    scope = "password_reset_request"
    rate = "5/hour"

    def get_cache_key(self, request, view):
        return self.cache_format % {"scope": self.scope, "ident": self.get_ident(request)}


class PasswordResetConfirmThrottle(PasswordResetRequestThrottle):
    scope = "password_reset_confirm"
    rate = "20/hour"


class PasswordResetRequestView(APIView):
    authentication_classes = []
    permission_classes = [AllowAny]
    throttle_classes = [PasswordResetRequestThrottle]

    def post(self, request):
        serializer = ResetRequestSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        email = normalize_email_address(serializer.validated_data["email"])
        send_password_reset_email(email)
        return Response(GENERIC_RESPONSE)


class PasswordResetConfirmView(APIView):
    authentication_classes = []
    permission_classes = [AllowAny]
    throttle_classes = [PasswordResetConfirmThrottle]

    def post(self, request):
        serializer = ResetConfirmSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        data = serializer.validated_data
        User = get_user_model()
        with transaction.atomic():
            try:
                uid = urlsafe_base64_decode(data["uid"]).decode()
                user = User.objects.select_for_update().get(pk=uid, is_active=True)
            except (ValueError, UnicodeError, OverflowError, User.DoesNotExist, DjangoValidationError):
                return Response({"detail": INVALID_LINK}, status=400)
            if not user.has_usable_password() or not default_token_generator.check_token(user, data["token"]):
                return Response({"detail": INVALID_LINK}, status=400)
            try:
                validate_password(data["new_password"], user=user)
            except DjangoValidationError as exc:
                return Response({"new_password": list(exc.messages)}, status=400)
            user.set_password(data["new_password"])
            user.save(update_fields=["password"])
            Token.objects.filter(user=user).delete()
        return Response({"detail": "Contraseña restablecida correctamente. Inicia sesión de nuevo."})


def send_password_reset_email(email):
    """Send synchronously; keep delivery failures out of the public response."""
    users = get_user_model().objects.filter(email__iexact=email, is_active=True)
    for user in users:
        if not user.has_usable_password():
            continue
        token = default_token_generator.make_token(user)
        uid = urlsafe_base64_encode(force_bytes(user.pk))
        url = f"{settings.FRONTEND_BASE_URL.rstrip('/')}/reset-password/{uid}/{token}"
        try:
            send_mail(
                "Restablece tu contraseña de ReCCool",
                "Se solicitó recuperar la contraseña de tu cuenta de ReCCool.\n\n"
                f"Restablece tu contraseña: {url}\n\n"
                "El enlace vence en 1 hora. Si no solicitaste este cambio, puedes ignorar este correo.",
                settings.DEFAULT_FROM_EMAIL,
                [user.email],
                fail_silently=False,
            )
        except Exception:
            # SMTP exceptions can contain the message or recipient; do not log them.
            logger.warning("Password reset email delivery failed")
