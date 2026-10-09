"""Superuser-only analytical detail report; no mutations or financial actions."""
from dataclasses import dataclass

from django.contrib.auth import get_user_model
from django.core.exceptions import PermissionDenied
from django.db.models import Count, Exists, OuterRef, Q, Subquery, Value
from django.db.models.functions import Coalesce
from django.utils import timezone

from core.models import Follow, VideoComment
from .catalog import DEFAULT_CREATOR_MIN_FOLLOWERS, INTERNAL_REPORTS
from .permissions import can_view_creator_eligibility
from .periods import REPORT_TZ, periods
from .queries import events, video_reactions


@dataclass
class CreatorResult:
    metadata: list
    summary: list
    methodology: list
    creators: list
    selected_periods: list
    min_followers: int
    period_heading: str
    title: str = INTERNAL_REPORTS["creator_eligibility"]
    report: str = "creator_eligibility"
    blocked: bool = False
    show_population: bool = False
    population: int | None = None
    averages: bool = False
    compares: bool = False
    column_count: int = 0  # This report pages detail rows, never metric columns.
    columns: tuple = ()

    @property
    def headers(self):
        return [self.period_heading, "Usuario", "User ID", "Seguidores actuales",
                "Video reacciones", "Likes recibidos", "Dislikes recibidos",
                "Interacciones recibidas", "Cumple umbral"]

    @property
    def header_groups(self):
        from .services import Header
        return [Header(label) for label in self.headers]

    @property
    def row_count(self):
        return len(self.creators) * len(self.selected_periods)

    def rows(self, offset=0, limit=None):
        size = len(self.creators)
        if not size:
            return
        end = self.row_count if limit is None else min(self.row_count, offset + limit)
        # Fetch only periods intersecting the requested row page. Two aggregate
        # queries per period, independent of the number of creators or videos.
        for index in range(offset // size, (end + size - 1) // size):
            period = self.selected_periods[index]
            videos = dict(events("videos", {}, period).values("user_id").annotate(n=Count("pk")).values_list("user_id", "n"))
            reactions = {row["video_comment__user_id"]: (row["likes"], row["dislikes"])
                         for row in video_reactions(period).values("video_comment__user_id").annotate(
                             likes=Count("pk", filter=Q(reaction_type="like")),
                             dislikes=Count("pk", filter=Q(reaction_type="dislike")))}
            records = []
            for creator in self.creators:
                user_id = creator["id"]
                likes, dislikes = reactions.get(user_id, (0, 0))
                eligible = creator["followers_current"] >= self.min_followers
                records.append([period.label, creator["username"], user_id,
                                creator["followers_current"], videos.get(user_id, 0),
                                likes, dislikes, likes + dislikes, "Sí" if eligible else "No"])
            records.sort(key=lambda row: (row[8] != "Sí", -row[7], -row[3], row[1], row[2]))
            start_in_period = max(0, offset - index * size)
            end_in_period = min(size, end - index * size)
            yield from records[start_in_period:end_in_period]


def generate_creator_eligibility(filters, user):
    # Guard the service as well as the URL: callers must explicitly supply the
    # authorized request user before any identifiable data is queried.
    if not can_view_creator_eligibility(user):
        raise PermissionDenied
    threshold = filters.get("min_followers", DEFAULT_CREATOR_MIN_FOLLOWERS)
    if isinstance(threshold, bool) or not isinstance(threshold, int) or threshold < 1:
        raise ValueError("Mínimo de seguidores debe ser un entero positivo.")
    if filters.get("total") or any(filters.get(field) for field in ("countries", "ages", "genders", "content_type")):
        raise ValueError("Elegibilidad de creadores solo utiliza periodo y mínimo de seguidores.")
    ps = periods(filters)
    follower_counts = Follow.objects.order_by().filter(following_id=OuterRef("pk")).values("following_id").annotate(n=Count("pk")).values("n")
    # A received video reaction necessarily belongs to an existing VideoComment,
    # so has_video also includes every creator with reactions in the range.
    candidates = get_user_model().objects.order_by().filter(is_superuser=False).annotate(
        followers_current=Coalesce(Subquery(follower_counts), Value(0)),
        has_video=Exists(VideoComment.objects.filter(user_id=OuterRef("pk"))),
    ).filter(Q(has_video=True) | Q(followers_current__gte=threshold))
    # Snapshot identity and current follower counts once, shared by all periods.
    creators = list(candidates.values("id", "username", "followers_current"))
    summary = [("Usuarios evaluados", len(creators)),
               ("Usuarios que cumplen umbral", sum(c["followers_current"] >= threshold for c in creators)),
               ("Umbral de seguidores utilizado", threshold),
               ("Periodo(s) seleccionados", "; ".join(p.label for p in ps))]
    metadata = [("Tipo de reporte", INTERNAL_REPORTS["creator_eligibility"]),
                ("Generado", timezone.localtime(timezone.now(), REPORT_TZ).isoformat()),
                ("Zona horaria", str(REPORT_TZ)), *summary]
    methodology = [
        ("Uso interno", "Exclusivamente para superusuarios. Analítica interna; Cumple umbral no constituye monetización, aceptación de programa ni derecho a compensación."),
        ("Seguidores actuales", "Follow vigentes donde following_id es el creador; captura al generar, repetida en todos los periodos. Follow no reconstruye unfollows históricos."),
        ("Videos", "VideoComment.user es el creador; publicaciones por created_at. Videos ocultados por moderación que todavía existen siguen contando."),
        ("Reacciones recibidas", "Se atribuyen mediante video_comment.user, no al actor VideoCommentReaction.user. Estado vigente like/dislike por updated_at; reacciones eliminadas y estados anteriores no se reconstruyen. No incluye comentarios públicos."),
        ("Periodos", "Meses y años cartesianos, en orden cronológico; Personalizado diario, Desde/Hasta inclusivos y [inicio, día siguiente) en America/Bogota."),
        ("Usuarios evaluados", "Cuentas no superusuarias con al menos un video existente, incluso fuera del rango, o que cumplen el umbral actual. Sin una marca confiable de cuenta técnica, no se excluyen usernames ni cuentas staff arbitrariamente. Inactivos incluidos."),
        ("Privacidad", "Excepción exclusiva de creator_eligibility, con autorización de superusuario; solo username y User ID, sin emails, nombre legal ni nacimiento. No modifica la protección de otros reportes."),
    ]
    return CreatorResult(metadata, summary, methodology, creators, ps, threshold,
                         "Fecha" if filters["custom"] else "Periodo")
