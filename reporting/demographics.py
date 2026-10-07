from django.db.models import F, Func, IntegerField, Q, Value
from django.db.models.functions import TruncDate
from django.utils import timezone
from .catalog import AGE_RANGES
from .periods import REPORT_TZ


def age_on(birth_date, reference):
    return reference.year - birth_date.year - ((reference.month, reference.day) < (birth_date.month, birth_date.day))


class AgeYears(Func):
    function = "AGE"
    template = "EXTRACT(YEAR FROM AGE(%(expressions)s))"
    output_field = IntegerField()


def segment(queryset, actor, event, filters, accumulated=False):
    profile = actor + "__profile" if actor else "profile"
    queryset = queryset.filter(**{profile + "__isnull": False})
    if filters.get("countries"):
        queryset = queryset.filter(**{profile + "__streaming_country__in": filters["countries"]})
    if filters.get("genders"):
        queryset = queryset.filter(**{profile + "__gender_identity__in": filters["genders"]})
    reference = Value(timezone.localdate(timezone=REPORT_TZ)) if accumulated else TruncDate(F(event), tzinfo=REPORT_TZ)
    queryset = queryset.annotate(report_age=AgeYears(reference, F(profile + "__birth_date")))
    if filters.get("ages"):
        selected = Q()
        for key in filters["ages"]:
            lo, hi = AGE_RANGES[key]
            clause = Q(report_age__gte=lo)
            if hi is not None:
                clause &= Q(report_age__lte=hi)
            selected |= clause
        queryset = queryset.filter(selected)
    return queryset
