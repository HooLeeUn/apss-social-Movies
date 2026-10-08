from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from zoneinfo import ZoneInfo
from .catalog import MONTH_NAMES, USER_REPORTS

REPORT_TZ = ZoneInfo("America/Bogota")

@dataclass(frozen=True)
class Period:
    label: str
    start: datetime | None
    end: datetime | None


def midnight(day):
    return datetime.combine(day, time.min, REPORT_TZ)


def month_period(value):
    year, month = map(int, value.split("-"))
    start = date(year, month, 1)
    end = date(year + (month == 12), 1 if month == 12 else month + 1, 1)
    return Period(f"{MONTH_NAMES[month-1]} {year}", midnight(start), midnight(end))


def periods(filters):
    if filters["report"] in USER_REPORTS and filters["total"]:
        return [Period("Acumulado total", None, None)]
    if filters["report"] in USER_REPORTS and filters["custom"]:
        return [Period(f'{filters["since"]} — {filters["until"]}', midnight(filters["since"]), midnight(filters["until"] + timedelta(days=1)))]
    return [month_period(f"{year:04}-{month:02}")
            for year in sorted(set(filters["years"]))
            for month in sorted(set(filters["months"]))]
