MESSAGE = "Muestra insuficiente para mostrar resultados"
MIN_USERS = 10


def segmented(filters):
    return bool(filters.get("countries") or filters.get("ages") or filters.get("genders") or filters["report"] in {"countries", "ages", "genders"})


def publishable(count, protect):
    return not protect or count >= MIN_USERS


def variation(previous, current):
    if previous is None or current is None:
        return MESSAGE, MESSAGE
    return current - previous, "No aplica" if previous == 0 else round((current - previous) * 100 / previous, 2)


def protected_sum(values):
    """Never publish a marginal total containing even one suppressed cell."""
    values = list(values)
    return None if any(value is None for value in values) else sum(values)
