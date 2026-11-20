"""Scoped alias for the shared cleanup engine; defaults remain local."""
from contextlib import contextmanager
from contextvars import ContextVar
from django.core.management.base import CommandError
from django.db import router

_scope = ContextVar('duplicate_cleanup_scope', default=None)


def execution_alias():
    scope = _scope.get()
    return scope['alias'] if scope else 'default'


def supported_dependencies():
    labels = {'core.MovieRating', 'core.UserDailyFeedCandidate'}
    if _scope.get():
        labels.add('core.WeeklyRecommendationItem')
    return labels


def session_validator():
    scope = _scope.get()
    return scope['validator'] if scope else None


class ScopedRouter:
    def db_for_read(self, model, **hints):
        return execution_alias() if _scope.get() else None

    db_for_write = db_for_read

    def allow_relation(self, first, second, **hints):
        if _scope.get():
            return first._state.db == second._state.db == execution_alias()
        return None


@contextmanager
def execution_scope(alias, validator):
    if alias == 'default' or router.routers or _scope.get():
        raise CommandError('Remote cleanup requires an independent alias and no existing routers/scopes.')
    token = _scope.set({'alias': alias, 'validator': validator})
    scoped = ScopedRouter()
    router.routers.insert(0, scoped)
    try:
        yield
    finally:
        router.routers.remove(scoped)
        _scope.reset(token)
