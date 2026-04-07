from rest_framework.pagination import PageNumberPagination


class DefaultPagination(PageNumberPagination):
    page_size = 10
    page_size_query_param = "page_size"
    max_page_size = 50


class FeedMoviesPagination(DefaultPagination):
    """Optimiza el count del feed con un queryset liviano."""

    def paginate_queryset(self, queryset, request, view=None):
        self._view = view
        return super().paginate_queryset(queryset, request, view=view)

    def get_count(self, queryset):
        view = getattr(self, "_view", None)
        if view and hasattr(view, "get_feed_count_queryset"):
            return view.get_feed_count_queryset().count()
        return super().get_count(queryset)
