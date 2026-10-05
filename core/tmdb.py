from __future__ import annotations

from typing import Any

import requests
from requests.adapters import HTTPAdapter
from django.conf import settings


class TMDbServiceError(Exception):
    """Raised when a TMDb request fails or configuration is invalid.

    The optional attributes let batch consumers make a retry decision without
    parsing the human-readable message.  Existing callers which only catch the
    exception remain fully compatible.
    """

    def __init__(self, message, *, status_code=None, retry_after=None, retryable=False):
        super().__init__(message)
        self.status_code = status_code
        self.retry_after = retry_after
        self.retryable = retryable


_SESSION = requests.Session()
_SESSION.mount("https://", HTTPAdapter(pool_connections=10, pool_maxsize=10))
_SESSION.mount("http://", HTTPAdapter(pool_connections=10, pool_maxsize=10))


def get_tmdb_json(
    path: str,
    params: dict[str, Any] | None = None,
    timeout: float | tuple[float, float] | None = None,
) -> dict[str, Any]:
    token = getattr(settings, "TMDB_READ_ACCESS_TOKEN", "")
    if not token:
        raise TMDbServiceError("TMDB_READ_ACCESS_TOKEN is not configured")

    base_url = getattr(settings, "TMDB_BASE_URL", "https://api.themoviedb.org/3")
    normalized_base_url = base_url.rstrip("/")
    normalized_path = path if path.startswith("/") else f"/{path}"

    request_timeout = (
        timeout
        if timeout is not None
        else getattr(settings, "TMDB_REQUEST_TIMEOUT", 10)
    )

    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
    }

    try:
        response = _SESSION.get(
            f"{normalized_base_url}{normalized_path}",
            params=params or {},
            headers=headers,
            timeout=request_timeout,
        )
    except requests.Timeout as exc:
        raise TMDbServiceError("TMDb request timed out", retryable=True) from exc
    except requests.RequestException as exc:
        raise TMDbServiceError(f"TMDb request failed: {exc}", retryable=True) from exc

    if response.status_code != 200:
        retry_after = response.headers.get("Retry-After")
        raise TMDbServiceError(
            f"TMDb returned status {response.status_code}: {response.text[:200]}",
            status_code=response.status_code,
            retry_after=retry_after,
            retryable=response.status_code == 429 or 500 <= response.status_code < 600,
        )

    try:
        data = response.json()
    except ValueError as exc:
        raise TMDbServiceError("TMDb returned invalid JSON") from exc

    if not isinstance(data, dict):
        raise TMDbServiceError("TMDb response JSON must be an object")

    return data
