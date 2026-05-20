from __future__ import annotations

from typing import Any

import requests
from django.conf import settings


class TMDbServiceError(Exception):
    """Raised when a TMDb request fails or configuration is invalid."""


def get_tmdb_json(path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
    token = getattr(settings, "TMDB_READ_ACCESS_TOKEN", "")
    if not token:
        raise TMDbServiceError("TMDB_READ_ACCESS_TOKEN is not configured")

    base_url = getattr(settings, "TMDB_BASE_URL", "https://api.themoviedb.org/3")
    normalized_base_url = base_url.rstrip("/")
    normalized_path = path if path.startswith("/") else f"/{path}"

    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
    }

    try:
        response = requests.get(
            f"{normalized_base_url}{normalized_path}",
            params=params or {},
            headers=headers,
            timeout=10,
        )
    except requests.Timeout as exc:
        raise TMDbServiceError("TMDb request timed out") from exc
    except requests.RequestException as exc:
        raise TMDbServiceError(f"TMDb request failed: {exc}") from exc

    if response.status_code != 200:
        raise TMDbServiceError(
            f"TMDb returned status {response.status_code}: {response.text[:200]}"
        )

    try:
        data = response.json()
    except ValueError as exc:
        raise TMDbServiceError("TMDb returned invalid JSON") from exc

    if not isinstance(data, dict):
        raise TMDbServiceError("TMDb response JSON must be an object")

    return data
