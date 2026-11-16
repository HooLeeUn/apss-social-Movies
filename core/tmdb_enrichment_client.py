"""Sequential TMDb v3 transport. No Django, ORM, credentials in errors or redirects."""
from datetime import timezone
from email.utils import parsedate_to_datetime
import json
import math
import random
import re
import time

import requests

OFFICIAL_BASE = "https://api.themoviedb.org/3"


class QueryError(Exception):
    def __init__(self, code, *, status=None, retryable=False, halt=None):
        super().__init__(code)
        self.code, self.status, self.retryable, self.halt = code, status, retryable, halt

    def as_dict(self):
        return {"code": self.code, "http_status": self.status,
                "retryable": self.retryable}


def strict_json(value):
    return json.dumps(value, ensure_ascii=False, allow_nan=False,
                      sort_keys=True, separators=(",", ":"))


def reject_constant(value):
    raise ValueError("non_finite_json")


def redact(value, secret):
    if not secret:
        return value
    if isinstance(value, str):
        return value.replace(secret, "[REDACTED]")
    if isinstance(value, list):
        return [redact(item, secret) for item in value]
    if isinstance(value, dict):
        return {redact(k, secret): redact(v, secret) for k, v in value.items()}
    return value


class TMDbMetadataClient:
    def __init__(self, token, *, base_url=OFFICIAL_BASE, requests_per_second=2,
                 max_retries=3, connect_timeout=3.05, read_timeout=15,
                 max_requests=10, session=None, sleep=time.sleep,
                 clock=time.monotonic, wall_clock=time.time,
                 jitter=random.uniform, event=None):
        if base_url.rstrip("/") != OFFICIAL_BASE:
            raise ValueError("Only the official HTTPS TMDb v3 base URL is permitted")
        if not isinstance(token, str) or not token.strip() or re.search(r"\s", token):
            raise ValueError("TMDB_READ_ACCESS_TOKEN is missing or invalid")
        if not all(math.isfinite(v) and v > 0 for v in
                   (requests_per_second, connect_timeout, read_timeout)):
            raise ValueError("Rate and timeouts must be positive and finite")
        if type(max_retries) is not int or not 0 <= max_retries <= 10:
            raise ValueError("Retries must be between zero and ten")
        if type(max_requests) is not int or max_requests < 1:
            raise ValueError("Request budget must be positive")
        self._token = token
        self.session = session or requests.Session()
        self.session.trust_env = False
        self.rate, self.max_retries = requests_per_second, max_retries
        self.timeout = (connect_timeout, read_timeout)
        self.max_requests = max_requests
        self.sleep, self.clock, self.wall_clock, self.jitter = sleep, clock, wall_clock, jitter
        self.event = event or (lambda *args: None)
        self.request_count = self.retry_count = 0
        self._next_request = self._cooldown = 0.0

    def close(self):
        self.session.close()

    def _retry_after(self, value):
        try:
            seconds = float(value)
        except (TypeError, ValueError):
            try:
                dt = parsedate_to_datetime(value)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                seconds = dt.timestamp() - self.wall_clock()
            except (TypeError, ValueError, OverflowError):
                return 0
        return max(0, seconds) if math.isfinite(seconds) else 0

    def _defer(self, seconds):
        self._cooldown = max(self._cooldown, self.clock() + seconds)
        self.event("cooldown", self.wall_clock() + seconds)

    def _wait(self):
        delay = max(self._next_request, self._cooldown) - self.clock()
        # Persist a long service cooldown rather than blocking a batch for hours.
        if delay > 60:
            raise QueryError("retry_deferred", retryable=True, halt="waiting")
        if delay > 0:
            self.sleep(delay)

    def fetch_part(self, production_type, tmdb_id, language):
        if (production_type not in ("movie", "series") or type(tmdb_id) is not int
                or not 0 < tmdb_id <= 2147483647 or language not in ("en-US", "es-ES")):
            raise ValueError("Invalid TMDb identity or language")
        kind = "movie" if production_type == "movie" else "tv"
        params = {"language": language}
        if language == "en-US":
            # Standard series credits have the same crew.job shape the existing
            # normalizer expects. No season/episode or person requests.
            params["append_to_response"] = "credits"
        for attempt in range(self.max_retries + 1):
            if self.request_count >= self.max_requests:
                raise QueryError("request_budget_exhausted", retryable=True, halt="budget_exhausted")
            self._wait()
            self.event("http_requests", 1)
            self.request_count += 1
            if attempt:
                self.event("retries", 1)
                self.retry_count += 1
            self._next_request = self.clock() + 1 / self.rate
            retry_after = 0
            try:
                response = self.session.get(
                    f"{OFFICIAL_BASE}/{kind}/{tmdb_id}", params=params,
                    headers={"Authorization": f"Bearer {self._token}", "Accept": "application/json"},
                    timeout=self.timeout, allow_redirects=False, verify=True,
                )
            except (requests.exceptions.SSLError, requests.exceptions.InvalidURL):
                raise QueryError("unsafe_transport", halt="blocked_security") from None
            except (requests.Timeout, requests.ConnectionError):
                error = QueryError("transport_error", retryable=True)
            except requests.RequestException:
                error = QueryError("request_error", retryable=True)
            else:
                try:
                    status = response.status_code
                    if status == 200:
                        try:
                            payload = response.json(parse_constant=reject_constant)
                            if not isinstance(payload, dict):
                                raise ValueError("not_an_object")
                            payload = redact(payload, self._token)
                            strict_json(payload)
                        except (ValueError, TypeError, OverflowError):
                            raise QueryError("invalid_json", retryable=True) from None
                        return payload
                    if status in (401, 403):
                        raise QueryError("authentication_failed", status=status, halt="blocked_auth")
                    if 300 <= status < 400:
                        raise QueryError("redirect_refused", status=status, halt="blocked_security")
                    if status == 404:
                        raise QueryError("tmdb_not_found", status=404)
                    if status == 429 or status in (500, 502, 503, 504):
                        retry_after = self._retry_after(response.headers.get("Retry-After"))
                        error = QueryError("rate_limited" if status == 429 else "server_error",
                                           status=status, retryable=True)
                    else:
                        raise QueryError("http_error", status=status)
                finally:
                    response.close()
            self.event("transient_errors", 1)
            backoff = min(30, 2 ** attempt)
            delay = max(retry_after, backoff + self.jitter(0, backoff / 4))
            self._defer(delay)
            if attempt == self.max_retries:
                raise error
        raise AssertionError("unreachable")
