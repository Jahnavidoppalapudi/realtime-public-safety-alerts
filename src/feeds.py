"""Polite HTTP polling with conditional requests.

Both feeds are polled every minute. Sending the ETag / Last-Modified from the previous
response lets the server answer 304 Not Modified, so unchanged feeds cost almost nothing.
"""

from __future__ import annotations

from dataclasses import dataclass, field


def build_session(user_agent: str):
    import requests
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry

    if not user_agent or "@" not in user_agent or "example.com" in user_agent:
        raise ValueError(
            "Set CONTACT_USER_AGENT in .env to 'app-name (your-email@domain.com)'. "
            "api.weather.gov rejects requests without a clear User-Agent."
        )
    session = requests.Session()
    session.headers.update({"User-Agent": user_agent, "Accept": "application/geo+json"})
    retry = Retry(
        total=3,
        backoff_factor=2.0,
        status_forcelist=(429, 500, 502, 503, 504),
        respect_retry_after_header=True,
        allowed_methods=("GET",),
    )
    session.mount("https://", HTTPAdapter(max_retries=retry))
    return session


@dataclass
class ConditionalFeed:
    name: str
    url: str
    etag: str | None = None
    last_modified: str | None = None
    not_modified_count: int = field(default=0)

    def fetch(self, session) -> list[dict] | None:
        """GeoJSON features, or None if the server says nothing changed (HTTP 304)."""
        headers = {}
        if self.etag:
            headers["If-None-Match"] = self.etag
        if self.last_modified:
            headers["If-Modified-Since"] = self.last_modified
        response = session.get(self.url, headers=headers, timeout=30)
        if response.status_code == 304:
            self.not_modified_count += 1
            return None
        response.raise_for_status()
        self.etag = response.headers.get("ETag") or self.etag
        self.last_modified = response.headers.get("Last-Modified") or self.last_modified
        body = response.json()
        features = body.get("features")
        if not isinstance(features, list):
            raise ValueError(f"{self.name}: response has no 'features' list")
        return features
