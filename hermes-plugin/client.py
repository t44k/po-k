"""Thin HTTP client for the Xpo-k API.

Reads connection details from environment:
  XPOK_URL          — required, e.g. http://localhost:8080
  XPOK_TOKEN_FILE   — path to bearer token file (preferred)
  XPOK_TOKEN        — inline bearer token (fallback)
"""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Any, Dict, Optional

import requests

logger = logging.getLogger(__name__)

_DEFAULT_TIMEOUT = 30  # seconds for most calls
_WAIT_TIMEOUT = 660  # /wait can block up to 600s server-side


def _load_token() -> str:
    """Resolve the bearer token from env."""
    token_file = os.getenv("XPOK_TOKEN_FILE", "")
    if token_file:
        p = Path(token_file).expanduser()
        if p.is_file():
            return p.read_text().strip()
        logger.warning("XPOK_TOKEN_FILE=%s not found, falling back to XPOK_TOKEN", token_file)
    return os.getenv("XPOK_TOKEN", "")


class XpokClient:
    """Synchronous HTTP client for the Xpo-k REST API."""

    def __init__(self, base_url: Optional[str] = None, token: Optional[str] = None):
        self.base_url = (base_url or os.environ.get("XPOK_URL", "")).rstrip("/")
        self.token = token if token is not None else _load_token()
        if not self.base_url:
            raise ValueError("XPOK_URL not set — cannot connect to Xpo-k")

    def _headers(self) -> Dict[str, str]:
        h: Dict[str, str] = {"Content-Type": "application/json"}
        if self.token:
            h["Authorization"] = f"Bearer {self.token}"
        return h

    # -- low-level request helpers --

    def _get(self, path: str, params: Optional[Dict] = None,
             timeout: int = _DEFAULT_TIMEOUT) -> Dict[str, Any]:
        url = f"{self.base_url}{path}"
        r = requests.get(url, headers=self._headers(), params=params, timeout=timeout)
        r.raise_for_status()
        return r.json()

    def _post(self, path: str, body: Optional[Dict] = None,
              timeout: int = _DEFAULT_TIMEOUT) -> Dict[str, Any]:
        url = f"{self.base_url}{path}"
        r = requests.post(url, headers=self._headers(), json=body, timeout=timeout)
        r.raise_for_status()
        return r.json()

    def _delete(self, path: str, timeout: int = _DEFAULT_TIMEOUT) -> Dict[str, Any]:
        url = f"{self.base_url}{path}"
        r = requests.delete(url, headers=self._headers(), timeout=timeout)
        r.raise_for_status()
        return r.json()

    # -- Xpo-k endpoints --

    def health(self) -> Dict[str, Any]:
        return self._get("/health")

    def clients(self) -> list:
        return self._get("/clients")

    def projects(self) -> list:
        return self._get("/projects")

    def sessions(self) -> list:
        return self._get("/sessions")

    def create_session(
        self,
        *,
        project: str = "",
        cwd: str = "",
        host: str = "",
        pok_id: str = "",
        profiles: Optional[list] = None,
        agent: str = "",
        model: str = "",
        effort: str = "",
        bare: bool = False,
    ) -> Dict[str, Any]:
        body: Dict[str, Any] = {}
        if project:
            body["project"] = project
        if cwd:
            body["cwd"] = cwd
        if host:
            body["host"] = host
        if pok_id:
            body["pok_id"] = pok_id
        if profiles:
            body["profiles"] = profiles
        if agent:
            body["agent"] = agent
        if model or effort:
            body["cc_flags"] = {}
            if model:
                body["cc_flags"]["model"] = model
            if effort:
                body["cc_flags"]["effort"] = effort
        if bare:
            body["bare"] = True
        return self._post("/sessions", body)

    def get_session(self, sid: str) -> Dict[str, Any]:
        return self._get(f"/sessions/{sid}")

    def delete_session(self, sid: str) -> Dict[str, Any]:
        return self._delete(f"/sessions/{sid}")

    def send_message(self, sid: str, text: str) -> Dict[str, Any]:
        return self._post(f"/sessions/{sid}/messages", {"text": text}, timeout=180)

    def interrupt(self, sid: str) -> Dict[str, Any]:
        return self._post(f"/sessions/{sid}/interrupt")

    def get_status(self, sid: str) -> Dict[str, Any]:
        return self._get(f"/sessions/{sid}/status")

    def wait(self, sid: str, since: int = 0, timeout: int = 600) -> Dict[str, Any]:
        params: Dict[str, Any] = {"timeout": timeout}
        if since:
            params["since"] = since
        return self._get(f"/sessions/{sid}/wait", params=params, timeout=_WAIT_TIMEOUT)

    def get_events(self, sid: str, since: int = 0, wait: int = 2) -> Dict[str, Any]:
        params: Dict[str, Any] = {"wait": wait}
        if since:
            params["since"] = since
        return self._get(f"/sessions/{sid}/events", params=params, timeout=wait + 10)

    def get_pane(self, sid: str) -> Dict[str, Any]:
        return self._get(f"/sessions/{sid}/pane")

    def get_cost(self, sid: str) -> Dict[str, Any]:
        return self._get(f"/sessions/{sid}/cost")

    def upload_file(self, sid: str, filename: str, content_base64: str) -> Dict[str, Any]:
        return self._post(f"/sessions/{sid}/files", {
            "filename": filename,
            "content_base64": content_base64,
        })

    def clear(self, sid: str) -> Dict[str, Any]:
        return self._post(f"/sessions/{sid}/clear")

    def get_capabilities(self, sid: str) -> Dict[str, Any]:
        return self._get(f"/sessions/{sid}/capabilities")

    def answer_permission(self, sid: str, req_id: str,
                          behavior: str, message: str = "") -> Dict[str, Any]:
        body: Dict[str, Any] = {"behavior": behavior}
        if message:
            body["message"] = message
        return self._post(f"/sessions/{sid}/permission_requests/{req_id}", body)

    def registry(self) -> Any:
        return self._get("/registry")

    # -- Profile endpoints --

    def list_profiles(self) -> Any:
        return self._get("/profiles")

    def get_profile(self, name: str) -> Dict[str, Any]:
        return self._get(f"/profiles/{name}")

    def create_profile(self, profile: Dict[str, Any]) -> Dict[str, Any]:
        return self._post("/profiles", profile)

    def update_profile(self, name: str, profile: Dict[str, Any]) -> Dict[str, Any]:
        url = f"{self.base_url}/profiles/{name}"
        r = requests.put(url, headers=self._headers(), json=profile, timeout=_DEFAULT_TIMEOUT)
        r.raise_for_status()
        return r.json()

    def delete_profile(self, name: str) -> Dict[str, Any]:
        return self._delete(f"/profiles/{name}")

    def merge_profiles(self, profile_names: list) -> Dict[str, Any]:
        return self._post("/profiles/merge", {"profiles": profile_names})


# Singleton-ish — lazily created on first tool call.
_client: Optional[XpokClient] = None


def get_client() -> XpokClient:
    """Return (and lazily create) the module-level XpokClient."""
    global _client
    if _client is None:
        _client = XpokClient()
    return _client
