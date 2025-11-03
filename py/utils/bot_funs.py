# py/utils/bot_funs.py
from py.constants.constants import BOT_TOKEN, CHAT_ID, CIAN_ALERTS_CHAT_ID

import os
import json
import time
import socket
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import urllib3.util.connection as urllib3_cn

# ---------- Force IPv4 in urllib3 (avoid dead IPv6 routes) ----------
def _force_ipv4():
    def allowed_gai_family():
        return socket.AF_INET
    urllib3_cn.allowed_gai_family = allowed_gai_family
_force_ipv4()

# ---------- Session with retries/timeouts ----------
_session = requests.Session()
_retry = Retry(
    total=5, connect=5, read=5,
    backoff_factor=0.8,
    status_forcelist=(429, 500, 502, 503, 504),
    allowed_methods=frozenset(["GET", "POST"]),
)
_adapter = HTTPAdapter(max_retries=_retry, pool_connections=10, pool_maxsize=10)
_session.mount("https://", _adapter)
_session.mount("http://", _adapter)

# Optional proxy via env (works with HTTPS or SOCKS5)
# Examples:
#   export TELEGRAM_HTTPS_PROXY="http://user:pass@proxy-host:3128"
#   export TELEGRAM_HTTPS_PROXY="socks5h://127.0.0.1:9050"
_PROXY = os.environ.get("TELEGRAM_HTTPS_PROXY")
_PROXIES = {"https": _PROXY, "http": _PROXY} if _PROXY else None

# Local disk queue in case network is blocked (non-fatal)
_QUEUE_DIR = Path(os.environ.get("TELEGRAM_FALLBACK_DIR", "/tmp/telegram_queue"))
_QUEUE_DIR.mkdir(parents=True, exist_ok=True)

def _enqueue_fallback(payload: dict) -> None:
    ts = int(time.time() * 1000)
    p = _QUEUE_DIR / f"{ts}.json"
    try:
        p.write_text(json.dumps(payload, ensure_ascii=False))
        print(f"[telegram] queued locally: {p}")
    except Exception as e:
        print(f"[telegram] failed to queue locally: {e}")

def send_telegram_message(message: str,
                          chat_type: str = "alerts",
                          parse_mode: str | None = None,
                          disable_notification: bool | None = None,
                          timeout: int = 10,
                          use_queue_on_fail: bool = True) -> bool:
    """
    Sends a Telegram message. Returns True on success, False otherwise.
    - Forces IPv4 to avoid Errno 101/113 due to broken IPv6.
    - Retries with backoff.
    - Honors optional HTTPS/SOCKS proxy via TELEGRAM_HTTPS_PROXY.
    - On failure, optionally writes a JSON payload to /tmp/telegram_queue (or TELEGRAM_FALLBACK_DIR).
    """
    chat_id = CIAN_ALERTS_CHAT_ID if chat_type == "alerts" else CHAT_ID
    if not BOT_TOKEN or not chat_id:
        print("[telegram] Skipped: missing BOT_TOKEN or CHAT_ID")
        return False

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    data = {"chat_id": chat_id, "text": message}
    if parse_mode:
        data["parse_mode"] = parse_mode
    if disable_notification is not None:
        data["disable_notification"] = bool(disable_notification)

    try:
        resp = _session.post(url, data=data, timeout=timeout, proxies=_PROXIES)
        resp.raise_for_status()
        return True
    except requests.RequestException as e:
        print(f"[telegram] send failed: {e.__class__.__name__}: {e}")
        if use_queue_on_fail:
            _enqueue_fallback({"url": url, "data": data, "err": str(e)})
        return False
