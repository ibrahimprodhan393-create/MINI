import hashlib
import hmac
import json
import time
from urllib.parse import parse_qsl

from fastapi import HTTPException

from .config import settings


def parse_and_verify_init_data(init_data: str) -> tuple[dict, dict]:
    if not init_data:
        raise HTTPException(status_code=401, detail="Missing Telegram initData.")
    if not settings.bot_token:
        raise HTTPException(status_code=500, detail="BOT_TOKEN is not configured.")

    parsed = dict(parse_qsl(init_data, keep_blank_values=True))
    received_hash = parsed.pop("hash", None)
    if not received_hash:
        raise HTTPException(status_code=401, detail="Telegram hash is missing.")

    data_check_string = "\n".join(f"{key}={value}" for key, value in sorted(parsed.items()))
    secret_key = hmac.new(
        b"WebAppData",
        settings.bot_token.encode("utf-8"),
        hashlib.sha256,
    ).digest()
    calculated_hash = hmac.new(
        secret_key,
        data_check_string.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()

    if not hmac.compare_digest(calculated_hash, received_hash):
        raise HTTPException(status_code=401, detail="Invalid Telegram login data.")

    auth_date = int(parsed.get("auth_date", "0") or "0")
    if auth_date and time.time() - auth_date > settings.telegram_auth_max_age_seconds:
        raise HTTPException(status_code=401, detail="Telegram login data expired.")

    try:
        user = json.loads(parsed.get("user", "{}"))
    except json.JSONDecodeError as exc:
        raise HTTPException(status_code=401, detail="Invalid Telegram user payload.") from exc

    if not user.get("id"):
        raise HTTPException(status_code=401, detail="Telegram user id is missing.")

    return user, parsed


def dev_user_payload() -> tuple[dict, dict]:
    telegram_id = settings.dev_telegram_user_id
    if settings.admin_telegram_ids:
        telegram_id = next(iter(settings.admin_telegram_ids))
    return (
        {
            "id": telegram_id,
            "first_name": "Development",
            "last_name": "User",
            "username": "dev_user",
            "photo_url": "",
        },
        {"start_param": ""},
    )
