import os
from decimal import Decimal


def _bool(value: str | None, default: bool = False) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _int_list(value: str | None) -> set[int]:
    if not value:
        return set()
    items: set[int] = set()
    for part in value.replace(" ", "").split(","):
        if part:
            items.add(int(part))
    return items


class Settings:
    app_name = os.getenv("APP_NAME", "Telegram Mini Shop")
    public_app_url = os.getenv("PUBLIC_APP_URL", "")
    database_url = os.getenv("DATABASE_URL", "")
    bot_token = os.getenv("BOT_TOKEN", "")
    bot_username = os.getenv("BOT_USERNAME", "")
    mini_app_short_name = os.getenv("MINI_APP_SHORT_NAME", "")
    telegram_webhook_secret = os.getenv("TELEGRAM_WEBHOOK_SECRET", "")
    auto_payment_webhook_secret = os.getenv("AUTO_PAYMENT_WEBHOOK_SECRET", "")
    admin_telegram_ids = _int_list(os.getenv("ADMIN_TELEGRAM_IDS"))
    debug = _bool(os.getenv("DEBUG"), False)
    auto_migrate = _bool(os.getenv("AUTO_MIGRATE"), True)
    dev_telegram_user_id = int(os.getenv("DEV_TELEGRAM_USER_ID", "100000001"))
    referral_bonus = Decimal(os.getenv("REFERRAL_BONUS", "10"))
    telegram_auth_max_age_seconds = int(os.getenv("TELEGRAM_AUTH_MAX_AGE_SECONDS", "86400"))
    db_pool_min_size = int(os.getenv("DB_POOL_MIN_SIZE", "1"))
    db_pool_max_size = int(os.getenv("DB_POOL_MAX_SIZE", "5"))


settings = Settings()
