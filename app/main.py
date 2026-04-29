from __future__ import annotations

import json
import random
import secrets
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from time import monotonic
from typing import Annotated, Any

import asyncpg
import httpx
from fastapi import Depends, FastAPI, Header, HTTPException, Query, Response
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field, field_validator

from .config import settings
from .db import close_db, connect_db, connection
from .security import dev_user_payload, parse_and_verify_init_data
from .telegram import notifier

BASE_DIR = Path(__file__).resolve().parent.parent

app = FastAPI(title=settings.app_name)
app.mount("/static", StaticFiles(directory=BASE_DIR / "app" / "static"), name="static")


class PaymentRequestIn(BaseModel):
    amount: Decimal = Field(gt=0)
    method_id: int
    transaction_id: str | None = Field(default=None, max_length=120)
    screenshot_data: str | None = None
    product_id: int | None = None
    duration_days: int | None = None
    coupon_code: str | None = None

    @field_validator("duration_days")
    @classmethod
    def valid_optional_duration(cls, value: int | None) -> int | None:
        if value is not None and value < 1:
            raise ValueError("Duration must be at least 1 day.")
        return value


class AutoPaymentConfirmIn(BaseModel):
    payment_id: int | None = None
    transaction_id: str | None = Field(default=None, max_length=120)
    amount: Decimal | None = Field(default=None, gt=0)


class OrderCreateIn(BaseModel):
    product_id: int
    duration_days: int
    coupon_code: str | None = None

    @field_validator("duration_days")
    @classmethod
    def valid_duration(cls, value: int) -> int:
        if value < 1:
            raise ValueError("Duration must be at least 1 day.")
        return value


class TicketCreateIn(BaseModel):
    subject: str = Field(min_length=2, max_length=160)
    message: str = Field(min_length=2, max_length=2000)


class TicketMessageIn(BaseModel):
    message: str = Field(min_length=2, max_length=2000)


class CouponValidateIn(BaseModel):
    code: str = Field(min_length=2, max_length=60)
    product_id: int
    duration_days: int


class CurrencySelectIn(BaseModel):
    code: str = Field(min_length=2, max_length=12)


LANGUAGE_CODES = {"en", "bn", "hi", "ur", "ar", "id", "ms", "ne", "fil", "ru", "th", "tr"}


class LanguageSelectIn(BaseModel):
    code: str = Field(min_length=2, max_length=16)

    @field_validator("code")
    @classmethod
    def valid_language_code(cls, value: str) -> str:
        clean = value.strip().lower()
        if clean not in LANGUAGE_CODES:
            raise ValueError("Language is not supported.")
        return clean


class AssistantChatIn(BaseModel):
    message: str = Field(min_length=1, max_length=1000)
    language: str = "en"


class SupportSettingsIn(BaseModel):
    display_name: str = Field(default="Store Support", min_length=1, max_length=120)
    telegram_username: str = Field(default="", max_length=120)
    telegram_user_id: str = Field(default="", max_length=80)
    note: str = Field(default="Tap to open Telegram inbox for help.", max_length=500)
    enabled: bool = True

    @field_validator("telegram_user_id")
    @classmethod
    def valid_telegram_user_id(cls, value: str) -> str:
        clean = value.strip()
        if clean and not clean.lstrip("-").isdigit():
            raise ValueError("Telegram user ID must be numeric.")
        return clean


class AiAssistantSettingsIn(BaseModel):
    intro: str = Field(default="Ask me anything about this Mini App.", min_length=1, max_length=300)
    custom_knowledge: str = Field(default="", max_length=5000)
    enabled: bool = True


class BrandingSettingsIn(BaseModel):
    logo_url: str = Field(default="", max_length=1000000)


class ProductDurationIn(BaseModel):
    duration_days: int = Field(gt=0, le=3650)
    price: Decimal = Field(ge=0)
    sort_order: int = 0


class ProductIn(BaseModel):
    category_key: str
    name: str = Field(min_length=2, max_length=160)
    description: str = ""
    feature_text: str = ""
    video_url: str = ""
    panel_url: str = ""
    image_url: str = ""
    price_1_day: Decimal = Field(default=Decimal("0"), ge=0)
    price_7_days: Decimal = Field(default=Decimal("0"), ge=0)
    price_30_days: Decimal = Field(default=Decimal("0"), ge=0)
    durations: list[ProductDurationIn] = Field(default_factory=list)
    stock_status: bool = True
    stock_quantity: int | None = Field(default=None, ge=0)
    active: bool = True


class ProductKeyUploadIn(BaseModel):
    product_id: int
    duration_days: int = 1
    keys: str = Field(min_length=1, max_length=60000)

    @field_validator("duration_days")
    @classmethod
    def valid_key_duration(cls, value: int) -> int:
        if value < 1:
            raise ValueError("Duration must be at least 1 day.")
        return value


class CategoryIn(BaseModel):
    key: str = Field(min_length=2, max_length=80)
    name: str = Field(min_length=2, max_length=160)
    icon: str = "box"
    description: str = ""
    parent_key: str | None = None
    sort_order: int = 0
    active: bool = True


class PaymentMethodIn(BaseModel):
    name: str = Field(min_length=2, max_length=120)
    instructions: str = ""
    method_type: str = "manual"
    account_label: str = ""
    account_value: str = ""
    logo_url: str = ""
    qr_image_url: str = ""
    active: bool = True
    sort_order: int = 0

    @field_validator("method_type")
    @classmethod
    def valid_method_type(cls, value: str) -> str:
        if value not in {"manual", "auto"}:
            raise ValueError("Payment method type must be manual or auto.")
        return value


class CouponIn(BaseModel):
    code: str = Field(min_length=2, max_length=60)
    discount_type: str
    discount_value: Decimal = Field(ge=0)
    expires_at: datetime | None = None
    active: bool = True
    max_uses: int | None = Field(default=None, ge=1)

    @field_validator("discount_type")
    @classmethod
    def valid_discount_type(cls, value: str) -> str:
        if value not in {"percent", "fixed"}:
            raise ValueError("Discount type must be percent or fixed.")
        return value


class PaymentReviewIn(BaseModel):
    reason: str | None = Field(default=None, max_length=300)


class OrderStatusIn(BaseModel):
    note: str | None = Field(default=None, max_length=500)
    delivery_text: str | None = Field(default=None, max_length=2000)


class BalanceAdjustIn(BaseModel):
    amount: Decimal
    reason: str = Field(default="Admin adjustment", max_length=300)

    @field_validator("amount")
    @classmethod
    def non_zero(cls, value: Decimal) -> Decimal:
        if value == 0:
            raise ValueError("Amount must not be zero.")
        return value


class BroadcastIn(BaseModel):
    message: str = Field(min_length=2, max_length=3000)
    target: str = Field(default="all")
    user_id: int | None = None
    notice_title: str | None = Field(default=None, max_length=120)


def jsonable(value: Any) -> Any:
    if isinstance(value, asyncpg.Record):
        return {key: jsonable(value[key]) for key in value.keys()}
    if isinstance(value, list):
        return [jsonable(item) for item in value]
    if isinstance(value, dict):
        return {key: jsonable(item) for key, item in value.items()}
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, datetime):
        return value.isoformat()
    return value


def invoice_id() -> str:
    stamp = datetime.now(UTC).strftime("%Y%m%d%H%M")
    return f"INV-{stamp}-{secrets.token_hex(3).upper()}"


def referral_code(telegram_id: int) -> str:
    return f"ref_{telegram_id}"


def referral_link_for_user(user: asyncpg.Record | dict[str, Any]) -> str:
    code = row_value(user, "referral_code", "")
    if settings.bot_username and settings.mini_app_short_name:
        return f"https://t.me/{settings.bot_username}/{settings.mini_app_short_name}?startapp={code}"
    return code


LEGACY_DURATIONS = (1, 7, 30)


def quote_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def row_value(row: asyncpg.Record | dict[str, Any], key: str, default: Any = None) -> Any:
    if isinstance(row, asyncpg.Record):
        return row[key] if key in row.keys() else default
    return row.get(key, default)


def legacy_duration_price(product: asyncpg.Record | dict[str, Any], duration_days: int) -> Decimal | None:
    fields = {1: "price_1_day", 7: "price_7_days", 30: "price_30_days"}
    field = fields.get(duration_days)
    if not field:
        return None
    return Decimal(row_value(product, field, 0) or 0)


def legacy_duration_payload(product: asyncpg.Record | dict[str, Any]) -> list[dict[str, Any]]:
    return [
        {
            "duration_days": days,
            "price": legacy_duration_price(product, days) or Decimal("0"),
            "sort_order": index * 10,
            "active": True,
        }
        for index, days in enumerate(LEGACY_DURATIONS, start=1)
    ]


def normalize_product_durations(data: ProductIn) -> list[dict[str, Any]]:
    raw = data.durations or [
        ProductDurationIn(duration_days=1, price=data.price_1_day, sort_order=10),
        ProductDurationIn(duration_days=7, price=data.price_7_days, sort_order=20),
        ProductDurationIn(duration_days=30, price=data.price_30_days, sort_order=30),
    ]
    by_days: dict[int, dict[str, Any]] = {}
    for index, item in enumerate(raw, start=1):
        by_days[item.duration_days] = {
            "duration_days": item.duration_days,
            "price": Decimal(item.price),
            "sort_order": item.sort_order or index * 10,
            "active": True,
        }
    if not by_days:
        raise HTTPException(status_code=400, detail="Add at least one product duration.")
    return sorted(by_days.values(), key=lambda item: (item["sort_order"], item["duration_days"]))


def legacy_prices_for_product(data: ProductIn, durations: list[dict[str, Any]]) -> tuple[Decimal, Decimal, Decimal]:
    prices = {
        1: Decimal(data.price_1_day or 0),
        7: Decimal(data.price_7_days or 0),
        30: Decimal(data.price_30_days or 0),
    }
    for duration in durations:
        if duration["duration_days"] in prices:
            prices[duration["duration_days"]] = Decimal(duration["price"])
    return prices[1], prices[7], prices[30]


MAX_SPIN_BONUS = Decimal("0.05")
RUNTIME_CACHE_DEFAULT_TTL = 25
USER_SEEN_WRITE_INTERVAL = timedelta(seconds=90)
_RUNTIME_CACHE: dict[str, tuple[float, Any]] = {}

PRODUCT_DURATIONS_SCHEMA_READY = False
PRODUCT_KEYS_SCHEMA_READY = False
PAYMENT_METHOD_SCHEMA_READY = False
USER_PREFERENCES_SCHEMA_READY = False
SPIN_SCHEMA_READY = False


def cache_get(key: str) -> Any | None:
    item = _RUNTIME_CACHE.get(key)
    if not item:
        return None
    expires_at, value = item
    if expires_at <= monotonic():
        _RUNTIME_CACHE.pop(key, None)
        return None
    return value


def cache_set(key: str, value: Any, ttl: int = RUNTIME_CACHE_DEFAULT_TTL) -> Any:
    _RUNTIME_CACHE[key] = (monotonic() + ttl, value)
    return value


def clear_runtime_cache(prefix: str | None = None) -> None:
    if prefix is None:
        _RUNTIME_CACHE.clear()
        return
    for key in list(_RUNTIME_CACHE.keys()):
        if key.startswith(prefix):
            _RUNTIME_CACHE.pop(key, None)


SUPPORT_SETTING_DEFAULTS = {
    "support_display_name": "Store Support",
    "support_telegram_username": "",
    "support_telegram_user_id": "",
    "support_note": "Tap to open Telegram inbox for help.",
    "support_enabled": "true",
}

RESELLER_SETTING_DEFAULTS = {
    "reseller_display_name": "Reseller Manager",
    "reseller_telegram_username": "",
    "reseller_telegram_user_id": "",
    "reseller_note": "Apply for reseller pricing through Telegram.",
    "reseller_enabled": "true",
}

AI_ASSISTANT_SETTING_DEFAULTS = {
    "ai_assistant_intro": "ACI AI is ready. Ask anything about wallet, payment, orders, products, spin, referral, support, reseller, currency, and account settings.",
    "ai_assistant_custom_knowledge": "",
    "ai_assistant_enabled": "true",
}

BRANDING_SETTING_DEFAULTS = {
    "app_logo_url": "",
}

ALL_SETTING_DEFAULTS = (
    SUPPORT_SETTING_DEFAULTS
    | RESELLER_SETTING_DEFAULTS
    | AI_ASSISTANT_SETTING_DEFAULTS
    | BRANDING_SETTING_DEFAULTS
)


def normalize_telegram_username(value: str) -> str:
    clean = value.strip()
    lower = clean.lower()
    for prefix in ("https://t.me/", "http://t.me/", "t.me/"):
        if lower.startswith(prefix):
            clean = clean[len(prefix):]
            break
    clean = clean.strip().lstrip("@").strip("/")
    if "/" in clean:
        clean = clean.split("/", 1)[0]
    if "?" in clean:
        clean = clean.split("?", 1)[0]
    return clean


def contact_settings_from_values(
    values: dict[str, str],
    prefix: str,
    fallback_name: str,
) -> dict[str, Any]:
    username = normalize_telegram_username(values[f"{prefix}_telegram_username"])
    user_id = values[f"{prefix}_telegram_user_id"].strip()
    return {
        "display_name": values[f"{prefix}_display_name"].strip() or fallback_name,
        "telegram_username": username,
        "telegram_user_id": user_id,
        "note": values[f"{prefix}_note"].strip(),
        "enabled": values[f"{prefix}_enabled"].lower() == "true",
        "has_contact": bool(username or user_id),
    }


def ai_settings_from_values(values: dict[str, str]) -> dict[str, Any]:
    return {
        "intro": values["ai_assistant_intro"].strip() or AI_ASSISTANT_SETTING_DEFAULTS["ai_assistant_intro"],
        "custom_knowledge": values["ai_assistant_custom_knowledge"].strip(),
        "enabled": values["ai_assistant_enabled"].lower() == "true",
    }


def branding_settings_from_values(values: dict[str, str]) -> dict[str, Any]:
    return {"logo_url": values["app_logo_url"].strip()}


async def fetch_public_settings_bundle(conn: asyncpg.Connection) -> dict[str, Any]:
    cached = cache_get("settings:public")
    if cached is not None:
        return cached
    rows = await conn.fetch(
        "select key, value from app_settings where key = any($1::text[])",
        list(ALL_SETTING_DEFAULTS.keys()),
    )
    values = ALL_SETTING_DEFAULTS | {row["key"]: row["value"] for row in rows}
    bundle = {
        "support": contact_settings_from_values(values, "support", "Store Support"),
        "reseller": contact_settings_from_values(values, "reseller", "Reseller Manager"),
        "assistant": ai_settings_from_values(values),
        "branding": branding_settings_from_values(values),
    }
    return cache_set("settings:public", bundle, 30)


async def fetch_contact_settings(
    conn: asyncpg.Connection,
    defaults: dict[str, str],
    prefix: str,
    fallback_name: str,
) -> dict[str, Any]:
    bundle = await fetch_public_settings_bundle(conn)
    return bundle[prefix]


async def fetch_support_settings(conn: asyncpg.Connection) -> dict[str, Any]:
    return await fetch_contact_settings(conn, SUPPORT_SETTING_DEFAULTS, "support", "Store Support")


async def fetch_reseller_settings(conn: asyncpg.Connection) -> dict[str, Any]:
    return await fetch_contact_settings(conn, RESELLER_SETTING_DEFAULTS, "reseller", "Reseller Manager")


async def fetch_ai_assistant_settings(conn: asyncpg.Connection) -> dict[str, Any]:
    bundle = await fetch_public_settings_bundle(conn)
    return bundle["assistant"]


async def fetch_branding_settings(conn: asyncpg.Connection) -> dict[str, Any]:
    bundle = await fetch_public_settings_bundle(conn)
    return bundle["branding"]


async def save_app_settings(conn: asyncpg.Connection, payload: dict[str, str]) -> None:
    for key, value in payload.items():
        await conn.execute(
            """
            insert into app_settings (key, value, updated_at)
            values ($1, $2, now())
            on conflict (key) do update set
                value = excluded.value,
                updated_at = now()
            """,
            key,
            value,
        )
    clear_runtime_cache("settings:")


async def ensure_user_preferences_runtime_schema(conn: asyncpg.Connection) -> None:
    global USER_PREFERENCES_SCHEMA_READY
    if USER_PREFERENCES_SCHEMA_READY:
        return
    await conn.execute("alter table users add column if not exists selected_language text not null default 'en'")
    USER_PREFERENCES_SCHEMA_READY = True


async def ensure_payment_method_runtime_schema(conn: asyncpg.Connection) -> None:
    global PAYMENT_METHOD_SCHEMA_READY
    if PAYMENT_METHOD_SCHEMA_READY:
        return
    await conn.execute("alter table payment_methods add column if not exists logo_url text not null default ''")
    PAYMENT_METHOD_SCHEMA_READY = True


async def fetch_active_currencies(conn: asyncpg.Connection) -> list[dict[str, Any]]:
    cached = cache_get("currencies:active")
    if cached is not None:
        return cached
    rows = await conn.fetch(
        """
        select code, symbol, name, rate_from_base, active, sort_order
          from currencies
         where active = true
         order by sort_order, code
        """
    )
    return cache_set("currencies:active", jsonable(rows), 60)


async def fetch_selected_currency(conn: asyncpg.Connection, code: str) -> dict[str, Any] | None:
    currencies = await fetch_active_currencies(conn)
    selected = next((currency for currency in currencies if currency["code"] == code), None)
    if selected:
        return selected
    row = await conn.fetchrow(
        """
        select code, symbol, name, rate_from_base, active, sort_order
          from currencies
         where code = $1 and active = true
        """,
        code,
    )
    return jsonable(row) if row else None


async def fetch_active_payment_methods(conn: asyncpg.Connection) -> list[dict[str, Any]]:
    cached = cache_get("payment_methods:active")
    if cached is not None:
        return cached
    await ensure_payment_method_runtime_schema(conn)
    rows = await conn.fetch(
        """
        select id, name, instructions, method_type, account_label, account_value,
               logo_url, qr_image_url, active, sort_order
          from payment_methods
         where active = true
         order by sort_order, name
        """
    )
    return cache_set("payment_methods:active", jsonable(rows), 20)


async def fetch_root_categories(conn: asyncpg.Connection) -> list[dict[str, Any]]:
    cached = cache_get("categories:root")
    if cached is not None:
        return cached
    rows = await conn.fetch(
        """
        select key, name, icon, description, parent_key, sort_order, active
          from categories
         where active = true
           and parent_key is null
         order by sort_order, name
        """
    )
    return cache_set("categories:root", jsonable(rows), 30)


async def fetch_child_categories(conn: asyncpg.Connection, parent: str | None) -> list[dict[str, Any]]:
    if not parent:
        return await fetch_root_categories(conn)
    key = f"categories:children:{parent}"
    cached = cache_get(key)
    if cached is not None:
        return cached
    rows = await conn.fetch(
        """
        select key, name, icon, description, parent_key, sort_order, active
          from categories
         where active = true and parent_key = $1
         order by sort_order, name
        """,
        parent,
    )
    return cache_set(key, jsonable(rows), 30)


async def fetch_active_notices(conn: asyncpg.Connection) -> list[dict[str, Any]]:
    cached = cache_get("notices:active")
    if cached is not None:
        return cached
    rows = await conn.fetch(
        """
        select id, title, body, active, starts_at, ends_at, created_at
          from notices
         where active = true
           and (starts_at is null or starts_at <= now())
           and (ends_at is null or ends_at >= now())
         order by created_at desc
         limit 3
        """
    )
    return cache_set("notices:active", jsonable(rows), 15)


async def drop_duration_check_constraints(conn: asyncpg.Connection) -> None:
    for table in ("payment_requests", "orders", "product_keys"):
        constraints = await conn.fetch(
            """
            select conname
              from pg_constraint
             where conrelid = to_regclass($1)
               and contype = 'c'
               and pg_get_constraintdef(oid) ilike '%duration_days%'
               and (
                   pg_get_constraintdef(oid) ilike '%1, 7, 30%'
                   or pg_get_constraintdef(oid) ilike '%1,7,30%'
               )
            """,
            table,
        )
        for constraint in constraints:
            await conn.execute(
                f"alter table {table} drop constraint if exists {quote_identifier(constraint['conname'])}"
            )


async def ensure_product_durations_runtime_schema(conn: asyncpg.Connection) -> None:
    global PRODUCT_DURATIONS_SCHEMA_READY
    if PRODUCT_DURATIONS_SCHEMA_READY:
        return
    await drop_duration_check_constraints(conn)
    await conn.execute(
        """
        create table if not exists product_durations (
            id bigserial primary key,
            product_id bigint not null references products(id) on delete cascade,
            duration_days int not null check (duration_days > 0),
            price numeric(12,2) not null default 0 check (price >= 0),
            sort_order int not null default 0,
            active boolean not null default true,
            created_at timestamptz not null default now(),
            updated_at timestamptz not null default now()
        )
        """
    )
    await conn.execute("alter table product_durations add column if not exists active boolean not null default true")
    await conn.execute("alter table product_durations add column if not exists sort_order int not null default 0")
    await conn.execute("create unique index if not exists idx_product_durations_unique on product_durations(product_id, duration_days)")
    await conn.execute("create index if not exists idx_product_durations_product on product_durations(product_id, sort_order, duration_days)")
    for index, days in enumerate(LEGACY_DURATIONS, start=1):
        await conn.execute(
            """
            insert into product_durations (product_id, duration_days, price, sort_order, active)
            select id,
                   $1::int,
                   case $1::int
                       when 1 then price_1_day
                       when 7 then price_7_days
                       else price_30_days
                   end,
                   $2::int,
                   true
              from products p
             where not exists (
                   select 1
                     from product_durations d
                    where d.product_id = p.id
                      and d.duration_days = $1
             )
            """,
            days,
            index * 10,
        )
    PRODUCT_DURATIONS_SCHEMA_READY = True


async def product_duration_map(
    conn: asyncpg.Connection,
    product_ids: list[int],
) -> dict[int, list[dict[str, Any]]]:
    if not product_ids:
        return {}
    await ensure_product_durations_runtime_schema(conn)
    rows = await conn.fetch(
        """
        select product_id, duration_days, price, sort_order, active
          from product_durations
         where product_id = any($1::bigint[])
           and active = true
         order by sort_order, duration_days
        """,
        product_ids,
    )
    grouped: dict[int, list[dict[str, Any]]] = {}
    for row in rows:
        grouped.setdefault(int(row["product_id"]), []).append({key: row[key] for key in row.keys()})
    return grouped


async def attach_product_durations(
    conn: asyncpg.Connection,
    rows: list[asyncpg.Record],
) -> list[dict[str, Any]]:
    products = [{key: row[key] for key in row.keys()} for row in rows]
    grouped = await product_duration_map(conn, [int(product["id"]) for product in products])
    for product in products:
        product["durations"] = grouped.get(int(product["id"])) or legacy_duration_payload(product)
    return products


async def fetch_product_duration(
    conn: asyncpg.Connection,
    product_id: int,
    duration_days: int,
) -> dict[str, Any] | None:
    if duration_days < 1:
        return None
    await ensure_product_durations_runtime_schema(conn)
    row = await conn.fetchrow(
        """
        select product_id, duration_days, price, sort_order, active
          from product_durations
         where product_id = $1
           and duration_days = $2
           and active = true
        """,
        product_id,
        duration_days,
    )
    if row:
        return {key: row[key] for key in row.keys()}
    product = await conn.fetchrow(
        "select id, price_1_day, price_7_days, price_30_days from products where id = $1",
        product_id,
    )
    if not product:
        return None
    legacy_price = legacy_duration_price(product, duration_days)
    if legacy_price is None:
        return None
    return {
        "product_id": product_id,
        "duration_days": duration_days,
        "price": legacy_price,
        "sort_order": duration_days,
        "active": True,
    }


async def replace_product_durations(
    conn: asyncpg.Connection,
    product_id: int,
    durations: list[dict[str, Any]],
) -> None:
    await ensure_product_durations_runtime_schema(conn)
    await conn.execute("delete from product_durations where product_id = $1", product_id)
    await conn.executemany(
        """
        insert into product_durations (product_id, duration_days, price, sort_order, active)
        values ($1, $2, $3, $4, true)
        on conflict (product_id, duration_days) do update set
            price = excluded.price,
            sort_order = excluded.sort_order,
            active = true,
            updated_at = now()
        """,
        [
            (product_id, item["duration_days"], item["price"], item["sort_order"])
            for item in durations
        ],
    )


def assistant_money(value: Any, currency: asyncpg.Record | None) -> str:
    code = currency["code"] if currency else "USD"
    symbol = currency["symbol"] if currency else "$"
    rate = Decimal(str(currency["rate_from_base"] if currency else 1))
    amount = (Decimal(str(value or 0)) * rate).quantize(Decimal("0.01"))
    return f"{symbol} {amount} {code}"


def contains_any(message: str, words: tuple[str, ...]) -> bool:
    lower = message.lower()
    return any(word.lower() in lower for word in words)


def tokenize_question(value: str) -> set[str]:
    clean = "".join(ch.lower() if ch.isalnum() else " " for ch in value)
    return {token for token in clean.split() if len(token) >= 2}


def custom_knowledge_answer(message: str, custom: str) -> str | None:
    if not custom.strip():
        return None
    question_tokens = tokenize_question(message)
    blocks = [block.strip() for block in custom.replace("\r\n", "\n").split("\n\n") if block.strip()]
    best_score = 0
    best_block = ""
    for block in blocks:
        head, _, body = block.partition("\n")
        haystack = f"{head} {body or block}"
        score = len(question_tokens & tokenize_question(haystack))
        if score > best_score:
            best_score = score
            best_block = block
    if best_score >= 2:
        return best_block[:1400]
    if len(blocks) == 1:
        return blocks[0][:1400]
    return None


def detect_language_hint(message: str) -> str:
    lower = message.lower()
    if any("\u0980" <= ch <= "\u09ff" for ch in message):
        return "bn"
    if any("\u0600" <= ch <= "\u06ff" for ch in message):
        return "ar"
    if any("\u0900" <= ch <= "\u097f" for ch in message):
        return "hi"
    if any("\u0e00" <= ch <= "\u0e7f" for ch in message):
        return "th"
    if any("\u0400" <= ch <= "\u04ff" for ch in message):
        return "ru"
    if any(ch in lower for ch in "ğüşöçıİ"):
        return "tr"
    if any(word in lower for word in ("selam", "merhaba", "tesekkur", "teşekkür")):
        return "tr"
    if any(word in lower for word in ("terima", "kasih", "bahasa", "bayar")):
        return "id"
    if any(word in lower for word in ("kumusta", "salamat", "paano")):
        return "fil"
    return "en"


def built_in_general_answer(message: str) -> tuple[str, list[str]]:
    lower = message.lower()
    lang = detect_language_hint(message)
    suggestions = ["Add Fund", "Order status", "Payment method", "Support"]
    if "how are you" in lower or "কেমন" in message:
        replies = {
            "bn": "আমি ভালো আছি। এই Mini App-এর payment, order, wallet, spin, referral, support, reseller, product এবং admin setup নিয়ে যেকোনো প্রশ্ন করতে পারেন।",
            "hi": "मैं ठीक हूं। आप इस Mini App के payment, order, wallet, spin, referral, support, reseller, product और admin setup के बारे में पूछ सकते हैं।",
            "ar": "أنا بخير. يمكنك سؤالي عن الدفع، الطلبات، المحفظة، العجلة اليومية، الإحالة، الدعم، المنتجات ولوحة الإدارة.",
            "tr": "İyiyim. Bu Mini App icin odeme, siparis, cuzdan, spin, referans, destek, bayi, urun ve admin sorularini cevaplayabilirim.",
            "ru": "У меня все хорошо. Я могу помочь с оплатой, заказами, кошельком, спином, рефералами, поддержкой, товарами и админ-панелью.",
            "th": "ฉันสบายดี คุณถามได้เรื่องการชำระเงิน คำสั่งซื้อ กระเป๋าเงิน สปินรายวัน แนะนำเพื่อน ซัพพอร์ต สินค้า และแอดมิน",
            "id": "Saya baik. Anda bisa bertanya tentang payment, order, wallet, spin, referral, support, reseller, produk, dan admin setup.",
            "fil": "Ayos lang ako. Maaari kang magtanong tungkol sa payment, orders, wallet, spin, referral, support, reseller, products, at admin setup.",
        }
        return (
            replies.get(lang, "I am good. You can ask me about this Mini App: payment, orders, wallet, daily spin, referral, support, reseller, products, currency, language, and admin setup."),
            suggestions,
        )
    if "hello" in lower or "hi" in lower or "হ্যালো" in message or "সালাম" in message:
        replies = {
            "bn": "হ্যালো! আমি আপনার Mini App Assistant. কী জানতে চান?",
            "hi": "नमस्ते! मैं आपका Mini App Assistant हूं। आप क्या जानना चाहते हैं?",
            "ar": "مرحبا! أنا مساعد تطبيقك المصغر. ماذا تريد أن تعرف؟",
            "tr": "Merhaba! Ben Mini App asistaniniz. Ne ogrenmek istersiniz?",
            "ru": "Здравствуйте! Я помощник Mini App. Что хотите узнать?",
            "th": "สวัสดี! ฉันคือผู้ช่วย Mini App คุณต้องการทราบอะไร?",
            "id": "Halo! Saya Mini App Assistant Anda. Apa yang ingin Anda ketahui?",
            "fil": "Hello! Ako ang iyong Mini App Assistant. Ano ang gusto mong malaman?",
        }
        return (replies.get(lang, "Hello! I am your Mini App Assistant. What would you like to know?"), ["Payment method", "Order history", "Daily spin"])
    if "thank" in lower or "ধন্যবাদ" in message:
        replies = {
            "bn": "স্বাগতম। আর কোনো প্রশ্ন থাকলে লিখুন, আমি সাহায্য করব।",
            "hi": "स्वागत है। कोई और सवाल हो तो लिखें, मैं मदद करूंगा।",
            "ar": "على الرحب والسعة. اكتب أي سؤال آخر وسأساعدك.",
            "tr": "Rica ederim. Baska bir sorunuz varsa yazin, yardim ederim.",
            "ru": "Пожалуйста. Если есть еще вопрос, напишите, я помогу.",
            "th": "ยินดีครับ หากมีคำถามเพิ่มเติมพิมพ์มาได้เลย",
            "id": "Sama-sama. Jika ada pertanyaan lain, tulis saja.",
            "fil": "Walang anuman. Kung may iba ka pang tanong, isulat mo lang.",
        }
        return (replies.get(lang, "You are welcome. Ask anything else and I will help."), ["Add Fund", "Referral", "Currency"])
    general = {
        "bn": "আমি আপনার Mini App Assistant. Custom text না থাকলেও আমি এই bot/app-এর Add Fund, active payment method, screenshot submit, order status, delivery key, product section, coupon, referral, daily spin, support, reseller, profile, currency, language এবং admin panel নিয়ে উত্তর দিতে পারি। আপনার প্রশ্নটি একটু নির্দিষ্ট করে লিখলে আমি সরাসরি উত্তর দেব।",
        "hi": "मैं आपका Mini App Assistant हूं। Custom text के बिना भी मैं Add Fund, active payment method, screenshot submit, order status, delivery key, product section, coupon, referral, daily spin, support, reseller, profile, currency, language और admin panel पर मदद कर सकता हूं।",
        "ar": "أنا مساعد Mini App. حتى بدون نص مخصص، يمكنني المساعدة في إضافة الرصيد، طرق الدفع النشطة، رفع لقطة الشاشة، حالة الطلب، مفاتيح التسليم، الأقسام، الكوبونات، الإحالة، العجلة اليومية، الدعم، البائع، الملف الشخصي، العملة، اللغة ولوحة الإدارة.",
        "tr": "Ben Mini App asistaniniz. Ozel metin olmadan da Add Fund, aktif odeme yontemi, ekran goruntusu, siparis durumu, teslimat anahtari, urun bolumleri, kupon, referans, gunluk spin, destek, bayi, profil, para birimi, dil ve admin paneli hakkinda yardim edebilirim.",
        "ru": "Я помощник Mini App. Даже без custom text я могу помочь с пополнением, активными методами оплаты, скриншотом, статусом заказа, ключами доставки, разделами товаров, купонами, рефералами, ежедневным спином, поддержкой, reseller, профилем, валютой, языком и админ-панелью.",
        "th": "ฉันคือ Mini App Assistant แม้ไม่มี custom text ฉันช่วยตอบเรื่อง Add Fund, วิธีชำระเงินที่เปิดอยู่, อัปโหลดสกรีนช็อต, สถานะคำสั่งซื้อ, delivery key, หมวดสินค้า, คูปอง, referral, daily spin, support, reseller, profile, currency, language และ admin panel ได้",
        "id": "Saya Mini App Assistant. Tanpa custom text pun saya dapat membantu tentang Add Fund, payment method aktif, screenshot submit, order status, delivery key, product section, coupon, referral, daily spin, support, reseller, profile, currency, language, dan admin panel.",
        "fil": "Ako ang Mini App Assistant. Kahit walang custom text, makakatulong ako tungkol sa Add Fund, active payment method, screenshot submit, order status, delivery key, product section, coupon, referral, daily spin, support, reseller, profile, currency, language, at admin panel.",
    }
    return (
        general.get(lang, "I am your Mini App Assistant. Even without custom text, I can answer broad customer questions about this bot/app: Add Fund, active payment methods, screenshot submission, order status, delivery keys, product sections, coupons, referral, daily spin, support, reseller, profile, currency, language, and admin panel. Ask a more specific question and I will answer directly."),
        ["How to add fund?", "Order status", "Payment method", "Support"],
    )


def english_assistant_answer(
    message: str,
    *,
    user: asyncpg.Record,
    currency: asyncpg.Record | None,
    stats: asyncpg.Record,
    payments: list[asyncpg.Record],
    orders: list[asyncpg.Record],
    methods: list[asyncpg.Record],
    categories: list[asyncpg.Record],
    products: list[asyncpg.Record],
    support: dict[str, Any],
    reseller: dict[str, Any],
    ai_settings: dict[str, Any],
) -> tuple[str, list[str]]:
    custom_answer = custom_knowledge_answer(message, ai_settings.get("custom_knowledge") or "")
    suggestions = ["How do I add funds?", "Where is my order?", "How does daily spin work?", "Contact support"]
    if custom_answer:
        return (custom_answer, suggestions)

    method_names = ", ".join(method["name"] for method in methods) or "no active payment method is available"
    category_names = ", ".join(category["name"] for category in categories[:8]) or "no active section is available yet"
    product_names = ", ".join(product["name"] for product in products[:6]) or "no active product is available yet"
    latest_order = orders[0] if orders else None
    latest_payment = payments[0] if payments else None
    pending_payments = sum(1 for payment in payments if payment["status"] == "pending")
    pending_orders = sum(1 for order in orders if order["status"] == "pending")
    wallet = assistant_money(user["wallet_balance"], currency)
    support_contact = (
        f"@{support['telegram_username']}"
        if support.get("telegram_username")
        else f"ID {support['telegram_user_id']}" if support.get("telegram_user_id") else "not set"
    )
    reseller_contact = (
        f"@{reseller['telegram_username']}"
        if reseller.get("telegram_username")
        else f"ID {reseller['telegram_user_id']}" if reseller.get("telegram_user_id") else "not set"
    )
    lower = message.lower()

    if contains_any(lower, ("balance", "wallet", "money", "credit")):
        return (
            f"Your current wallet balance is {wallet}. To add money, open Account > Add Funds, choose an active payment method, copy the payment address, submit the amount, transaction ID, and screenshot. Admin approval will add the balance.",
            suggestions,
        )
    if contains_any(lower, ("add fund", "addfund", "deposit", "payment", "pay", "bkash", "nagad", "screenshot", "utr", "transaction")):
        detail = f"Active payment methods: {method_names}."
        if latest_payment:
            detail += f" Your latest payment request was {assistant_money(latest_payment['amount'], currency)} with status {latest_payment['status']}."
        if pending_payments:
            detail += f" You have {pending_payments} pending payment request(s)."
        return (
            f"Payment flow: choose an active method card such as bKash, Nagad, USDT, or another method set by admin. Copy the address/number, pay outside the app, then submit amount, transaction ID/UTR, and screenshot. {detail}",
            ["Show active payment methods", "Payment is pending", "How to copy payment address?"],
        )
    if contains_any(lower, ("order", "invoice", "delivery", "key", "checkout", "buy", "purchase")):
        detail = f"You have {stats['total_orders'] or 0} total order(s) and {stats['active_subscriptions'] or 0} active subscription(s)."
        if latest_order:
            detail += f" Latest order: {latest_order['invoice_id']} for {latest_order['product_name'] or 'Product'} is {latest_order['status']}."
        if pending_orders:
            detail += f" Pending order count: {pending_orders}."
        return (
            f"Checkout supports Wallet Pay and manual payment. If you use manual payment, the order will be created automatically after admin approves the payment request. {detail}",
            ["Where is my delivery key?", "Wallet pay", "Manual checkout payment"],
        )
    if contains_any(lower, ("product", "category", "section", "panel", "android", "iphone", "pc")):
        return (
            f"Open Shop to browse sections. Current sections: {category_names}. Recent products: {product_names}. Product details show image, description, duration, price, stock, and checkout.",
            ["Product duration", "Stock status", "How to buy?"],
        )
    if contains_any(lower, ("spin", "lucky", "bonus", "reward")):
        next_spin = dict(user).get("next_spin_at")
        next_text = f" Your next spin time is {next_spin}." if next_spin else " If the spin button is active, you can spin now."
        return (
            f"Daily Spinner can be used once every 24 hours. The maximum wallet reward is $0.05.{next_text}",
            ["Spin locked", "Maximum spin reward", "Next spin time"],
        )
    if contains_any(lower, ("referral", "refer", "invite", "bonus")):
        return (
            "Referral program: copy your referral link from Account. When a new user joins through your link, you earn $0.05 wallet bonus. The referral card shows total referrals, total earned, pending earned, and referral history.",
            ["Copy referral link", "Referral bonus", "Referral history"],
        )
    if contains_any(lower, ("support", "ticket", "help", "contact")):
        return (
            f"Support is available from Account > Support. Telegram support contact: {support_contact}. You can also create tickets and read admin replies inside the app.",
            ["Open support inbox", "Create ticket", "Ticket reply"],
        )
    if contains_any(lower, ("reseller", "seller")):
        return (
            f"Use Account > Apply for Reseller to contact the reseller manager. Current reseller contact: {reseller_contact}.",
            ["Apply for reseller", "Reseller contact", "Special pricing"],
        )
    if contains_any(lower, ("currency", "language", "english", "bangla", "usd", "bdt")):
        return (
            "Currency and language can be changed from Account. When currency changes, the app updates price symbols and currency icons automatically.",
            ["Change currency", "AI language", "USD to BDT"],
        )
    if contains_any(lower, ("admin", "manage", "logo", "broadcast", "user")):
        admin_text = "Your account is an admin account, so you can open Admin Panel from Account." if user["is_admin"] else "Admin Panel is visible only to configured admin Telegram IDs."
        return (
            f"{admin_text} Admin can manage products, sections, payment methods, logo, product keys, orders, users, coupons, support, reseller contact, AI settings, tickets, and broadcasts.",
            ["Change app logo", "Add payment method", "Upload product keys"],
        )
    return (
        "I am ACI AI. I can answer customer questions about this mini app: payments, checkout, wallet, orders, product delivery, daily spin, referral bonus, support, reseller requests, profile, currency, language, and admin rules. Please ask your question with a little more detail and I will answer in English.",
        suggestions,
    )


def mini_app_assistant_answer(
    message: str,
    *,
    user: asyncpg.Record,
    currency: asyncpg.Record | None,
    stats: asyncpg.Record,
    payments: list[asyncpg.Record],
    orders: list[asyncpg.Record],
    methods: list[asyncpg.Record],
    categories: list[asyncpg.Record],
    products: list[asyncpg.Record],
    support: dict[str, Any],
    reseller: dict[str, Any],
    ai_settings: dict[str, Any],
) -> tuple[str, list[str]]:
    return english_assistant_answer(
        message,
        user=user,
        currency=currency,
        stats=stats,
        payments=payments,
        orders=orders,
        methods=methods,
        categories=categories,
        products=products,
        support=support,
        reseller=reseller,
        ai_settings=ai_settings,
    )
    custom = ai_settings.get("custom_knowledge") or ""
    method_names = ", ".join(method["name"] for method in methods) or "No active payment method set"
    category_names = ", ".join(category["name"] for category in categories[:8]) or "No active section yet"
    product_names = ", ".join(product["name"] for product in products[:6]) or "No active product yet"
    latest_order = orders[0] if orders else None
    latest_payment = payments[0] if payments else None
    pending_payments = sum(1 for payment in payments if payment["status"] == "pending")
    pending_orders = sum(1 for order in orders if order["status"] == "pending")
    wallet = assistant_money(user["wallet_balance"], currency)
    support_contact = (
        f"@{support['telegram_username']}"
        if support.get("telegram_username")
        else f"ID {support['telegram_user_id']}" if support.get("telegram_user_id") else "not set"
    )
    reseller_contact = (
        f"@{reseller['telegram_username']}"
        if reseller.get("telegram_username")
        else f"ID {reseller['telegram_user_id']}" if reseller.get("telegram_user_id") else "not set"
    )
    suggestions = ["Add fund কিভাবে করব?", "আমার অর্ডার কোথায়?", "Daily spin কখন পাব?", "Support কোথায়?"]

    custom_answer = custom_knowledge_answer(message, custom)
    if custom_answer:
        return (custom_answer, suggestions)

    if contains_any(message, ("balance", "wallet", "ব্যালেন্স", "ওয়ালেট", "ওয়ালেট")):
        return (
            f"আপনার বর্তমান wallet balance {wallet}. Account থেকে Add Fund করলে payment request admin approve করার পর balance যোগ হবে. History tab-এ শুধু transaction history দেখা যাবে.",
            suggestions,
        )
    if contains_any(message, ("addfund", "add fund", "add balance", "deposit", "payment method", "address copy", "screenshot", "manual payment", "অ্যাড ফান্ড", "এড ফান্ড", "স্ক্রিনশট")):
        detail = f"Active payment methods: {method_names}."
        if latest_payment:
            detail += f" Latest payment request {assistant_money(latest_payment['amount'], currency)} - status {latest_payment['status']}."
        return (
            f"Add Fund flow: Account > Add Fund এ আগে payment method card select করবেন. Method চাপলে payment address/details দেখা যাবে এবং Copy button দিয়ে address copy করা যাবে. এরপর amount লিখে screenshot upload করে Submit Payment চাপবেন. Transaction ID/Note optional. {detail}",
            ["Payment address copy", "Screenshot submit", "Payment pending কেন?"],
        )
    if contains_any(message, ("payment", "pay", "fund", "add money", "add fund", "পেমেন্ট", "ফান্ড", "টাকা", "বিকাশ", "নগদ")):
        detail = f"Active payment methods: {method_names}."
        if latest_payment:
            detail += f" আপনার শেষ payment request {assistant_money(latest_payment['amount'], currency)} - status {latest_payment['status']}."
        if pending_payments:
            detail += f" Pending payment আছে {pending_payments}টি."
        return (
            f"Account section-এর Add Fund অংশে payment method card select করে address copy করবেন, তারপর amount এবং screenshot submit করবেন. Transaction ID/Note optional. {detail} Manual payment admin approve করলে balance add হবে; auto payment method হলে webhook confirm করতে পারে.",
            ["Payment address copy", "Screenshot submit", "Payment pending কেন?"],
        )
    if contains_any(message, ("order", "invoice", "delivery", "key", "অর্ডার", "ইনভয়েস", "ডেলিভারি", "কি", "চাবি")):
        detail = f"মোট order {stats['total_orders'] or 0}, active subscription {stats['active_subscriptions'] or 0}."
        if latest_order:
            detail += f" Latest order {latest_order['invoice_id']} - {latest_order['product_name'] or 'Product'} - status {latest_order['status']}."
        if pending_orders:
            detail += f" Pending order আছে {pending_orders}টি."
        return (
            f"Orders tab-এ শুধু order history থাকবে. {detail} Admin approve/deliver করলে delivery key/file/link order card-এ দেখাবে.",
            ["Order status কী?", "Delivery key কোথায়?", "Wallet Pay কিভাবে?"],
        )
    if contains_any(message, ("product", "category", "section", "পণ্য", "প্রোডাক্ট", "ক্যাটাগরি", "সেকশন", "প্যানেল")):
        return (
            f"Shop tab-এ section/category দেখা যাবে. Current sections: {category_names}. Recent products: {product_names}. Product details থেকে 1 Day, 7 Days, 30 Days duration বেছে order করা যাবে.",
            ["Android section কোথায়?", "Duration কীভাবে বাছব?", "Stock status কী?"],
        )
    if contains_any(message, ("spin", "lucky", "bonus", "স্পিন", "বোনাস", "লাকি")):
        next_spin = dict(user).get("next_spin_at")
        next_text = f" Next spin time: {next_spin}." if next_spin else " Spin available থাকলে Daily Spin খুলে spin করতে পারবেন."
        return (
            f"Daily Spin Account section থেকে খুলবেন. প্রত্যেক user 24 ঘণ্টায় একবার spin করতে পারে, সর্বোচ্চ bonus 0.05 wallet credit.{next_text}",
            ["Spin কাজ করছে না", "Bonus কত?", "Next spin কখন?"],
        )
    if contains_any(message, ("referral", "refer", "রেফার", "রেফারেল")):
        return (
            "Account section থেকে Referral খুলে আপনার referral link copy করবেন. নতুন user সেই link দিয়ে join করলে referral bonus wallet transaction হিসেবে যোগ হবে.",
            ["Referral link কোথায়?", "Bonus কখন পাব?", "Referral history কোথায়?"],
        )
    if contains_any(message, ("coupon", "promo", "discount", "কুপন", "প্রোমো", "ডিসকাউন্ট")):
        return (
            "Account section থেকে Promo Code খুলে product ও duration select করে coupon check করবেন. Coupon percent বা fixed discount হতে পারে এবং expiry/max usage admin set করে.",
            ["Coupon কাজ করছে না", "Discount কত?", "Expiry date কী?"],
        )
    if contains_any(message, ("support", "ticket", "help", "সাপোর্ট", "টিকেট", "হেল্প")):
        return (
            f"Account section-এর Support থেকে Telegram inbox বা ticket খুলতে পারবেন. Current support contact: {support_contact}. Ticket করলে admin reply app-এর ভিতরেই দেখা যাবে.",
            ["Support inbox খুলছে না", "Ticket reply কোথায়?", "Admin contact কী?"],
        )
    if contains_any(message, ("reseller", "seller", "রিসেলার", "রিসেলর")):
        return (
            f"Account section-এর Apply for Reseller button reseller Telegram inbox খুলবে. Current reseller contact: {reseller_contact}. Admin panel থেকে এই contact change করা যাবে.",
            ["Apply for Reseller কোথায়?", "Reseller contact set করব কীভাবে?", "Reseller pricing কী?"],
        )
    if contains_any(message, ("currency", "language", "কারেন্সি", "ভাষা", "ল্যাঙ্গুয়েজ", "ল্যাঙ্গুয়েজ")):
        return (
            f"Account section-এ Language এবং Currency আলাদা selector আছে. Currency বদলালে price/wallet display selected currency rate অনুযায়ী দেখাবে; language preference account-এ save হবে.",
            ["Currency কীভাবে বদলাব?", "Bangla language আছে?", "USD থেকে BDT করব কীভাবে?"],
        )
    if contains_any(message, ("admin", "manage", "অ্যাডমিন", "ম্যানেজ")):
        admin_text = "আপনার account admin, তাই Account থেকে Admin Panel খুলতে পারবেন." if user["is_admin"] else "Admin Panel শুধু configured admin Telegram ID-র জন্য দেখা যাবে."
        return (
            f"{admin_text} Admin panel থেকে product, section, key store, payment method, orders, users, coupons, support/reseller, AI knowledge, tickets এবং broadcast manage করা যায়.",
            ["Admin ID কোথায় দেব?", "Product key upload", "Payment approve"],
        )
    return built_in_general_answer(message)


def assistant_context_text(
    *,
    user: asyncpg.Record,
    currency: asyncpg.Record | None,
    stats: dict[str, Any] | asyncpg.Record,
    payments: list[Any],
    orders: list[Any],
    methods: list[Any],
    categories: list[Any],
    products: list[Any],
    support: dict[str, Any],
    reseller: dict[str, Any],
    ai_settings: dict[str, Any],
) -> str:
    method_names = ", ".join(row_value(method, "name", "") for method in methods if row_value(method, "name", ""))
    category_names = ", ".join(row_value(category, "name", "") for category in categories if row_value(category, "name", ""))
    product_names = ", ".join(row_value(product, "name", "") for product in products if row_value(product, "name", ""))
    latest_order = orders[0] if orders else None
    latest_payment = payments[0] if payments else None
    support_contact = (
        f"@{support['telegram_username']}"
        if support.get("telegram_username")
        else f"ID {support['telegram_user_id']}" if support.get("telegram_user_id") else "not set"
    )
    reseller_contact = (
        f"@{reseller['telegram_username']}"
        if reseller.get("telegram_username")
        else f"ID {reseller['telegram_user_id']}" if reseller.get("telegram_user_id") else "not set"
    )
    return "\n".join(
        [
            f"User: {row_value(user, 'first_name', '')} @{row_value(user, 'username', '')} Telegram ID {row_value(user, 'telegram_id', '')}.",
            f"Wallet balance: {assistant_money(row_value(user, 'wallet_balance', 0), currency)}.",
            f"Orders: {row_value(stats, 'total_orders', 0)} total, {row_value(stats, 'active_subscriptions', 0)} active subscriptions.",
            f"Active payment methods: {method_names or 'none'}.",
            f"Top categories: {category_names or 'none'}.",
            f"Recent products: {product_names or 'none'}.",
            f"Latest order: {row_value(latest_order, 'invoice_id', 'none') if latest_order else 'none'}.",
            f"Latest payment: {row_value(latest_payment, 'status', 'none') if latest_payment else 'none'}.",
            f"Support contact: {support_contact}. Reseller contact: {reseller_contact}.",
            f"Store knowledge from admin: {ai_settings.get('custom_knowledge') or 'none'}",
        ]
    )


async def external_ai_answer(
    message: str,
    language: str,
    *,
    user: asyncpg.Record,
    currency: asyncpg.Record | None,
    stats: dict[str, Any] | asyncpg.Record,
    payments: list[Any],
    orders: list[Any],
    methods: list[Any],
    categories: list[Any],
    products: list[Any],
    support: dict[str, Any],
    reseller: dict[str, Any],
    ai_settings: dict[str, Any],
) -> str | None:
    if not settings.ai_api_key:
        return None
    url = settings.ai_api_url.strip() or "https://api.openai.com/v1/chat/completions"
    context = assistant_context_text(
        user=user,
        currency=currency,
        stats=stats,
        payments=payments,
        orders=orders,
        methods=methods,
        categories=categories,
        products=products,
        support=support,
        reseller=reseller,
        ai_settings=ai_settings,
    )
    system_prompt = (
        "You are ACI AI inside a Telegram Mini App shop. Answer customer questions clearly, "
        "politely, and practically. You can answer general questions too, not only store FAQ. "
        "Use the app context when the question is about payment, products, orders, wallet, "
        "daily spin, referral, reseller, support, admin, language, or currency. If the selected "
        f"language code is '{language}', reply in that language when possible; otherwise reply in English. "
        "Never invent private payment confirmations or delivered keys. If a payment/order status is unknown, "
        "tell the user where to check it in the app.\n\nApp context:\n"
        f"{context}"
    )
    payload = {
        "model": settings.ai_model,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": message},
        ],
        "temperature": 0.35,
        "max_tokens": 450,
    }
    try:
        async with httpx.AsyncClient(timeout=20) as client:
            response = await client.post(
                url,
                headers={
                    "Authorization": f"Bearer {settings.ai_api_key}",
                    "Content-Type": "application/json",
                },
                json=payload,
            )
            response.raise_for_status()
        result = response.json()
        if result.get("output_text"):
            return str(result["output_text"]).strip()
        choices = result.get("choices") or []
        if choices:
            content = choices[0].get("message", {}).get("content")
            if isinstance(content, str) and content.strip():
                return content.strip()
    except Exception:
        return None
    return None


async def run_schema() -> None:
    if not settings.auto_migrate or not settings.database_url:
        return
    schema_path = BASE_DIR / "db" / "schema.sql"
    async with connection() as conn:
        await conn.execute(schema_path.read_text(encoding="utf-8"))
        await ensure_product_durations_runtime_schema(conn)


async def ensure_spin_runtime_schema(conn: asyncpg.Connection) -> None:
    global SPIN_SCHEMA_READY
    if SPIN_SCHEMA_READY:
        return
    await conn.execute("alter table users add column if not exists next_spin_at timestamptz")
    await conn.execute(
        """
        create table if not exists spin_prizes (
            id bigserial primary key,
            title text not null,
            amount numeric(12,2) not null default 0,
            weight int not null default 1,
            active boolean not null default true,
            sort_order int not null default 0,
            created_at timestamptz not null default now()
        )
        """
    )
    await conn.execute(
        """
        create table if not exists spin_history (
            id bigserial primary key,
            user_id bigint not null references users(id) on delete cascade,
            prize_id bigint references spin_prizes(id) on delete set null,
            prize_title text not null,
            amount numeric(12,2) not null default 0,
            created_at timestamptz not null default now()
        )
        """
    )
    await conn.execute(
        """
        insert into spin_prizes (title, amount, weight, sort_order)
        select 'Try Again', 0, 50, 10 where not exists (select 1 from spin_prizes where title = 'Try Again')
        """
    )
    await conn.execute(
        "insert into spin_prizes (title, amount, weight, sort_order) select 'Small Bonus', 0.05, 25, 20 where not exists (select 1 from spin_prizes where title = 'Small Bonus')"
    )
    await conn.execute(
        "insert into spin_prizes (title, amount, weight, sort_order) select 'Wallet Bonus', 0.02, 15, 30 where not exists (select 1 from spin_prizes where title = 'Wallet Bonus')"
    )
    await conn.execute(
        "insert into spin_prizes (title, amount, weight, sort_order) select 'Lucky Reward', 0.03, 8, 40 where not exists (select 1 from spin_prizes where title = 'Lucky Reward')"
    )
    await conn.execute(
        "insert into spin_prizes (title, amount, weight, sort_order) select 'Mega Reward', 0.05, 2, 50 where not exists (select 1 from spin_prizes where title = 'Mega Reward')"
    )
    await conn.execute(
        """
        with duplicates as (
            select duplicate.id as duplicate_id, keeper.id as keeper_id
              from spin_prizes duplicate
              join spin_prizes keeper on keeper.title = duplicate.title
             where duplicate.id > keeper.id
        )
        update spin_history
           set prize_id = duplicates.keeper_id
          from duplicates
         where spin_history.prize_id = duplicates.duplicate_id
        """
    )
    await conn.execute(
        """
        delete from spin_prizes duplicate
        using spin_prizes keeper
        where duplicate.title = keeper.title
          and duplicate.id > keeper.id
        """
    )
    await conn.execute("update spin_prizes set amount = 0, weight = 50, sort_order = 10 where title = 'Try Again'")
    await conn.execute("update spin_prizes set amount = 0.05, weight = 25, sort_order = 20 where title = 'Small Bonus'")
    await conn.execute("update spin_prizes set amount = 0.02, weight = 15, sort_order = 30 where title = 'Wallet Bonus'")
    await conn.execute("update spin_prizes set amount = 0.03, weight = 8, sort_order = 40 where title = 'Lucky Reward'")
    await conn.execute("update spin_prizes set amount = 0.05, weight = 2, sort_order = 50 where title = 'Mega Reward'")
    await conn.execute("update spin_prizes set amount = least(amount, $1)", MAX_SPIN_BONUS)
    SPIN_SCHEMA_READY = True


async def fetch_spin_prizes(conn: asyncpg.Connection) -> list[dict[str, Any]]:
    cached = cache_get("spin:prizes")
    if cached is not None:
        return cached
    await ensure_spin_runtime_schema(conn)
    rows = await conn.fetch(
        """
        select id, title, amount, weight, active, sort_order, created_at
          from spin_prizes
         where active = true
         order by sort_order, amount
        """
    )
    return cache_set("spin:prizes", jsonable(rows), 30)


async def ensure_product_keys_runtime_schema(conn: asyncpg.Connection) -> None:
    global PRODUCT_KEYS_SCHEMA_READY
    if PRODUCT_KEYS_SCHEMA_READY:
        return
    await conn.execute(
        """
        create table if not exists product_keys (
            id bigserial primary key,
            product_id bigint not null references products(id) on delete cascade,
            duration_days int not null default 1 check (duration_days > 0),
            key_value text not null,
            status text not null default 'available' check (status in ('available', 'delivered')),
            assigned_order_id bigint references orders(id) on delete set null,
            assigned_user_id bigint references users(id) on delete set null,
            uploaded_by bigint references users(id) on delete set null,
            created_at timestamptz not null default now(),
            delivered_at timestamptz
        )
        """
    )
    await drop_duration_check_constraints(conn)
    await conn.execute(
        "alter table product_keys add column if not exists duration_days int not null default 1 check (duration_days > 0)"
    )
    await conn.execute(
        "create index if not exists idx_product_keys_product_duration_status on product_keys(product_id, duration_days, status, created_at)"
    )
    await conn.execute(
        "create index if not exists idx_product_keys_product_status on product_keys(product_id, status, created_at)"
    )
    PRODUCT_KEYS_SCHEMA_READY = True


async def upsert_telegram_user(
    conn: asyncpg.Connection,
    tg_user: dict[str, Any],
    init_payload: dict[str, str],
) -> asyncpg.Record:
    telegram_id = int(tg_user["id"])
    first_name = tg_user.get("first_name", "")
    last_name = tg_user.get("last_name", "")
    username = tg_user.get("username", "")
    photo_url = tg_user.get("photo_url", "")
    is_admin = telegram_id in settings.admin_telegram_ids
    start_param = init_payload.get("start_param") or init_payload.get("tgWebAppStartParam") or ""
    referrer = None

    existing = await conn.fetchrow("select * from users where telegram_id = $1", telegram_id)
    if existing:
        last_seen = existing["last_seen_at"]
        if last_seen and last_seen.tzinfo is None:
            last_seen = last_seen.replace(tzinfo=UTC)
        should_touch = last_seen is None or datetime.now(UTC) - last_seen > USER_SEEN_WRITE_INTERVAL
        profile_changed = (
            existing["first_name"] != first_name
            or existing["last_name"] != last_name
            or existing["username"] != username
            or existing["photo_url"] != photo_url
            or (is_admin and not existing["is_admin"])
        )
        if not should_touch and not profile_changed:
            return existing
        return await conn.fetchrow(
            """
            update users
               set first_name = $2,
                   last_name = $3,
                   username = $4,
                   photo_url = $5,
                   is_admin = case when $6 then true else is_admin end,
                   last_seen_at = case when $7 then now() else last_seen_at end
             where telegram_id = $1
         returning *
            """,
            telegram_id,
            first_name,
            last_name,
            username,
            photo_url,
            is_admin,
            should_touch,
        )

    if start_param:
        referrer = await conn.fetchrow(
            "select id from users where referral_code = $1 and telegram_id <> $2",
            start_param,
            telegram_id,
        )

    async with conn.transaction():
        user = await conn.fetchrow(
            """
            insert into users (
                telegram_id, first_name, last_name, username, photo_url,
                referral_code, referred_by_user_id, is_admin, last_seen_at
            )
            values ($1, $2, $3, $4, $5, $6, $7, $8, now())
            returning *
            """,
            telegram_id,
            first_name,
            last_name,
            username,
            photo_url,
            referral_code(telegram_id),
            referrer["id"] if referrer else None,
            is_admin,
        )

        if referrer and settings.referral_bonus > 0:
            updated = await conn.fetchrow(
                """
                update users
                   set wallet_balance = wallet_balance + $1
                 where id = $2
             returning wallet_balance
                """,
                settings.referral_bonus,
                referrer["id"],
            )
            referral = await conn.fetchrow(
                """
                insert into referrals (referrer_user_id, referred_user_id, bonus_amount, status)
                values ($1, $2, $3, 'rewarded')
                returning id
                """,
                referrer["id"],
                user["id"],
                settings.referral_bonus,
            )
            await conn.execute(
                """
                insert into wallet_transactions (
                    user_id, type, amount, balance_after, reference_type, reference_id, note
                )
                values ($1, 'referral_bonus', $2, $3, 'referral', $4, 'Referral bonus')
                """,
                referrer["id"],
                settings.referral_bonus,
                updated["wallet_balance"],
                referral["id"],
            )

    return user


async def current_user(
    x_telegram_init_data: Annotated[str | None, Header(alias="X-Telegram-Init-Data")] = None,
) -> asyncpg.Record:
    if x_telegram_init_data:
        tg_user, payload = parse_and_verify_init_data(x_telegram_init_data)
    elif settings.debug:
        tg_user, payload = dev_user_payload()
    else:
        raise HTTPException(status_code=401, detail="Open this app from Telegram.")

    async with connection() as conn:
        user = await upsert_telegram_user(conn, tg_user, payload)
        if user["is_banned"]:
            raise HTTPException(status_code=403, detail="Your account is banned.")
        return user


async def admin_user(user: Annotated[asyncpg.Record, Depends(current_user)]) -> asyncpg.Record:
    if not user["is_admin"]:
        raise HTTPException(status_code=403, detail="Admin access required.")
    return user


async def compute_coupon_discount(
    conn: asyncpg.Connection,
    code: str | None,
    subtotal: Decimal,
) -> tuple[asyncpg.Record | None, Decimal]:
    if not code:
        return None, Decimal("0")
    coupon = await conn.fetchrow(
        """
        select id, code, discount_type, discount_value, expires_at, active, max_uses, used_count, created_at
          from coupons
         where lower(code) = lower($1)
           and active = true
           and (expires_at is null or expires_at >= now())
           and (max_uses is null or used_count < max_uses)
        """,
        code.strip(),
    )
    if not coupon:
        raise HTTPException(status_code=400, detail="Invalid or expired coupon.")

    value = Decimal(coupon["discount_value"])
    if coupon["discount_type"] == "percent":
        discount = (subtotal * value / Decimal("100")).quantize(Decimal("0.01"))
    else:
        discount = value
    if discount > subtotal:
        discount = subtotal
    return coupon, discount


async def auto_deliver_order_key_locked(
    conn: asyncpg.Connection,
    order_id: int,
) -> tuple[asyncpg.Record, asyncpg.Record | None]:
    await ensure_product_keys_runtime_schema(conn)
    order = await conn.fetchrow(
        "select * from orders where id = $1 for update",
        order_id,
    )
    if not order:
        raise HTTPException(status_code=404, detail="Order not found.")
    if not order["product_id"] or order["status"] in {"delivered", "cancelled"}:
        return order, None

    key = await conn.fetchrow(
        """
        select *
          from product_keys
         where product_id = $1
           and duration_days = $2
           and status = 'available'
         order by created_at, id
         for update skip locked
         limit 1
        """,
        order["product_id"],
        order["duration_days"],
    )
    if not key:
        return order, None

    delivery_text = f"Product key/file/link:\n{key['key_value']}"
    delivered = await conn.fetchrow(
        """
        update orders
           set status = 'delivered',
               delivered_at = now(),
               delivery_text = $2
         where id = $1
     returning *
        """,
        order_id,
        delivery_text,
    )
    updated_key = await conn.fetchrow(
        """
        update product_keys
           set status = 'delivered',
               assigned_order_id = $2,
               assigned_user_id = $3,
               delivered_at = now()
         where id = $1
     returning *
        """,
        key["id"],
        order_id,
        order["user_id"],
    )
    return delivered, updated_key


async def place_wallet_order_locked(
    conn: asyncpg.Connection,
    user_id: int,
    product_id: int,
    duration_days: int,
    coupon_code: str | None,
) -> tuple[asyncpg.Record, Decimal, asyncpg.Record | None]:
    fresh_user = await conn.fetchrow("select id, wallet_balance from users where id = $1 for update", user_id)
    product = await conn.fetchrow(
        """
        select p.id, p.name, p.category_key, p.image_url, p.video_url,
               p.stock_status, p.stock_quantity, c.name as category_name
          from products p
          join categories c on c.key = p.category_key
         where p.id = $1 and p.active = true
         for update of p
        """,
        product_id,
    )
    if not product:
        raise HTTPException(status_code=404, detail="Product not found.")
    if not product["stock_status"] or product["stock_quantity"] == 0:
        raise HTTPException(status_code=400, detail="Product is out of stock.")

    duration = await fetch_product_duration(conn, product_id, duration_days)
    if not duration:
        raise HTTPException(status_code=400, detail="This product duration is not available.")
    subtotal = Decimal(duration["price"])
    coupon, discount = await compute_coupon_discount(conn, coupon_code, subtotal)
    total = subtotal - discount
    if Decimal(fresh_user["wallet_balance"]) < total:
        raise HTTPException(status_code=402, detail="Insufficient wallet balance.")

    updated_user = await conn.fetchrow(
        """
        update users
           set wallet_balance = wallet_balance - $1
         where id = $2
     returning wallet_balance
        """,
        total,
        user_id,
    )
    snapshot = {
        "id": product["id"],
        "name": product["name"],
        "category": product["category_name"],
        "image_url": product["image_url"],
        "video_url": product["video_url"],
        "duration_days": duration_days,
        "duration_price": str(subtotal),
    }
    order = await conn.fetchrow(
        """
        insert into orders (
            invoice_id, user_id, product_id, product_snapshot, duration_days,
            coupon_id, subtotal, discount, total, status
        )
        values ($1, $2, $3, $4::jsonb, $5, $6, $7, $8, $9, 'pending')
        returning *
        """,
        invoice_id(),
        user_id,
        product["id"],
        json.dumps(snapshot),
        duration_days,
        coupon["id"] if coupon else None,
        subtotal,
        discount,
        total,
    )
    await conn.execute(
        """
        insert into wallet_transactions (
            user_id, type, amount, balance_after, reference_type, reference_id, note
        )
        values ($1, 'order_payment', $2, $3, 'order', $4, $5)
        """,
        user_id,
        -total,
        updated_user["wallet_balance"],
        order["id"],
        f"Order {order['invoice_id']}",
    )
    if coupon:
        await conn.execute("update coupons set used_count = used_count + 1 where id = $1", coupon["id"])
    if product["stock_quantity"] is not None:
        await conn.execute(
            "update products set stock_quantity = greatest(stock_quantity - 1, 0) where id = $1",
            product["id"],
        )
    delivered_order, delivered_key = await auto_deliver_order_key_locked(conn, order["id"])
    return delivered_order, total, delivered_key


async def approve_payment_request_locked(
    conn: asyncpg.Connection,
    payment_id: int,
    reviewer_id: int | None,
) -> dict[str, Any]:
    payment = await conn.fetchrow(
        """
        select p.*, pm.method_type
          from payment_requests p
          left join payment_methods pm on pm.id = p.method_id
         where p.id = $1
         for update of p
        """,
        payment_id,
    )
    if not payment:
        raise HTTPException(status_code=404, detail="Payment not found.")
    if payment["status"] != "pending":
        raise HTTPException(status_code=400, detail="Payment already reviewed.")

    updated_user = await conn.fetchrow(
        """
        update users
           set wallet_balance = wallet_balance + $1
         where id = $2
     returning *
        """,
        payment["amount"],
        payment["user_id"],
    )
    reviewed = await conn.fetchrow(
        """
        update payment_requests
           set status = 'approved', reviewed_by = $2, reviewed_at = now()
         where id = $1
     returning *
        """,
        payment_id,
        reviewer_id,
    )
    await conn.execute(
        """
        insert into wallet_transactions (
            user_id, type, amount, balance_after, reference_type, reference_id, note
        )
        values ($1, 'deposit', $2, $3, 'payment', $4, 'Payment approved')
        """,
        payment["user_id"],
        payment["amount"],
        updated_user["wallet_balance"],
        payment_id,
    )

    auto_order = None
    auto_total = None
    auto_key = None
    if payment["checkout_product_id"] and payment["checkout_duration_days"]:
        auto_order, auto_total, auto_key = await place_wallet_order_locked(
            conn,
            payment["user_id"],
            payment["checkout_product_id"],
            payment["checkout_duration_days"],
            payment["checkout_coupon_code"],
        )
        await conn.execute(
            "update payment_requests set auto_order_id = $2 where id = $1",
            payment_id,
            auto_order["id"],
        )

    return {
        "payment": payment,
        "reviewed": reviewed,
        "updated_user": updated_user,
        "auto_order": auto_order,
        "auto_total": auto_total,
        "auto_key": auto_key,
    }


@app.on_event("startup")
async def startup() -> None:
    await connect_db()
    if settings.database_url:
        await run_schema()


@app.on_event("shutdown")
async def shutdown() -> None:
    await close_db()


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.head("/health")
async def health_head() -> Response:
    return Response(status_code=200)


@app.post("/telegram/webhook")
async def telegram_webhook(
    update: dict[str, Any],
    x_telegram_bot_api_secret_token: Annotated[
        str | None,
        Header(alias="X-Telegram-Bot-Api-Secret-Token"),
    ] = None,
) -> dict[str, bool]:
    if settings.telegram_webhook_secret:
        if x_telegram_bot_api_secret_token != settings.telegram_webhook_secret:
            raise HTTPException(status_code=403, detail="Invalid Telegram webhook secret.")

    message = update.get("message") or update.get("edited_message")
    if not message:
        return {"ok": True}

    chat_id = message.get("chat", {}).get("id")
    text = (message.get("text") or "").strip()
    if not chat_id:
        return {"ok": True}

    if text.startswith("/start") or text.startswith("/help"):
        await notifier.send_web_app_button(chat_id)
    else:
        await notifier.send_message(chat_id, "Send /start to open the Mini App.")

    return {"ok": True}


@app.get("/telegram/webhook")
async def telegram_webhook_status() -> dict[str, str]:
    return {"status": "telegram webhook endpoint is ready"}


@app.get("/api/session")
async def session(user: Annotated[asyncpg.Record, Depends(current_user)]) -> dict[str, Any]:
    return {"user": jsonable(user), "is_admin": bool(user["is_admin"])}


@app.get("/api/dashboard")
async def dashboard(user: Annotated[asyncpg.Record, Depends(current_user)]) -> dict[str, Any]:
    async with connection() as conn:
        categories = await fetch_root_categories(conn)
        notices = await fetch_active_notices(conn)
        stats = await conn.fetchrow(
            """
            select
                count(*) filter (where status <> 'cancelled') as total_orders,
                count(*) filter (
                    where status in ('approved', 'delivered')
                      and created_at + (duration_days || ' days')::interval >= now()
                ) as active_subscriptions
              from orders
             where user_id = $1
            """,
            user["id"],
        )
        currencies = await fetch_active_currencies(conn)
        currency = next((item for item in currencies if item["code"] == user["selected_currency"]), None)
        settings_bundle = await fetch_public_settings_bundle(conn)
        support_settings = settings_bundle["support"]
        reseller_settings = settings_bundle["reseller"]
        ai_settings = settings_bundle["assistant"]
        branding = settings_bundle["branding"]
    return {
        "user": jsonable(user),
        "stats": jsonable(stats),
        "categories": categories,
        "products": [],
        "notices": notices,
        "currency": currency,
        "currencies": currencies,
        "support": jsonable(support_settings),
        "reseller": jsonable(reseller_settings),
        "assistant": jsonable({"intro": ai_settings["intro"], "enabled": ai_settings["enabled"]}),
        "branding": jsonable(branding),
    }


@app.get("/api/categories")
async def categories(
    user: Annotated[asyncpg.Record, Depends(current_user)],
    parent: str | None = None,
) -> dict[str, Any]:
    async with connection() as conn:
        rows = await fetch_child_categories(conn, parent)
    return {"categories": rows}


@app.get("/api/payment-methods")
async def payment_methods(user: Annotated[asyncpg.Record, Depends(current_user)]) -> dict[str, Any]:
    async with connection() as conn:
        rows = await fetch_active_payment_methods(conn)
    return {"methods": rows}


@app.get("/api/support-settings")
async def support_settings(user: Annotated[asyncpg.Record, Depends(current_user)]) -> dict[str, Any]:
    async with connection() as conn:
        support = (await fetch_public_settings_bundle(conn))["support"]
    return {"support": jsonable(support)}


@app.get("/api/reseller-settings")
async def reseller_settings(user: Annotated[asyncpg.Record, Depends(current_user)]) -> dict[str, Any]:
    async with connection() as conn:
        reseller = (await fetch_public_settings_bundle(conn))["reseller"]
    return {"reseller": jsonable(reseller)}


@app.post("/api/assistant/chat")
async def assistant_chat(
    data: AssistantChatIn,
    user: Annotated[asyncpg.Record, Depends(current_user)],
) -> dict[str, Any]:
    currency = None
    stats: dict[str, Any] | asyncpg.Record = {"total_orders": 0, "active_subscriptions": 0}
    payments: list[Any] = []
    orders: list[Any] = []
    methods: list[Any] = []
    categories: list[Any] = []
    products: list[Any] = []
    support: dict[str, Any] = {
        "telegram_username": "",
        "telegram_user_id": "",
        "display_name": "Store Support",
        "note": "Tap to open Telegram inbox for help.",
        "enabled": True,
    }
    reseller: dict[str, Any] = {
        "telegram_username": "",
        "telegram_user_id": "",
        "display_name": "Reseller Manager",
        "note": "Apply for reseller pricing through Telegram.",
        "enabled": True,
    }
    ai_settings: dict[str, Any] = {
        "intro": AI_ASSISTANT_SETTING_DEFAULTS["ai_assistant_intro"],
        "custom_knowledge": "",
        "enabled": True,
    }
    async with connection() as conn:
        try:
            currency = await fetch_selected_currency(conn, user["selected_currency"])
        except Exception:
            currency = None
        try:
            row = await conn.fetchrow(
                """
                select
                    count(*) filter (where status <> 'cancelled') as total_orders,
                    count(*) filter (
                        where status in ('approved', 'delivered')
                          and created_at + (duration_days || ' days')::interval >= now()
                    ) as active_subscriptions
                  from orders
                 where user_id = $1
                """,
                user["id"],
            )
            stats = row or stats
        except Exception:
            pass
        try:
            payments = list(await conn.fetch(
                """
                select p.id,
                       p.user_id,
                       p.amount,
                       p.method_id,
                       coalesce(nullif(p.method_name, ''), m.name, 'Payment') as method_name,
                       p.transaction_id,
                       p.screenshot_data,
                       p.status,
                       p.rejection_reason,
                       p.reviewed_at,
                       p.created_at
                  from payment_requests p
                  left join payment_methods m on m.id = p.method_id
                 where p.user_id = $1
                 order by p.created_at desc
                 limit 8
                """,
                user["id"],
            ))
        except Exception:
            payments = []
        try:
            orders = list(await conn.fetch(
                """
                select o.*, coalesce(p.name, 'Product') as product_name
                  from orders o
                  left join products p on p.id = o.product_id
                 where o.user_id = $1
                 order by o.created_at desc
                 limit 8
                """,
                user["id"],
            ))
        except Exception:
            orders = []
        try:
            await ensure_payment_method_runtime_schema(conn)
            methods = await fetch_active_payment_methods(conn)
        except Exception:
            methods = []
        try:
            categories = (await fetch_root_categories(conn))[:8]
        except Exception:
            categories = []
        try:
            products = list(await conn.fetch(
                """
                select id, name, category_key, price_1_day, price_7_days, price_30_days,
                       stock_status, stock_quantity, active, created_at
                  from products
                 where active = true
                 order by created_at desc
                 limit 6
                """
            ))
        except Exception:
            products = []
        try:
            support = await fetch_support_settings(conn)
        except Exception:
            pass
        try:
            reseller = await fetch_reseller_settings(conn)
        except Exception:
            pass
        try:
            ai_settings = await fetch_ai_assistant_settings(conn)
        except Exception:
            pass
    if not ai_settings["enabled"]:
        return {
            "reply": "ACI AI is currently inactive from the admin panel. Please use Support from the Account page.",
            "suggestions": ["Contact support", "Payment pending", "Order status"],
        }
    suggestions = ["How do I add funds?", "Where is my order?", "Daily spin reward", "Contact support"]
    ai_reply = await external_ai_answer(
        data.message.strip(),
        data.language.strip().lower() or "en",
        user=user,
        currency=currency,
        stats=stats,
        payments=list(payments),
        orders=list(orders),
        methods=list(methods),
        categories=list(categories),
        products=list(products),
        support=support,
        reseller=reseller,
        ai_settings=ai_settings,
    )
    if ai_reply:
        return {"reply": ai_reply, "suggestions": suggestions}
    try:
        reply, suggestions = mini_app_assistant_answer(
            data.message.strip(),
            user=user,
            currency=currency,
            stats=stats,
            payments=list(payments),
            orders=list(orders),
            methods=list(methods),
            categories=list(categories),
            products=list(products),
            support=support,
            reseller=reseller,
            ai_settings=ai_settings,
        )
    except Exception:
        reply = (
            "ACI AI is answering in basic mode. To add funds, open Account > Add Funds, select an active payment method, "
            "copy the payment address, then submit the amount, transaction ID, and screenshot. Orders tab shows order history. "
            "History tab shows wallet transactions."
        )
        suggestions = ["Add funds", "Order status", "Contact support", "Daily spin"]
    return {"reply": reply, "suggestions": suggestions}


@app.get("/api/currencies")
async def currencies(user: Annotated[asyncpg.Record, Depends(current_user)]) -> dict[str, Any]:
    async with connection() as conn:
        rows = await fetch_active_currencies(conn)
        selected = next((currency for currency in rows if currency["code"] == user["selected_currency"]), None)
    return {"currencies": rows, "selected": selected}


@app.post("/api/profile/currency")
async def set_currency(
    data: CurrencySelectIn,
    user: Annotated[asyncpg.Record, Depends(current_user)],
) -> dict[str, Any]:
    code = data.code.strip().upper()
    async with connection() as conn:
        currency = await fetch_selected_currency(conn, code)
        if not currency:
            raise HTTPException(status_code=404, detail="Currency not found.")
        updated = await conn.fetchrow(
            "update users set selected_currency = $2 where id = $1 returning *",
            user["id"],
            code,
        )
    return {"user": jsonable(updated), "currency": currency}


@app.post("/api/profile/language")
async def set_language(
    data: LanguageSelectIn,
    user: Annotated[asyncpg.Record, Depends(current_user)],
) -> dict[str, Any]:
    async with connection() as conn:
        await ensure_user_preferences_runtime_schema(conn)
        updated = await conn.fetchrow(
            "update users set selected_language = $2 where id = $1 returning *",
            user["id"],
            data.code,
        )
    return {"user": jsonable(updated), "language": data.code}


@app.get("/api/products")
async def products(
    user: Annotated[asyncpg.Record, Depends(current_user)],
    category: str | None = None,
    search: str | None = None,
) -> dict[str, Any]:
    cache_key = f"products:list:{category or ''}:{(search or '').strip().lower()}"
    cached = cache_get(cache_key)
    if cached is not None:
        return {"products": cached}
    async with connection() as conn:
        rows = await conn.fetch(
            """
            select p.id, p.category_key, p.name, p.description, p.image_url,
                   p.price_1_day, p.price_7_days, p.price_30_days,
                   p.stock_status, p.stock_quantity, p.active, p.created_at,
                   c.name as category_name
              from products p
              join categories c on c.key = p.category_key
             where p.active = true
               and ($1::text is null or p.category_key = $1)
               and ($2::text is null or p.name ilike '%' || $2 || '%')
             order by p.created_at desc
             limit 80
            """,
            category,
            search,
        )
        products_with_prices = await attach_product_durations(conn, rows)
    return {"products": cache_set(cache_key, jsonable(products_with_prices), 15)}


@app.get("/api/products/{product_id}")
async def product_detail(
    product_id: int,
    user: Annotated[asyncpg.Record, Depends(current_user)],
) -> dict[str, Any]:
    cache_key = f"products:detail:{product_id}"
    cached = cache_get(cache_key)
    if cached is not None:
        return {"product": cached}
    async with connection() as conn:
        row = await conn.fetchrow(
            """
            select p.id, p.category_key, p.name, p.description, p.feature_text,
                   p.video_url, p.panel_url, p.image_url,
                   p.price_1_day, p.price_7_days, p.price_30_days,
                   p.stock_status, p.stock_quantity, p.active, p.created_at, p.updated_at,
                   c.name as category_name
              from products p
              join categories c on c.key = p.category_key
             where p.id = $1 and p.active = true
            """,
            product_id,
        )
        products_with_prices = await attach_product_durations(conn, [row] if row else [])
    if not row:
        raise HTTPException(status_code=404, detail="Product not found.")
    return {"product": cache_set(cache_key, jsonable(products_with_prices[0]), 15)}


@app.post("/api/coupons/validate")
async def validate_coupon(
    data: CouponValidateIn,
    user: Annotated[asyncpg.Record, Depends(current_user)],
) -> dict[str, Any]:
    async with connection() as conn:
        duration = await fetch_product_duration(conn, data.product_id, data.duration_days)
        if not duration:
            raise HTTPException(status_code=404, detail="Product duration not found.")
        coupon, discount = await compute_coupon_discount(conn, data.code, Decimal(duration["price"]))
    return {
        "coupon": jsonable(coupon),
        "subtotal": jsonable(duration["price"]),
        "discount": jsonable(discount),
        "total": jsonable(Decimal(duration["price"]) - discount),
    }


@app.post("/api/orders")
async def create_order(
    data: OrderCreateIn,
    user: Annotated[asyncpg.Record, Depends(current_user)],
) -> dict[str, Any]:
    async with connection() as conn:
        async with conn.transaction():
            order, total, delivered_key = await place_wallet_order_locked(
                conn,
                user["id"],
                data.product_id,
                data.duration_days,
                data.coupon_code,
            )

    status_text = "Delivered automatically" if delivered_key else "Pending"
    await notifier.notify_admins(
        f"New order\nInvoice: <b>{order['invoice_id']}</b>\nUser: {user['first_name']} ({user['telegram_id']})\nTotal: {total}\nStatus: {status_text}"
    )
    user_message = f"Order placed\nInvoice: <b>{order['invoice_id']}</b>\nStatus: {status_text}"
    if delivered_key:
        user_message += f"\n\n{order['delivery_text']}"
    await notifier.send_message(user["telegram_id"], user_message)
    return {"order": jsonable(order)}


@app.get("/api/orders")
async def my_orders(user: Annotated[asyncpg.Record, Depends(current_user)]) -> dict[str, Any]:
    async with connection() as conn:
        rows = await conn.fetch(
            """
            select o.*, coalesce(p.name, o.product_snapshot->>'name', 'Product') as product_name, p.image_url
             from orders o
              left join products p on p.id = o.product_id
             where o.user_id = $1
             order by o.created_at desc
             limit 80
            """,
            user["id"],
        )
    return {"orders": jsonable(rows)}


@app.get("/api/wallet/transactions")
async def wallet_transactions(user: Annotated[asyncpg.Record, Depends(current_user)]) -> dict[str, Any]:
    async with connection() as conn:
        rows = await conn.fetch(
            """
            select id, type, amount, balance_after, reference_type, reference_id, note, created_at
              from wallet_transactions
             where user_id = $1
             order by created_at desc
             limit 80
            """,
            user["id"],
        )
    return {"transactions": jsonable(rows)}


@app.post("/api/payments")
async def create_payment(
    data: PaymentRequestIn,
    user: Annotated[asyncpg.Record, Depends(current_user)],
) -> dict[str, Any]:
    async with connection() as conn:
        method = await conn.fetchrow(
            "select id, name, method_type from payment_methods where id = $1 and active = true",
            data.method_id,
        )
        if not method:
            raise HTTPException(status_code=404, detail="Payment method not found.")
        if data.product_id and data.duration_days:
            duration = await fetch_product_duration(conn, data.product_id, data.duration_days)
            if not duration:
                raise HTTPException(status_code=400, detail="This product duration is not available.")
        transaction_id = (data.transaction_id or "").strip() or f"MANUAL-{secrets.token_hex(4).upper()}"
        payment = await conn.fetchrow(
            """
            insert into payment_requests (
                user_id, amount, method_id, method_name, transaction_id, screenshot_data,
                checkout_product_id, checkout_duration_days, checkout_coupon_code, status
            )
            values ($1, $2, $3, $4, $5, $6, $7, $8, $9, 'pending')
            returning *
            """,
            user["id"],
            data.amount,
            method["id"],
            method["name"],
            transaction_id,
            data.screenshot_data,
            data.product_id,
            data.duration_days,
            data.coupon_code,
        )
    await notifier.notify_admins(
        f"Payment pending\nUser: {user['first_name']} ({user['telegram_id']})\nAmount: {data.amount}\nTXID: {transaction_id}"
    )
    await notifier.send_message(
        user["telegram_id"],
        f"Payment request submitted\nAmount: <b>{data.amount}</b>\nStatus: Pending",
    )
    return {"payment": jsonable(payment)}


@app.get("/api/payments")
async def my_payments(user: Annotated[asyncpg.Record, Depends(current_user)]) -> dict[str, Any]:
    async with connection() as conn:
        rows = await conn.fetch(
            """
            select id, amount, method_id, method_name, transaction_id, status,
                   rejection_reason, reviewed_at, checkout_product_id,
                   checkout_duration_days, checkout_coupon_code, auto_order_id, created_at
              from payment_requests
             where user_id = $1
             order by created_at desc
             limit 80
            """,
            user["id"],
        )
    return {"payments": jsonable(rows)}


@app.get("/api/referrals")
async def referrals(user: Annotated[asyncpg.Record, Depends(current_user)]) -> dict[str, Any]:
    async with connection() as conn:
        rows = await conn.fetch(
            """
            select r.id, r.bonus_amount, r.status, r.created_at,
                   u.first_name, u.username, u.photo_url
              from referrals r
              join users u on u.id = r.referred_user_id
             where r.referrer_user_id = $1
             order by r.created_at desc
             limit 80
            """,
            user["id"],
        )
        summary = await conn.fetchrow(
            """
            select count(*) as total_referrals,
                   coalesce(sum(bonus_amount) filter (where status = 'rewarded'), 0) as total_earned,
                   coalesce(sum(bonus_amount) filter (where status <> 'rewarded'), 0) as pending_earned
              from referrals
             where referrer_user_id = $1
            """,
            user["id"],
        )
    return {
        "referral_code": user["referral_code"],
        "referral_link": referral_link_for_user(user),
        "referrals": jsonable(rows),
        "summary": jsonable(summary),
        "bonus_per_referral": jsonable(settings.referral_bonus),
    }


@app.get("/api/spin")
async def spin_info(user: Annotated[asyncpg.Record, Depends(current_user)]) -> dict[str, Any]:
    async with connection() as conn:
        await ensure_spin_runtime_schema(conn)
        prizes = await fetch_spin_prizes(conn)
        history = await conn.fetch(
            """
            select id, prize_id, prize_title, amount, created_at
              from spin_history
             where user_id = $1
             order by created_at desc
             limit 20
            """,
            user["id"],
        )
        lock = await conn.fetchrow(
            """
            select next_spin_at,
                   (next_spin_at is null or next_spin_at <= now()) as can_spin
              from users
             where id = $1
            """,
            user["id"],
        )
    prize_list = []
    for prize in prizes:
        item = dict(prize)
        item["amount"] = jsonable(min(Decimal(str(prize["amount"])), MAX_SPIN_BONUS))
        prize_list.append(item)
    return {
        "prizes": prize_list,
        "history": jsonable(history),
        "spins_left": 1 if lock and lock["can_spin"] else 0,
        "next_spin_at": jsonable(lock["next_spin_at"] if lock else None),
        "max_bonus": jsonable(MAX_SPIN_BONUS),
    }


@app.get("/api/account")
async def account_bundle(user: Annotated[asyncpg.Record, Depends(current_user)]) -> dict[str, Any]:
    async with connection() as conn:
        settings_bundle = await fetch_public_settings_bundle(conn)
        methods = await fetch_active_payment_methods(conn)
        payments = await conn.fetch(
            """
            select id, amount, method_id, method_name, transaction_id, status,
                   rejection_reason, reviewed_at, checkout_product_id,
                   checkout_duration_days, checkout_coupon_code, auto_order_id, created_at
              from payment_requests
             where user_id = $1
             order by created_at desc
             limit 40
            """,
            user["id"],
        )
        transactions = await conn.fetch(
            """
            select id, type, amount, balance_after, reference_type, reference_id, note, created_at
              from wallet_transactions
             where user_id = $1
             order by created_at desc
             limit 60
            """,
            user["id"],
        )
        referral_rows = await conn.fetch(
            """
            select r.id, r.bonus_amount, r.status, r.created_at,
                   u.first_name, u.username, u.photo_url
              from referrals r
              join users u on u.id = r.referred_user_id
             where r.referrer_user_id = $1
             order by r.created_at desc
             limit 60
            """,
            user["id"],
        )
        referral_summary = await conn.fetchrow(
            """
            select count(*) as total_referrals,
                   coalesce(sum(bonus_amount) filter (where status = 'rewarded'), 0) as total_earned,
                   coalesce(sum(bonus_amount) filter (where status <> 'rewarded'), 0) as pending_earned
              from referrals
             where referrer_user_id = $1
            """,
            user["id"],
        )
        await ensure_spin_runtime_schema(conn)
        prizes = await fetch_spin_prizes(conn)
        spin_history = await conn.fetch(
            """
            select id, prize_id, prize_title, amount, created_at
              from spin_history
             where user_id = $1
             order by created_at desc
             limit 12
            """,
            user["id"],
        )
        spin_lock = await conn.fetchrow(
            """
            select next_spin_at,
                   (next_spin_at is null or next_spin_at <= now()) as can_spin
              from users
             where id = $1
            """,
            user["id"],
        )
    prize_list = []
    for prize in prizes:
        item = dict(prize)
        item["amount"] = jsonable(min(Decimal(str(prize["amount"])), MAX_SPIN_BONUS))
        prize_list.append(item)
    return {
        "methods": methods,
        "payments": jsonable(payments),
        "transactions": jsonable(transactions),
        "support": jsonable(settings_bundle["support"]),
        "reseller": jsonable(settings_bundle["reseller"]),
        "referrals": {
            "referral_code": user["referral_code"],
            "referral_link": referral_link_for_user(user),
            "referrals": jsonable(referral_rows),
            "summary": jsonable(referral_summary),
            "bonus_per_referral": jsonable(settings.referral_bonus),
        },
        "spin": {
            "prizes": prize_list,
            "history": jsonable(spin_history),
            "spins_left": 1 if spin_lock and spin_lock["can_spin"] else 0,
            "next_spin_at": jsonable(spin_lock["next_spin_at"] if spin_lock else None),
            "max_bonus": jsonable(MAX_SPIN_BONUS),
        },
    }


@app.post("/api/spin/play")
async def spin_play(user: Annotated[asyncpg.Record, Depends(current_user)]) -> dict[str, Any]:
    async with connection() as conn:
        await ensure_spin_runtime_schema(conn)
        async with conn.transaction():
            locked_user = await conn.fetchrow(
                """
                select wallet_balance, next_spin_at,
                       (next_spin_at is null or next_spin_at <= now()) as can_spin
                  from users
                 where id = $1
                 for update
                """,
                user["id"],
            )
            if locked_user and not locked_user["can_spin"]:
                raise HTTPException(status_code=400, detail=f"Spin locked until {locked_user['next_spin_at']}.")
            prizes = await fetch_spin_prizes(conn)
            if not prizes:
                raise HTTPException(status_code=404, detail="No spin prizes configured.")
            weighted: list[dict[str, Any]] = []
            for prize in prizes:
                weighted.extend([prize] * max(1, int(prize["weight"])))
            prize = random.choice(weighted)
            amount = min(Decimal(str(prize["amount"])), MAX_SPIN_BONUS).quantize(Decimal("0.01"))
            if amount > 0:
                updated_user = await conn.fetchrow(
                    """
                    update users
                       set wallet_balance = wallet_balance + $1,
                           next_spin_at = now() + interval '24 hours'
                     where id = $2
                 returning wallet_balance, next_spin_at
                    """,
                    amount,
                    user["id"],
                )
            else:
                updated_user = await conn.fetchrow(
                    """
                    update users
                       set next_spin_at = now() + interval '24 hours'
                     where id = $1
                 returning wallet_balance, next_spin_at
                    """,
                    user["id"],
                )
            history = await conn.fetchrow(
                """
                insert into spin_history (user_id, prize_id, prize_title, amount)
                values ($1, $2, $3, $4)
                returning *
                """,
                user["id"],
                prize["id"],
                prize["title"],
                amount,
            )
            if amount > 0:
                await conn.execute(
                    """
                    insert into wallet_transactions (
                        user_id, type, amount, balance_after, reference_type, reference_id, note
                    )
                    values ($1, 'spin_bonus', $2, $3, 'spin', $4, $5)
                    """,
                    user["id"],
                    amount,
                    updated_user["wallet_balance"],
                    history["id"],
                    prize["title"],
                )
    prize_payload = jsonable(prize)
    prize_payload["amount"] = jsonable(amount)
    return {
        "prize": prize_payload,
        "history": jsonable(history),
        "balance": jsonable(updated_user["wallet_balance"]),
        "next_spin_at": jsonable(updated_user["next_spin_at"]),
        "cooldown_hours": 24,
    }


@app.post("/api/tickets")
async def create_ticket(
    data: TicketCreateIn,
    user: Annotated[asyncpg.Record, Depends(current_user)],
) -> dict[str, Any]:
    async with connection() as conn:
        async with conn.transaction():
            ticket = await conn.fetchrow(
                """
                insert into support_tickets (user_id, subject, status)
                values ($1, $2, 'open')
                returning *
                """,
                user["id"],
                data.subject,
            )
            await conn.execute(
                """
                insert into support_messages (ticket_id, sender_user_id, is_admin, message)
                values ($1, $2, false, $3)
                """,
                ticket["id"],
                user["id"],
                data.message,
            )
    await notifier.notify_admins(
        f"New support ticket\n#{ticket['id']} - {data.subject}\nUser: {user['first_name']}"
    )
    return {"ticket": jsonable(ticket)}


@app.get("/api/tickets")
async def my_tickets(user: Annotated[asyncpg.Record, Depends(current_user)]) -> dict[str, Any]:
    async with connection() as conn:
        rows = await conn.fetch(
            "select * from support_tickets where user_id = $1 order by updated_at desc",
            user["id"],
        )
        messages = await conn.fetch(
            """
            select m.*
              from support_messages m
              join support_tickets t on t.id = m.ticket_id
             where t.user_id = $1
             order by m.created_at
            """,
            user["id"],
        )
        support = await fetch_support_settings(conn)
    return {"tickets": jsonable(rows), "messages": jsonable(messages), "support": jsonable(support)}


@app.post("/api/tickets/{ticket_id}/messages")
async def ticket_message(
    ticket_id: int,
    data: TicketMessageIn,
    user: Annotated[asyncpg.Record, Depends(current_user)],
) -> dict[str, Any]:
    async with connection() as conn:
        ticket = await conn.fetchrow(
            "select * from support_tickets where id = $1 and user_id = $2",
            ticket_id,
            user["id"],
        )
        if not ticket:
            raise HTTPException(status_code=404, detail="Ticket not found.")
        message = await conn.fetchrow(
            """
            insert into support_messages (ticket_id, sender_user_id, is_admin, message)
            values ($1, $2, false, $3)
            returning *
            """,
            ticket_id,
            user["id"],
            data.message,
        )
        await conn.execute(
            "update support_tickets set status = 'open', updated_at = now() where id = $1",
            ticket_id,
        )
    return {"message": jsonable(message)}


@app.get("/api/admin/dashboard")
async def admin_dashboard(admin: Annotated[asyncpg.Record, Depends(admin_user)]) -> dict[str, Any]:
    async with connection() as conn:
        stats = await conn.fetchrow(
            """
            select
                (select count(*) from users) as total_users,
                (select count(*) from orders) as total_orders,
                (select coalesce(sum(total), 0) from orders where status in ('approved', 'delivered')) as total_sales,
                (select count(*) from payment_requests where status = 'pending') as pending_payments,
                (select count(*) from orders where status in ('approved', 'delivered') and created_at + (duration_days || ' days')::interval >= now()) as active_subscriptions,
                (select coalesce(sum(total), 0) from orders where status in ('approved', 'delivered') and created_at::date = current_date) as todays_sales,
                (select count(*) from referrals) as total_referrals,
                (select coalesce(sum(bonus_amount), 0) from referrals where status = 'rewarded') as total_referral_bonus
            """
        )
        recent_orders = await conn.fetch(
            """
            select o.*, u.first_name, u.username, u.telegram_id
              from orders o
              join users u on u.id = o.user_id
             order by o.created_at desc
             limit 12
            """
        )
    return {"stats": jsonable(stats), "recent_orders": jsonable(recent_orders)}


@app.get("/api/admin/products")
async def admin_products(admin: Annotated[asyncpg.Record, Depends(admin_user)]) -> dict[str, Any]:
    async with connection() as conn:
        await ensure_product_durations_runtime_schema(conn)
        await ensure_product_keys_runtime_schema(conn)
        rows = await conn.fetch(
            """
            select p.*, c.name as category_name,
                   (select count(*) from product_keys k where k.product_id = p.id and k.status = 'available') as available_keys,
                   (select count(*) from product_keys k where k.product_id = p.id and k.status = 'delivered') as delivered_keys,
                   (
                       select coalesce(jsonb_agg(jsonb_build_object(
                           'duration_days', grouped.duration_days,
                           'available', grouped.available,
                           'delivered', grouped.delivered
                       ) order by grouped.duration_days), '[]'::jsonb)
                         from (
                             select duration_days,
                                    count(*) filter (where status = 'available') as available,
                                    count(*) filter (where status = 'delivered') as delivered
                               from product_keys
                              where product_id = p.id
                              group by duration_days
                         ) grouped
                   ) as key_counts
              from products p
              left join categories c on c.key = p.category_key
             order by p.created_at desc
            """
        )
        cats = await conn.fetch("select * from categories order by sort_order")
        products_with_prices = await attach_product_durations(conn, rows)
    return {"products": jsonable(products_with_prices), "categories": jsonable(cats)}


@app.get("/api/admin/product-keys")
async def admin_product_keys(
    admin: Annotated[asyncpg.Record, Depends(admin_user)],
    product_id: int | None = None,
    duration_days: int | None = None,
) -> dict[str, Any]:
    if duration_days is not None and duration_days < 1:
        raise HTTPException(status_code=400, detail="Duration must be at least 1 day.")
    async with connection() as conn:
        await ensure_product_durations_runtime_schema(conn)
        await ensure_product_keys_runtime_schema(conn)
        products = await conn.fetch(
            """
            select p.*, c.name as category_name,
                   (select count(*) from product_keys k where k.product_id = p.id and k.status = 'available') as available_keys,
                   (select count(*) from product_keys k where k.product_id = p.id and k.status = 'delivered') as delivered_keys,
                   (
                       select coalesce(jsonb_agg(jsonb_build_object(
                           'duration_days', grouped.duration_days,
                           'available', grouped.available,
                           'delivered', grouped.delivered
                       ) order by grouped.duration_days), '[]'::jsonb)
                         from (
                             select duration_days,
                                    count(*) filter (where status = 'available') as available,
                                    count(*) filter (where status = 'delivered') as delivered
                               from product_keys
                              where product_id = p.id
                              group by duration_days
                         ) grouped
                   ) as key_counts
              from products p
              left join categories c on c.key = p.category_key
             order by c.sort_order nulls last, p.name
            """
        )
        keys = await conn.fetch(
            """
            select k.*, p.name as product_name, c.name as category_name,
                   o.invoice_id, u.first_name, u.username, u.telegram_id
              from product_keys k
              join products p on p.id = k.product_id
              left join categories c on c.key = p.category_key
              left join orders o on o.id = k.assigned_order_id
              left join users u on u.id = k.assigned_user_id
             where ($1::bigint is null or k.product_id = $1)
               and ($2::int is null or k.duration_days = $2)
             order by k.created_at desc
             limit 500
            """,
            product_id,
            duration_days,
        )
        products_with_prices = await attach_product_durations(conn, products)
    return {"products": jsonable(products_with_prices), "keys": jsonable(keys)}


@app.post("/api/admin/product-keys")
async def admin_upload_product_keys(
    data: ProductKeyUploadIn,
    admin: Annotated[asyncpg.Record, Depends(admin_user)],
) -> dict[str, Any]:
    raw_keys = []
    seen = set()
    for line in data.keys.splitlines():
        key = line.strip()
        if key and key not in seen:
            raw_keys.append(key)
            seen.add(key)
    if not raw_keys:
        raise HTTPException(status_code=400, detail="No keys found.")
    if len(raw_keys) > 1000:
        raise HTTPException(status_code=400, detail="Upload up to 1000 keys at a time.")

    async with connection() as conn:
        await ensure_product_keys_runtime_schema(conn)
        product = await conn.fetchrow("select id from products where id = $1", data.product_id)
        if not product:
            raise HTTPException(status_code=404, detail="Product not found.")
        duration = await fetch_product_duration(conn, data.product_id, data.duration_days)
        if not duration:
            raise HTTPException(status_code=400, detail="This product duration is not available.")
        rows = await conn.fetch(
            """
            insert into product_keys (product_id, duration_days, key_value, uploaded_by)
            select $1, $2, unnest($3::text[]), $4
            returning *
            """,
            data.product_id,
            data.duration_days,
            raw_keys,
            admin["id"],
        )
    return {"inserted": len(rows), "keys": jsonable(rows)}


@app.delete("/api/admin/product-keys/{key_id}")
async def admin_delete_product_key(
    key_id: int,
    admin: Annotated[asyncpg.Record, Depends(admin_user)],
) -> dict[str, str]:
    async with connection() as conn:
        await ensure_product_keys_runtime_schema(conn)
        result = await conn.execute("delete from product_keys where id = $1", key_id)
    if result.endswith("0"):
        raise HTTPException(status_code=404, detail="Key not found.")
    return {"status": "deleted"}


@app.get("/api/admin/categories")
async def admin_categories(admin: Annotated[asyncpg.Record, Depends(admin_user)]) -> dict[str, Any]:
    async with connection() as conn:
        rows = await conn.fetch(
            """
            select c.*, p.name as parent_name
              from categories c
              left join categories p on p.key = c.parent_key
             order by c.parent_key nulls first, c.sort_order, c.name
            """
        )
    return {"categories": jsonable(rows)}


@app.post("/api/admin/categories")
async def admin_create_category(
    data: CategoryIn,
    admin: Annotated[asyncpg.Record, Depends(admin_user)],
) -> dict[str, Any]:
    async with connection() as conn:
        row = await conn.fetchrow(
            """
            insert into categories (key, name, icon, description, parent_key, sort_order, active)
            values (lower($1), $2, $3, $4, nullif($5, ''), $6, $7)
            returning *
            """,
            data.key.strip(),
            data.name,
            data.icon,
            data.description,
            data.parent_key,
            data.sort_order,
            data.active,
        )
    clear_runtime_cache("categories:")
    clear_runtime_cache("products:")
    return {"category": jsonable(row)}


@app.put("/api/admin/categories/{category_key}")
async def admin_update_category(
    category_key: str,
    data: CategoryIn,
    admin: Annotated[asyncpg.Record, Depends(admin_user)],
) -> dict[str, Any]:
    async with connection() as conn:
        row = await conn.fetchrow(
            """
            update categories
               set name = $2,
                   icon = $3,
                   description = $4,
                   parent_key = nullif($5, ''),
                   sort_order = $6,
                   active = $7
             where key = $1
         returning *
            """,
            category_key,
            data.name,
            data.icon,
            data.description,
            data.parent_key,
            data.sort_order,
            data.active,
        )
    clear_runtime_cache("categories:")
    clear_runtime_cache("products:")
    if not row:
        raise HTTPException(status_code=404, detail="Category not found.")
    return {"category": jsonable(row)}


@app.delete("/api/admin/categories/{category_key}")
async def admin_delete_category(
    category_key: str,
    admin: Annotated[asyncpg.Record, Depends(admin_user)],
) -> dict[str, str]:
    async with connection() as conn:
        result = await conn.execute("delete from categories where key = $1", category_key)
    clear_runtime_cache("categories:")
    clear_runtime_cache("products:")
    if result.endswith("0"):
        raise HTTPException(status_code=404, detail="Category not found.")
    return {"status": "deleted"}


@app.post("/api/admin/products")
async def admin_create_product(
    data: ProductIn,
    admin: Annotated[asyncpg.Record, Depends(admin_user)],
) -> dict[str, Any]:
    durations = normalize_product_durations(data)
    price_1_day, price_7_days, price_30_days = legacy_prices_for_product(data, durations)
    async with connection() as conn:
        async with conn.transaction():
            row = await conn.fetchrow(
                """
                insert into products (
                    category_key, name, description, feature_text, video_url, panel_url,
                    image_url, price_1_day, price_7_days, price_30_days,
                    stock_status, stock_quantity, active
                )
                values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
                returning *
                """,
                data.category_key,
                data.name,
                data.description,
                data.feature_text,
                data.video_url,
                data.panel_url,
                data.image_url,
                price_1_day,
                price_7_days,
                price_30_days,
                data.stock_status,
                data.stock_quantity,
                data.active,
            )
            await replace_product_durations(conn, row["id"], durations)
            product = (await attach_product_durations(conn, [row]))[0]
    clear_runtime_cache("products:")
    return {"product": jsonable(product)}


@app.put("/api/admin/products/{product_id}")
async def admin_update_product(
    product_id: int,
    data: ProductIn,
    admin: Annotated[asyncpg.Record, Depends(admin_user)],
) -> dict[str, Any]:
    durations = normalize_product_durations(data)
    price_1_day, price_7_days, price_30_days = legacy_prices_for_product(data, durations)
    async with connection() as conn:
        async with conn.transaction():
            row = await conn.fetchrow(
                """
                update products
                   set category_key = $2,
                       name = $3,
                       description = $4,
                       feature_text = $5,
                       video_url = $6,
                       panel_url = $7,
                       image_url = $8,
                       price_1_day = $9,
                       price_7_days = $10,
                       price_30_days = $11,
                       stock_status = $12,
                       stock_quantity = $13,
                       active = $14,
                       updated_at = now()
                 where id = $1
             returning *
                """,
                product_id,
                data.category_key,
                data.name,
                data.description,
                data.feature_text,
                data.video_url,
                data.panel_url,
                data.image_url,
                price_1_day,
                price_7_days,
                price_30_days,
                data.stock_status,
                data.stock_quantity,
                data.active,
            )
            if row:
                await replace_product_durations(conn, row["id"], durations)
                product = (await attach_product_durations(conn, [row]))[0]
    if not row:
        raise HTTPException(status_code=404, detail="Product not found.")
    clear_runtime_cache("products:")
    return {"product": jsonable(product)}


@app.delete("/api/admin/products/{product_id}")
async def admin_delete_product(
    product_id: int,
    admin: Annotated[asyncpg.Record, Depends(admin_user)],
) -> dict[str, str]:
    async with connection() as conn:
        result = await conn.execute("delete from products where id = $1", product_id)
    clear_runtime_cache("products:")
    if result.endswith("0"):
        raise HTTPException(status_code=404, detail="Product not found.")
    return {"status": "deleted"}


@app.get("/api/admin/coupons")
async def admin_coupons(admin: Annotated[asyncpg.Record, Depends(admin_user)]) -> dict[str, Any]:
    async with connection() as conn:
        rows = await conn.fetch("select * from coupons order by created_at desc")
    return {"coupons": jsonable(rows)}


@app.post("/api/admin/coupons")
async def admin_create_coupon(
    data: CouponIn,
    admin: Annotated[asyncpg.Record, Depends(admin_user)],
) -> dict[str, Any]:
    async with connection() as conn:
        row = await conn.fetchrow(
            """
            insert into coupons (code, discount_type, discount_value, expires_at, active, max_uses)
            values (upper($1), $2, $3, $4, $5, $6)
            returning *
            """,
            data.code.strip(),
            data.discount_type,
            data.discount_value,
            data.expires_at,
            data.active,
            data.max_uses,
        )
    return {"coupon": jsonable(row)}


@app.put("/api/admin/coupons/{coupon_id}")
async def admin_update_coupon(
    coupon_id: int,
    data: CouponIn,
    admin: Annotated[asyncpg.Record, Depends(admin_user)],
) -> dict[str, Any]:
    async with connection() as conn:
        row = await conn.fetchrow(
            """
            update coupons
               set code = upper($2),
                   discount_type = $3,
                   discount_value = $4,
                   expires_at = $5,
                   active = $6,
                   max_uses = $7
             where id = $1
         returning *
            """,
            coupon_id,
            data.code.strip(),
            data.discount_type,
            data.discount_value,
            data.expires_at,
            data.active,
            data.max_uses,
        )
    if not row:
        raise HTTPException(status_code=404, detail="Coupon not found.")
    return {"coupon": jsonable(row)}


@app.delete("/api/admin/coupons/{coupon_id}")
async def admin_delete_coupon(
    coupon_id: int,
    admin: Annotated[asyncpg.Record, Depends(admin_user)],
) -> dict[str, str]:
    async with connection() as conn:
        result = await conn.execute("delete from coupons where id = $1", coupon_id)
    if result.endswith("0"):
        raise HTTPException(status_code=404, detail="Coupon not found.")
    return {"status": "deleted"}


@app.get("/api/admin/payments")
async def admin_payments(
    admin: Annotated[asyncpg.Record, Depends(admin_user)],
    status: str | None = None,
) -> dict[str, Any]:
    async with connection() as conn:
        await ensure_payment_method_runtime_schema(conn)
        rows = await conn.fetch(
            """
            select p.*, u.first_name, u.username, u.telegram_id
              from payment_requests p
              join users u on u.id = p.user_id
             where ($1::text is null or p.status = $1)
             order by p.created_at desc
            """,
            status,
        )
        methods = await conn.fetch("select * from payment_methods order by sort_order, name")
    return {"payments": jsonable(rows), "methods": jsonable(methods)}


@app.get("/api/admin/payment-methods")
async def admin_payment_methods(admin: Annotated[asyncpg.Record, Depends(admin_user)]) -> dict[str, Any]:
    async with connection() as conn:
        await ensure_payment_method_runtime_schema(conn)
        rows = await conn.fetch("select * from payment_methods order by sort_order, name")
    return {"methods": jsonable(rows)}


@app.post("/api/admin/payment-methods")
async def admin_create_payment_method(
    data: PaymentMethodIn,
    admin: Annotated[asyncpg.Record, Depends(admin_user)],
) -> dict[str, Any]:
    async with connection() as conn:
        await ensure_payment_method_runtime_schema(conn)
        row = await conn.fetchrow(
            """
            insert into payment_methods (
                name, instructions, method_type, account_label,
                account_value, logo_url, qr_image_url, active, sort_order
            )
            values ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            returning *
            """,
            data.name,
            data.instructions,
            data.method_type,
            data.account_label,
            data.account_value,
            data.logo_url,
            data.qr_image_url,
            data.active,
            data.sort_order,
        )
    clear_runtime_cache("payment_methods:")
    return {"method": jsonable(row)}


@app.put("/api/admin/payment-methods/{method_id}")
async def admin_update_payment_method(
    method_id: int,
    data: PaymentMethodIn,
    admin: Annotated[asyncpg.Record, Depends(admin_user)],
) -> dict[str, Any]:
    async with connection() as conn:
        await ensure_payment_method_runtime_schema(conn)
        row = await conn.fetchrow(
            """
            update payment_methods
               set name = $2,
                   instructions = $3,
                   method_type = $4,
                   account_label = $5,
                   account_value = $6,
                   logo_url = $7,
                   qr_image_url = $8,
                   active = $9,
                   sort_order = $10
             where id = $1
         returning *
            """,
            method_id,
            data.name,
            data.instructions,
            data.method_type,
            data.account_label,
            data.account_value,
            data.logo_url,
            data.qr_image_url,
            data.active,
            data.sort_order,
        )
    clear_runtime_cache("payment_methods:")
    if not row:
        raise HTTPException(status_code=404, detail="Payment method not found.")
    return {"method": jsonable(row)}


@app.delete("/api/admin/payment-methods/{method_id}")
async def admin_delete_payment_method(
    method_id: int,
    admin: Annotated[asyncpg.Record, Depends(admin_user)],
) -> dict[str, str]:
    async with connection() as conn:
        result = await conn.execute("delete from payment_methods where id = $1", method_id)
    clear_runtime_cache("payment_methods:")
    if result.endswith("0"):
        raise HTTPException(status_code=404, detail="Payment method not found.")
    return {"status": "deleted"}


@app.post("/api/admin/payments/{payment_id}/approve")
async def approve_payment(
    payment_id: int,
    admin: Annotated[asyncpg.Record, Depends(admin_user)],
) -> dict[str, Any]:
    async with connection() as conn:
        async with conn.transaction():
            result = await approve_payment_request_locked(conn, payment_id, admin["id"])
    payment = result["payment"]
    reviewed = result["reviewed"]
    updated_user = result["updated_user"]
    auto_order = result["auto_order"]
    auto_total = result["auto_total"]
    auto_key = result["auto_key"]
    await notifier.send_message(
        updated_user["telegram_id"],
        f"Payment approved\nAmount: <b>{payment['amount']}</b>\nBalance: {updated_user['wallet_balance']}",
    )
    if auto_order:
        await notifier.notify_admins(
            f"Auto order created\nInvoice: <b>{auto_order['invoice_id']}</b>\nPayment: #{payment_id}\nTotal: {auto_total}"
        )
        await notifier.send_message(
            updated_user["telegram_id"],
            f"Order placed automatically\nInvoice: <b>{auto_order['invoice_id']}</b>\nStatus: {'Delivered' if auto_key else 'Pending'}"
            + (f"\n\n{auto_order['delivery_text']}" if auto_key else ""),
        )
    return {"payment": jsonable(reviewed)}


@app.post("/api/admin/payments/{payment_id}/reject")
async def reject_payment(
    payment_id: int,
    data: PaymentReviewIn,
    admin: Annotated[asyncpg.Record, Depends(admin_user)],
) -> dict[str, Any]:
    async with connection() as conn:
        reviewed = await conn.fetchrow(
            """
            update payment_requests
               set status = 'rejected',
                   rejection_reason = $2,
                   reviewed_by = $3,
                   reviewed_at = now()
             where id = $1 and status = 'pending'
         returning *
            """,
            payment_id,
            data.reason or "Rejected by admin",
            admin["id"],
        )
        if not reviewed:
            raise HTTPException(status_code=404, detail="Pending payment not found.")
        target = await conn.fetchrow("select telegram_id from users where id = $1", reviewed["user_id"])
    await notifier.send_message(
        target["telegram_id"],
        f"Payment rejected\nReason: {reviewed['rejection_reason']}",
    )
    return {"payment": jsonable(reviewed)}


@app.delete("/api/admin/payments/{payment_id}")
async def admin_delete_rejected_payment(
    payment_id: int,
    admin: Annotated[asyncpg.Record, Depends(admin_user)],
) -> dict[str, str]:
    async with connection() as conn:
        result = await conn.execute(
            "delete from payment_requests where id = $1 and status = 'rejected'",
            payment_id,
        )
    if result.endswith("0"):
        raise HTTPException(status_code=404, detail="Rejected payment not found.")
    return {"status": "deleted"}


@app.post("/api/auto-payments/confirm")
async def auto_confirm_payment(
    data: AutoPaymentConfirmIn,
    x_auto_payment_secret: Annotated[str | None, Header(alias="X-Auto-Payment-Secret")] = None,
) -> dict[str, Any]:
    if not settings.auto_payment_webhook_secret:
        raise HTTPException(status_code=503, detail="Auto payment webhook is not configured.")
    if x_auto_payment_secret != settings.auto_payment_webhook_secret:
        raise HTTPException(status_code=403, detail="Invalid auto payment secret.")

    async with connection() as conn:
        async with conn.transaction():
            payment_id = data.payment_id
            if not payment_id:
                if not data.transaction_id:
                    raise HTTPException(status_code=400, detail="payment_id or transaction_id is required.")
                payment_id = await conn.fetchval(
                    """
                    select p.id
                      from payment_requests p
                      join payment_methods pm on pm.id = p.method_id
                     where p.status = 'pending'
                       and pm.method_type = 'auto'
                       and p.transaction_id = $1
                       and ($2::numeric is null or p.amount = $2)
                     order by p.created_at
                     limit 1
                    """,
                    data.transaction_id.strip(),
                    data.amount,
                )
                if not payment_id:
                    raise HTTPException(status_code=404, detail="Pending auto payment not found.")
            result = await approve_payment_request_locked(conn, payment_id, None)
            if result["payment"]["method_type"] != "auto":
                raise HTTPException(status_code=400, detail="Payment method is not auto.")

    payment = result["payment"]
    updated_user = result["updated_user"]
    auto_order = result["auto_order"]
    auto_key = result["auto_key"]
    await notifier.send_message(
        updated_user["telegram_id"],
        f"Auto payment approved\nAmount: <b>{payment['amount']}</b>\nBalance: {updated_user['wallet_balance']}",
    )
    if auto_order:
        await notifier.send_message(
            updated_user["telegram_id"],
            f"Order placed automatically\nInvoice: <b>{auto_order['invoice_id']}</b>\nStatus: {'Delivered' if auto_key else 'Pending'}"
            + (f"\n\n{auto_order['delivery_text']}" if auto_key else ""),
        )
    return {"payment": jsonable(result["reviewed"]), "auto_order": jsonable(auto_order)}


@app.get("/api/admin/orders")
async def admin_orders(
    admin: Annotated[asyncpg.Record, Depends(admin_user)],
    status: str | None = None,
) -> dict[str, Any]:
    async with connection() as conn:
        rows = await conn.fetch(
            """
            select o.*,
                   coalesce(p.name, o.product_snapshot->>'name', 'Product') as product_name,
                   u.first_name, u.username, u.telegram_id
              from orders o
              join users u on u.id = o.user_id
              left join products p on p.id = o.product_id
             where ($1::text is null or o.status = $1)
             order by o.created_at desc
            """,
            status,
        )
    return {"orders": jsonable(rows)}


@app.post("/api/admin/orders/{order_id}/approve")
async def approve_order(
    order_id: int,
    data: OrderStatusIn,
    admin: Annotated[asyncpg.Record, Depends(admin_user)],
) -> dict[str, Any]:
    async with connection() as conn:
        order = await conn.fetchrow(
            """
            update orders
               set status = 'approved',
                   approved_at = now(),
                   admin_note = $2
             where id = $1 and status = 'pending'
         returning *
            """,
            order_id,
            data.note,
        )
        if not order:
            raise HTTPException(status_code=404, detail="Pending order not found.")
        target = await conn.fetchrow("select telegram_id from users where id = $1", order["user_id"])
    await notifier.send_message(target["telegram_id"], f"Order approved\nInvoice: <b>{order['invoice_id']}</b>")
    return {"order": jsonable(order)}


@app.post("/api/admin/orders/{order_id}/deliver")
async def deliver_order(
    order_id: int,
    data: OrderStatusIn,
    admin: Annotated[asyncpg.Record, Depends(admin_user)],
) -> dict[str, Any]:
    async with connection() as conn:
        async with conn.transaction():
            if data.delivery_text:
                order = await conn.fetchrow(
                    """
                    update orders
                       set status = 'delivered',
                           delivered_at = now(),
                           delivery_text = $2,
                           admin_note = $3
                     where id = $1 and status in ('pending', 'approved')
                 returning *
                    """,
                    order_id,
                    data.delivery_text,
                    data.note,
                )
                key = None
            else:
                order, key = await auto_deliver_order_key_locked(conn, order_id)
                if order["status"] != "delivered":
                    raise HTTPException(status_code=400, detail=f"No available {order['duration_days']} day key. Enter manual delivery text or upload keys for this duration first.")
                if data.note:
                    order = await conn.fetchrow(
                        "update orders set admin_note = $2 where id = $1 returning *",
                        order_id,
                        data.note,
                    )
            if not order:
                raise HTTPException(status_code=404, detail="Order not found or already closed.")
        target = await conn.fetchrow("select telegram_id from users where id = $1", order["user_id"])
    await notifier.send_message(
        target["telegram_id"],
        f"Order delivered\nInvoice: <b>{order['invoice_id']}</b>\n{order['delivery_text'] or ''}",
    )
    return {"order": jsonable(order)}


@app.post("/api/admin/orders/{order_id}/cancel")
async def cancel_order(
    order_id: int,
    data: OrderStatusIn,
    admin: Annotated[asyncpg.Record, Depends(admin_user)],
) -> dict[str, Any]:
    async with connection() as conn:
        async with conn.transaction():
            order = await conn.fetchrow("select * from orders where id = $1 for update", order_id)
            if not order or order["status"] == "cancelled":
                raise HTTPException(status_code=404, detail="Active order not found.")
            if order["status"] == "delivered":
                raise HTTPException(status_code=400, detail="Delivered order cannot be cancelled.")
            updated_user = await conn.fetchrow(
                """
                update users
                   set wallet_balance = wallet_balance + $1
                 where id = $2
             returning *
                """,
                order["total"],
                order["user_id"],
            )
            cancelled = await conn.fetchrow(
                """
                update orders
                   set status = 'cancelled',
                       cancelled_at = now(),
                       admin_note = $2
                 where id = $1
             returning *
                """,
                order_id,
                data.note,
            )
            await conn.execute(
                """
                insert into wallet_transactions (
                    user_id, type, amount, balance_after, reference_type, reference_id, note
                )
                values ($1, 'refund', $2, $3, 'order', $4, 'Order cancelled refund')
                """,
                order["user_id"],
                order["total"],
                updated_user["wallet_balance"],
                order_id,
            )
    await notifier.send_message(
        updated_user["telegram_id"],
        f"Order cancelled\nInvoice: <b>{order['invoice_id']}</b>\nRefund: {order['total']}",
    )
    return {"order": jsonable(cancelled)}


@app.get("/api/admin/users")
async def admin_users(
    admin: Annotated[asyncpg.Record, Depends(admin_user)],
    search: str | None = Query(default=None, max_length=120),
) -> dict[str, Any]:
    async with connection() as conn:
        rows = await conn.fetch(
            """
            select u.*,
                   (select count(*) from orders o where o.user_id = u.id) as order_count,
                   (select count(*) from payment_requests p where p.user_id = u.id) as payment_count,
                   (select count(*) from referrals r where r.referrer_user_id = u.id) as referral_count,
                   (select coalesce(sum(r.bonus_amount), 0) from referrals r where r.referrer_user_id = u.id and r.status = 'rewarded') as referral_earned,
                   (select coalesce(sum(r.bonus_amount), 0) from referrals r where r.referrer_user_id = u.id and r.status <> 'rewarded') as referral_pending,
                   referred_by.telegram_id as referred_by_telegram_id,
                   referred_by.username as referred_by_username
              from users u
              left join users referred_by on referred_by.id = u.referred_by_user_id
             where $1::text is null
                or u.first_name ilike '%' || $1 || '%'
                or u.username ilike '%' || $1 || '%'
                or u.telegram_id::text ilike '%' || $1 || '%'
             order by u.joined_at desc
             limit 120
            """,
            search,
        )
    return {"users": jsonable(rows)}


@app.post("/api/admin/users/{user_id}/balance")
async def admin_adjust_balance(
    user_id: int,
    data: BalanceAdjustIn,
    admin: Annotated[asyncpg.Record, Depends(admin_user)],
) -> dict[str, Any]:
    async with connection() as conn:
        async with conn.transaction():
            current = await conn.fetchrow("select * from users where id = $1 for update", user_id)
            if not current:
                raise HTTPException(status_code=404, detail="User not found.")
            if Decimal(current["wallet_balance"]) + data.amount < 0:
                raise HTTPException(status_code=400, detail="Balance cannot go below zero.")
            user = await conn.fetchrow(
                """
                update users
                   set wallet_balance = wallet_balance + $1
                 where id = $2
             returning *
                """,
                data.amount,
                user_id,
            )
            await conn.execute(
                """
                insert into wallet_transactions (
                    user_id, type, amount, balance_after, reference_type, reference_id, note
                )
                values ($1, 'admin_adjust', $2, $3, 'admin', $4, $5)
                """,
                user_id,
                data.amount,
                user["wallet_balance"],
                admin["id"],
                data.reason,
            )
    action = "added" if data.amount > 0 else "removed"
    await notifier.send_message(
        user["telegram_id"],
        f"Wallet balance {action}\nAmount: <b>{abs(data.amount)}</b>\nBalance: {user['wallet_balance']}\nReason: {data.reason}",
    )
    return {"user": jsonable(user)}


@app.post("/api/admin/users/{user_id}/ban")
async def admin_ban_user(user_id: int, admin: Annotated[asyncpg.Record, Depends(admin_user)]) -> dict[str, Any]:
    async with connection() as conn:
        user = await conn.fetchrow("update users set is_banned = true where id = $1 returning *", user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found.")
    return {"user": jsonable(user)}


@app.post("/api/admin/users/{user_id}/unban")
async def admin_unban_user(user_id: int, admin: Annotated[asyncpg.Record, Depends(admin_user)]) -> dict[str, Any]:
    async with connection() as conn:
        user = await conn.fetchrow("update users set is_banned = false where id = $1 returning *", user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found.")
    return {"user": jsonable(user)}


@app.get("/api/admin/support-settings")
async def admin_support_settings(admin: Annotated[asyncpg.Record, Depends(admin_user)]) -> dict[str, Any]:
    async with connection() as conn:
        support = await fetch_support_settings(conn)
        reseller = await fetch_reseller_settings(conn)
        assistant = await fetch_ai_assistant_settings(conn)
        branding = await fetch_branding_settings(conn)
    return {
        "support": jsonable(support),
        "reseller": jsonable(reseller),
        "assistant": jsonable(assistant),
        "branding": jsonable(branding),
    }


@app.post("/api/admin/support-settings")
async def admin_update_support_settings(
    data: SupportSettingsIn,
    admin: Annotated[asyncpg.Record, Depends(admin_user)],
) -> dict[str, Any]:
    payload = {
        "support_display_name": data.display_name.strip(),
        "support_telegram_username": normalize_telegram_username(data.telegram_username),
        "support_telegram_user_id": data.telegram_user_id.strip(),
        "support_note": data.note.strip(),
        "support_enabled": "true" if data.enabled else "false",
    }
    async with connection() as conn:
        async with conn.transaction():
            await save_app_settings(conn, payload)
            support = await fetch_support_settings(conn)
            reseller = await fetch_reseller_settings(conn)
            assistant = await fetch_ai_assistant_settings(conn)
            branding = await fetch_branding_settings(conn)
    return {"support": jsonable(support), "reseller": jsonable(reseller), "assistant": jsonable(assistant), "branding": jsonable(branding)}


@app.post("/api/admin/branding-settings")
async def admin_update_branding_settings(
    data: BrandingSettingsIn,
    admin: Annotated[asyncpg.Record, Depends(admin_user)],
) -> dict[str, Any]:
    async with connection() as conn:
        async with conn.transaction():
            await save_app_settings(conn, {"app_logo_url": data.logo_url.strip()})
            support = await fetch_support_settings(conn)
            reseller = await fetch_reseller_settings(conn)
            assistant = await fetch_ai_assistant_settings(conn)
            branding = await fetch_branding_settings(conn)
    return {
        "support": jsonable(support),
        "reseller": jsonable(reseller),
        "assistant": jsonable(assistant),
        "branding": jsonable(branding),
    }


@app.post("/api/admin/reseller-settings")
async def admin_update_reseller_settings(
    data: SupportSettingsIn,
    admin: Annotated[asyncpg.Record, Depends(admin_user)],
) -> dict[str, Any]:
    payload = {
        "reseller_display_name": data.display_name.strip(),
        "reseller_telegram_username": normalize_telegram_username(data.telegram_username),
        "reseller_telegram_user_id": data.telegram_user_id.strip(),
        "reseller_note": data.note.strip(),
        "reseller_enabled": "true" if data.enabled else "false",
    }
    async with connection() as conn:
        async with conn.transaction():
            await save_app_settings(conn, payload)
            support = await fetch_support_settings(conn)
            reseller = await fetch_reseller_settings(conn)
            assistant = await fetch_ai_assistant_settings(conn)
            branding = await fetch_branding_settings(conn)
    return {"support": jsonable(support), "reseller": jsonable(reseller), "assistant": jsonable(assistant), "branding": jsonable(branding)}


@app.post("/api/admin/assistant-settings")
async def admin_update_assistant_settings(
    data: AiAssistantSettingsIn,
    admin: Annotated[asyncpg.Record, Depends(admin_user)],
) -> dict[str, Any]:
    payload = {
        "ai_assistant_intro": data.intro.strip(),
        "ai_assistant_custom_knowledge": data.custom_knowledge.strip(),
        "ai_assistant_enabled": "true" if data.enabled else "false",
    }
    async with connection() as conn:
        async with conn.transaction():
            await save_app_settings(conn, payload)
            support = await fetch_support_settings(conn)
            reseller = await fetch_reseller_settings(conn)
            assistant = await fetch_ai_assistant_settings(conn)
            branding = await fetch_branding_settings(conn)
    return {"support": jsonable(support), "reseller": jsonable(reseller), "assistant": jsonable(assistant), "branding": jsonable(branding)}


@app.get("/api/admin/tickets")
async def admin_tickets(admin: Annotated[asyncpg.Record, Depends(admin_user)]) -> dict[str, Any]:
    async with connection() as conn:
        tickets = await conn.fetch(
            """
            select t.*, u.first_name, u.username, u.telegram_id
              from support_tickets t
              join users u on u.id = t.user_id
             order by t.updated_at desc
            """
        )
        messages = await conn.fetch("select * from support_messages order by created_at")
    return {"tickets": jsonable(tickets), "messages": jsonable(messages)}


@app.post("/api/admin/tickets/{ticket_id}/messages")
async def admin_ticket_message(
    ticket_id: int,
    data: TicketMessageIn,
    admin: Annotated[asyncpg.Record, Depends(admin_user)],
) -> dict[str, Any]:
    async with connection() as conn:
        ticket = await conn.fetchrow("select * from support_tickets where id = $1", ticket_id)
        if not ticket:
            raise HTTPException(status_code=404, detail="Ticket not found.")
        message = await conn.fetchrow(
            """
            insert into support_messages (ticket_id, sender_user_id, is_admin, message)
            values ($1, $2, true, $3)
            returning *
            """,
            ticket_id,
            admin["id"],
            data.message,
        )
        await conn.execute(
            "update support_tickets set status = 'replied', updated_at = now() where id = $1",
            ticket_id,
        )
        target = await conn.fetchrow("select telegram_id from users where id = $1", ticket["user_id"])
    await notifier.send_message(target["telegram_id"], f"Support reply\nTicket #{ticket_id}: {data.message}")
    return {"message": jsonable(message)}


@app.post("/api/admin/broadcast")
async def admin_broadcast(
    data: BroadcastIn,
    admin: Annotated[asyncpg.Record, Depends(admin_user)],
) -> dict[str, Any]:
    async with connection() as conn:
        if data.target == "user" and data.user_id:
            recipients = await conn.fetch(
                "select telegram_id from users where id = $1 and is_banned = false",
                data.user_id,
            )
            target_user_id = data.user_id
        else:
            recipients = await conn.fetch("select telegram_id from users where is_banned = false")
            target_user_id = None
        row = await conn.fetchrow(
            """
            insert into broadcasts (admin_user_id, target_type, target_user_id, message)
            values ($1, $2, $3, $4)
            returning *
            """,
            admin["id"],
            data.target,
            target_user_id,
            data.message,
        )
        if data.notice_title:
            await conn.execute(
                """
                insert into notices (title, body, active, starts_at)
                values ($1, $2, true, now())
                """,
                data.notice_title,
                data.message,
            )
            clear_runtime_cache("notices:")
    sent = 0
    for recipient in recipients:
        await notifier.send_message(recipient["telegram_id"], data.message)
        sent += 1
    return {"broadcast": jsonable(row), "sent": sent}


@app.get("/")
async def index() -> FileResponse:
    return FileResponse(BASE_DIR / "app" / "templates" / "index.html")


@app.head("/")
async def index_head() -> Response:
    return Response(status_code=200)


@app.get("/{path:path}")
async def spa_fallback(path: str) -> FileResponse:
    return FileResponse(BASE_DIR / "app" / "templates" / "index.html")


@app.head("/{path:path}")
async def spa_fallback_head(path: str) -> Response:
    return Response(status_code=200)
