from __future__ import annotations

import json
import random
import secrets
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Annotated, Any

import asyncpg
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
    transaction_id: str = Field(min_length=2, max_length=120)
    screenshot_data: str | None = None
    product_id: int | None = None
    duration_days: int | None = None
    coupon_code: str | None = None

    @field_validator("duration_days")
    @classmethod
    def valid_optional_duration(cls, value: int | None) -> int | None:
        if value is not None and value not in {1, 7, 30}:
            raise ValueError("Duration must be 1, 7, or 30 days.")
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
        if value not in {1, 7, 30}:
            raise ValueError("Duration must be 1, 7, or 30 days.")
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


class ProductIn(BaseModel):
    category_key: str
    name: str = Field(min_length=2, max_length=160)
    description: str = ""
    feature_text: str = ""
    video_url: str = ""
    panel_url: str = ""
    image_url: str = ""
    price_1_day: Decimal = Field(ge=0)
    price_7_days: Decimal = Field(ge=0)
    price_30_days: Decimal = Field(ge=0)
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
        if value not in {1, 7, 30}:
            raise ValueError("Duration must be 1, 7, or 30 days.")
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


def price_field(duration_days: int) -> str:
    return {1: "price_1_day", 7: "price_7_days", 30: "price_30_days"}[duration_days]


MAX_SPIN_BONUS = Decimal("0.50")


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


async def fetch_contact_settings(
    conn: asyncpg.Connection,
    defaults: dict[str, str],
    prefix: str,
    fallback_name: str,
) -> dict[str, Any]:
    rows = await conn.fetch(
        "select key, value from app_settings where key = any($1::text[])",
        list(defaults.keys()),
    )
    values = defaults | {row["key"]: row["value"] for row in rows}
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


async def fetch_support_settings(conn: asyncpg.Connection) -> dict[str, Any]:
    return await fetch_contact_settings(conn, SUPPORT_SETTING_DEFAULTS, "support", "Store Support")


async def fetch_reseller_settings(conn: asyncpg.Connection) -> dict[str, Any]:
    return await fetch_contact_settings(conn, RESELLER_SETTING_DEFAULTS, "reseller", "Reseller Manager")


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


async def ensure_user_preferences_runtime_schema(conn: asyncpg.Connection) -> None:
    await conn.execute("alter table users add column if not exists selected_language text not null default 'en'")


async def run_schema() -> None:
    if not settings.auto_migrate or not settings.database_url:
        return
    schema_path = BASE_DIR / "db" / "schema.sql"
    async with connection() as conn:
        await conn.execute(schema_path.read_text(encoding="utf-8"))


async def ensure_spin_runtime_schema(conn: asyncpg.Connection) -> None:
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
        "insert into spin_prizes (title, amount, weight, sort_order) select 'Wallet Bonus', 0.10, 15, 30 where not exists (select 1 from spin_prizes where title = 'Wallet Bonus')"
    )
    await conn.execute(
        "insert into spin_prizes (title, amount, weight, sort_order) select 'Lucky Reward', 0.25, 8, 40 where not exists (select 1 from spin_prizes where title = 'Lucky Reward')"
    )
    await conn.execute(
        "insert into spin_prizes (title, amount, weight, sort_order) select 'Mega Reward', 0.50, 2, 50 where not exists (select 1 from spin_prizes where title = 'Mega Reward')"
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
    await conn.execute("update spin_prizes set amount = 0.10, weight = 15, sort_order = 30 where title = 'Wallet Bonus'")
    await conn.execute("update spin_prizes set amount = 0.25, weight = 8, sort_order = 40 where title = 'Lucky Reward'")
    await conn.execute("update spin_prizes set amount = 0.50, weight = 2, sort_order = 50 where title = 'Mega Reward'")
    await conn.execute("update spin_prizes set amount = least(amount, $1)", MAX_SPIN_BONUS)


async def ensure_product_keys_runtime_schema(conn: asyncpg.Connection) -> None:
    await conn.execute(
        """
        create table if not exists product_keys (
            id bigserial primary key,
            product_id bigint not null references products(id) on delete cascade,
            duration_days int not null default 1 check (duration_days in (1, 7, 30)),
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
    await conn.execute(
        "alter table product_keys add column if not exists duration_days int not null default 1 check (duration_days in (1, 7, 30))"
    )
    await conn.execute(
        "create index if not exists idx_product_keys_product_duration_status on product_keys(product_id, duration_days, status, created_at)"
    )
    await conn.execute(
        "create index if not exists idx_product_keys_product_status on product_keys(product_id, status, created_at)"
    )


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
        return await conn.fetchrow(
            """
            update users
               set first_name = $2,
                   last_name = $3,
                   username = $4,
                   photo_url = $5,
                   is_admin = case when $6 then true else is_admin end,
                   last_seen_at = now()
             where telegram_id = $1
         returning *
            """,
            telegram_id,
            first_name,
            last_name,
            username,
            photo_url,
            is_admin,
        )

    if start_param:
        referrer = await conn.fetchrow(
            "select * from users where referral_code = $1 and telegram_id <> $2",
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
        select *
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
    field = price_field(duration_days)
    fresh_user = await conn.fetchrow("select * from users where id = $1 for update", user_id)
    product = await conn.fetchrow(
        f"""
        select p.*, c.name as category_name, {field} as selected_price
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

    subtotal = Decimal(product["selected_price"])
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
        categories = await conn.fetch(
            """
            select *
              from categories
             where active = true
               and parent_key is null
             order by sort_order, name
            """
        )
        notices = await conn.fetch(
            """
            select *
              from notices
             where active = true
               and (starts_at is null or starts_at <= now())
               and (ends_at is null or ends_at >= now())
             order by created_at desc
             limit 3
            """
        )
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
        currency = await conn.fetchrow(
            "select * from currencies where code = $1 and active = true",
            user["selected_currency"],
        )
        currencies = await conn.fetch(
            "select * from currencies where active = true order by sort_order, code"
        )
        support_settings = await fetch_support_settings(conn)
        reseller_settings = await fetch_reseller_settings(conn)
    return {
        "user": jsonable(user),
        "stats": jsonable(stats),
        "categories": jsonable(categories),
        "products": [],
        "notices": jsonable(notices),
        "currency": jsonable(currency),
        "currencies": jsonable(currencies),
        "support": jsonable(support_settings),
        "reseller": jsonable(reseller_settings),
    }


@app.get("/api/categories")
async def categories(
    user: Annotated[asyncpg.Record, Depends(current_user)],
    parent: str | None = None,
) -> dict[str, Any]:
    async with connection() as conn:
        if parent:
            rows = await conn.fetch(
                """
                select * from categories
                 where active = true and parent_key = $1
                 order by sort_order, name
                """,
                parent,
            )
        else:
            rows = await conn.fetch(
                """
                select * from categories
                 where active = true and parent_key is null
                 order by sort_order, name
                """
            )
    return {"categories": jsonable(rows)}


@app.get("/api/payment-methods")
async def payment_methods(user: Annotated[asyncpg.Record, Depends(current_user)]) -> dict[str, Any]:
    async with connection() as conn:
        rows = await conn.fetch(
            "select * from payment_methods where active = true order by sort_order, name"
        )
    return {"methods": jsonable(rows)}


@app.get("/api/support-settings")
async def support_settings(user: Annotated[asyncpg.Record, Depends(current_user)]) -> dict[str, Any]:
    async with connection() as conn:
        support = await fetch_support_settings(conn)
    return {"support": jsonable(support)}


@app.get("/api/reseller-settings")
async def reseller_settings(user: Annotated[asyncpg.Record, Depends(current_user)]) -> dict[str, Any]:
    async with connection() as conn:
        reseller = await fetch_reseller_settings(conn)
    return {"reseller": jsonable(reseller)}


@app.get("/api/currencies")
async def currencies(user: Annotated[asyncpg.Record, Depends(current_user)]) -> dict[str, Any]:
    async with connection() as conn:
        rows = await conn.fetch("select * from currencies where active = true order by sort_order, code")
        selected = await conn.fetchrow(
            "select * from currencies where code = $1 and active = true",
            user["selected_currency"],
        )
    return {"currencies": jsonable(rows), "selected": jsonable(selected)}


@app.post("/api/profile/currency")
async def set_currency(
    data: CurrencySelectIn,
    user: Annotated[asyncpg.Record, Depends(current_user)],
) -> dict[str, Any]:
    code = data.code.strip().upper()
    async with connection() as conn:
        currency = await conn.fetchrow(
            "select * from currencies where code = $1 and active = true",
            code,
        )
        if not currency:
            raise HTTPException(status_code=404, detail="Currency not found.")
        updated = await conn.fetchrow(
            "update users set selected_currency = $2 where id = $1 returning *",
            user["id"],
            code,
        )
    return {"user": jsonable(updated), "currency": jsonable(currency)}


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
    async with connection() as conn:
        rows = await conn.fetch(
            """
            select p.*, c.name as category_name
              from products p
              join categories c on c.key = p.category_key
             where p.active = true
               and ($1::text is null or p.category_key = $1)
               and ($2::text is null or p.name ilike '%' || $2 || '%')
             order by p.created_at desc
            """,
            category,
            search,
        )
    return {"products": jsonable(rows)}


@app.get("/api/products/{product_id}")
async def product_detail(
    product_id: int,
    user: Annotated[asyncpg.Record, Depends(current_user)],
) -> dict[str, Any]:
    async with connection() as conn:
        row = await conn.fetchrow(
            """
            select p.*, c.name as category_name
              from products p
              join categories c on c.key = p.category_key
             where p.id = $1 and p.active = true
            """,
            product_id,
        )
    if not row:
        raise HTTPException(status_code=404, detail="Product not found.")
    return {"product": jsonable(row)}


@app.post("/api/coupons/validate")
async def validate_coupon(
    data: CouponValidateIn,
    user: Annotated[asyncpg.Record, Depends(current_user)],
) -> dict[str, Any]:
    field = price_field(data.duration_days)
    async with connection() as conn:
        product = await conn.fetchrow(f"select id, {field} as price from products where id = $1", data.product_id)
        if not product:
            raise HTTPException(status_code=404, detail="Product not found.")
        coupon, discount = await compute_coupon_discount(conn, data.code, Decimal(product["price"]))
    return {
        "coupon": jsonable(coupon),
        "subtotal": jsonable(product["price"]),
        "discount": jsonable(discount),
        "total": jsonable(Decimal(product["price"]) - discount),
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
            """,
            user["id"],
        )
    return {"orders": jsonable(rows)}


@app.get("/api/wallet/transactions")
async def wallet_transactions(user: Annotated[asyncpg.Record, Depends(current_user)]) -> dict[str, Any]:
    async with connection() as conn:
        rows = await conn.fetch(
            "select * from wallet_transactions where user_id = $1 order by created_at desc limit 80",
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
            "select * from payment_methods where id = $1 and active = true",
            data.method_id,
        )
        if not method:
            raise HTTPException(status_code=404, detail="Payment method not found.")
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
            data.transaction_id.strip(),
            data.screenshot_data,
            data.product_id,
            data.duration_days,
            data.coupon_code,
        )
    await notifier.notify_admins(
        f"Payment pending\nUser: {user['first_name']} ({user['telegram_id']})\nAmount: {data.amount}\nTXID: {data.transaction_id}"
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
            "select * from payment_requests where user_id = $1 order by created_at desc",
            user["id"],
        )
    return {"payments": jsonable(rows)}


@app.get("/api/referrals")
async def referrals(user: Annotated[asyncpg.Record, Depends(current_user)]) -> dict[str, Any]:
    async with connection() as conn:
        rows = await conn.fetch(
            """
            select r.*, u.first_name, u.username, u.photo_url
              from referrals r
              join users u on u.id = r.referred_user_id
             where r.referrer_user_id = $1
             order by r.created_at desc
            """,
            user["id"],
        )
    if settings.bot_username and settings.mini_app_short_name:
        link = f"https://t.me/{settings.bot_username}/{settings.mini_app_short_name}?startapp={user['referral_code']}"
    else:
        link = user["referral_code"]
    return {"referral_code": user["referral_code"], "referral_link": link, "referrals": jsonable(rows)}


@app.get("/api/spin")
async def spin_info(user: Annotated[asyncpg.Record, Depends(current_user)]) -> dict[str, Any]:
    async with connection() as conn:
        await ensure_spin_runtime_schema(conn)
        prizes = await conn.fetch(
            "select * from spin_prizes where active = true order by sort_order, amount"
        )
        history = await conn.fetch(
            "select * from spin_history where user_id = $1 order by created_at desc limit 20",
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
        item = jsonable(prize)
        item["amount"] = jsonable(min(Decimal(prize["amount"]), MAX_SPIN_BONUS))
        prize_list.append(item)
    return {
        "prizes": prize_list,
        "history": jsonable(history),
        "spins_left": 1 if lock and lock["can_spin"] else 0,
        "next_spin_at": jsonable(lock["next_spin_at"] if lock else None),
        "max_bonus": jsonable(MAX_SPIN_BONUS),
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
            prizes = await conn.fetch("select * from spin_prizes where active = true")
            if not prizes:
                raise HTTPException(status_code=404, detail="No spin prizes configured.")
            weighted: list[asyncpg.Record] = []
            for prize in prizes:
                weighted.extend([prize] * max(1, int(prize["weight"])))
            prize = random.choice(weighted)
            amount = min(Decimal(prize["amount"]), MAX_SPIN_BONUS).quantize(Decimal("0.01"))
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
                (select coalesce(sum(total), 0) from orders where status in ('approved', 'delivered') and created_at::date = current_date) as todays_sales
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
        rows = await conn.fetch(
            """
            select p.*, c.name as category_name,
                   (select count(*) from product_keys k where k.product_id = p.id and k.status = 'available') as available_keys,
                   (select count(*) from product_keys k where k.product_id = p.id and k.status = 'delivered') as delivered_keys,
                   (select count(*) from product_keys k where k.product_id = p.id and k.duration_days = 1 and k.status = 'available') as available_1_day_keys,
                   (select count(*) from product_keys k where k.product_id = p.id and k.duration_days = 7 and k.status = 'available') as available_7_day_keys,
                   (select count(*) from product_keys k where k.product_id = p.id and k.duration_days = 30 and k.status = 'available') as available_30_day_keys
              from products p
              left join categories c on c.key = p.category_key
             order by p.created_at desc
            """
        )
        cats = await conn.fetch("select * from categories order by sort_order")
    return {"products": jsonable(rows), "categories": jsonable(cats)}


@app.get("/api/admin/product-keys")
async def admin_product_keys(
    admin: Annotated[asyncpg.Record, Depends(admin_user)],
    product_id: int | None = None,
    duration_days: int | None = None,
) -> dict[str, Any]:
    if duration_days is not None and duration_days not in {1, 7, 30}:
        raise HTTPException(status_code=400, detail="Duration must be 1, 7, or 30 days.")
    async with connection() as conn:
        await ensure_product_keys_runtime_schema(conn)
        products = await conn.fetch(
            """
            select p.id, p.name, c.name as category_name,
                   (select count(*) from product_keys k where k.product_id = p.id and k.status = 'available') as available_keys,
                   (select count(*) from product_keys k where k.product_id = p.id and k.status = 'delivered') as delivered_keys,
                   (select count(*) from product_keys k where k.product_id = p.id and k.duration_days = 1 and k.status = 'available') as available_1_day_keys,
                   (select count(*) from product_keys k where k.product_id = p.id and k.duration_days = 7 and k.status = 'available') as available_7_day_keys,
                   (select count(*) from product_keys k where k.product_id = p.id and k.duration_days = 30 and k.status = 'available') as available_30_day_keys
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
    return {"products": jsonable(products), "keys": jsonable(keys)}


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
    if result.endswith("0"):
        raise HTTPException(status_code=404, detail="Category not found.")
    return {"status": "deleted"}


@app.post("/api/admin/products")
async def admin_create_product(
    data: ProductIn,
    admin: Annotated[asyncpg.Record, Depends(admin_user)],
) -> dict[str, Any]:
    async with connection() as conn:
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
            data.price_1_day,
            data.price_7_days,
            data.price_30_days,
            data.stock_status,
            data.stock_quantity,
            data.active,
        )
    return {"product": jsonable(row)}


@app.put("/api/admin/products/{product_id}")
async def admin_update_product(
    product_id: int,
    data: ProductIn,
    admin: Annotated[asyncpg.Record, Depends(admin_user)],
) -> dict[str, Any]:
    async with connection() as conn:
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
            data.price_1_day,
            data.price_7_days,
            data.price_30_days,
            data.stock_status,
            data.stock_quantity,
            data.active,
        )
    if not row:
        raise HTTPException(status_code=404, detail="Product not found.")
    return {"product": jsonable(row)}


@app.delete("/api/admin/products/{product_id}")
async def admin_delete_product(
    product_id: int,
    admin: Annotated[asyncpg.Record, Depends(admin_user)],
) -> dict[str, str]:
    async with connection() as conn:
        result = await conn.execute("delete from products where id = $1", product_id)
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
        rows = await conn.fetch("select * from payment_methods order by sort_order, name")
    return {"methods": jsonable(rows)}


@app.post("/api/admin/payment-methods")
async def admin_create_payment_method(
    data: PaymentMethodIn,
    admin: Annotated[asyncpg.Record, Depends(admin_user)],
) -> dict[str, Any]:
    async with connection() as conn:
        row = await conn.fetchrow(
            """
            insert into payment_methods (
                name, instructions, method_type, account_label,
                account_value, qr_image_url, active, sort_order
            )
            values ($1, $2, $3, $4, $5, $6, $7, $8)
            returning *
            """,
            data.name,
            data.instructions,
            data.method_type,
            data.account_label,
            data.account_value,
            data.qr_image_url,
            data.active,
            data.sort_order,
        )
    return {"method": jsonable(row)}


@app.put("/api/admin/payment-methods/{method_id}")
async def admin_update_payment_method(
    method_id: int,
    data: PaymentMethodIn,
    admin: Annotated[asyncpg.Record, Depends(admin_user)],
) -> dict[str, Any]:
    async with connection() as conn:
        row = await conn.fetchrow(
            """
            update payment_methods
               set name = $2,
                   instructions = $3,
                   method_type = $4,
                   account_label = $5,
                   account_value = $6,
                   qr_image_url = $7,
                   active = $8,
                   sort_order = $9
             where id = $1
         returning *
            """,
            method_id,
            data.name,
            data.instructions,
            data.method_type,
            data.account_label,
            data.account_value,
            data.qr_image_url,
            data.active,
            data.sort_order,
        )
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
                   (select count(*) from payment_requests p where p.user_id = u.id) as payment_count
              from users u
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
    return {"support": jsonable(support), "reseller": jsonable(reseller)}


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
    return {"support": jsonable(support), "reseller": jsonable(reseller)}


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
    return {"support": jsonable(support), "reseller": jsonable(reseller)}


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
    sent = 0
    for recipient in recipients:
        await notifier.send_message(recipient["telegram_id"], data.message)
        sent += 1
    return {"broadcast": jsonable(row), "sent": sent}


@app.get("/")
async def index() -> FileResponse:
    return FileResponse(BASE_DIR / "app" / "templates" / "index.html")


@app.get("/{path:path}")
async def spa_fallback(path: str) -> FileResponse:
    return FileResponse(BASE_DIR / "app" / "templates" / "index.html")
