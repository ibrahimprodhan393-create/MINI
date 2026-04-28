# Telegram Mini Shop

Telegram Mini App for selling digital products with wallet balance, payment approval, orders, referrals, coupons, support tickets, Telegram notifications, and an in-app admin panel.

## Features

- Telegram auto-login with verified `Telegram.WebApp.initData`
- User profile sync: Telegram ID, name, username, photo, join date
- Home dashboard: wallet, orders, subscriptions, notices, categories, products
- Product categories: Android, iPhone, PC, Root Device, Premium Tools, Subscription Plans
- Nested sections/sub-sections controlled from admin
- Product details: image, name, feature panel, video/YouTube embed, panel link, stock, 1/7/30 day prices, coupon, buy
- Product key store: admins upload/delete 1 Day, 7 Days, and 30 Days keys, file links, or panel login lines per product
- Wallet: add balance request, payment method, transaction ID, screenshot upload
- Account screen: add fund, payment details, payment request status, daily spin, language, referral, support, profile, currency, reseller apply
- Account screen uses compact action rows for Add Fund, AI Assistant, Daily Spin, Language, Referral, Promo Code, Support, Profile/Currency, and Reseller
- AI Assistant chat answers Mini App questions about wallet, payment, orders, products, spin, referral, support, reseller, currency, language, and admin usage
- History screen contains only wallet/transaction history, and Orders screen contains only order history
- Admin users: add balance or remove balance from any user
- Checkout payment flow: Wallet Pay auto-deducts; manual payment approval can auto-create the order
- Manual payments stay admin-approved, and auto payments can be confirmed by a secure webhook
- Rejected payments can be removed from the admin panel
- Automatic delivery: if a stored key is available, paid orders are delivered instantly
- Automatic delivery picks the stored key from the matching 1/7/30 day bucket
- Admin payment method editor: name, instructions, account details, QR image, active/off
- Orders: invoice ID, pending, approved, delivered, cancelled, refund on cancel
- Referral link and bonus wallet credit for new user joins
- Coupons: percent or fixed discount, expiry, max usage
- Lucky spin with max 0.50 wallet bonus and one spin every 24 hours per user
- Language selector in account: English, Bangla, Hindi, Urdu, Arabic, Indonesian, Malay, Nepali, Filipino, Russian, Thai, Turkish
- Multi-currency selector in profile: AED, BDT, EUR, GBP, IDR, INR, MYR, NPR, PHP, PKR, RUB, SAR, THB, TRY, USD
- Support page with admin-configurable Telegram inbox button and support tickets with admin replies
- Admin-configurable reseller Telegram contact for the Apply for Reseller button
- Telegram notifications for payments, orders, delivery, support
- Admin dashboard, sections, products, key store, orders, payments, users, coupons, supporter/reseller/AI settings, tickets, broadcast

## Deploy To Neon And Render

1. Create a Neon project and copy the pooled connection string. It should look like:

   ```text
   postgresql://user:password@ep-example-pooler.region.aws.neon.tech/dbname?sslmode=require
   ```

2. Create a Telegram bot in BotFather and copy the bot token.

3. Create a Telegram Mini App in BotFather, then set the Web App URL to your Render URL after the first deploy.

4. Deploy this folder to Render as a Python web service.

   Build command:

   ```bash
   pip install -r requirements.txt
   ```

   Start command:

   ```bash
   uvicorn app.main:app --host 0.0.0.0 --port $PORT
   ```

5. Add Render environment variables from `.env.example`:

   ```text
   PYTHON_VERSION=3.12.4
   DATABASE_URL
   BOT_TOKEN
   BOT_USERNAME
   MINI_APP_SHORT_NAME
   TELEGRAM_WEBHOOK_SECRET
   AUTO_PAYMENT_WEBHOOK_SECRET
   ADMIN_TELEGRAM_IDS
   PUBLIC_APP_URL
   AUTO_MIGRATE=true
   DEBUG=false
   ```

6. Open the Render app once. With `AUTO_MIGRATE=true`, the app creates the Neon tables from `db/schema.sql`.

7. Put the final Render URL in BotFather as the Mini App URL.

8. Connect Telegram messages to Render by setting the webhook:

   ```text
   https://api.telegram.org/botYOUR_BOT_TOKEN/setWebhook?url=https://your-render-service.onrender.com/telegram/webhook
   ```

   Check it:

   ```text
   https://api.telegram.org/botYOUR_BOT_TOKEN/getWebhookInfo
   ```

   After this, `/start` in Telegram sends an **Open Store** Mini App button.

## Admin Access

Set `ADMIN_TELEGRAM_IDS` to your numeric Telegram user ID, for example:

```text
ADMIN_TELEGRAM_IDS=123456789,987654321
```

When that Telegram user opens the Mini App, the backend marks them as admin automatically.

## Local Development

Create `.env` from `.env.example`, set `DEBUG=true`, then run:

```bash
pip install -r requirements.txt
uvicorn app.main:app --reload
```

In production, users must open the app from Telegram because the server verifies `initData`.

## Database

The full schema is in `db/schema.sql`. You can run it manually in the Neon SQL editor, or let the app run it on startup with:

```text
AUTO_MIGRATE=true
```

## Keeping Render Awake

Render Free web services spin down after 15 minutes without incoming traffic. For 24-hour availability, use a paid Render instance or set an external monitor to ping:

```text
https://your-render-service.onrender.com/health
```

Use `GET` if UptimeRobot asks for a method. The app also accepts `HEAD` on `/health` for monitors that use HEAD checks.

## Auto Payment Webhook

Set `AUTO_PAYMENT_WEBHOOK_SECRET`, then your payment automation can approve a pending auto payment:

```http
POST /api/auto-payments/confirm
X-Auto-Payment-Secret: your-secret

{
  "payment_id": 123
}
```

You can also match by transaction ID and amount:

```json
{
  "transaction_id": "TX123",
  "amount": 10
}
```
