# Telegram Mini Shop

Telegram Mini App for selling digital products with wallet balance, payment approval, orders, referrals, coupons, support tickets, Telegram notifications, and an in-app admin panel.

## Features

- Telegram auto-login with verified `Telegram.WebApp.initData`
- User profile sync: Telegram ID, name, username, photo, join date
- Home dashboard: wallet, orders, subscriptions, notices, categories, products
- Product categories: Android, iPhone, PC, Root Device, Premium Tools, Subscription Plans
- Product details: image, name, description, stock, 1/7/30 day prices, coupon, buy
- Wallet: add balance request, payment method, transaction ID, screenshot upload
- Admin payment approval: approved deposits update wallet ledger
- Orders: invoice ID, pending, approved, delivered, cancelled, refund on cancel
- Referral link and bonus wallet credit for new user joins
- Coupons: percent or fixed discount, expiry, max usage
- Support tickets with admin replies
- Telegram notifications for payments, orders, delivery, support
- Admin dashboard, products, orders, payments, users, coupons, tickets, broadcast

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
   ADMIN_TELEGRAM_IDS
   PUBLIC_APP_URL
   AUTO_MIGRATE=true
   DEBUG=false
   ```

6. Open the Render app once. With `AUTO_MIGRATE=true`, the app creates the Neon tables from `db/schema.sql`.

7. Put the final Render URL in BotFather as the Mini App URL.

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
