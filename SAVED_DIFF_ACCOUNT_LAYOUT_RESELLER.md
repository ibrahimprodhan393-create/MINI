# Saved Diff: Account Layout And Reseller Update

Date: 2026-04-28

This file saves the latest change set in plain text because Git is not available in this environment.

## Changed Files

- `app/main.py`
- `app/static/app.js`
- `app/static/styles.css`
- `db/schema.sql`
- `db/NEON_RUN_INSTRUCTIONS.md`
- `README.md`

## User App Changes

- Bottom navigation now has four main tabs: Shop, Orders, History, Account.
- History now shows only wallet/transaction history.
- Orders remains only order history.
- Account now uses compact action rows and contains:
  - Wallet balance
  - Add Fund payment method cards
  - Payment address/details copy button
  - Amount and screenshot submit form
  - Payment method details
  - Payment request status/history
  - AI Assistant
  - Daily Spin button
  - Language selector
  - Referral and Promo Code buttons
  - Support Telegram inbox and ticket access
  - Profile details
  - Currency selector
  - Apply for Reseller button

## Admin Changes

- Supporter settings still control the support Telegram inbox.
- New reseller settings were added in the same admin Supporter tab:
  - Display name
  - Telegram username
  - Telegram numeric user ID
  - Note
  - Active/inactive status
- Users pressing Apply for Reseller open the reseller Telegram contact configured by admin.
- New AI Assistant settings were added in the same admin Supporter tab:
  - Intro message
  - Custom knowledge
  - Active/inactive status

## AI Assistant

- Added a new AI Assistant section from the Account screen.
- It answers questions about wallet, payment, orders, delivery, product sections, lucky spin, referral, coupon, support, reseller, language/currency, and admin usage.
- It uses live app data and admin custom knowledge, without requiring an external AI API key.
- Assistant suggestion buttons now send the question directly.
- Assistant errors are shown inside the chat instead of silently failing.

## Database Changes

- Added `users.selected_language` with default `en`.
- Added reseller keys in `app_settings`:
  - `reseller_display_name`
  - `reseller_telegram_username`
  - `reseller_telegram_user_id`
  - `reseller_note`
  - `reseller_enabled`
- Added AI Assistant keys in `app_settings`:
  - `ai_assistant_intro`
  - `ai_assistant_custom_knowledge`
  - `ai_assistant_enabled`

## New API Endpoints

- `GET /api/reseller-settings`
- `POST /api/assistant/chat`
- `POST /api/profile/language`
- `POST /api/admin/reseller-settings`
- `POST /api/admin/assistant-settings`

## Uptime Monitor Fix

- Added `HEAD /` and `HEAD /{path}` responses so root URL monitors do not produce `405 Method Not Allowed`.

## Render Start Command

```bash
uvicorn app.main:app --host 0.0.0.0 --port $PORT
```

## Neon Note

Run the latest `db/schema.sql` in Neon SQL Editor, or keep `AUTO_MIGRATE=true` on Render startup.
