# Neon Database Setup

Run `schema.sql` in the Neon SQL Editor.

Steps:

1. Open your Neon project.
2. Go to **SQL Editor**.
3. Open `schema.sql`.
4. Paste the full SQL into the editor.
5. Click **Run**.

The schema creates all tables and starter data for:

- users and Telegram auto-login profile storage
- categories and products
- nested sections/sub-sections
- wallet transactions and payment requests
- editable payment methods
- orders and invoice IDs
- referrals and bonuses
- coupons
- lucky spin prizes and history
- selectable display currencies
- admin-configurable Telegram support inbox
- support tickets and replies
- notices and broadcasts

After the SQL runs, set your Render `DATABASE_URL` to the Neon pooled connection string.
