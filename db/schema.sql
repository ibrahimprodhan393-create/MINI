create table if not exists users (
    id bigserial primary key,
    telegram_id bigint not null unique,
    first_name text not null default '',
    last_name text not null default '',
    username text not null default '',
    photo_url text not null default '',
    joined_at timestamptz not null default now(),
    last_seen_at timestamptz,
    wallet_balance numeric(12,2) not null default 0,
    selected_currency text not null default 'USD',
    selected_language text not null default 'en',
    next_spin_at timestamptz,
    is_admin boolean not null default false,
    is_banned boolean not null default false,
    referral_code text not null unique,
    referred_by_user_id bigint references users(id)
);

alter table users add column if not exists selected_currency text not null default 'USD';
alter table users add column if not exists selected_language text not null default 'en';
alter table users add column if not exists next_spin_at timestamptz;

create table if not exists currencies (
    code text primary key,
    symbol text not null,
    name text not null,
    rate_from_base numeric(18,6) not null default 1,
    active boolean not null default true,
    sort_order int not null default 0
);

create table if not exists app_settings (
    key text primary key,
    value text not null default '',
    updated_at timestamptz not null default now()
);

create table if not exists categories (
    key text primary key,
    name text not null,
    icon text not null default '',
    description text not null default '',
    parent_key text references categories(key) on delete set null,
    sort_order int not null default 0,
    active boolean not null default true
);

alter table categories add column if not exists description text not null default '';
alter table categories add column if not exists parent_key text references categories(key) on delete set null;

create table if not exists products (
    id bigserial primary key,
    category_key text not null references categories(key),
    name text not null,
    description text not null default '',
    feature_text text not null default '',
    video_url text not null default '',
    panel_url text not null default '',
    image_url text not null default '',
    price_1_day numeric(12,2) not null default 0,
    price_7_days numeric(12,2) not null default 0,
    price_30_days numeric(12,2) not null default 0,
    stock_status boolean not null default true,
    stock_quantity int,
    active boolean not null default true,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

alter table products add column if not exists feature_text text not null default '';
alter table products add column if not exists video_url text not null default '';
alter table products add column if not exists panel_url text not null default '';

create table if not exists payment_methods (
    id bigserial primary key,
    name text not null unique,
    instructions text not null default '',
    method_type text not null default 'manual',
    account_label text not null default '',
    account_value text not null default '',
    qr_image_url text not null default '',
    active boolean not null default true,
    sort_order int not null default 0
);

alter table payment_methods add column if not exists method_type text not null default 'manual';
alter table payment_methods add column if not exists account_label text not null default '';
alter table payment_methods add column if not exists account_value text not null default '';
alter table payment_methods add column if not exists qr_image_url text not null default '';

create table if not exists payment_requests (
    id bigserial primary key,
    user_id bigint not null references users(id) on delete cascade,
    amount numeric(12,2) not null check (amount > 0),
    method_id bigint references payment_methods(id),
    method_name text not null,
    transaction_id text not null,
    screenshot_data text,
    checkout_product_id bigint references products(id) on delete set null,
    checkout_duration_days int check (checkout_duration_days in (1, 7, 30)),
    checkout_coupon_code text,
    auto_order_id bigint,
    status text not null default 'pending' check (status in ('pending', 'approved', 'rejected')),
    rejection_reason text,
    reviewed_by bigint references users(id),
    reviewed_at timestamptz,
    created_at timestamptz not null default now()
);

alter table payment_requests add column if not exists checkout_product_id bigint references products(id) on delete set null;
alter table payment_requests add column if not exists checkout_duration_days int check (checkout_duration_days in (1, 7, 30));
alter table payment_requests add column if not exists checkout_coupon_code text;
alter table payment_requests add column if not exists auto_order_id bigint;

create table if not exists wallet_transactions (
    id bigserial primary key,
    user_id bigint not null references users(id) on delete cascade,
    type text not null,
    amount numeric(12,2) not null,
    balance_after numeric(12,2) not null,
    reference_type text,
    reference_id bigint,
    note text not null default '',
    created_at timestamptz not null default now()
);

create table if not exists coupons (
    id bigserial primary key,
    code text not null unique,
    discount_type text not null check (discount_type in ('percent', 'fixed')),
    discount_value numeric(12,2) not null check (discount_value >= 0),
    expires_at timestamptz,
    active boolean not null default true,
    max_uses int,
    used_count int not null default 0,
    created_at timestamptz not null default now()
);

create table if not exists orders (
    id bigserial primary key,
    invoice_id text not null unique,
    user_id bigint not null references users(id) on delete cascade,
    product_id bigint references products(id) on delete set null,
    product_snapshot jsonb not null default '{}'::jsonb,
    duration_days int not null check (duration_days in (1, 7, 30)),
    coupon_id bigint references coupons(id),
    subtotal numeric(12,2) not null default 0,
    discount numeric(12,2) not null default 0,
    total numeric(12,2) not null default 0,
    status text not null default 'pending' check (status in ('pending', 'approved', 'delivered', 'cancelled')),
    delivery_text text,
    admin_note text,
    created_at timestamptz not null default now(),
    approved_at timestamptz,
    delivered_at timestamptz,
    cancelled_at timestamptz
);

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
);

alter table product_keys add column if not exists duration_days int not null default 1 check (duration_days in (1, 7, 30));

create table if not exists referrals (
    id bigserial primary key,
    referrer_user_id bigint not null references users(id) on delete cascade,
    referred_user_id bigint not null references users(id) on delete cascade,
    bonus_amount numeric(12,2) not null default 0,
    status text not null default 'rewarded',
    created_at timestamptz not null default now(),
    unique(referrer_user_id, referred_user_id)
);

create table if not exists support_tickets (
    id bigserial primary key,
    user_id bigint not null references users(id) on delete cascade,
    subject text not null,
    status text not null default 'open' check (status in ('open', 'replied', 'closed')),
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create table if not exists support_messages (
    id bigserial primary key,
    ticket_id bigint not null references support_tickets(id) on delete cascade,
    sender_user_id bigint references users(id) on delete set null,
    is_admin boolean not null default false,
    message text not null,
    created_at timestamptz not null default now()
);

create table if not exists broadcasts (
    id bigserial primary key,
    admin_user_id bigint references users(id) on delete set null,
    target_type text not null default 'all',
    target_user_id bigint references users(id) on delete set null,
    message text not null,
    created_at timestamptz not null default now()
);

create table if not exists notices (
    id bigserial primary key,
    title text not null,
    body text not null default '',
    active boolean not null default true,
    starts_at timestamptz,
    ends_at timestamptz,
    created_at timestamptz not null default now()
);

create table if not exists spin_prizes (
    id bigserial primary key,
    title text not null,
    amount numeric(12,2) not null default 0,
    weight int not null default 1,
    active boolean not null default true,
    sort_order int not null default 0,
    created_at timestamptz not null default now()
);

create table if not exists spin_history (
    id bigserial primary key,
    user_id bigint not null references users(id) on delete cascade,
    prize_id bigint references spin_prizes(id) on delete set null,
    prize_title text not null,
    amount numeric(12,2) not null default 0,
    created_at timestamptz not null default now()
);

create index if not exists idx_categories_parent on categories(parent_key, sort_order);
create index if not exists idx_products_category on products(category_key);
create index if not exists idx_orders_user on orders(user_id, created_at desc);
create index if not exists idx_product_keys_product_duration_status on product_keys(product_id, duration_days, status, created_at);
create index if not exists idx_product_keys_product_status on product_keys(product_id, status, created_at);
create index if not exists idx_payment_requests_status on payment_requests(status, created_at desc);
create index if not exists idx_wallet_transactions_user on wallet_transactions(user_id, created_at desc);
create index if not exists idx_support_tickets_user on support_tickets(user_id, updated_at desc);

insert into categories (key, name, icon, description, parent_key, sort_order) values
    ('devices', 'Devices', 'home', 'Browse device sections', null, 10),
    ('android', 'Android (non root)', 'smartphone', 'Android panels and tools', 'devices', 10),
    ('iphone', 'iPhone', 'smartphone', 'iOS tools and subscriptions', 'devices', 20),
    ('pc', 'PC', 'monitor', 'PC tools and panels', 'devices', 30),
    ('root-device', 'Root Device Android', 'shield', 'Root device Android panels', 'devices', 40),
    ('premium-tools', 'Premium Tools', 'sparkles', 'Premium tools and utilities', null, 50),
    ('subscriptions', 'Subscription Plans', 'calendar', 'Subscription packages', null, 60)
on conflict (key) do update set
    name = excluded.name,
    icon = excluded.icon,
    description = excluded.description,
    parent_key = excluded.parent_key,
    sort_order = excluded.sort_order,
    active = true;

insert into currencies (code, symbol, name, rate_from_base, sort_order) values
    ('AED', 'AED', 'UAE Dirham', 3.672500, 10),
    ('BDT', '৳', 'Bangladeshi Taka', 117.000000, 20),
    ('EUR', '€', 'Euro', 0.920000, 30),
    ('GBP', '£', 'British Pound', 0.790000, 40),
    ('IDR', 'Rp', 'Indonesian Rupiah', 16200.000000, 50),
    ('INR', '₹', 'Indian Rupee', 83.500000, 60),
    ('MYR', 'RM', 'Malaysian Ringgit', 4.700000, 70),
    ('NPR', 'रू', 'Nepalese Rupee', 133.600000, 80),
    ('PHP', '₱', 'Philippine Peso', 57.500000, 90),
    ('PKR', 'Rs', 'Pakistani Rupee', 278.000000, 100),
    ('RUB', '₽', 'Russian Ruble', 92.000000, 110),
    ('SAR', 'SAR', 'Saudi Riyal ر.س', 3.750000, 120),
    ('THB', '฿', 'Thai Baht', 36.700000, 130),
    ('TRY', '₺', 'Turkish Lira', 32.300000, 140),
    ('USD', '$', 'US Dollar', 1.000000, 150)
on conflict (code) do update set
    symbol = excluded.symbol,
    name = excluded.name,
    rate_from_base = excluded.rate_from_base,
    sort_order = excluded.sort_order,
    active = true;

insert into app_settings (key, value) values
    ('support_display_name', 'Store Support'),
    ('support_telegram_username', ''),
    ('support_telegram_user_id', ''),
    ('support_note', 'Tap to open Telegram inbox for help.'),
    ('support_enabled', 'true'),
    ('reseller_display_name', 'Reseller Manager'),
    ('reseller_telegram_username', ''),
    ('reseller_telegram_user_id', ''),
    ('reseller_note', 'Apply for reseller pricing through Telegram.'),
    ('reseller_enabled', 'true')
on conflict (key) do nothing;

insert into payment_methods (name, instructions, method_type, account_label, account_value, sort_order)
select 'bKash', 'Send money, then submit transaction ID and screenshot.', 'manual', 'Number', '01316743068', 10
where not exists (select 1 from payment_methods where name = 'bKash');
insert into payment_methods (name, instructions, method_type, account_label, account_value, sort_order)
select 'Nagad', 'Send money, then submit transaction ID and screenshot.', 'manual', 'Number', '01700000000', 20
where not exists (select 1 from payment_methods where name = 'Nagad');
insert into payment_methods (name, instructions, method_type, account_label, account_value, sort_order)
select 'Rocket', 'Send money, then submit transaction ID and screenshot.', 'manual', 'Number', '01800000000', 30
where not exists (select 1 from payment_methods where name = 'Rocket');
insert into payment_methods (name, instructions, method_type, account_label, account_value, sort_order)
select 'USDT', 'Send USDT, then submit transaction hash and screenshot.', 'manual', 'TRC20 Address', 'TM1FVE5T2zvRQG...', 40
where not exists (select 1 from payment_methods where name = 'USDT');

insert into coupons (code, discount_type, discount_value, active, max_uses) values
    ('WELCOME10', 'percent', 10, true, 500)
on conflict (code) do nothing;

insert into notices (title, body, active, starts_at)
select 'Welcome', 'Add balance, apply coupons, and place orders directly from Telegram.', true, now()
where not exists (select 1 from notices where title = 'Welcome' and body like 'Add balance%');

insert into spin_prizes (title, amount, weight, sort_order)
select 'Try Again', 0, 50, 10 where not exists (select 1 from spin_prizes where title = 'Try Again');
insert into spin_prizes (title, amount, weight, sort_order)
select 'Small Bonus', 0.05, 25, 20 where not exists (select 1 from spin_prizes where title = 'Small Bonus');
insert into spin_prizes (title, amount, weight, sort_order)
select 'Wallet Bonus', 0.10, 15, 30 where not exists (select 1 from spin_prizes where title = 'Wallet Bonus');
insert into spin_prizes (title, amount, weight, sort_order)
select 'Lucky Reward', 0.25, 8, 40 where not exists (select 1 from spin_prizes where title = 'Lucky Reward');
insert into spin_prizes (title, amount, weight, sort_order)
select 'Mega Reward', 0.50, 2, 50 where not exists (select 1 from spin_prizes where title = 'Mega Reward');

with duplicates as (
    select duplicate.id as duplicate_id, keeper.id as keeper_id
      from spin_prizes duplicate
      join spin_prizes keeper on keeper.title = duplicate.title
     where duplicate.id > keeper.id
)
update spin_history
   set prize_id = duplicates.keeper_id
  from duplicates
 where spin_history.prize_id = duplicates.duplicate_id;

delete from spin_prizes duplicate
using spin_prizes keeper
where duplicate.title = keeper.title
  and duplicate.id > keeper.id;

update spin_prizes set amount = 0, weight = 50, sort_order = 10 where title = 'Try Again';
update spin_prizes set amount = 0.05, weight = 25, sort_order = 20 where title = 'Small Bonus';
update spin_prizes set amount = 0.10, weight = 15, sort_order = 30 where title = 'Wallet Bonus';
update spin_prizes set amount = 0.25, weight = 8, sort_order = 40 where title = 'Lucky Reward';
update spin_prizes set amount = 0.50, weight = 2, sort_order = 50 where title = 'Mega Reward';
update spin_prizes set amount = least(amount, 0.50);
