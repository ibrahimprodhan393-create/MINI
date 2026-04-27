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
    is_admin boolean not null default false,
    is_banned boolean not null default false,
    referral_code text not null unique,
    referred_by_user_id bigint references users(id)
);

create table if not exists categories (
    key text primary key,
    name text not null,
    icon text not null default '',
    sort_order int not null default 0,
    active boolean not null default true
);

create table if not exists products (
    id bigserial primary key,
    category_key text not null references categories(key),
    name text not null,
    description text not null default '',
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

create table if not exists payment_methods (
    id bigserial primary key,
    name text not null unique,
    instructions text not null default '',
    active boolean not null default true,
    sort_order int not null default 0
);

create table if not exists payment_requests (
    id bigserial primary key,
    user_id bigint not null references users(id) on delete cascade,
    amount numeric(12,2) not null check (amount > 0),
    method_id bigint references payment_methods(id),
    method_name text not null,
    transaction_id text not null,
    screenshot_data text,
    status text not null default 'pending' check (status in ('pending', 'approved', 'rejected')),
    rejection_reason text,
    reviewed_by bigint references users(id),
    reviewed_at timestamptz,
    created_at timestamptz not null default now()
);

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

create index if not exists idx_products_category on products(category_key);
create index if not exists idx_orders_user on orders(user_id, created_at desc);
create index if not exists idx_payment_requests_status on payment_requests(status, created_at desc);
create index if not exists idx_wallet_transactions_user on wallet_transactions(user_id, created_at desc);
create index if not exists idx_support_tickets_user on support_tickets(user_id, updated_at desc);

insert into categories (key, name, icon, sort_order) values
    ('android', 'Android', 'smartphone', 10),
    ('iphone', 'iPhone', 'apple', 20),
    ('pc', 'PC', 'monitor', 30),
    ('root-device', 'Root Device', 'shield', 40),
    ('premium-tools', 'Premium Tools', 'sparkles', 50),
    ('subscriptions', 'Subscription Plans', 'calendar', 60)
on conflict (key) do update set
    name = excluded.name,
    icon = excluded.icon,
    sort_order = excluded.sort_order,
    active = true;

insert into payment_methods (name, instructions, sort_order) values
    ('bKash', 'Send money to your bKash merchant/personal number, then submit transaction ID and screenshot.', 10),
    ('Nagad', 'Send money to your Nagad number, then submit transaction ID and screenshot.', 20),
    ('Rocket', 'Send money to your Rocket number, then submit transaction ID and screenshot.', 30),
    ('USDT', 'Send USDT to your wallet address, then submit transaction hash and screenshot.', 40)
on conflict do nothing;

insert into coupons (code, discount_type, discount_value, active, max_uses) values
    ('WELCOME10', 'percent', 10, true, 500)
on conflict (code) do nothing;

insert into notices (title, body, active, starts_at)
select 'Welcome', 'Add balance, apply coupons, and place orders directly from Telegram.', true, now()
where not exists (select 1 from notices where title = 'Welcome' and body like 'Add balance%');
