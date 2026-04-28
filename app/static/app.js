const root = document.getElementById("app");
const tg = window.Telegram?.WebApp;

const state = {
  route: "home",
  session: null,
  dashboard: null,
  currency: null,
  currencies: [],
  categories: [],
  products: [],
  categoryKey: null,
  categoryStack: [],
  categoryChildren: [],
  categoryProducts: [],
  product: null,
  selectedDuration: 1,
  checkout: null,
  selectedPaymentMethodId: null,
  methods: [],
  payments: [],
  transactions: [],
  orders: [],
  referrals: null,
  spin: null,
  tickets: [],
  ticketMessages: [],
  supportSettings: null,
  adminTab: "dashboard",
  admin: {},
  editingProduct: null,
  editingCategory: null,
  editingPaymentMethod: null,
  editingCoupon: null,
  keyProductId: null,
};

let mainButtonBound = false;
let mainButtonHandler = null;
let backButtonHandler = null;

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

function money(value) {
  const currency = state.currency || { code: "USD", symbol: "$", rate_from_base: 1 };
  const converted = Number(value || 0) * Number(currency.rate_from_base || 1);
  return `${escapeHtml(currency.symbol)} ${converted.toFixed(2)} ${escapeHtml(currency.code)}`;
}

function textMoney(value) {
  const currency = state.currency || { code: "USD", symbol: "$", rate_from_base: 1 };
  const converted = Number(value || 0) * Number(currency.rate_from_base || 1);
  return `${currency.code} ${converted.toFixed(2)}`;
}

function shortDate(value) {
  if (!value) return "-";
  return new Date(value).toLocaleString([], {
    year: "numeric",
    month: "short",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
  });
}

function icon(name) {
  return `<i data-lucide="${name}"></i>`;
}

function initials(name) {
  const clean = String(name || "U").trim();
  return escapeHtml(clean.slice(0, 1).toUpperCase() || "U");
}

function productImage(url, label, className = "product-image") {
  if (url) {
    return `<img class="${className}" src="${escapeHtml(url)}" alt="${escapeHtml(label)}" />`;
  }
  return `<div class="${className}">${initials(label)}</div>`;
}

function statusBadge(status) {
  const map = {
    approved: "success",
    delivered: "success",
    pending: "warning",
    rejected: "danger",
    cancelled: "danger",
    open: "warning",
    replied: "success",
    closed: "danger",
  };
  return `<span class="badge ${map[status] || ""}">${escapeHtml(status || "active")}</span>`;
}

function priceFor(product, duration) {
  if (!product) return 0;
  if (duration === 7) return product.price_7_days;
  if (duration === 30) return product.price_30_days;
  return product.price_1_day;
}

function videoEmbed(url) {
  if (!url) return "";
  const safe = escapeHtml(url);
  const match = String(url).match(/(?:youtube\.com\/watch\?v=|youtu\.be\/|youtube\.com\/shorts\/)([A-Za-z0-9_-]+)/);
  if (match) {
    return `<iframe class="video-panel" src="https://www.youtube.com/embed/${match[1]}" title="Product video" allowfullscreen></iframe>`;
  }
  return `<video class="video-panel" src="${safe}" controls playsinline></video>`;
}

function supportTelegramUrl(support) {
  const username = String(support?.telegram_username || "").trim().replace(/^@/, "");
  if (username) return `https://t.me/${encodeURIComponent(username)}`;
  const userId = String(support?.telegram_user_id || "").trim();
  if (userId) return `tg://user?id=${encodeURIComponent(userId)}`;
  return "";
}

async function api(path, options = {}) {
  const headers = { Accept: "application/json", ...(options.headers || {}) };
  if (tg?.initData) headers["X-Telegram-Init-Data"] = tg.initData;
  if (options.body && !(options.body instanceof FormData)) {
    headers["Content-Type"] = "application/json";
    options.body = JSON.stringify(options.body);
  }
  const response = await fetch(path, { ...options, headers });
  const contentType = response.headers.get("content-type") || "";
  let payload = {};
  if (contentType.includes("application/json")) {
    payload = await response.json().catch(() => ({}));
  } else {
    const text = await response.text().catch(() => "");
    payload = { detail: text };
  }
  if (!response.ok) {
    const detail = Array.isArray(payload.detail)
      ? payload.detail.map((item) => item.msg || item.message || JSON.stringify(item)).join(", ")
      : payload.detail;
    throw new Error(detail || `Request failed (${response.status})`);
  }
  return payload;
}

function toast(message) {
  const old = document.querySelector(".toast");
  if (old) old.remove();
  const node = document.createElement("div");
  node.className = "toast";
  node.textContent = message;
  document.body.appendChild(node);
  setTimeout(() => node.remove(), 2800);
}

async function fileToDataUrl(file) {
  if (!file || !file.size) return "";
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = () => resolve(reader.result);
    reader.onerror = reject;
    reader.readAsDataURL(file);
  });
}

function userName(user) {
  const full = `${user?.first_name || ""} ${user?.last_name || ""}`.trim();
  return full || user?.username || `User ${user?.telegram_id || ""}`;
}

async function loadDashboard() {
  state.dashboard = await api("/api/dashboard");
  state.currency = state.dashboard.currency || state.currency || { code: "USD", symbol: "$", rate_from_base: 1 };
  state.currencies = state.dashboard.currencies || state.currencies || [];
  state.supportSettings = state.dashboard.support || state.supportSettings;
  state.categories = state.dashboard.categories || [];
  state.products = state.dashboard.products || [];
}

async function loadRouteData(route = state.route) {
  if (route === "home") await loadDashboard();
  if (route === "category" && state.categoryKey) {
    const [children, products] = await Promise.all([
      api(`/api/categories?parent=${encodeURIComponent(state.categoryKey)}`),
      api(`/api/products?category=${encodeURIComponent(state.categoryKey)}`),
    ]);
    state.categoryChildren = children.categories || [];
    state.categoryProducts = products.products || [];
  }
  if (route === "checkout") {
    const methods = await api("/api/payment-methods");
    state.methods = methods.methods || [];
  }
  if (route === "wallet" || route === "add-balance") {
    const [methods, payments, transactions] = await Promise.all([
      api("/api/payment-methods"),
      api("/api/payments"),
      api("/api/wallet/transactions"),
    ]);
    state.methods = methods.methods || [];
    state.payments = payments.payments || [];
    state.transactions = transactions.transactions || [];
  }
  if (route === "orders") {
    const data = await api("/api/orders");
    state.orders = data.orders || [];
  }
  if (route === "referral") {
    state.referrals = await api("/api/referrals");
  }
  if (route === "coupon") {
    const data = await api("/api/products");
    state.products = data.products || [];
  }
  if (route === "spin") {
    state.spin = await api("/api/spin");
  }
  if (route === "support") {
    const data = await api("/api/tickets");
    state.tickets = data.tickets || [];
    state.ticketMessages = data.messages || [];
    state.supportSettings = data.support || state.supportSettings;
  }
  if (route === "admin") {
    await loadAdminData(state.adminTab);
  }
}

async function setRoute(route) {
  state.route = route;
  await loadRouteData(route);
  render();
}

async function openProduct(id) {
  const data = await api(`/api/products/${id}`);
  state.product = data.product;
  state.selectedDuration = 1;
  state.route = "product";
  render();
}

async function openCategory(key, name) {
  state.categoryKey = key || null;
  if (key) {
    if (state.route !== "category") state.categoryStack = [];
    const exists = state.categoryStack.find((item) => item.key === key);
    if (!exists) state.categoryStack.push({ key, name: name || key });
  }
  await setRoute("category");
}

async function categoryBack() {
  if (state.categoryStack.length > 1) {
    state.categoryStack.pop();
    const previous = state.categoryStack[state.categoryStack.length - 1];
    state.categoryKey = previous.key;
    await setRoute("category");
  } else {
    state.categoryStack = [];
    state.categoryKey = null;
    await setRoute("home");
  }
}

async function loadAdminData(tab) {
  state.adminTab = tab;
  if (tab === "dashboard") state.admin.dashboard = await api("/api/admin/dashboard");
  if (tab === "categories") state.admin.categories = await api("/api/admin/categories");
  if (tab === "products") state.admin.products = await api("/api/admin/products");
  if (tab === "keys") {
    const query = state.keyProductId ? `?product_id=${encodeURIComponent(state.keyProductId)}` : "";
    state.admin.keys = await api(`/api/admin/product-keys${query}`);
  }
  if (tab === "payments") state.admin.payments = await api("/api/admin/payments");
  if (tab === "orders") state.admin.orders = await api("/api/admin/orders");
  if (tab === "users") state.admin.users = await api("/api/admin/users");
  if (tab === "tickets") state.admin.tickets = await api("/api/admin/tickets");
  if (tab === "support") state.admin.support = await api("/api/admin/support-settings");
  if (tab === "coupons") state.admin.coupons = await api("/api/admin/coupons");
}

function topbar() {
  const user = state.dashboard?.user || state.session?.user || {};
  const name = userName(user);
  return `
    <header class="topbar">
      <div class="profile-chip">
        <div class="avatar">${user.photo_url ? `<img src="${escapeHtml(user.photo_url)}" alt="${escapeHtml(name)}" />` : initials(name)}</div>
        <div>
          <h2>${escapeHtml(name)}</h2>
          <p>${state.session?.is_admin ? "ADMIN" : "CUSTOMER"}</p>
        </div>
      </div>
      <button class="balance-chip" data-action="set-route" data-route="wallet" aria-label="Wallet balance">
        <span>Balance</span>
        <strong>${money(user.wallet_balance)}</strong>
      </button>
    </header>
  `;
}

function bottomNav() {
  const tabs = [
    ["home", "Shop", "shopping-cart"],
    ["orders", "Orders", "receipt-text"],
    ["wallet", "History", "history"],
    ["profile", "Account", "wallet-cards"],
    ["support", "Support", "bot"],
  ];
  return `
    <nav class="bottom-nav">
      ${tabs.map(([route, label, iconName]) => `
        <button class="tab-btn ${state.route === route ? "active" : ""}" data-action="set-route" data-route="${route}">
          ${icon(iconName)}
          <span>${label}</span>
        </button>
      `).join("")}
    </nav>
  `;
}

function walletCard(user) {
  return `
    <section class="wallet-card">
      <div>
        <small>Wallet balance</small>
        <strong>${money(user?.wallet_balance)}</strong>
      </div>
      <div class="row">
        <small>Telegram ID ${escapeHtml(user?.telegram_id || "")}</small>
        <button class="action-btn secondary" style="width:auto; min-height:38px; padding:0 12px;" data-action="set-route" data-route="add-balance">
          ${icon("plus")} Add
        </button>
      </div>
    </section>
  `;
}

function renderHome() {
  const stats = state.dashboard?.stats || {};
  const notices = state.dashboard?.notices || [];
  return `
    <div class="notice-stack">
      ${notices.map((notice) => `
        <div class="notice">
          <strong>${escapeHtml(notice.title)}</strong>
          <p>${escapeHtml(notice.body)}</p>
        </div>
      `).join("")}
    </div>
    <section class="metric-grid">
      <div class="metric"><span>Total orders</span><strong>${Number(stats.total_orders || 0)}</strong></div>
      <div class="metric"><span>Active subscriptions</span><strong>${Number(stats.active_subscriptions || 0)}</strong></div>
    </section>
    <div class="section-head">
      <h3>Product Category</h3>
    </div>
    <section class="category-grid">
      ${state.categories.map((category) => `
        <button class="category-card" data-action="open-category" data-key="${escapeHtml(category.key)}" data-name="${escapeHtml(category.name)}">
          <span class="inline-icon">${icon(category.icon || "box")}</span>
          <span>
            <strong>${escapeHtml(category.name)}</strong>
            <small>${escapeHtml(category.description || "Tap to browse products")}</small>
          </span>
        </button>
      `).join("")}
    </section>
  `;
}

function renderProductList(products) {
  if (!products?.length) return `<div class="empty">No products found</div>`;
  return `
    <section class="product-list">
      ${products.map((product) => `
        <button class="product-card" data-action="open-product" data-id="${product.id}">
          ${productImage(product.image_url, product.name)}
          <div>
            <h4>${escapeHtml(product.name)}</h4>
            <p>${escapeHtml(product.description || product.category_name || "")}</p>
            <div class="price-row">
              <span class="price">${money(product.price_1_day)}</span>
              ${product.stock_status ? `<span class="badge success">In stock</span>` : `<span class="badge danger">Out</span>`}
            </div>
          </div>
        </button>
      `).join("")}
    </section>
  `;
}

function renderCategory() {
  const category = [...state.categories, ...state.categoryChildren, ...state.categoryStack].find((item) => item.key === state.categoryKey);
  return `
    <div class="section-head">
      <h3>${escapeHtml(category?.name || "Products")}</h3>
      <button data-action="category-back">Back</button>
    </div>
    ${state.categoryChildren.length ? `
      <section class="category-grid">
        ${state.categoryChildren.map((child) => `
          <button class="category-card" data-action="open-category" data-key="${escapeHtml(child.key)}" data-name="${escapeHtml(child.name)}">
            <span class="inline-icon">${icon(child.icon || "box")}</span>
            <span>
              <strong>${escapeHtml(child.name)}</strong>
              <small>${escapeHtml(child.description || "Tap to browse products")}</small>
            </span>
          </button>
        `).join("")}
      </section>
    ` : ""}
    ${renderProductList(state.categoryProducts)}
  `;
}

function renderProductDetail() {
  const product = state.product;
  if (!product) return `<div class="empty">Product not found</div>`;
  const duration = state.selectedDuration;
  return `
    <section class="panel">
      ${productImage(product.image_url, product.name, "detail-hero")}
      <div class="status-row">
        <span class="badge">${escapeHtml(product.category_name || product.category_key)}</span>
        ${product.stock_status ? `<span class="badge success">In stock</span>` : `<span class="badge danger">Out of stock</span>`}
      </div>
      <h1 class="detail-title">${escapeHtml(product.name)}</h1>
      ${product.feature_text ? `<div class="feature-box">${escapeHtml(product.feature_text)}</div>` : ""}
      <p class="description">${escapeHtml(product.description || "")}</p>
      ${videoEmbed(product.video_url)}
      ${product.panel_url ? `<a class="action-btn secondary" href="${escapeHtml(product.panel_url)}" target="_blank" rel="noopener">${icon("external-link")} Open Panel Link</a>` : ""}
      <form class="form-grid" id="buy-form">
        <div class="field">
          <label>Promo code</label>
          <input name="coupon_code" placeholder="WELCOME10" autocomplete="off" />
        </div>
      </form>
    </section>
    <div class="section-head"><h3>Duration</h3></div>
    <section class="duration-list">
      ${[1, 7, 30].map((days) => `
        <article class="duration-card">
          <div>
            <h4>${days === 1 ? "1 Day" : `${days} Days`}</h4>
            <p>${days} days</p>
          </div>
          <strong class="price">${money(priceFor(product, days))}</strong>
          <button class="action-btn" data-action="quick-buy" data-duration="${days}" type="button">${icon("shopping-cart")} Buy</button>
        </article>
      `).join("")}
    </section>
  `;
}

function renderCheckout() {
  const product = state.product || state.checkout?.product;
  const duration = state.selectedDuration;
  if (!product) return `<div class="empty">Checkout item not found</div>`;
  const amount = Number(priceFor(product, duration));
  const user = state.dashboard?.user || state.session?.user || {};
  const wallet = Number(user.wallet_balance || 0);
  const selected = state.methods.find((method) => String(method.id) === String(state.selectedPaymentMethodId));
  return `
    <section class="panel">
      <div class="status-row">
        <h3 style="margin:0;">Select Payment Method</h3>
        <button class="icon-btn" data-action="open-product" data-id="${product.id}" type="button">${icon("x")}</button>
      </div>
      <p class="muted">${escapeHtml(product.name)} - ${duration} Day</p>
      <p class="muted">Price: <strong class="price">${money(amount)}</strong></p>
      <button class="payment-option ${wallet >= amount ? "" : "disabled"}" data-action="wallet-pay" type="button">
        <span class="inline-icon">${icon("wallet")}</span>
        <span><strong>Wallet Pay</strong><small>Balance: ${money(wallet)}</small></span>
        <span class="badge ${wallet >= amount ? "success" : "danger"}">${wallet >= amount ? "Auto" : "Insufficient"}</span>
      </button>
    </section>
    <section class="panel">
      <div class="section-head"><h3>Manual Payment</h3></div>
      <p class="muted">Select a payment method and submit transaction ID. Admin approval will create the order automatically.</p>
      <div class="table-lite">
        ${state.methods.map((method) => `
          <button class="payment-option ${String(method.id) === String(state.selectedPaymentMethodId) ? "active" : ""}" data-action="select-payment-method" data-id="${method.id}" type="button">
            <span class="inline-icon">${icon(method.method_type === "auto" ? "badge-check" : "scan-line")}</span>
            <span>
              <strong>${escapeHtml(method.name)}</strong>
              <small>${escapeHtml(method.account_label || "Details")}: ${escapeHtml(method.account_value || method.instructions || "")}</small>
            </span>
          </button>
        `).join("") || `<div class="empty">No payment methods</div>`}
      </div>
      ${selected ? `
        <form class="form-grid" id="checkout-payment-form" style="margin-top:12px;">
          ${selected.qr_image_url ? `<img class="screenshot" src="${escapeHtml(selected.qr_image_url)}" alt="${escapeHtml(selected.name)} QR" />` : ""}
          <div class="notice"><strong>${escapeHtml(selected.name)}</strong><p>${escapeHtml(selected.instructions || "")}</p></div>
          <div class="field"><label>Transaction ID</label><input name="transaction_id" required autocomplete="off" /></div>
          <div class="field"><label>Screenshot</label><input name="screenshot" type="file" accept="image/*" /></div>
          <button class="action-btn" type="submit">${icon("send")} Submit Payment</button>
        </form>
      ` : ""}
    </section>
  `;
}

function renderWallet() {
  const user = state.dashboard?.user || state.session?.user || {};
  return `
    ${walletCard(user)}
    <div class="section-head"><h3>Add Balance</h3></div>
    ${renderPaymentForm()}
    <div class="section-head"><h3>Payment Requests</h3></div>
    <section class="table-lite">
      ${state.payments.length ? state.payments.map((payment) => `
        <article class="order-card">
          <div class="status-row"><h4>${money(payment.amount)}</h4>${statusBadge(payment.status)}</div>
          <div class="muted">${escapeHtml(payment.method_name)} - ${escapeHtml(payment.transaction_id)}</div>
          <div class="muted">${shortDate(payment.created_at)}</div>
        </article>
      `).join("") : `<div class="empty">No payment requests</div>`}
    </section>
    <div class="section-head"><h3>Wallet History</h3></div>
    <section class="table-lite">
      ${state.transactions.length ? state.transactions.map((tx) => `
        <article class="order-card">
          <div class="status-row"><h4>${escapeHtml(tx.type)}</h4><strong>${money(tx.amount)}</strong></div>
          <div class="muted">${escapeHtml(tx.note)} - Balance ${money(tx.balance_after)}</div>
          <div class="muted">${shortDate(tx.created_at)}</div>
        </article>
      `).join("") : `<div class="empty">No wallet history</div>`}
    </section>
  `;
}

function renderPaymentForm() {
  return `
    <form class="panel form-grid" id="payment-form">
      <div class="field">
        <label>Amount</label>
        <input name="amount" type="number" step="0.01" min="1" required />
      </div>
      <div class="field">
        <label>Payment method</label>
        <select name="method_id" required>
          ${state.methods.map((method) => `<option value="${method.id}">${escapeHtml(method.name)}</option>`).join("")}
        </select>
      </div>
      <div class="field">
        <label>Transaction ID</label>
        <input name="transaction_id" required autocomplete="off" />
      </div>
      <div class="field">
        <label>Screenshot</label>
        <input name="screenshot" type="file" accept="image/*" />
      </div>
      <button class="action-btn" type="submit">${icon("send")} Submit Payment</button>
    </form>
    <section class="table-lite" style="margin-top:10px;">
      ${state.methods.map((method) => `
        <article class="order-card">
          <h4>${escapeHtml(method.name)}</h4>
          <div class="muted">${escapeHtml(method.account_label || "Details")}: ${escapeHtml(method.account_value || "-")}</div>
          <div class="muted">${escapeHtml(method.instructions)}</div>
          ${method.qr_image_url ? `<img class="screenshot" src="${escapeHtml(method.qr_image_url)}" alt="${escapeHtml(method.name)} QR" />` : ""}
        </article>
      `).join("")}
    </section>
  `;
}

function renderOrders() {
  return `
    <div class="section-head"><h3>Order History</h3></div>
    <section class="table-lite">
      ${state.orders.length ? state.orders.map((order) => `
        <article class="order-card">
          <div class="status-row"><h4>${escapeHtml(order.invoice_id)}</h4>${statusBadge(order.status)}</div>
          <div>${escapeHtml(order.product_name || "Product")}</div>
          <div class="muted">${order.duration_days} Day - ${money(order.total)} - ${shortDate(order.created_at)}</div>
          ${order.delivery_text ? `<div class="notice"><strong>Delivery</strong><p>${escapeHtml(order.delivery_text)}</p></div>` : ""}
        </article>
      `).join("") : `<div class="empty">No orders yet</div>`}
    </section>
  `;
}

function renderReferral() {
  const data = state.referrals || {};
  return `
    <div class="section-head"><h3>Referral</h3><button data-action="set-route" data-route="profile">Back</button></div>
    <section class="panel form-grid">
      <div class="field">
        <label>Your referral link</label>
        <input value="${escapeHtml(data.referral_link || data.referral_code || "")}" readonly />
      </div>
      <button class="action-btn" data-action="copy-referral" type="button">${icon("copy")} Copy Link</button>
    </section>
    <div class="section-head"><h3>Referral History</h3></div>
    <section class="table-lite">
      ${(data.referrals || []).length ? data.referrals.map((row) => `
        <article class="order-card">
          <div class="status-row"><h4>${escapeHtml(row.first_name || row.username || "New user")}</h4>${statusBadge(row.status)}</div>
          <div class="muted">Bonus ${money(row.bonus_amount)} - ${shortDate(row.created_at)}</div>
        </article>
      `).join("") : `<div class="empty">No referrals yet</div>`}
    </section>
  `;
}

function renderSpin() {
  const data = state.spin || {};
  const prizes = data.prizes || [];
  const canSpin = Number(data.spins_left || 0) > 0;
  return `
    <div class="section-head"><h3>Lucky Spin</h3><button data-action="set-route" data-route="profile">Back</button></div>
    <section class="panel spin-panel">
      <div class="spin-wheel">
        ${prizes.slice(0, 8).map((prize) => `<span>${escapeHtml(prize.title)}</span>`).join("")}
      </div>
      <p class="muted">Max bonus: ${money(data.max_bonus ?? 0.5)}</p>
      <p class="muted">${canSpin ? "Spin available now" : `Next spin: ${shortDate(data.next_spin_at)}`}</p>
      <button class="action-btn" data-action="play-spin" type="button" ${canSpin ? "" : "disabled"}>${icon("rotate-cw")} Spin Now</button>
    </section>
    <div class="section-head"><h3>Spin History</h3></div>
    <section class="table-lite">
      ${(data.history || []).length ? data.history.map((row) => `
        <article class="order-card">
          <div class="status-row"><h4>${escapeHtml(row.prize_title)}</h4><strong class="price">${money(row.amount)}</strong></div>
          <div class="muted">${shortDate(row.created_at)}</div>
        </article>
      `).join("") : `<div class="empty">No spins yet</div>`}
    </section>
  `;
}

function renderCouponPage() {
  return `
    <div class="section-head"><h3>Coupon</h3><button data-action="set-route" data-route="profile">Back</button></div>
    <form class="panel form-grid" id="coupon-check-form">
      <div class="field">
        <label>Promo code</label>
        <input name="code" placeholder="WELCOME10" required />
      </div>
      <div class="field">
        <label>Product</label>
        <select name="product_id">
          ${state.products.map((product) => `<option value="${product.id}">${escapeHtml(product.name)}</option>`).join("")}
        </select>
      </div>
      <div class="field">
        <label>Duration</label>
        <select name="duration_days">
          <option value="1">1 Day</option>
          <option value="7">7 Days</option>
          <option value="30">30 Days</option>
        </select>
      </div>
      <button class="action-btn" type="submit">${icon("badge-percent")} Check Coupon</button>
    </form>
  `;
}

function renderSupport() {
  const support = state.supportSettings || {};
  const supportUrl = supportTelegramUrl(support);
  const messagesByTicket = new Map();
  state.ticketMessages.forEach((message) => {
    const list = messagesByTicket.get(message.ticket_id) || [];
    list.push(message);
    messagesByTicket.set(message.ticket_id, list);
  });
  return `
    <div class="section-head"><h3>Telegram Support</h3></div>
    <section class="panel support-card">
      <div class="support-icon">${icon("headphones")}</div>
      <div>
        <h3>${escapeHtml(support.display_name || "Store Support")}</h3>
        <p>${escapeHtml(support.note || "Tap to open Telegram inbox for help.")}</p>
        <small>${support.telegram_username ? `@${escapeHtml(support.telegram_username)}` : support.telegram_user_id ? `ID ${escapeHtml(support.telegram_user_id)}` : "Support contact not set"}</small>
      </div>
      <button class="action-btn ${support.enabled && supportUrl ? "" : "secondary"}" data-action="open-support-chat" data-url="${escapeHtml(support.enabled ? supportUrl : "")}" type="button">
        ${icon("send")} Open Inbox
      </button>
    </section>
    <div class="section-head"><h3>Support Ticket</h3></div>
    <form class="panel form-grid" id="ticket-form">
      <div class="field">
        <label>Subject</label>
        <input name="subject" required />
      </div>
      <div class="field">
        <label>Message</label>
        <textarea name="message" required></textarea>
      </div>
      <button class="action-btn" type="submit">${icon("message-circle-plus")} Create Ticket</button>
    </form>
    <div class="section-head"><h3>Tickets</h3></div>
    <section class="table-lite">
      ${state.tickets.length ? state.tickets.map((ticket) => `
        <article class="ticket-card">
          <div class="status-row"><h4>#${ticket.id} ${escapeHtml(ticket.subject)}</h4>${statusBadge(ticket.status)}</div>
          ${(messagesByTicket.get(ticket.id) || []).map((message) => `
            <div class="notice">
              <strong>${message.is_admin ? "Admin" : "You"}</strong>
              <p>${escapeHtml(message.message)}</p>
            </div>
          `).join("")}
          <form class="form-grid ticket-reply-form" data-ticket-id="${ticket.id}">
            <div class="field"><input name="message" placeholder="Reply" required /></div>
            <button class="action-btn secondary" type="submit">${icon("reply")} Reply</button>
          </form>
        </article>
      `).join("") : `<div class="empty">No tickets yet</div>`}
    </section>
  `;
}

function renderProfile() {
  const user = state.dashboard?.user || state.session?.user || {};
  const name = userName(user);
  return `
    <section class="panel">
      <div class="profile-chip">
        <div class="avatar">${user.photo_url ? `<img src="${escapeHtml(user.photo_url)}" alt="${escapeHtml(name)}" />` : initials(name)}</div>
        <div>
          <h2>${escapeHtml(name)}</h2>
          <p>${user.username ? `@${escapeHtml(user.username)}` : `ID ${escapeHtml(user.telegram_id)}`}</p>
        </div>
      </div>
      <div class="metric-grid">
        <div class="metric"><span>User ID</span><strong>${escapeHtml(user.telegram_id)}</strong></div>
        <div class="metric"><span>Join date</span><strong style="font-size:15px;">${shortDate(user.joined_at)}</strong></div>
      </div>
      <form class="form-grid" id="currency-form">
        <div class="field">
          <label>Preferred Currency</label>
          <select name="code">
            ${state.currencies.map((currency) => `
              <option value="${escapeHtml(currency.code)}" ${currency.code === state.currency?.code ? "selected" : ""}>
                ${escapeHtml(currency.symbol)} ${escapeHtml(currency.code)} - ${escapeHtml(currency.name)}
              </option>
            `).join("")}
          </select>
        </div>
        <button class="action-btn secondary" type="submit">${icon("coins")} Save Currency</button>
      </form>
      <div class="form-grid">
        <button class="action-btn secondary" data-action="set-route" data-route="spin">${icon("rotate-cw")} Lucky Spin</button>
        <button class="action-btn secondary" data-action="set-route" data-route="referral">${icon("share-2")} Referral</button>
        <button class="action-btn secondary" data-action="set-route" data-route="coupon">${icon("badge-percent")} Coupon</button>
        ${state.session?.is_admin ? `<button class="action-btn" data-action="set-route" data-route="admin">${icon("shield-check")} Admin Panel</button>` : ""}
      </div>
    </section>
  `;
}

function adminTabs() {
  const tabs = [
    ["dashboard", "Dashboard"],
    ["categories", "Sections"],
    ["products", "Products"],
    ["keys", "Keys"],
    ["orders", "Orders"],
    ["payments", "Payments"],
    ["users", "Users"],
    ["coupons", "Coupons"],
    ["tickets", "Tickets"],
    ["support", "Supporter"],
    ["broadcast", "Broadcast"],
  ];
  return `
    <div class="admin-tabs">
      ${tabs.map(([tab, label]) => `<button class="${state.adminTab === tab ? "active" : ""}" data-action="admin-tab" data-tab="${tab}">${label}</button>`).join("")}
    </div>
  `;
}

function renderAdmin() {
  return `
    <div class="section-head"><h3>Admin Panel</h3><button data-action="set-route" data-route="profile">Back</button></div>
    ${adminTabs()}
    ${renderAdminTab()}
  `;
}

function renderAdminTab() {
  if (state.adminTab === "dashboard") return renderAdminDashboard();
  if (state.adminTab === "categories") return renderAdminCategories();
  if (state.adminTab === "products") return renderAdminProducts();
  if (state.adminTab === "keys") return renderAdminKeys();
  if (state.adminTab === "orders") return renderAdminOrders();
  if (state.adminTab === "payments") return renderAdminPayments();
  if (state.adminTab === "users") return renderAdminUsers();
  if (state.adminTab === "coupons") return renderAdminCoupons();
  if (state.adminTab === "tickets") return renderAdminTickets();
  if (state.adminTab === "support") return renderAdminSupport();
  if (state.adminTab === "broadcast") return renderAdminBroadcast();
  return "";
}

function renderAdminDashboard() {
  const stats = state.admin.dashboard?.stats || {};
  const recent = state.admin.dashboard?.recent_orders || [];
  return `
    <section class="metric-grid">
      <div class="metric"><span>Total users</span><strong>${stats.total_users || 0}</strong></div>
      <div class="metric"><span>Total orders</span><strong>${stats.total_orders || 0}</strong></div>
      <div class="metric"><span>Total sales</span><strong>${money(stats.total_sales)}</strong></div>
      <div class="metric"><span>Pending payments</span><strong>${stats.pending_payments || 0}</strong></div>
      <div class="metric"><span>Active subscriptions</span><strong>${stats.active_subscriptions || 0}</strong></div>
      <div class="metric"><span>Today's sales</span><strong>${money(stats.todays_sales)}</strong></div>
    </section>
    <div class="section-head"><h3>Recent Orders</h3></div>
    <section class="table-lite">
      ${recent.map((order) => `
        <article class="admin-row">
          <div class="status-row"><h4>${escapeHtml(order.invoice_id)}</h4>${statusBadge(order.status)}</div>
          <div class="muted">${escapeHtml(order.first_name)} - ${money(order.total)}</div>
        </article>
      `).join("") || `<div class="empty">No orders</div>`}
    </section>
  `;
}

function renderAdminCategories() {
  const categories = state.admin.categories?.categories || [];
  const edit = state.editingCategory;
  return `
    <form class="panel form-grid" id="admin-category-form">
      <div class="two-col">
        <div class="field"><label>Section key</label><input name="key" required value="${escapeHtml(edit?.key || "")}" ${edit ? "readonly" : ""} /></div>
        <div class="field"><label>Icon</label><input name="icon" value="${escapeHtml(edit?.icon || "box")}" /></div>
      </div>
      <div class="field"><label>Section name</label><input name="name" required value="${escapeHtml(edit?.name || "")}" /></div>
      <div class="field"><label>Description</label><input name="description" value="${escapeHtml(edit?.description || "")}" /></div>
      <div class="two-col">
        <div class="field">
          <label>Parent section</label>
          <select name="parent_key">
            <option value="">Top level</option>
            ${categories.filter((cat) => cat.key !== edit?.key).map((cat) => `<option value="${escapeHtml(cat.key)}" ${edit?.parent_key === cat.key ? "selected" : ""}>${escapeHtml(cat.name)}</option>`).join("")}
          </select>
        </div>
        <div class="field"><label>Sort</label><input name="sort_order" type="number" value="${escapeHtml(edit?.sort_order ?? 0)}" /></div>
      </div>
      <button class="action-btn" type="submit">${icon(edit ? "save" : "plus")} ${edit ? "Save Section" : "Add Section"}</button>
      ${edit ? `<button class="action-btn secondary" data-action="cancel-category-edit" type="button">Cancel Edit</button>` : ""}
    </form>
    <section class="table-lite" style="margin-top:10px;">
      ${categories.map((category) => `
        <article class="admin-row">
          <div class="status-row"><h4>${escapeHtml(category.name)}</h4>${category.active ? `<span class="badge success">Active</span>` : `<span class="badge danger">Off</span>`}</div>
          <div class="muted">Key: ${escapeHtml(category.key)} ${category.parent_name ? `- Parent: ${escapeHtml(category.parent_name)}` : "- Top level"}</div>
          <div class="two-col">
            <button class="action-btn secondary" data-action="edit-category" data-key="${escapeHtml(category.key)}" type="button">${icon("pencil")} Edit</button>
            <button class="action-btn danger" data-action="delete-category" data-key="${escapeHtml(category.key)}" type="button">${icon("trash-2")} Delete</button>
          </div>
        </article>
      `).join("") || `<div class="empty">No sections</div>`}
    </section>
  `;
}

function renderAdminProducts() {
  const data = state.admin.products || {};
  const products = data.products || [];
  const categories = data.categories || state.categories;
  const edit = state.editingProduct;
  return `
    <form class="panel form-grid" id="admin-product-form">
      <div class="two-col">
        <div class="field">
          <label>Category</label>
          <select name="category_key">
            ${categories.map((category) => `<option value="${escapeHtml(category.key)}" ${edit?.category_key === category.key ? "selected" : ""}>${escapeHtml(category.name)}</option>`).join("")}
          </select>
        </div>
        <div class="field">
          <label>Stock quantity</label>
          <input name="stock_quantity" type="number" min="0" value="${escapeHtml(edit?.stock_quantity ?? "")}" />
        </div>
      </div>
      <div class="field"><label>Product name</label><input name="name" required value="${escapeHtml(edit?.name || "")}" /></div>
      <div class="field"><label>Features panel text</label><textarea name="feature_text">${escapeHtml(edit?.feature_text || "")}</textarea></div>
      <div class="field"><label>Description</label><textarea name="description">${escapeHtml(edit?.description || "")}</textarea></div>
      <div class="field"><label>Product image URL</label><input name="image_url" value="${escapeHtml(edit?.image_url || "")}" /></div>
      <div class="field"><label>Video URL / YouTube URL</label><input name="video_url" value="${escapeHtml(edit?.video_url || "")}" /></div>
      <div class="field"><label>Panel file/link URL</label><input name="panel_url" value="${escapeHtml(edit?.panel_url || "")}" /></div>
      <div class="field"><label>Product image upload</label><input name="image_file" type="file" accept="image/*" /></div>
      <div class="two-col">
        <div class="field"><label>1 Day price</label><input name="price_1_day" type="number" step="0.01" min="0" required value="${escapeHtml(edit?.price_1_day ?? "")}" /></div>
        <div class="field"><label>7 Days price</label><input name="price_7_days" type="number" step="0.01" min="0" required value="${escapeHtml(edit?.price_7_days ?? "")}" /></div>
      </div>
      <div class="two-col">
        <div class="field"><label>30 Days price</label><input name="price_30_days" type="number" step="0.01" min="0" required value="${escapeHtml(edit?.price_30_days ?? "")}" /></div>
        <div class="field"><label>Stock on/off</label><select name="stock_status"><option value="true" ${edit?.stock_status !== false ? "selected" : ""}>On</option><option value="false" ${edit?.stock_status === false ? "selected" : ""}>Off</option></select></div>
      </div>
      <button class="action-btn" type="submit">${icon(edit ? "save" : "plus")} ${edit ? "Save Product" : "Add Product"}</button>
      ${edit ? `<button class="action-btn secondary" data-action="cancel-product-edit" type="button">Cancel Edit</button>` : ""}
    </form>
    <div class="section-head"><h3>Products</h3></div>
    <section class="table-lite">
      ${products.map((product) => `
        <article class="admin-row">
          <div class="status-row"><h4>${escapeHtml(product.name)}</h4>${product.stock_status ? `<span class="badge success">Stock</span>` : `<span class="badge danger">Off</span>`}</div>
          <div class="muted">${escapeHtml(product.category_name || product.category_key)} - ${money(product.price_1_day)} / ${money(product.price_7_days)} / ${money(product.price_30_days)}</div>
          <div class="muted">Keys: ${Number(product.available_keys || 0)} available - ${Number(product.delivered_keys || 0)} delivered</div>
          <div class="two-col">
            <button class="action-btn secondary" data-action="edit-product" data-id="${product.id}" type="button">${icon("pencil")} Edit</button>
            <button class="action-btn secondary" data-action="manage-product-keys" data-id="${product.id}" type="button">${icon("key-round")} Keys</button>
          </div>
          <button class="action-btn danger" data-action="delete-product" data-id="${product.id}" type="button">${icon("trash-2")} Delete</button>
        </article>
      `).join("") || `<div class="empty">No products</div>`}
    </section>
  `;
}

function renderAdminKeys() {
  const data = state.admin.keys || {};
  const products = data.products || [];
  const keys = data.keys || [];
  const selectedId = state.keyProductId ? String(state.keyProductId) : "";
  return `
    <form class="panel form-grid" id="admin-key-filter-form">
      <div class="field">
        <label>View keys for product</label>
        <select name="product_id">
          <option value="">All products</option>
          ${products.map((product) => `<option value="${product.id}" ${String(product.id) === selectedId ? "selected" : ""}>${escapeHtml(product.name)} (${Number(product.available_keys || 0)} available)</option>`).join("")}
        </select>
      </div>
      <button class="action-btn secondary" type="submit">${icon("list-filter")} Load Keys</button>
    </form>
    <form class="panel form-grid" id="admin-key-upload-form">
      <div class="field">
        <label>Product</label>
        <select name="product_id" required>
          ${products.map((product) => `<option value="${product.id}" ${String(product.id) === selectedId ? "selected" : ""}>${escapeHtml(product.name)} - ${escapeHtml(product.category_name || "No section")}</option>`).join("")}
        </select>
      </div>
      <div class="field">
        <label>Keys / files / links</label>
        <textarea name="keys" placeholder="One key, file link, or login panel per line" required></textarea>
      </div>
      <button class="action-btn" type="submit">${icon("upload")} Upload Keys</button>
    </form>
    <div class="section-head"><h3>Stored Keys</h3></div>
    <section class="table-lite">
      ${keys.map((key) => `
        <article class="admin-row">
          <div class="status-row"><h4>${escapeHtml(key.product_name)}</h4>${statusBadge(key.status)}</div>
          <div class="muted">${escapeHtml(key.category_name || "No section")} - ${shortDate(key.created_at)}</div>
          <div class="key-value">${escapeHtml(key.key_value)}</div>
          ${key.invoice_id ? `<div class="muted">Delivered to ${escapeHtml(key.first_name || key.username || key.telegram_id || "user")} - Invoice ${escapeHtml(key.invoice_id)}</div>` : ""}
          <button class="action-btn danger" data-action="delete-product-key" data-id="${key.id}" type="button">${icon("trash-2")} Delete Key</button>
        </article>
      `).join("") || `<div class="empty">No keys found</div>`}
    </section>
  `;
}

function renderAdminOrders() {
  const orders = state.admin.orders?.orders || [];
  return `
    <section class="table-lite">
      ${orders.map((order) => `
        <article class="admin-row">
          <div class="status-row"><h4>${escapeHtml(order.invoice_id)}</h4>${statusBadge(order.status)}</div>
          <div>${escapeHtml(order.first_name)} - ${escapeHtml(order.product_name || "Product")}</div>
          <div class="muted">${money(order.total)} - ${order.duration_days} Day - ${shortDate(order.created_at)}</div>
          <div class="two-col">
            <button class="action-btn secondary" data-action="approve-order" data-id="${order.id}" type="button">${icon("check")} Approve</button>
            <button class="action-btn" data-action="deliver-order" data-id="${order.id}" type="button">${icon("package-check")} Deliver</button>
          </div>
          <button class="action-btn danger" data-action="cancel-order" data-id="${order.id}" type="button">${icon("x")} Cancel</button>
        </article>
      `).join("") || `<div class="empty">No orders</div>`}
    </section>
  `;
}

function renderAdminPayments() {
  const payments = state.admin.payments?.payments || [];
  const methods = state.admin.payments?.methods || [];
  const edit = state.editingPaymentMethod;
  return `
    <form class="panel form-grid" id="admin-payment-method-form">
      <div class="two-col">
        <div class="field"><label>Method name</label><input name="name" required value="${escapeHtml(edit?.name || "")}" /></div>
        <div class="field"><label>Type</label><select name="method_type"><option value="manual" ${edit?.method_type !== "auto" ? "selected" : ""}>Manual</option><option value="auto" ${edit?.method_type === "auto" ? "selected" : ""}>Auto</option></select></div>
      </div>
      <div class="two-col">
        <div class="field"><label>Label</label><input name="account_label" value="${escapeHtml(edit?.account_label || "")}" /></div>
        <div class="field"><label>Value</label><input name="account_value" value="${escapeHtml(edit?.account_value || "")}" /></div>
      </div>
      <div class="field"><label>Instructions</label><textarea name="instructions">${escapeHtml(edit?.instructions || "")}</textarea></div>
      <div class="field"><label>QR image URL</label><input name="qr_image_url" value="${escapeHtml(edit?.qr_image_url || "")}" /></div>
      <div class="two-col">
        <div class="field"><label>Sort</label><input name="sort_order" type="number" value="${escapeHtml(edit?.sort_order ?? 0)}" /></div>
        <div class="field"><label>Status</label><select name="active"><option value="true" ${edit?.active !== false ? "selected" : ""}>Active</option><option value="false" ${edit?.active === false ? "selected" : ""}>Inactive</option></select></div>
      </div>
      <button class="action-btn" type="submit">${icon(edit ? "save" : "plus")} ${edit ? "Save Method" : "Add Method"}</button>
      ${edit ? `<button class="action-btn secondary" data-action="cancel-payment-method-edit" type="button">Cancel Edit</button>` : ""}
    </form>
    <div class="section-head"><h3>Payment Methods</h3></div>
    <section class="table-lite">
      ${methods.map((method) => `
        <article class="admin-row">
          <div class="status-row"><h4>${escapeHtml(method.name)}</h4>${method.active ? `<span class="badge success">Active</span>` : `<span class="badge danger">Off</span>`}</div>
          <div class="muted">${escapeHtml(method.account_label || "Details")}: ${escapeHtml(method.account_value || "-")}</div>
          <div class="two-col">
            <button class="action-btn secondary" data-action="edit-payment-method" data-id="${method.id}" type="button">${icon("pencil")} Edit</button>
            <button class="action-btn danger" data-action="delete-payment-method" data-id="${method.id}" type="button">${icon("trash-2")} Delete</button>
          </div>
        </article>
      `).join("") || `<div class="empty">No methods</div>`}
    </section>
    <div class="section-head"><h3>Payment Requests</h3></div>
    <section class="table-lite">
      ${payments.map((payment) => `
        <article class="admin-row">
          <div class="status-row"><h4>${money(payment.amount)}</h4>${statusBadge(payment.status)}</div>
          <div>${escapeHtml(payment.first_name)} - ${escapeHtml(payment.method_name)}</div>
          <div class="muted">TXID ${escapeHtml(payment.transaction_id)} - ${shortDate(payment.created_at)}</div>
          ${payment.screenshot_data ? `<img class="screenshot" src="${escapeHtml(payment.screenshot_data)}" alt="Payment screenshot" />` : ""}
          ${payment.status === "pending" ? `<div class="two-col">
            <button class="action-btn secondary" data-action="approve-payment" data-id="${payment.id}" type="button">${icon("check")} Approve</button>
            <button class="action-btn danger" data-action="reject-payment" data-id="${payment.id}" type="button">${icon("x")} Reject</button>
          </div>` : ""}
          ${payment.status === "rejected" ? `<button class="action-btn danger" data-action="delete-payment" data-id="${payment.id}" type="button">${icon("trash-2")} Remove Rejected Payment</button>` : ""}
        </article>
      `).join("") || `<div class="empty">No payments</div>`}
    </section>
  `;
}

function renderAdminUsers() {
  const users = state.admin.users?.users || [];
  return `
    <form class="panel form-grid" id="admin-user-search-form">
      <div class="field"><label>Search user</label><input name="search" /></div>
      <button class="action-btn" type="submit">${icon("search")} Search</button>
    </form>
    <section class="table-lite" style="margin-top:10px;">
      ${users.map((user) => `
        <article class="admin-row">
          <div class="status-row"><h4>${escapeHtml(userName(user))}</h4>${user.is_banned ? `<span class="badge danger">Banned</span>` : `<span class="badge success">Active</span>`}</div>
          <div class="muted">@${escapeHtml(user.username || "-")} - ID ${escapeHtml(user.telegram_id)}</div>
          <div class="muted">Balance ${money(user.wallet_balance)} - Orders ${user.order_count} - Payments ${user.payment_count}</div>
          <div class="two-col">
            <button class="action-btn secondary" data-action="add-balance" data-id="${user.id}" type="button">${icon("plus")} Add Balance</button>
            <button class="action-btn danger" data-action="remove-balance" data-id="${user.id}" type="button">${icon("minus")} Remove</button>
          </div>
          <div class="two-col">
            <button class="action-btn secondary" data-action="adjust-balance" data-id="${user.id}" type="button">${icon("wallet-cards")} Custom</button>
            <button class="action-btn ${user.is_banned ? "secondary" : "danger"}" data-action="${user.is_banned ? "unban-user" : "ban-user"}" data-id="${user.id}" type="button">${icon(user.is_banned ? "unlock" : "ban")} ${user.is_banned ? "Unban" : "Ban"}</button>
          </div>
        </article>
      `).join("") || `<div class="empty">No users</div>`}
    </section>
  `;
}

function renderAdminCoupons() {
  const coupons = state.admin.coupons?.coupons || [];
  const edit = state.editingCoupon;
  return `
    <form class="panel form-grid" id="admin-coupon-form">
      <div class="two-col">
        <div class="field"><label>Promo code</label><input name="code" required value="${escapeHtml(edit?.code || "")}" /></div>
        <div class="field"><label>Discount type</label><select name="discount_type"><option value="percent" ${edit?.discount_type !== "fixed" ? "selected" : ""}>Percent</option><option value="fixed" ${edit?.discount_type === "fixed" ? "selected" : ""}>Fixed</option></select></div>
      </div>
      <div class="two-col">
        <div class="field"><label>Discount value</label><input name="discount_value" type="number" step="0.01" min="0" required value="${escapeHtml(edit?.discount_value ?? "")}" /></div>
        <div class="field"><label>Max uses</label><input name="max_uses" type="number" min="1" value="${escapeHtml(edit?.max_uses ?? "")}" /></div>
      </div>
      <div class="two-col">
        <div class="field"><label>Expiry date</label><input name="expires_at" type="datetime-local" /></div>
        <div class="field"><label>Status</label><select name="active"><option value="true" ${edit?.active !== false ? "selected" : ""}>Active</option><option value="false" ${edit?.active === false ? "selected" : ""}>Inactive</option></select></div>
      </div>
      <button class="action-btn" type="submit">${icon(edit ? "save" : "plus")} ${edit ? "Save Coupon" : "Add Coupon"}</button>
      ${edit ? `<button class="action-btn secondary" data-action="cancel-coupon-edit" type="button">Cancel Edit</button>` : ""}
    </form>
    <section class="table-lite" style="margin-top:10px;">
      ${coupons.map((coupon) => `
        <article class="admin-row">
          <div class="status-row"><h4>${escapeHtml(coupon.code)}</h4>${coupon.active ? `<span class="badge success">Active</span>` : `<span class="badge danger">Off</span>`}</div>
          <div class="muted">${escapeHtml(coupon.discount_type)} ${coupon.discount_value} - Used ${coupon.used_count}${coupon.max_uses ? `/${coupon.max_uses}` : ""}</div>
          <div class="two-col">
            <button class="action-btn secondary" data-action="edit-coupon" data-id="${coupon.id}" type="button">${icon("pencil")} Edit</button>
            <button class="action-btn danger" data-action="delete-coupon" data-id="${coupon.id}" type="button">${icon("trash-2")} Delete</button>
          </div>
        </article>
      `).join("") || `<div class="empty">No coupons</div>`}
    </section>
  `;
}

function renderAdminSupport() {
  const support = state.admin.support?.support || {};
  return `
    <form class="panel form-grid" id="admin-support-settings-form">
      <div class="field">
        <label>Support display name</label>
        <input name="display_name" required value="${escapeHtml(support.display_name || "Store Support")}" />
      </div>
      <div class="field">
        <label>Telegram username</label>
        <input name="telegram_username" placeholder="support_username" value="${escapeHtml(support.telegram_username || "")}" />
      </div>
      <div class="field">
        <label>Telegram numeric user ID</label>
        <input name="telegram_user_id" inputmode="numeric" value="${escapeHtml(support.telegram_user_id || "")}" />
      </div>
      <div class="field">
        <label>Support note</label>
        <textarea name="note">${escapeHtml(support.note || "Tap to open Telegram inbox for help.")}</textarea>
      </div>
      <div class="field">
        <label>Status</label>
        <select name="enabled">
          <option value="true" ${support.enabled !== false ? "selected" : ""}>Active</option>
          <option value="false" ${support.enabled === false ? "selected" : ""}>Inactive</option>
        </select>
      </div>
      <button class="action-btn" type="submit">${icon("save")} Save Supporter</button>
    </form>
  `;
}

function renderAdminTickets() {
  const data = state.admin.tickets || {};
  const messagesByTicket = new Map();
  (data.messages || []).forEach((message) => {
    const list = messagesByTicket.get(message.ticket_id) || [];
    list.push(message);
    messagesByTicket.set(message.ticket_id, list);
  });
  return `
    <section class="table-lite">
      ${(data.tickets || []).map((ticket) => `
        <article class="admin-row">
          <div class="status-row"><h4>#${ticket.id} ${escapeHtml(ticket.subject)}</h4>${statusBadge(ticket.status)}</div>
          <div class="muted">${escapeHtml(ticket.first_name)} - ${shortDate(ticket.updated_at)}</div>
          ${(messagesByTicket.get(ticket.id) || []).map((message) => `
            <div class="notice"><strong>${message.is_admin ? "Admin" : "User"}</strong><p>${escapeHtml(message.message)}</p></div>
          `).join("")}
          <form class="form-grid admin-ticket-reply-form" data-ticket-id="${ticket.id}">
            <div class="field"><input name="message" placeholder="Reply" required /></div>
            <button class="action-btn" type="submit">${icon("reply")} Reply</button>
          </form>
        </article>
      `).join("") || `<div class="empty">No tickets</div>`}
    </section>
  `;
}

function renderAdminBroadcast() {
  return `
    <form class="panel form-grid" id="admin-broadcast-form">
      <div class="field"><label>Message</label><textarea name="message" required></textarea></div>
      <div class="field"><label>Target</label><select name="target"><option value="all">All users</option><option value="user">Specific user</option></select></div>
      <div class="field"><label>User database ID</label><input name="user_id" type="number" min="1" /></div>
      <div class="field"><label>Notice title</label><input name="notice_title" /></div>
      <button class="action-btn" type="submit">${icon("radio")} Send Broadcast</button>
    </form>
  `;
}

function renderView() {
  if (state.route === "home") return renderHome();
  if (state.route === "category") return renderCategory();
  if (state.route === "product") return renderProductDetail();
  if (state.route === "checkout") return renderCheckout();
  if (state.route === "wallet" || state.route === "add-balance") return renderWallet();
  if (state.route === "orders") return renderOrders();
  if (state.route === "referral") return renderReferral();
  if (state.route === "spin") return renderSpin();
  if (state.route === "coupon") return renderCouponPage();
  if (state.route === "support") return renderSupport();
  if (state.route === "profile") return renderProfile();
  if (state.route === "admin") return renderAdmin();
  return renderHome();
}

function syncTelegramControls() {
  if (tg?.BackButton) {
    if (!backButtonHandler) {
      backButtonHandler = () => setRoute("home").catch((error) => toast(error.message));
      tg.BackButton.onClick(backButtonHandler);
    }
    if (state.route === "home") tg.BackButton.hide();
    else tg.BackButton.show();
  }

  if (tg?.MainButton) {
    if (!mainButtonBound) {
      tg.MainButton.onClick(() => {
        if (typeof mainButtonHandler === "function") mainButtonHandler();
      });
      mainButtonBound = true;
    }
    if (state.route === "product" && state.product?.stock_status) {
      tg.MainButton.setText(`Checkout ${textMoney(priceFor(state.product, state.selectedDuration))}`);
      tg.MainButton.show();
      mainButtonHandler = () => openCheckout();
    } else {
      tg.MainButton.hide();
      mainButtonHandler = null;
    }
  }
}

function render() {
  root.innerHTML = `${topbar()}${bottomNav()}<main>${renderView()}</main>`;
  window.lucide?.createIcons();
  syncTelegramControls();
}

function renderError(error) {
  root.innerHTML = `
    <section class="splash">
      <div class="brand-mark">!</div>
      <div>
        <h1>Mini App</h1>
        <p>${escapeHtml(error.message || error)}</p>
      </div>
    </section>
  `;
}

async function openCheckout() {
  if (!state.product) return;
  const coupon = document.querySelector("#buy-form [name='coupon_code']")?.value || "";
  state.checkout = { product: state.product, duration: state.selectedDuration, coupon_code: coupon };
  state.route = "checkout";
  state.selectedPaymentMethodId = null;
  await loadRouteData("checkout");
  render();
}

async function submitOrder(form) {
  const couponInput = form?.elements?.coupon_code || document.querySelector("#buy-form [name='coupon_code']");
  const couponCode = couponInput?.value?.trim() || state.checkout?.coupon_code || "";
  if (!state.product) return;
  const ok = tg?.showConfirm
    ? await new Promise((resolve) => tg.showConfirm("Place this order?", resolve))
    : window.confirm("Place this order?");
  if (!ok) return;
  await api("/api/orders", {
    method: "POST",
    body: {
      product_id: state.product.id,
      duration_days: state.selectedDuration,
      coupon_code: couponCode || null,
    },
  });
  toast("Order placed");
  await loadDashboard();
  await setRoute("orders");
}

document.addEventListener("click", async (event) => {
  const button = event.target.closest("[data-action]");
  if (!button) return;
  const action = button.dataset.action;
  try {
    if (action === "set-route") {
      await setRoute(button.dataset.route);
    }
    if (action === "open-category") {
      await openCategory(button.dataset.key, button.dataset.name);
    }
    if (action === "category-back") {
      await categoryBack();
    }
    if (action === "open-product") {
      await openProduct(button.dataset.id);
    }
    if (action === "select-duration") {
      state.selectedDuration = Number(button.dataset.duration);
      render();
    }
    if (action === "quick-buy") {
      state.selectedDuration = Number(button.dataset.duration);
      await openCheckout();
    }
    if (action === "wallet-pay") {
      if (button.classList.contains("disabled")) {
        toast("Insufficient wallet balance");
      } else {
        await submitOrder();
      }
    }
    if (action === "select-payment-method") {
      state.selectedPaymentMethodId = Number(button.dataset.id);
      render();
    }
    if (action === "play-spin") {
      const result = await api("/api/spin/play", { method: "POST" });
      toast(`${result.prize.title}: ${money(result.prize.amount)}`);
      await loadDashboard();
      await loadRouteData("spin");
      render();
    }
    if (action === "copy-referral") {
      await navigator.clipboard.writeText(state.referrals?.referral_link || state.referrals?.referral_code || "");
      toast("Referral link copied");
    }
    if (action === "open-support-chat") {
      const url = button.dataset.url || "";
      if (!url) {
        toast("Support contact not set");
        return;
      }
      if (url.startsWith("https://t.me/") && tg?.openTelegramLink) {
        tg.openTelegramLink(url);
      } else {
        window.location.href = url;
      }
    }
    if (action === "admin-tab") {
      await loadAdminData(button.dataset.tab);
      render();
    }
    if (action === "cancel-category-edit") {
      state.editingCategory = null;
      render();
    }
    if (action === "edit-category") {
      const list = state.admin.categories?.categories || [];
      state.editingCategory = list.find((item) => String(item.key) === String(button.dataset.key));
      render();
    }
    if (action === "delete-category") {
      await api(`/api/admin/categories/${encodeURIComponent(button.dataset.key)}`, { method: "DELETE" });
      toast("Section deleted");
      await loadAdminData("categories");
      render();
    }
    if (action === "cancel-product-edit") {
      state.editingProduct = null;
      render();
    }
    if (action === "edit-product") {
      const list = state.admin.products?.products || [];
      state.editingProduct = list.find((item) => String(item.id) === String(button.dataset.id));
      render();
    }
    if (action === "manage-product-keys") {
      state.keyProductId = Number(button.dataset.id);
      await loadAdminData("keys");
      render();
    }
    if (action === "delete-product") {
      await api(`/api/admin/products/${button.dataset.id}`, { method: "DELETE" });
      toast("Product deleted");
      await loadAdminData("products");
      render();
    }
    if (action === "delete-product-key") {
      await api(`/api/admin/product-keys/${button.dataset.id}`, { method: "DELETE" });
      toast("Key deleted");
      await loadAdminData("keys");
      render();
    }
    if (action === "approve-payment") {
      await api(`/api/admin/payments/${button.dataset.id}/approve`, { method: "POST" });
      toast("Payment approved");
      await loadAdminData("payments");
      render();
    }
    if (action === "reject-payment") {
      const reason = window.prompt("Reject reason") || "Rejected by admin";
      await api(`/api/admin/payments/${button.dataset.id}/reject`, { method: "POST", body: { reason } });
      toast("Payment rejected");
      await loadAdminData("payments");
      render();
    }
    if (action === "delete-payment") {
      await api(`/api/admin/payments/${button.dataset.id}`, { method: "DELETE" });
      toast("Rejected payment removed");
      await loadAdminData("payments");
      render();
    }
    if (action === "edit-payment-method") {
      const list = state.admin.payments?.methods || [];
      state.editingPaymentMethod = list.find((item) => String(item.id) === String(button.dataset.id));
      render();
    }
    if (action === "cancel-payment-method-edit") {
      state.editingPaymentMethod = null;
      render();
    }
    if (action === "delete-payment-method") {
      await api(`/api/admin/payment-methods/${button.dataset.id}`, { method: "DELETE" });
      toast("Payment method deleted");
      await loadAdminData("payments");
      render();
    }
    if (action === "approve-order") {
      await api(`/api/admin/orders/${button.dataset.id}/approve`, { method: "POST", body: { note: "" } });
      toast("Order approved");
      await loadAdminData("orders");
      render();
    }
    if (action === "deliver-order") {
      const deliveryText = window.prompt("Product key/file/link. Leave blank to use next stored key.") || "";
      await api(`/api/admin/orders/${button.dataset.id}/deliver`, { method: "POST", body: { delivery_text: deliveryText } });
      toast("Order delivered");
      await loadAdminData("orders");
      render();
    }
    if (action === "cancel-order") {
      const note = window.prompt("Cancel reason") || "Cancelled by admin";
      await api(`/api/admin/orders/${button.dataset.id}/cancel`, { method: "POST", body: { note } });
      toast("Order cancelled");
      await loadAdminData("orders");
      render();
    }
    if (action === "adjust-balance") {
      const amount = Number(window.prompt("Amount, use negative to remove") || "0");
      if (!amount) return;
      const reason = window.prompt("Reason") || "Admin adjustment";
      await api(`/api/admin/users/${button.dataset.id}/balance`, { method: "POST", body: { amount, reason } });
      toast("Balance updated");
      await loadAdminData("users");
      render();
    }
    if (action === "add-balance" || action === "remove-balance") {
      const amount = Number(window.prompt(action === "add-balance" ? "Amount to add" : "Amount to remove") || "0");
      if (!amount) return;
      const reason = window.prompt("Reason") || (action === "add-balance" ? "Admin balance add" : "Admin balance remove");
      await api(`/api/admin/users/${button.dataset.id}/balance`, {
        method: "POST",
        body: { amount: action === "add-balance" ? Math.abs(amount) : -Math.abs(amount), reason },
      });
      toast("Balance updated");
      await loadAdminData("users");
      render();
    }
    if (action === "ban-user" || action === "unban-user") {
      await api(`/api/admin/users/${button.dataset.id}/${action === "ban-user" ? "ban" : "unban"}`, { method: "POST" });
      toast(action === "ban-user" ? "User banned" : "User unbanned");
      await loadAdminData("users");
      render();
    }
    if (action === "edit-coupon") {
      const list = state.admin.coupons?.coupons || [];
      state.editingCoupon = list.find((item) => String(item.id) === String(button.dataset.id));
      render();
    }
    if (action === "cancel-coupon-edit") {
      state.editingCoupon = null;
      render();
    }
    if (action === "delete-coupon") {
      await api(`/api/admin/coupons/${button.dataset.id}`, { method: "DELETE" });
      toast("Coupon deleted");
      await loadAdminData("coupons");
      render();
    }
  } catch (error) {
    toast(error.message);
  }
});

document.addEventListener("submit", async (event) => {
  const form = event.target;
  try {
    if (form.id === "buy-form") {
      event.preventDefault();
      await submitOrder(form);
    }
    if (form.id === "payment-form") {
      event.preventDefault();
      const data = new FormData(form);
      const screenshot = await fileToDataUrl(data.get("screenshot"));
      await api("/api/payments", {
        method: "POST",
        body: {
          amount: Number(data.get("amount")),
          method_id: Number(data.get("method_id")),
          transaction_id: data.get("transaction_id"),
          screenshot_data: screenshot || null,
        },
      });
      toast("Payment submitted");
      await setRoute("wallet");
    }
    if (form.id === "currency-form") {
      event.preventDefault();
      const data = new FormData(form);
      const result = await api("/api/profile/currency", {
        method: "POST",
        body: { code: data.get("code") },
      });
      state.currency = result.currency;
      state.session.user = result.user;
      await loadDashboard();
      toast("Currency updated");
      render();
    }
    if (form.id === "checkout-payment-form") {
      event.preventDefault();
      const data = new FormData(form);
      const screenshot = await fileToDataUrl(data.get("screenshot"));
      const product = state.product || state.checkout?.product;
      await api("/api/payments", {
        method: "POST",
        body: {
          amount: Number(priceFor(product, state.selectedDuration)),
          method_id: Number(state.selectedPaymentMethodId),
          transaction_id: data.get("transaction_id"),
          screenshot_data: screenshot || null,
          product_id: product.id,
          duration_days: state.selectedDuration,
          coupon_code: state.checkout?.coupon_code || null,
        },
      });
      toast("Payment submitted. Order will be created after approval.");
      await setRoute("orders");
    }
    if (form.id === "ticket-form") {
      event.preventDefault();
      const data = new FormData(form);
      await api("/api/tickets", {
        method: "POST",
        body: { subject: data.get("subject"), message: data.get("message") },
      });
      toast("Ticket created");
      await setRoute("support");
    }
    if (form.classList.contains("ticket-reply-form")) {
      event.preventDefault();
      const data = new FormData(form);
      await api(`/api/tickets/${form.dataset.ticketId}/messages`, {
        method: "POST",
        body: { message: data.get("message") },
      });
      toast("Reply sent");
      await setRoute("support");
    }
    if (form.id === "coupon-check-form") {
      event.preventDefault();
      const data = new FormData(form);
      const result = await api("/api/coupons/validate", {
        method: "POST",
        body: {
          code: data.get("code"),
          product_id: Number(data.get("product_id")),
          duration_days: Number(data.get("duration_days")),
        },
      });
      toast(`Discount ${textMoney(result.discount)} - Total ${textMoney(result.total)}`);
    }
    if (form.id === "admin-product-form") {
      event.preventDefault();
      const data = new FormData(form);
      const imageFromFile = await fileToDataUrl(data.get("image_file"));
      const payload = {
        category_key: data.get("category_key"),
        name: data.get("name"),
        description: data.get("description"),
        feature_text: data.get("feature_text"),
        video_url: data.get("video_url") || "",
        panel_url: data.get("panel_url") || "",
        image_url: imageFromFile || data.get("image_url") || "",
        price_1_day: Number(data.get("price_1_day")),
        price_7_days: Number(data.get("price_7_days")),
        price_30_days: Number(data.get("price_30_days")),
        stock_status: data.get("stock_status") === "true",
        stock_quantity: data.get("stock_quantity") ? Number(data.get("stock_quantity")) : null,
        active: true,
      };
      const path = state.editingProduct ? `/api/admin/products/${state.editingProduct.id}` : "/api/admin/products";
      await api(path, { method: state.editingProduct ? "PUT" : "POST", body: payload });
      state.editingProduct = null;
      toast("Product saved");
      await loadAdminData("products");
      render();
    }
    if (form.id === "admin-category-form") {
      event.preventDefault();
      const data = new FormData(form);
      const rawKey = String(data.get("key") || "").trim().toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/^-|-$/g, "");
      const payload = {
        key: rawKey,
        name: data.get("name"),
        icon: data.get("icon") || "box",
        description: data.get("description") || "",
        parent_key: data.get("parent_key") || null,
        sort_order: Number(data.get("sort_order") || 0),
        active: true,
      };
      const path = state.editingCategory ? `/api/admin/categories/${encodeURIComponent(state.editingCategory.key)}` : "/api/admin/categories";
      await api(path, { method: state.editingCategory ? "PUT" : "POST", body: payload });
      state.editingCategory = null;
      toast("Section saved");
      await loadAdminData("categories");
      render();
    }
    if (form.id === "admin-key-filter-form") {
      event.preventDefault();
      const value = new FormData(form).get("product_id");
      state.keyProductId = value ? Number(value) : null;
      await loadAdminData("keys");
      render();
    }
    if (form.id === "admin-key-upload-form") {
      event.preventDefault();
      const data = new FormData(form);
      const result = await api("/api/admin/product-keys", {
        method: "POST",
        body: {
          product_id: Number(data.get("product_id")),
          keys: data.get("keys"),
        },
      });
      state.keyProductId = Number(data.get("product_id"));
      toast(`${result.inserted} keys uploaded`);
      await loadAdminData("keys");
      render();
    }
    if (form.id === "admin-payment-method-form") {
      event.preventDefault();
      const data = new FormData(form);
      const payload = {
        name: data.get("name"),
        instructions: data.get("instructions") || "",
        method_type: data.get("method_type"),
        account_label: data.get("account_label") || "",
        account_value: data.get("account_value") || "",
        qr_image_url: data.get("qr_image_url") || "",
        active: data.get("active") === "true",
        sort_order: Number(data.get("sort_order") || 0),
      };
      const path = state.editingPaymentMethod ? `/api/admin/payment-methods/${state.editingPaymentMethod.id}` : "/api/admin/payment-methods";
      await api(path, { method: state.editingPaymentMethod ? "PUT" : "POST", body: payload });
      state.editingPaymentMethod = null;
      toast("Payment method saved");
      await loadAdminData("payments");
      render();
    }
    if (form.id === "admin-user-search-form") {
      event.preventDefault();
      const query = new FormData(form).get("search") || "";
      state.admin.users = await api(`/api/admin/users?search=${encodeURIComponent(query)}`);
      render();
    }
    if (form.id === "admin-coupon-form") {
      event.preventDefault();
      const data = new FormData(form);
      const expiry = data.get("expires_at") ? new Date(data.get("expires_at")).toISOString() : null;
      const payload = {
        code: data.get("code"),
        discount_type: data.get("discount_type"),
        discount_value: Number(data.get("discount_value")),
        expires_at: expiry,
        active: data.get("active") === "true",
        max_uses: data.get("max_uses") ? Number(data.get("max_uses")) : null,
      };
      const path = state.editingCoupon ? `/api/admin/coupons/${state.editingCoupon.id}` : "/api/admin/coupons";
      await api(path, { method: state.editingCoupon ? "PUT" : "POST", body: payload });
      state.editingCoupon = null;
      toast("Coupon saved");
      await loadAdminData("coupons");
      render();
    }
    if (form.id === "admin-support-settings-form") {
      event.preventDefault();
      const data = new FormData(form);
      const result = await api("/api/admin/support-settings", {
        method: "POST",
        body: {
          display_name: data.get("display_name"),
          telegram_username: data.get("telegram_username") || "",
          telegram_user_id: data.get("telegram_user_id") || "",
          note: data.get("note") || "",
          enabled: data.get("enabled") === "true",
        },
      });
      state.admin.support = result;
      state.supportSettings = result.support;
      toast("Supporter saved");
      render();
    }
    if (form.classList.contains("admin-ticket-reply-form")) {
      event.preventDefault();
      const data = new FormData(form);
      await api(`/api/admin/tickets/${form.dataset.ticketId}/messages`, {
        method: "POST",
        body: { message: data.get("message") },
      });
      toast("Reply sent");
      await loadAdminData("tickets");
      render();
    }
    if (form.id === "admin-broadcast-form") {
      event.preventDefault();
      const data = new FormData(form);
      const result = await api("/api/admin/broadcast", {
        method: "POST",
        body: {
          message: data.get("message"),
          target: data.get("target"),
          user_id: data.get("user_id") ? Number(data.get("user_id")) : null,
          notice_title: data.get("notice_title") || null,
        },
      });
      toast(`Broadcast sent to ${result.sent}`);
      form.reset();
    }
  } catch (error) {
    toast(error.message);
  }
});

async function boot() {
  try {
    if (tg) {
      tg.ready();
      tg.expand();
      tg.enableClosingConfirmation?.();
    }
    state.session = await api("/api/session");
    await loadDashboard();
    render();
  } catch (error) {
    renderError(error);
  }
}

boot();
