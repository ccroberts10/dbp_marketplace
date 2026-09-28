require('dotenv').config();
const express = require('express');
const cors = require('cors');
const stripe = require('stripe')(process.env.STRIPE_SECRET_KEY);
const Database = require('better-sqlite3');
const multer = require('multer');
const path = require('path');
const fs = require('fs');
const { v4: uuidv4 } = require('uuid');
const { Resend } = require('resend');
const { S3Client, PutObjectCommand, ListObjectsV2Command } = require('@aws-sdk/client-s3');

const app = express();
const PORT = process.env.PORT || 3000;
const BASE_URL = process.env.BASE_URL || 'https://goodkit.com';
const resend = new Resend(process.env.RESEND_API_KEY);
const FROM_EMAIL = 'marketplace@goodkit.com';
const NOTIFY_EMAIL = process.env.NOTIFY_EMAIL || 'casey@goodkit.com';

const r2 = new S3Client({
  region: 'auto',
  endpoint: process.env.R2_ENDPOINT || 'https://placeholder.r2.cloudflarestorage.com',
  credentials: {
    accessKeyId:     process.env.R2_ACCESS_KEY_ID     || 'placeholder',
    secretAccessKey: process.env.R2_SECRET_ACCESS_KEY || 'placeholder'
  }
});
const R2_BUCKET = process.env.R2_BUCKET || 'goodkit-marketplace-backups';
const R2_ENABLED = !!(process.env.R2_ENDPOINT && process.env.R2_ACCESS_KEY_ID && process.env.R2_SECRET_ACCESS_KEY);

const DB_PATH = process.env.DB_PATH || './marketplace.db';
const db = new Database(DB_PATH);

// ── SCHEMA ──────────────────────────────────────────────────────────────────
// Clean P2P schema — no concierge, no staff, no dropoff tiers
// weight_oz is used to select prepaid shipping label tier at checkout
db.exec(`
  CREATE TABLE IF NOT EXISTS listings (
    id TEXT PRIMARY KEY,
    seller_name TEXT,
    seller_email TEXT,
    stripe_account_id TEXT,
    title TEXT,
    category TEXT,
    description TEXT,
    condition TEXT,
    price INTEGER,
    weight_oz INTEGER DEFAULT 0,
    photos TEXT DEFAULT '[]',
    shipping_estimate INTEGER DEFAULT 0,
    status TEXT DEFAULT 'approved',
    view_count INTEGER DEFAULT 0,
    created_at TEXT DEFAULT (datetime('now')),
    sold_at TEXT
  );
  CREATE TABLE IF NOT EXISTS sales (
    id TEXT PRIMARY KEY,
    listing_id TEXT,
    buyer_email TEXT,
    payment_intent_id TEXT,
    amount INTEGER,
    seller_payout INTEGER,
    platform_payout INTEGER,
    delivery_type TEXT,
    label_tier TEXT,
    label_cost INTEGER DEFAULT 0,
    status TEXT DEFAULT 'pending',
    created_at TEXT DEFAULT (datetime('now'))
  );
  CREATE TABLE IF NOT EXISTS seller_sessions (
    id TEXT PRIMARY KEY,
    email TEXT NOT NULL,
    token TEXT NOT NULL UNIQUE,
    role TEXT DEFAULT 'seller',
    expires_at TEXT NOT NULL,
    used INTEGER DEFAULT 0,
    created_at TEXT DEFAULT (datetime('now'))
  );
  CREATE TABLE IF NOT EXISTS offers (
    id TEXT PRIMARY KEY,
    listing_id TEXT NOT NULL,
    buyer_name TEXT,
    buyer_email TEXT NOT NULL,
    amount INTEGER NOT NULL,
    message TEXT,
    status TEXT DEFAULT 'pending',
    counter_amount INTEGER,
    counter_message TEXT,
    created_at TEXT DEFAULT (datetime('now')),
    updated_at TEXT DEFAULT (datetime('now'))
  );
  CREATE TABLE IF NOT EXISTS messages (
    id TEXT PRIMARY KEY,
    listing_id TEXT NOT NULL,
    offer_id TEXT,
    from_email TEXT NOT NULL,
    from_name TEXT,
    from_role TEXT NOT NULL,
    body TEXT NOT NULL,
    created_at TEXT DEFAULT (datetime('now'))
  );
  CREATE TABLE IF NOT EXISTS listing_alerts (
    id TEXT PRIMARY KEY,
    email TEXT NOT NULL,
    category TEXT,
    max_price INTEGER,
    condition TEXT,
    active INTEGER DEFAULT 1,
    unsubscribe_token TEXT NOT NULL,
    created_at TEXT DEFAULT (datetime('now'))
  );
`);

// Safe migrations for existing DBs
[
  `ALTER TABLE listings ADD COLUMN view_count INTEGER DEFAULT 0`,
  `ALTER TABLE listings ADD COLUMN weight_oz INTEGER DEFAULT 0`,
  `ALTER TABLE sales ADD COLUMN label_tier TEXT`,
  `ALTER TABLE sales ADD COLUMN label_cost INTEGER DEFAULT 0`,
  `ALTER TABLE sales ADD COLUMN platform_payout INTEGER DEFAULT 0`,
  `ALTER TABLE seller_sessions ADD COLUMN role TEXT DEFAULT 'seller'`,
].forEach(sql => { try { db.exec(sql); } catch(e) {} });

// ── SHIPPING LABEL TIERS ─────────────────────────────────────────────────────
// Flat-rate tiers (cents). These are what we CHARGE buyers.
// Actual EasyPost label cost will be less — margin is platform revenue.
const LABEL_TIERS = {
  xs: { label: '0–1 lb',   maxOz: 16,   price: 600  }, // $6
  sm: { label: '1–5 lb',   maxOz: 80,   price: 1000 }, // $10
  md: { label: '5–20 lb',  maxOz: 320,  price: 1600 }, // $16
  lg: { label: '20+ lb',   maxOz: 99999,price: 4500 }, // $45
};

function getLabelTier(weightOz) {
  if (!weightOz || weightOz <= 16)  return { tier: 'xs', ...LABEL_TIERS.xs };
  if (weightOz <= 80)               return { tier: 'sm', ...LABEL_TIERS.sm };
  if (weightOz <= 320)              return { tier: 'md', ...LABEL_TIERS.md };
  return                                   { tier: 'lg', ...LABEL_TIERS.lg };
}

// ── FEE STRUCTURE ────────────────────────────────────────────────────────────
// Seller keeps 88%. GoodKit takes 12% of item price only.
// Shipping passes through at cost (flat tier price charged to buyer).
// Stripe fee (~2.9% + $0.30) comes out of platform's 12%.
function calculateSplit(itemPriceCents, shippingCents) {
  shippingCents = shippingCents || 0;
  const totalCents    = itemPriceCents + shippingCents;
  const stripeFee     = Math.round(totalCents * 0.029 + 30);
  const platformFee   = Math.round(itemPriceCents * 0.12);   // 12% of item only
  const sellerNet     = itemPriceCents - platformFee + shippingCents;
  const platformNet   = Math.max(platformFee - stripeFee, 0);
  return {
    itemPrice:    itemPriceCents / 100,
    shipping:     shippingCents  / 100,
    total:        totalCents     / 100,
    sellerNet:    sellerNet      / 100,
    platformFee:  platformFee    / 100,
    platformNet:  platformNet    / 100,
    stripeFee:    stripeFee      / 100,
    sellerPct:    88,
    platformPct:  12
  };
}

// ── MIDDLEWARE ───────────────────────────────────────────────────────────────
app.use(cors({
  origin: ['https://goodkit.com', 'https://www.goodkit.com', 'http://localhost:3000'],
  credentials: true
}));
app.use('/webhook', express.raw({ type: 'application/json' }));
app.use(express.json());

const uploadDir = process.env.UPLOAD_DIR || './uploads';
if (!fs.existsSync(uploadDir)) fs.mkdirSync(uploadDir, { recursive: true });
const upload = multer({
  storage: multer.diskStorage({
    destination: uploadDir,
    filename: (req, file, cb) => cb(null, uuidv4() + path.extname(file.originalname))
  }),
  limits: { fileSize: 10 * 1024 * 1024 },
  fileFilter: (req, file, cb) => {
    if (file.mimetype.startsWith('image/')) cb(null, true);
    else cb(new Error('Images only'));
  }
});
app.use('/uploads', express.static(uploadDir));

// ── EMAIL ────────────────────────────────────────────────────────────────────
async function sendEmail(to, subject, html) {
  try { await resend.emails.send({ from: FROM_EMAIL, to, subject, html }); }
  catch(err) { console.error('Email error:', err.message); }
}

// GoodKit brand: Trail Burn #FF5C1A, Paper #F5F0E8, Gravel ink #1A1A14
function emailTemplate(title, body) {
  return `<div style="font-family:Arial,sans-serif;max-width:560px;margin:0 auto;background:#F5F0E8;">
    <div style="background:#1A1A14;padding:28px 32px;">
      <p style="font-size:11px;letter-spacing:0.25em;text-transform:uppercase;color:#666;margin:0 0 6px;">GoodKit</p>
      <h1 style="font-size:22px;color:#F5F0E8;margin:0;font-weight:400;">${title}</h1>
    </div>
    <div style="padding:28px 32px;">${body}</div>
    <div style="background:#E8E0D0;padding:16px 32px;">
      <p style="font-size:11px;color:#888;margin:0;">
        goodkit.com · <a href="${BASE_URL}" style="color:#FF5C1A;">it's supposed to be fun.</a>
      </p>
    </div>
  </div>`;
}

// ── LISTING ALERTS ───────────────────────────────────────────────────────────
async function fireListingAlerts(listing) {
  try {
    const alerts = db.prepare("SELECT * FROM listing_alerts WHERE active = 1").all();
    for (const alert of alerts) {
      const priceMatch    = !alert.max_price || listing.price <= alert.max_price;
      const categoryMatch = !alert.category  || alert.category === listing.category;
      const conditionMatch = !alert.condition || alert.condition === listing.condition;
      if (!priceMatch || !categoryMatch || !conditionMatch) continue;
      const unsubUrl = `${BASE_URL}/marketplace?unsubscribe=${alert.unsubscribe_token}`;
      await sendEmail(alert.email, `New listing: ${listing.title}`, emailTemplate('New Listing Alert',
        `<p style="font-size:15px;color:#1A1A14;line-height:1.7;margin:0 0 16px;">A new item matching your alert just dropped on GoodKit.</p>
         <div style="background:#E8E0D0;padding:16px 20px;margin-bottom:20px;">
           <p style="font-size:11px;letter-spacing:0.15em;text-transform:uppercase;color:#888;margin:0 0 4px;">${listing.category}</p>
           <p style="font-size:20px;font-weight:700;color:#1A1A14;margin:0 0 4px;">${listing.title}</p>
           <p style="font-size:13px;color:#888;margin:0 0 12px;">${listing.condition}</p>
           <p style="font-size:28px;font-weight:900;color:#FF5C1A;margin:0;">$${(listing.price/100).toFixed(0)}</p>
         </div>
         <a href="${BASE_URL}/shop" style="display:inline-block;background:#FF5C1A;color:white;padding:14px 28px;font-size:14px;font-weight:600;text-decoration:none;">View Listing →</a>
         <p style="font-size:11px;color:#aaa;margin:20px 0 0;"><a href="${unsubUrl}" style="color:#aaa;">Unsubscribe</a></p>`
      ));
    }
  } catch(err) { console.error('Alert fire error:', err.message); }
}

// ── ROUTES ───────────────────────────────────────────────────────────────────

app.get('/', (req, res) => res.json({ status: 'GoodKit Marketplace running' }));

// Seller onboarding via Stripe Connect Express
app.post('/seller/onboard', async (req, res) => {
  try {
    const { email, name, phone, dob_day, dob_month, dob_year, address_line1, address_city, address_state, address_zip } = req.body;
    if (!email || !name) return res.status(400).json({ error: 'Email and name required' });
    const nameParts  = name.trim().split(' ');
    const individual = { first_name: nameParts[0], last_name: nameParts.slice(1).join(' ') || 'Seller', email };
    if (phone) individual.phone = phone;
    if (dob_day && dob_month && dob_year) individual.dob = { day: parseInt(dob_day), month: parseInt(dob_month), year: parseInt(dob_year) };
    if (address_line1 && address_city && address_zip) individual.address = { line1: address_line1, city: address_city, state: address_state || 'CO', postal_code: address_zip, country: 'US' };
    const account = await stripe.accounts.create({
      type: 'express', country: 'US', email,
      business_type: 'individual', individual,
      capabilities: { card_payments: { requested: true }, transfers: { requested: true } },
      business_profile: {
        url: BASE_URL,
        mcc: '5941',
        product_description: 'Used cycling components, apparel, and gear sold through GoodKit marketplace'
      },
      settings: { payouts: { schedule: { interval: 'manual' } } },
      metadata: { seller_name: name, seller_email: email }
    });
    const accountLink = await stripe.accountLinks.create({
      account: account.id,
      refresh_url: `${BASE_URL}/sell?reauth=true&account=${account.id}`,
      return_url:  `${BASE_URL}/sell?onboarded=true&account=${account.id}`,
      type: 'account_onboarding',
      collection_options: { fields: 'currently_due', future_requirements: 'omit' }
    });
    res.json({ url: accountLink.url, accountId: account.id });
  } catch(err) { console.error('Onboard error:', err); res.status(500).json({ error: err.message }); }
});

app.get('/seller/status/:accountId', async (req, res) => {
  try {
    const account = await stripe.accounts.retrieve(req.params.accountId);
    res.json({ ready: account.charges_enabled && account.payouts_enabled, detailsSubmitted: account.details_submitted });
  } catch(err) { res.status(500).json({ error: err.message }); }
});

// Magic link auth — seller only (no staff role in GoodKit)
app.post('/seller/magic-link', async (req, res) => {
  try {
    const { email } = req.body;
    if (!email) return res.status(400).json({ error: 'Email required' });
    const emailClean = email.toLowerCase().trim();
    const hasListings = db.prepare("SELECT id FROM listings WHERE seller_email = ? LIMIT 1").get(emailClean);
    if (!hasListings) return res.status(404).json({ error: 'No seller account found for this email.' });
    const token     = uuidv4() + uuidv4();
    const expiresAt = new Date(Date.now() + 7 * 24 * 60 * 60 * 1000).toISOString();
    db.prepare("INSERT INTO seller_sessions (id, email, token, role, expires_at) VALUES (?, ?, ?, 'seller', ?)").run(uuidv4(), emailClean, token, expiresAt);
    const link = `${BASE_URL}/seller-portal?token=${token}`;
    await sendEmail(email, 'Your GoodKit Seller Portal Login', emailTemplate('Seller Portal Access',
      `<p style="font-size:15px;color:#1A1A14;line-height:1.7;margin:0 0 20px;">Click below to access your seller portal. Link expires in 30 minutes.</p>
       <a href="${link}" style="display:inline-block;background:#FF5C1A;color:white;padding:14px 28px;font-size:14px;font-weight:600;text-decoration:none;">Access Portal →</a>
       <p style="font-size:12px;color:#888;margin:20px 0 0;">If you didn't request this, ignore this email.</p>`
    ));
    res.json({ success: true, message: 'Login link sent to ' + email });
  } catch(err) { console.error('Magic link error:', err); res.status(500).json({ error: err.message }); }
});

app.get('/seller/verify-token', (req, res) => {
  try {
    const { token } = req.query;
    if (!token) return res.status(400).json({ error: 'Token required' });
    const session = db.prepare("SELECT * FROM seller_sessions WHERE token = ? AND used = 0").get(token);
    if (!session) return res.status(401).json({ error: 'Invalid or expired link.' });
    if (new Date(session.expires_at) < new Date()) return res.status(401).json({ error: 'This link has expired. Please request a new one.' });
    db.prepare("UPDATE seller_sessions SET used = 1 WHERE token = ?").run(token);
    res.json({ success: true, email: session.email, role: 'seller' });
  } catch(err) { res.status(500).json({ error: err.message }); }
});

app.get('/seller/portal', (req, res) => {
  try {
    const { token } = req.query;
    if (!token) return res.status(401).json({ error: 'Token required' });
    const session = db.prepare("SELECT * FROM seller_sessions WHERE token = ? AND used = 1").get(token);
    if (!session || session.role !== 'seller') return res.status(401).json({ error: 'Invalid session' });
    if (new Date(session.expires_at) < new Date()) return res.status(401).json({ error: 'Session expired' });
    const email    = session.email;
    const listings = db.prepare("SELECT * FROM listings WHERE seller_email = ? ORDER BY created_at DESC").all(email)
      .map(l => ({
        ...l,
        photos: JSON.parse(l.photos || '[]'),
        price:  l.price / 100,
        shipping_estimate: (l.shipping_estimate || 0) / 100,
        split:  calculateSplit(l.price, l.shipping_estimate || 0),
        label:  getLabelTier(l.weight_oz)
      }));
    const sales = db.prepare("SELECT s.*, l.title, l.photos FROM sales s JOIN listings l ON s.listing_id=l.id WHERE l.seller_email=? ORDER BY s.created_at DESC").all(email)
      .map(s => ({
        ...s,
        photos:          JSON.parse(s.photos || '[]'),
        amount:          s.amount / 100,
        seller_payout:   s.seller_payout / 100,
        platform_payout: s.platform_payout / 100
      }));
    const offers = db.prepare("SELECT o.*, l.title, l.price as list_price, l.photos FROM offers o JOIN listings l ON o.listing_id=l.id WHERE l.seller_email=? AND o.status IN ('pending','countered') ORDER BY o.created_at DESC").all(email)
      .map(o => ({
        ...o,
        photos:         JSON.parse(o.photos || '[]'),
        amount:         o.amount / 100,
        list_price:     o.list_price / 100,
        counter_amount: o.counter_amount ? o.counter_amount / 100 : null
      }));
    const threads = db.prepare("SELECT m.*, l.title as listing_title FROM messages m JOIN listings l ON m.listing_id=l.id WHERE l.seller_email=? OR m.from_email=? ORDER BY m.created_at DESC").all(email, email);
    res.json({
      success: true,
      seller: { email, name: listings[0]?.seller_name || email },
      stats: {
        totalEarned:    sales.filter(s => s.status === 'paid_out').reduce((sum, s) => sum + s.seller_payout, 0),
        pendingPayout:  sales.filter(s => s.status === 'delivered').reduce((sum, s) => sum + s.seller_payout, 0),
        activeListings: listings.filter(l => l.status === 'approved').length,
        soldListings:   listings.filter(l => l.status === 'sold').length,
        totalViews:     listings.reduce((sum, l) => sum + (l.view_count || 0), 0)
      },
      listings, sales, offers, threads
    });
  } catch(err) { console.error('Portal error:', err); res.status(500).json({ error: err.message }); }
});

// ── LISTINGS ─────────────────────────────────────────────────────────────────

app.post('/listings', upload.array('photos', 8), async (req, res) => {
  try {
    const { seller_name, seller_email, stripe_account_id, title, category, description, condition, price, shipping_estimate, weight_oz } = req.body;
    if (!seller_name || !seller_email || !title || !price) return res.status(400).json({ error: 'Missing required fields' });
    if (!stripe_account_id) return res.status(400).json({ error: 'Seller must complete Stripe onboarding first' });
    const priceInCents  = Math.round(parseFloat(price) * 100);
    const shippingCents = Math.round(parseFloat(shipping_estimate || 0) * 100);
    const weightOz      = parseInt(weight_oz || 0);
    if (priceInCents < 100) return res.status(400).json({ error: 'Minimum price is $1.00' });
    const photos   = req.files ? req.files.map(f => '/uploads/' + f.filename) : [];
    if (!photos.length) return res.status(400).json({ error: 'At least one photo is required' });
    const id       = uuidv4();
    const stripeId = (stripe_account_id && stripe_account_id.trim() !== '') ? stripe_account_id.trim() : null;
    db.prepare(`INSERT INTO listings (id,seller_name,seller_email,stripe_account_id,title,category,description,condition,price,shipping_estimate,weight_oz,photos,status) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,'approved')`)
      .run(id, seller_name.trim(), seller_email.trim(), stripeId, title.trim(), category || 'Other', description || '', condition || 'Good', priceInCents, shippingCents, weightOz, JSON.stringify(photos));
    const newListing = db.prepare("SELECT * FROM listings WHERE id = ?").get(id);
    await fireListingAlerts(newListing);
    await sendEmail(NOTIFY_EMAIL, `[GoodKit] New listing: ${title}`, `Seller: ${seller_name} · ${seller_email}\nItem: ${title}\nPrice: $${priceInCents/100}\nWeight: ${weightOz}oz`);
    const split = calculateSplit(priceInCents, shippingCents);
    const label = getLabelTier(weightOz);
    res.json({ success: true, listingId: id, split, label });
  } catch(err) { console.error('Listing error:', err); res.status(500).json({ error: err.message }); }
});

app.get('/listings', (req, res) => {
  try {
    const { category, maxPrice, condition } = req.query;
    let listings = db.prepare("SELECT * FROM listings WHERE status='approved' ORDER BY created_at DESC").all()
      .map(l => ({
        ...l,
        photos:            JSON.parse(l.photos || '[]'),
        price:             l.price / 100,
        shipping_estimate: (l.shipping_estimate || 0) / 100,
        split:             calculateSplit(l.price, l.shipping_estimate || 0),
        label:             getLabelTier(l.weight_oz)
      }));
    if (category) listings = listings.filter(l => l.category === category);
    if (maxPrice)  listings = listings.filter(l => l.price <= parseFloat(maxPrice));
    if (condition) listings = listings.filter(l => l.condition === condition);
    res.json({ success: true, listings });
  } catch(err) { res.status(500).json({ error: err.message }); }
});

app.get('/listings/:id', (req, res) => {
  try {
    const listing = db.prepare("SELECT * FROM listings WHERE id=? AND status='approved'").get(req.params.id);
    if (!listing) return res.status(404).json({ error: 'Listing not found' });
    db.prepare("UPDATE listings SET view_count=view_count+1 WHERE id=?").run(req.params.id);
    listing.photos            = JSON.parse(listing.photos || '[]');
    listing.price             = listing.price / 100;
    listing.shipping_estimate = (listing.shipping_estimate || 0) / 100;
    listing.split             = calculateSplit(listing.price * 100, listing.shipping_estimate * 100);
    listing.label             = getLabelTier(listing.weight_oz);
    res.json({ success: true, listing });
  } catch(err) { res.status(500).json({ error: err.message }); }
});

app.post('/listings/:id/view', (req, res) => {
  try { db.prepare("UPDATE listings SET view_count=view_count+1 WHERE id=?").run(req.params.id); res.json({ success: true }); }
  catch(err) { res.status(500).json({ error: err.message }); }
});

// ── ALERTS ───────────────────────────────────────────────────────────────────

app.post('/alerts/subscribe', async (req, res) => {
  try {
    const { email, category, maxPrice, condition } = req.body;
    if (!email) return res.status(400).json({ error: 'Email required' });
    const emailClean = email.toLowerCase().trim();
    const existing = db.prepare("SELECT id FROM listing_alerts WHERE email=? AND (category IS ? OR (category IS NULL AND ? IS NULL)) AND active=1").get(emailClean, category || null, category || null);
    if (existing) return res.json({ success: true, message: 'Alert already set up.' });
    const id = uuidv4(), unsubscribeToken = uuidv4();
    db.prepare("INSERT INTO listing_alerts (id,email,category,max_price,condition,unsubscribe_token) VALUES (?,?,?,?,?,?)").run(id, emailClean, category || null, maxPrice ? Math.round(parseFloat(maxPrice) * 100) : null, condition || null, unsubscribeToken);
    await sendEmail(email, 'GoodKit Alert Set Up', emailTemplate('Alert Confirmed ✓',
      `<p style="font-size:15px;color:#1A1A14;line-height:1.7;margin:0 0 16px;">You're all set! We'll notify you when matching gear drops.</p>
       <div style="background:#E8E0D0;padding:14px 18px;margin-bottom:20px;">
         <p style="font-size:14px;color:#1A1A14;margin:0 0 4px;"><strong>Category:</strong> ${category || 'All'}</p>
         ${maxPrice ? `<p style="font-size:14px;color:#1A1A14;margin:0 0 4px;"><strong>Max price:</strong> $${maxPrice}</p>` : ''}
         ${condition ? `<p style="font-size:14px;color:#1A1A14;margin:0;"><strong>Condition:</strong> ${condition}</p>` : ''}
       </div>
       <a href="${BASE_URL}/shop" style="display:inline-block;background:#FF5C1A;color:white;padding:14px 28px;font-size:14px;font-weight:600;text-decoration:none;">Browse GoodKit →</a>
       <p style="font-size:11px;color:#aaa;margin:20px 0 0;"><a href="${BASE_URL}/shop?unsubscribe=${unsubscribeToken}" style="color:#aaa;">Unsubscribe</a></p>`
    ));
    res.json({ success: true, message: "Alert set! We'll email you when matching gear is listed." });
  } catch(err) { res.status(500).json({ error: err.message }); }
});

app.get('/alerts/unsubscribe', (req, res) => {
  try {
    const { token } = req.query;
    if (!token) return res.status(400).json({ error: 'Token required' });
    db.prepare("UPDATE listing_alerts SET active=0 WHERE unsubscribe_token=?").run(token);
    res.json({ success: true, message: 'Unsubscribed successfully.' });
  } catch(err) { res.status(500).json({ error: err.message }); }
});

// ── CHECKOUT ─────────────────────────────────────────────────────────────────
// Confirmed working pattern: application_fee_amount + transfer_data: {destination}
// DO NOT add amount to transfer_data — scopes the PI to connected account and breaks confirmCardPayment
app.post('/checkout', async (req, res) => {
  try {
    const { listingId, buyerEmail, deliveryType } = req.body;
    const listing = db.prepare("SELECT * FROM listings WHERE id=? AND status='approved'").get(listingId);
    if (!listing) return res.status(404).json({ error: 'Listing not found or no longer available' });

    const shippingCents = (deliveryType === 'pickup') ? 0 : (listing.shipping_estimate || 0);
    const split         = calculateSplit(listing.price, shippingCents);
    const totalCharge   = listing.price + shippingCents;
    const labelInfo     = getLabelTier(listing.weight_oz);

    const piParams = {
      amount:               totalCharge,
      currency:             'usd',
      receipt_email:        buyerEmail,
      payment_method_types: ['card'],
      metadata: {
        listingId:    listing.id,
        listingTitle: listing.title,
        sellerEmail:  listing.seller_email,
        deliveryType: deliveryType || 'shipping',
        labelTier:    labelInfo.tier
      }
    };

    const hasStripeAccount = listing.stripe_account_id &&
      listing.stripe_account_id.trim() !== '' &&
      listing.stripe_account_id.startsWith('acct_');

    if (hasStripeAccount) {
      // platform keeps 12% of item price — Stripe sends remainder to seller automatically
      const appFee = Math.round(split.platformFee * 100);
      piParams.application_fee_amount = Math.max(appFee, 0);
      piParams.transfer_data = { destination: listing.stripe_account_id };
    }

    const paymentIntent = await stripe.paymentIntents.create(piParams);

    const saleId = uuidv4();
    db.prepare(`INSERT INTO sales (id,listing_id,buyer_email,payment_intent_id,amount,seller_payout,platform_payout,delivery_type,label_tier) VALUES (?,?,?,?,?,?,?,?,?)`)
      .run(saleId, listingId, buyerEmail, paymentIntent.id, listing.price,
        Math.round(split.sellerNet * 100),
        Math.round(split.platformFee * 100),
        deliveryType || 'shipping',
        labelInfo.tier
      );

    res.json({
      success:        true,
      clientSecret:   paymentIntent.client_secret,
      publishableKey: process.env.STRIPE_PUBLISHABLE_KEY,
      listing: {
        title:  listing.title,
        price:  listing.price / 100,
        photos: JSON.parse(listing.photos || '[]')
      },
      split: {
        seller:      split.sellerNet,
        platform:    split.platformNet,
        sellerPct:   split.sellerPct,
        platformPct: split.platformPct
      },
      label: labelInfo
    });
  } catch(err) { console.error('Checkout error:', err); res.status(500).json({ error: err.message }); }
});

// ── OFFERS ───────────────────────────────────────────────────────────────────

app.post('/offers', async (req, res) => {
  try {
    const { listingId, buyerName, buyerEmail, amount, message } = req.body;
    if (!listingId || !buyerEmail || !amount) return res.status(400).json({ error: 'Missing required fields' });
    const listing = db.prepare("SELECT * FROM listings WHERE id=? AND status='approved'").get(listingId);
    if (!listing) return res.status(404).json({ error: 'Listing not found' });
    const amountCents = Math.round(parseFloat(amount) * 100);
    if (amountCents < 100) return res.status(400).json({ error: 'Minimum offer is $1.00' });
    const id = uuidv4();
    db.prepare("INSERT INTO offers (id,listing_id,buyer_name,buyer_email,amount,message) VALUES (?,?,?,?,?,?)").run(id, listingId, buyerName || buyerEmail, buyerEmail, amountCents, message || '');
    await sendEmail(listing.seller_email, `New offer on your ${listing.title}`, emailTemplate('You Have a New Offer',
      `<p style="font-size:15px;color:#1A1A14;margin:0 0 16px;">Hi ${listing.seller_name}, <strong>${buyerName || buyerEmail}</strong> made an offer on <strong>${listing.title}</strong>.</p>
       <div style="background:#E8E0D0;padding:16px 20px;margin-bottom:20px;">
         <p style="font-size:13px;color:#888;margin:0 0 4px;">Listed price</p>
         <p style="font-size:24px;font-weight:700;color:#1A1A14;margin:0 0 12px;">$${(listing.price/100).toFixed(0)}</p>
         <p style="font-size:13px;color:#888;margin:0 0 4px;">Offer</p>
         <p style="font-size:24px;font-weight:700;color:#FF5C1A;margin:0;">$${parseFloat(amount).toFixed(2)}</p>
         ${message ? `<p style="font-size:13px;color:#555;margin:12px 0 0;font-style:italic;">"${message}"</p>` : ''}
       </div>
       <a href="${BASE_URL}/seller-portal" style="display:inline-block;background:#FF5C1A;color:white;padding:14px 28px;font-size:14px;font-weight:600;text-decoration:none;">Respond in Portal →</a>`
    ));
    await sendEmail(NOTIFY_EMAIL, `[GoodKit] New offer on ${listing.title}`, `Offer: $${amount} from ${buyerEmail}`);
    res.json({ success: true, offerId: id });
  } catch(err) { res.status(500).json({ error: err.message }); }
});

app.post('/offers/:id/respond', async (req, res) => {
  try {
    const { token, action, counterAmount, counterMessage } = req.body;
    const session = db.prepare("SELECT * FROM seller_sessions WHERE token=? AND used=1").get(token);
    if (!session) return res.status(401).json({ error: 'Invalid session' });
    const offer = db.prepare("SELECT o.*, l.title, l.seller_email, l.seller_name, l.price as list_price FROM offers o JOIN listings l ON o.listing_id=l.id WHERE o.id=?").get(req.params.id);
    if (!offer) return res.status(404).json({ error: 'Offer not found' });
    if (offer.seller_email !== session.email) return res.status(403).json({ error: 'Unauthorized' });
    if (action === 'accept') {
      db.prepare("UPDATE offers SET status='accepted', updated_at=datetime('now') WHERE id=?").run(offer.id);
      await sendEmail(offer.buyer_email, `Your offer on ${offer.title} was accepted!`, emailTemplate('Offer Accepted 🎉',
        `<p style="font-size:15px;color:#1A1A14;margin:0 0 16px;">Your offer of <strong>$${(offer.amount/100).toFixed(2)}</strong> on <strong>${offer.title}</strong> was accepted.</p>
         <a href="${BASE_URL}/shop" style="display:inline-block;background:#FF5C1A;color:white;padding:14px 28px;font-size:14px;font-weight:600;text-decoration:none;">Complete Purchase →</a>`
      ));
    } else if (action === 'counter') {
      if (!counterAmount) return res.status(400).json({ error: 'Counter amount required' });
      db.prepare("UPDATE offers SET status='countered', counter_amount=?, counter_message=?, updated_at=datetime('now') WHERE id=?").run(Math.round(parseFloat(counterAmount) * 100), counterMessage || '', offer.id);
      await sendEmail(offer.buyer_email, `Counter offer on ${offer.title}`, emailTemplate('Counter Offer Received',
        `<p style="font-size:15px;color:#1A1A14;margin:0 0 16px;">The seller countered your offer on <strong>${offer.title}</strong>.</p>
         <div style="background:#E8E0D0;padding:16px 20px;margin-bottom:20px;">
           <p style="font-size:13px;color:#888;margin:0 0 4px;">Your offer</p>
           <p style="font-size:20px;font-weight:700;color:#555;margin:0 0 12px;">$${(offer.amount/100).toFixed(2)}</p>
           <p style="font-size:13px;color:#888;margin:0 0 4px;">Counter offer</p>
           <p style="font-size:24px;font-weight:700;color:#FF5C1A;margin:0;">$${parseFloat(counterAmount).toFixed(2)}</p>
           ${counterMessage ? `<p style="font-size:13px;color:#555;margin:12px 0 0;font-style:italic;">"${counterMessage}"</p>` : ''}
         </div>
         <a href="${BASE_URL}/shop" style="display:inline-block;background:#FF5C1A;color:white;padding:14px 28px;font-size:14px;font-weight:600;text-decoration:none;">View Listing →</a>`
      ));
    } else if (action === 'decline') {
      db.prepare("UPDATE offers SET status='declined', updated_at=datetime('now') WHERE id=?").run(offer.id);
      await sendEmail(offer.buyer_email, `Update on your offer for ${offer.title}`, emailTemplate('Offer Update',
        `<p style="font-size:15px;color:#1A1A14;margin:0 0 20px;">The seller declined your offer on <strong>${offer.title}</strong>.</p>
         <a href="${BASE_URL}/shop" style="display:inline-block;background:#FF5C1A;color:white;padding:14px 28px;font-size:14px;font-weight:600;text-decoration:none;">Browse GoodKit →</a>`
      ));
    }
    res.json({ success: true });
  } catch(err) { res.status(500).json({ error: err.message }); }
});

// ── MESSAGES ─────────────────────────────────────────────────────────────────

app.post('/messages', async (req, res) => {
  try {
    const { listingId, offerId, fromEmail, fromName, fromRole, body } = req.body;
    if (!listingId || !fromEmail || !body) return res.status(400).json({ error: 'Missing required fields' });
    const listing = db.prepare("SELECT * FROM listings WHERE id=?").get(listingId);
    if (!listing) return res.status(404).json({ error: 'Listing not found' });
    const id = uuidv4();
    db.prepare("INSERT INTO messages (id,listing_id,offer_id,from_email,from_name,from_role,body) VALUES (?,?,?,?,?,?,?)").run(id, listingId, offerId || null, fromEmail, fromName || fromEmail, fromRole || 'buyer', body);
    if (fromRole === 'buyer') {
      await sendEmail(listing.seller_email, `New message about ${listing.title}`, emailTemplate('New Message',
        `<p style="font-size:15px;color:#1A1A14;margin:0 0 16px;"><strong>${fromName || fromEmail}</strong> asked about <strong>${listing.title}</strong>:</p>
         <div style="background:#E8E0D0;padding:16px 20px;margin-bottom:20px;border-left:3px solid #FF5C1A;">
           <p style="font-size:15px;color:#1A1A14;margin:0;">"${body}"</p>
         </div>
         <a href="${BASE_URL}/seller-portal" style="display:inline-block;background:#FF5C1A;color:white;padding:14px 28px;font-size:14px;font-weight:600;text-decoration:none;">Reply in Portal →</a>`
      ));
      await sendEmail(NOTIFY_EMAIL, `[GoodKit] Message on ${listing.title}`, `From: ${fromEmail}\n${body}`);
    } else {
      const buyerMsg = db.prepare("SELECT from_email FROM messages WHERE listing_id=? AND from_role='buyer' ORDER BY created_at ASC LIMIT 1").get(listingId);
      if (buyerMsg) await sendEmail(buyerMsg.from_email, `Reply about ${listing.title}`, emailTemplate('Message from Seller',
        `<p style="font-size:15px;color:#1A1A14;margin:0 0 16px;">The seller replied about <strong>${listing.title}</strong>:</p>
         <div style="background:#E8E0D0;padding:16px 20px;margin-bottom:20px;border-left:3px solid #1A1A14;">
           <p style="font-size:15px;color:#1A1A14;margin:0;">"${body}"</p>
         </div>
         <a href="${BASE_URL}/shop" style="display:inline-block;background:#FF5C1A;color:white;padding:14px 28px;font-size:14px;font-weight:600;text-decoration:none;">View Listing →</a>`
      ));
    }
    res.json({ success: true, messageId: id });
  } catch(err) { res.status(500).json({ error: err.message }); }
});

app.get('/messages/:listingId', (req, res) => {
  try { res.json({ success: true, messages: db.prepare("SELECT * FROM messages WHERE listing_id=? ORDER BY created_at ASC").all(req.params.listingId) }); }
  catch(err) { res.status(500).json({ error: err.message }); }
});

// ── WEBHOOK ───────────────────────────────────────────────────────────────────

app.post('/webhook', async (req, res) => {
  const sig = req.headers['stripe-signature'];
  let event;
  try { event = stripe.webhooks.constructEvent(req.body, sig, process.env.STRIPE_WEBHOOK_SECRET); }
  catch(err) { return res.status(400).send('Webhook error: ' + err.message); }

  if (event.type === 'payment_intent.succeeded') {
    const pi        = event.data.object;
    const listingId = pi.metadata.listingId;

    db.prepare("UPDATE listings SET status='sold', sold_at=datetime('now') WHERE id=?").run(listingId);
    db.prepare("UPDATE sales SET status='delivered' WHERE payment_intent_id=?").run(pi.id);

    const listing = db.prepare("SELECT * FROM listings WHERE id=?").get(listingId);
    const sale    = db.prepare("SELECT * FROM sales WHERE payment_intent_id=?").get(pi.id);

    if (listing && sale) {
      const deliveryMsg = sale.delivery_type === 'pickup'
        ? '📍 Buyer will pick up — arrange directly with them.'
        : `📦 A prepaid shipping label will be emailed to you shortly. Drop it off at any USPS/UPS location.`;

      await sendEmail(listing.seller_email, `Your ${listing.title} sold! 🎉`, emailTemplate('Item Sold!',
        `<p style="font-size:15px;color:#1A1A14;margin:0 0 16px;">Hi ${listing.seller_name}, your <strong>${listing.title}</strong> just sold!</p>
         <div style="background:#E8E0D0;padding:16px 20px;margin-bottom:20px;">
           <p style="font-size:13px;color:#888;margin:0 0 4px;">Your payout</p>
           <p style="font-size:28px;font-weight:700;color:#1A1A14;margin:0;">$${(sale.seller_payout/100).toFixed(2)}</p>
           <p style="font-size:12px;color:#888;margin:8px 0 0;">${deliveryMsg}</p>
           <p style="font-size:12px;color:#888;margin:4px 0 0;">Transferred automatically 72 hours after delivery confirmation.</p>
         </div>`
      ));
      await sendEmail(NOTIFY_EMAIL, `[GoodKit] SOLD: ${listing.title}`, `Seller: ${listing.seller_email}\nPayout: $${(sale.seller_payout/100).toFixed(2)}\nLabel tier: ${sale.label_tier}`);
    }
    console.log('Sold:', listingId);
  }
  res.json({ received: true });
});

// ── ADMIN ─────────────────────────────────────────────────────────────────────

app.post('/admin/release-payouts', async (req, res) => {
  const { adminKey } = req.body;
  if (adminKey !== process.env.ADMIN_KEY) return res.status(401).json({ error: 'Unauthorized' });
  try {
    const cutoff  = new Date(Date.now() - 72 * 60 * 60 * 1000).toISOString();
    const pending = db.prepare("SELECT s.*, l.stripe_account_id FROM sales s JOIN listings l ON s.listing_id=l.id WHERE s.status='delivered' AND s.created_at<?").all(cutoff);
    let released  = 0;
    for (const sale of pending) {
      try {
        db.prepare("UPDATE sales SET status='paid_out' WHERE id=?").run(sale.id);
        released++;
      } catch(err) { console.error('Payout error', sale.id, err.message); }
    }
    res.json({ success: true, released });
  } catch(err) { res.status(500).json({ error: err.message }); }
});

app.get('/admin/listings', (req, res) => {
  const { adminKey } = req.query;
  if (adminKey !== process.env.ADMIN_KEY) return res.status(401).json({ error: 'Unauthorized' });
  try {
    res.json({
      success: true,
      listings: db.prepare("SELECT * FROM listings ORDER BY created_at DESC").all()
        .map(l => ({ ...l, photos: JSON.parse(l.photos || '[]'), price: l.price / 100, shipping_estimate: (l.shipping_estimate || 0) / 100 }))
    });
  } catch(err) { res.status(500).json({ error: err.message }); }
});

app.patch('/admin/listings/:id', (req, res) => {
  const { adminKey, status } = req.body;
  if (adminKey !== process.env.ADMIN_KEY) return res.status(401).json({ error: 'Unauthorized' });
  try { db.prepare("UPDATE listings SET status=? WHERE id=?").run(status, req.params.id); res.json({ success: true }); }
  catch(err) { res.status(500).json({ error: err.message }); }
});

app.patch('/admin/listings/:id/stripe-account', (req, res) => {
  const { adminKey, stripe_account_id } = req.body;
  if (adminKey !== process.env.ADMIN_KEY) return res.status(401).json({ error: 'Unauthorized' });
  try { db.prepare("UPDATE listings SET stripe_account_id=? WHERE id=?").run(stripe_account_id || null, req.params.id); res.json({ success: true }); }
  catch(err) { res.status(500).json({ error: err.message }); }
});

app.get('/admin/sales', (req, res) => {
  const { adminKey } = req.query;
  if (adminKey !== process.env.ADMIN_KEY) return res.status(401).json({ error: 'Unauthorized' });
  try {
    const sales = db.prepare("SELECT s.*, l.title, l.seller_email, l.seller_name FROM sales s JOIN listings l ON s.listing_id=l.id ORDER BY s.created_at DESC").all()
      .map(s => ({ ...s, amount: s.amount/100, seller_payout: s.seller_payout/100, platform_payout: s.platform_payout/100 }));
    const totalRevenue  = sales.filter(s => s.status !== 'pending').reduce((sum, s) => sum + s.platform_payout, 0);
    const totalPaidOut  = sales.filter(s => s.status === 'paid_out').reduce((sum, s) => sum + s.seller_payout, 0);
    res.json({ success: true, stats: { totalRevenue, totalPaidOut, saleCount: sales.length }, sales });
  } catch(err) { res.status(500).json({ error: err.message }); }
});

// Utility: preview fee split for any price
app.get('/split/:price', (req, res) => {
  const priceInCents = Math.round(parseFloat(req.params.price) * 100);
  if (isNaN(priceInCents) || priceInCents < 100) return res.status(400).json({ error: 'Invalid price' });
  const shippingCents = Math.round(parseFloat(req.query.shipping || 0) * 100);
  const weightOz      = parseInt(req.query.weight || 0);
  const split = calculateSplit(priceInCents, shippingCents);
  const label = getLabelTier(weightOz);
  res.json({ ...split, label });
});

// ── BACKUP ────────────────────────────────────────────────────────────────────

async function runBackup() {
  if (!R2_ENABLED) { console.log('Backup skipped — R2 not configured'); return { success: false, reason: 'R2 not configured' }; }
  try {
    const dbBuffer  = fs.readFileSync(DB_PATH);
    const now       = new Date();
    const timestamp = now.toISOString().replace(/[:.]/g, '-').slice(0, 19);
    const key       = `backups/${now.getFullYear()}/${String(now.getMonth()+1).padStart(2,'0')}/marketplace-${timestamp}.db`;
    await r2.send(new PutObjectCommand({
      Bucket: R2_BUCKET, Key: key, Body: dbBuffer,
      ContentType: 'application/octet-stream',
      Metadata: {
        timestamp:     now.toISOString(),
        db_size:       String(dbBuffer.length),
        listing_count: String(db.prepare("SELECT COUNT(*) as c FROM listings").get().c),
        sale_count:    String(db.prepare("SELECT COUNT(*) as c FROM sales").get().c)
      }
    }));
    console.log(`✓ Backup: ${key} (${(dbBuffer.length/1024).toFixed(1)} KB)`);
    if (now.getDay() === 0) {
      const lc = db.prepare("SELECT COUNT(*) as c FROM listings").get().c;
      const sc = db.prepare("SELECT COUNT(*) as c FROM sales").get().c;
      const po = db.prepare("SELECT SUM(seller_payout) as t FROM sales WHERE status='paid_out'").get().t || 0;
      const pr = db.prepare("SELECT SUM(platform_payout) as t FROM sales WHERE status IN ('delivered','paid_out')").get().t || 0;
      await sendEmail(NOTIFY_EMAIL, 'GoodKit — Weekly Backup', emailTemplate('Weekly Backup ✓',
        `<div style="background:#E8E0D0;padding:16px 20px;">
           <p style="font-size:14px;color:#1A1A14;margin:0 0 6px;"><strong>File:</strong> ${key}</p>
           <p style="font-size:14px;color:#1A1A14;margin:0 0 6px;"><strong>Size:</strong> ${(dbBuffer.length/1024).toFixed(1)} KB</p>
           <p style="font-size:14px;color:#1A1A14;margin:0 0 6px;"><strong>Listings:</strong> ${lc}</p>
           <p style="font-size:14px;color:#1A1A14;margin:0 0 6px;"><strong>Sales:</strong> ${sc}</p>
           <p style="font-size:14px;color:#1A1A14;margin:0 0 6px;"><strong>Total paid out to sellers:</strong> $${(po/100).toFixed(2)}</p>
           <p style="font-size:14px;color:#FF5C1A;margin:0;"><strong>Platform revenue (12% fees):</strong> $${(pr/100).toFixed(2)}</p>
         </div>`
      ));
    }
    return { success: true, key, size: dbBuffer.length };
  } catch(err) { console.error('Backup error:', err.message); return { success: false, error: err.message }; }
}

app.post('/admin/backup', async (req, res) => {
  const { adminKey } = req.body;
  if (adminKey !== process.env.ADMIN_KEY) return res.status(401).json({ error: 'Unauthorized' });
  res.json(await runBackup());
});

app.get('/admin/backups', async (req, res) => {
  const { adminKey } = req.query;
  if (adminKey !== process.env.ADMIN_KEY) return res.status(401).json({ error: 'Unauthorized' });
  try {
    const response = await r2.send(new ListObjectsV2Command({ Bucket: R2_BUCKET, Prefix: 'backups/', MaxKeys: 50 }));
    const files = (response.Contents || []).sort((a, b) => new Date(b.LastModified) - new Date(a.LastModified))
      .map(f => ({ key: f.Key, size: (f.Size/1024).toFixed(1) + ' KB', lastModified: f.LastModified }));
    res.json({ success: true, count: files.length, backups: files });
  } catch(err) { res.status(500).json({ error: err.message }); }
});

// ── START ─────────────────────────────────────────────────────────────────────

app.listen(PORT, () => console.log(`GoodKit Marketplace running on port ${PORT}`));

// Auto-release payouts 72h after delivery (runs hourly)
async function releasePayouts() {
  try {
    const cutoff  = new Date(Date.now() - 72 * 60 * 60 * 1000).toISOString();
    const pending = db.prepare("SELECT s.*, l.stripe_account_id FROM sales s JOIN listings l ON s.listing_id=l.id WHERE s.status='delivered' AND s.created_at<?").all(cutoff);
    for (const sale of pending) {
      try {
        db.prepare("UPDATE sales SET status='paid_out' WHERE id=?").run(sale.id);
        console.log(`Payout released: ${sale.id}`);
      } catch(err) { console.error(`Payout failed: ${sale.id}`, err.message); }
    }
  } catch(err) { console.error('releasePayouts error:', err.message); }
}
setTimeout(() => { releasePayouts(); setInterval(releasePayouts, 60 * 60 * 1000); }, 15000);

// Nightly backup at 2am UTC
function scheduleNightlyBackup() {
  const now   = new Date();
  const next2 = new Date();
  next2.setUTCHours(2, 0, 0, 0);
  if (next2 <= now) next2.setDate(next2.getDate() + 1);
  const msUntil = next2 - now;
  console.log(`Next backup in ${Math.round(msUntil/1000/60)} minutes`);
  setTimeout(() => { runBackup(); setInterval(runBackup, 24 * 60 * 60 * 1000); }, msUntil);
}
scheduleNightlyBackup();
