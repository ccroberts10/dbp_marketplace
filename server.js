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

const app = express();
const PORT = process.env.PORT || 3000;
const BASE_URL = process.env.BASE_URL || 'https://www.durangobikeproject.com';
const resend = new Resend(process.env.RESEND_API_KEY);
const FROM_EMAIL = 'marketplace@durangobikeproject.com';
const NOTIFY_EMAIL = 'durangobikeproject@gmail.com';

// ── DATABASE ──
const DB_PATH = process.env.DB_PATH || './marketplace.db';
const db = new Database(DB_PATH);

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
    photos TEXT,
    listing_type TEXT DEFAULT 'seller',
    dropoff_tier TEXT DEFAULT 'self',
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
    dbp_payout INTEGER,
    devo_payout INTEGER,
    delivery_type TEXT,
    status TEXT DEFAULT 'pending',
    created_at TEXT DEFAULT (datetime('now'))
  );

  CREATE TABLE IF NOT EXISTS seller_sessions (
    id TEXT PRIMARY KEY,
    email TEXT NOT NULL,
    token TEXT NOT NULL UNIQUE,
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
`);

// Add view_count column if upgrading existing DB
try { db.exec(`ALTER TABLE listings ADD COLUMN view_count INTEGER DEFAULT 0`); } catch(e) {}

// ── SPLITS ──
const HANDLING_FEES = { small: 1500, medium: 2500, large: 5000 };

function calculateSplit(itemPriceCents, shippingCents, listingType, dropoffTier) {
  shippingCents = shippingCents || 0;
  const totalCents = itemPriceCents + shippingCents;
  const stripeFee = Math.round(totalCents * 0.029 + 30);

  let sellerItemPct, dbpPct, devoPct;
  if (listingType === 'dbp') {
    sellerItemPct = 0.00; dbpPct = 0.95; devoPct = 0.05;
  } else {
    sellerItemPct = 0.80; dbpPct = 0.15; devoPct = 0.05;
  }

  const handlingFee = dropoffTier && dropoffTier !== 'self' ? (HANDLING_FEES[dropoffTier] || 0) : 0;
  const devo        = Math.round(itemPriceCents * devoPct);
  const sellerItem  = Math.round(itemPriceCents * sellerItemPct);
  const sellerNet   = Math.max(sellerItem + shippingCents - handlingFee, 0);
  const dbpGross    = itemPriceCents - sellerItem - devo;
  const dbpNet      = Math.max(dbpGross - stripeFee, 0);

  return {
    itemPrice: itemPriceCents / 100, shipping: shippingCents / 100,
    total: totalCents / 100, sellerItem: sellerItem / 100,
    sellerNet: sellerNet / 100, devo: devo / 100,
    dbpGross: dbpGross / 100, dbpNet: dbpNet / 100,
    stripeFee: stripeFee / 100, handlingFee: handlingFee / 100,
    sellerPct: Math.round(sellerItemPct * 100),
    dbpPct: Math.round(dbpPct * 100),
    devoPct: Math.round(devoPct * 100)
  };
}

// ── MIDDLEWARE ──
app.use(cors({ origin: ['https://www.durangobikeproject.com', 'http://localhost:3000'] }));
app.use('/webhook', express.raw({ type: 'application/json' }));
app.use(express.json());

const uploadDir = process.env.UPLOAD_DIR || './uploads';
if (!fs.existsSync(uploadDir)) fs.mkdirSync(uploadDir, { recursive: true });

const storage = multer.diskStorage({
  destination: uploadDir,
  filename: (req, file, cb) => cb(null, uuidv4() + path.extname(file.originalname))
});
const upload = multer({
  storage, limits: { fileSize: 10 * 1024 * 1024 },
  fileFilter: (req, file, cb) => {
    if (file.mimetype.startsWith('image/')) cb(null, true);
    else cb(new Error('Images only'));
  }
});
app.use('/uploads', express.static(uploadDir));

// ── EMAIL HELPERS ──
async function sendEmail(to, subject, html) {
  try {
    await resend.emails.send({ from: FROM_EMAIL, to, subject, html });
  } catch(err) {
    console.error('Email error:', err.message);
  }
}

function emailTemplate(title, body) {
  return `
    <div style="font-family:Arial,sans-serif;max-width:560px;margin:0 auto;background:#f4f0e8;padding:0;">
      <div style="background:#1d3a2e;padding:28px 32px;">
        <p style="font-family:Georgia,serif;font-size:11px;letter-spacing:0.25em;text-transform:uppercase;color:#7a9e7e;margin:0 0 6px;">Durango Bike Project</p>
        <h1 style="font-family:Georgia,serif;font-size:22px;color:#f4f0e8;margin:0;font-weight:400;">${title}</h1>
      </div>
      <div style="padding:28px 32px;background:#f4f0e8;">
        ${body}
      </div>
      <div style="background:#e8dcc8;padding:16px 32px;">
        <p style="font-size:11px;color:#8a7a65;margin:0;">225 E 8th Ave, Durango CO · <a href="https://www.durangobikeproject.com/marketplace" style="color:#c0531a;">durangobikeproject.com</a></p>
      </div>
    </div>`;
}

// ── ROUTES ──
app.get('/', (req, res) => res.json({ status: 'DBP Marketplace running' }));

// ── SELLER ONBOARDING ──
app.post('/seller/onboard', async (req, res) => {
  try {
    const { email, name, phone, dob_day, dob_month, dob_year, address_line1, address_city, address_state, address_zip } = req.body;
    if (!email || !name) return res.status(400).json({ error: 'Email and name required' });

    const nameParts = name.trim().split(' ');
    const firstName = nameParts[0];
    const lastName  = nameParts.slice(1).join(' ') || 'Seller';

    const individual = { first_name: firstName, last_name: lastName, email };
    if (phone) individual.phone = phone;
    if (dob_day && dob_month && dob_year) {
      individual.dob = { day: parseInt(dob_day), month: parseInt(dob_month), year: parseInt(dob_year) };
    }
    if (address_line1 && address_city && address_zip) {
      individual.address = { line1: address_line1, city: address_city, state: address_state || 'CO', postal_code: address_zip, country: 'US' };
    }

    const account = await stripe.accounts.create({
      type: 'express', country: 'US', email,
      business_type: 'individual', individual,
      capabilities: { card_payments: { requested: true }, transfers: { requested: true } },
      business_profile: {
        url: BASE_URL, mcc: '5941',
        product_description: 'Used cycling gear and bikes sold through Durango Bike Project marketplace'
      },
      settings: { payouts: { schedule: { interval: 'manual' } } },
      metadata: { seller_name: name, seller_email: email }
    });

    const accountLink = await stripe.accountLinks.create({
      account: account.id,
      refresh_url: `${BASE_URL}/marketplace-sell?reauth=true&account=${account.id}`,
      return_url:  `${BASE_URL}/marketplace-sell?onboarded=true&account=${account.id}`,
      type: 'account_onboarding',
      collection_options: { fields: 'currently_due', future_requirements: 'omit' }
    });

    res.json({ url: accountLink.url, accountId: account.id });
  } catch(err) {
    console.error('Onboard error:', err);
    res.status(500).json({ error: err.message });
  }
});

app.get('/seller/status/:accountId', async (req, res) => {
  try {
    const account = await stripe.accounts.retrieve(req.params.accountId);
    res.json({ ready: account.charges_enabled && account.payouts_enabled, detailsSubmitted: account.details_submitted });
  } catch(err) {
    res.status(500).json({ error: err.message });
  }
});

// ── MAGIC LINK AUTH ──
app.post('/seller/magic-link', async (req, res) => {
  try {
    const { email } = req.body;
    if (!email) return res.status(400).json({ error: 'Email required' });

    // Check if seller has any listings
    const hasListings = db.prepare("SELECT id FROM listings WHERE seller_email = ? LIMIT 1").get(email.toLowerCase().trim());
    if (!hasListings) return res.status(404).json({ error: 'No seller account found for this email.' });

    const token     = uuidv4() + uuidv4(); // long random token
    const expiresAt = new Date(Date.now() + 30 * 60 * 1000).toISOString(); // 30 min

    db.prepare("INSERT INTO seller_sessions (id, email, token, expires_at) VALUES (?, ?, ?, ?)")
      .run(uuidv4(), email.toLowerCase().trim(), token, expiresAt);

    const link = `${BASE_URL}/marketplace-seller?token=${token}`;

    await sendEmail(email, 'Your DBP Seller Portal Login Link', emailTemplate(
      'Seller Portal Access',
      `<p style="font-size:15px;color:#2a1f0e;line-height:1.7;margin:0 0 20px;">Click the button below to access your seller portal. This link expires in 30 minutes.</p>
       <a href="${link}" style="display:inline-block;background:#c0531a;color:white;padding:14px 28px;font-size:14px;font-weight:600;text-decoration:none;letter-spacing:0.05em;">Access My Portal →</a>
       <p style="font-size:12px;color:#8a7a65;margin:20px 0 0;line-height:1.6;">If you didn't request this, you can safely ignore this email.<br>Link: <a href="${link}" style="color:#c0531a;">${link}</a></p>`
    ));

    res.json({ success: true, message: 'Magic link sent to ' + email });
  } catch(err) {
    console.error('Magic link error:', err);
    res.status(500).json({ error: err.message });
  }
});

app.get('/seller/verify-token', (req, res) => {
  try {
    const { token } = req.query;
    if (!token) return res.status(400).json({ error: 'Token required' });

    const session = db.prepare("SELECT * FROM seller_sessions WHERE token = ? AND used = 0").get(token);
    if (!session) return res.status(401).json({ error: 'Invalid or expired link.' });

    const now = new Date();
    if (new Date(session.expires_at) < now) {
      return res.status(401).json({ error: 'This link has expired. Please request a new one.' });
    }

    // Mark token as used but keep session valid for the portal session
    db.prepare("UPDATE seller_sessions SET used = 1 WHERE token = ?").run(token);

    res.json({ success: true, email: session.email });
  } catch(err) {
    res.status(500).json({ error: err.message });
  }
});

// ── SELLER PORTAL DATA ──
app.get('/seller/portal', (req, res) => {
  try {
    const { token } = req.query;
    if (!token) return res.status(401).json({ error: 'Token required' });

    const session = db.prepare("SELECT * FROM seller_sessions WHERE token = ? AND used = 1").get(token);
    if (!session) return res.status(401).json({ error: 'Invalid session' });
    if (new Date(session.expires_at) < new Date()) return res.status(401).json({ error: 'Session expired' });

    const email = session.email;

    // Listings
    const listings = db.prepare("SELECT * FROM listings WHERE seller_email = ? ORDER BY created_at DESC").all(email)
      .map(l => ({
        ...l,
        photos: JSON.parse(l.photos || '[]'),
        price: l.price / 100,
        shipping_estimate: (l.shipping_estimate || 0) / 100,
        split: calculateSplit(l.price, l.shipping_estimate || 0, l.listing_type, l.dropoff_tier)
      }));

    // Sales
    const sales = db.prepare(`
      SELECT s.*, l.title, l.price as list_price, l.photos
      FROM sales s JOIN listings l ON s.listing_id = l.id
      WHERE l.seller_email = ? ORDER BY s.created_at DESC
    `).all(email).map(s => ({
      ...s,
      photos: JSON.parse(s.photos || '[]'),
      amount: s.amount / 100,
      seller_payout: s.seller_payout / 100,
      dbp_payout: s.dbp_payout / 100,
      devo_payout: s.devo_payout / 100
    }));

    // Offers on seller's listings
    const offers = db.prepare(`
      SELECT o.*, l.title, l.price as list_price, l.photos
      FROM offers o JOIN listings l ON o.listing_id = l.id
      WHERE l.seller_email = ? AND o.status IN ('pending','countered')
      ORDER BY o.created_at DESC
    `).all(email).map(o => ({
      ...o,
      photos: JSON.parse(o.photos || '[]'),
      amount: o.amount / 100,
      list_price: o.list_price / 100,
      counter_amount: o.counter_amount ? o.counter_amount / 100 : null
    }));

    // Messages threads
    const threads = db.prepare(`
      SELECT m.*, l.title as listing_title
      FROM messages m JOIN listings l ON m.listing_id = l.id
      WHERE l.seller_email = ? OR m.from_email = ?
      ORDER BY m.created_at DESC
    `).all(email, email);

    // Stats
    const totalEarned = sales.filter(s => s.status === 'paid_out').reduce((sum, s) => sum + s.seller_payout, 0);
    const pendingPayout = sales.filter(s => s.status === 'delivered').reduce((sum, s) => sum + s.seller_payout, 0);
    const activeListings = listings.filter(l => l.status === 'approved').length;
    const soldListings   = listings.filter(l => l.status === 'sold').length;
    const totalViews     = listings.reduce((sum, l) => sum + (l.view_count || 0), 0);

    res.json({
      success: true,
      seller: { email, name: listings[0]?.seller_name || email },
      stats: { totalEarned, pendingPayout, activeListings, soldListings, totalViews, totalListings: listings.length },
      listings, sales, offers, threads
    });
  } catch(err) {
    console.error('Portal error:', err);
    res.status(500).json({ error: err.message });
  }
});

// ── LISTINGS ──
app.post('/listings', upload.array('photos', 8), (req, res) => {
  try {
    const { seller_name, seller_email, stripe_account_id, title, category, description, condition, price, listing_type, dropoff_tier, shipping_estimate } = req.body;

    if (!seller_name || !seller_email || !title || !price) return res.status(400).json({ error: 'Missing required fields' });
    if (!stripe_account_id) return res.status(400).json({ error: 'Seller must complete Stripe onboarding first' });

    const priceInCents    = Math.round(parseFloat(price) * 100);
    const shippingCents   = Math.round(parseFloat(shipping_estimate || 0) * 100);
    const listingTypeSafe = listing_type  || 'seller';
    const dropoffTierSafe = dropoff_tier  || 'self';

    if (priceInCents < 100) return res.status(400).json({ error: 'Minimum price is $1.00' });

    const photos = req.files ? req.files.map(f => '/uploads/' + f.filename) : [];
    const id     = uuidv4();
    const split  = calculateSplit(priceInCents, shippingCents, listingTypeSafe, dropoffTierSafe);

    db.prepare(`
      INSERT INTO listings (id, seller_name, seller_email, stripe_account_id, title, category, description, condition, price, shipping_estimate, photos, listing_type, dropoff_tier, status)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'approved')
    `).run(id, seller_name.trim(), seller_email.trim(), stripe_account_id.trim(), title.trim(),
      category || 'Other', description || '', condition || 'Good',
      priceInCents, shippingCents, JSON.stringify(photos), listingTypeSafe, dropoffTierSafe);

    res.json({
      success: true, listingId: id,
      split: { price: priceInCents / 100, seller: split.sellerNet, dbp: split.dbpNet, devo: split.devo,
               sellerPct: split.sellerPct, dbpPct: split.dbpPct, devoPct: split.devoPct },
      message: 'Listing submitted and live.'
    });
  } catch(err) {
    console.error('Listing error:', err);
    res.status(500).json({ error: err.message });
  }
});

app.get('/listings', (req, res) => {
  try {
    const { category, maxPrice, condition } = req.query;
    let listings = db.prepare("SELECT * FROM listings WHERE status = 'approved' ORDER BY created_at DESC").all()
      .map(l => ({
        ...l,
        photos: JSON.parse(l.photos || '[]'),
        price: l.price / 100,
        shipping_estimate: (l.shipping_estimate || 0) / 100,
        split: calculateSplit(l.price, l.shipping_estimate || 0, l.listing_type, l.dropoff_tier)
      }));

    if (category) listings = listings.filter(l => l.category === category);
    if (maxPrice) listings = listings.filter(l => l.price <= parseFloat(maxPrice));
    if (condition) listings = listings.filter(l => l.condition === condition);

    res.json({ success: true, listings });
  } catch(err) {
    res.status(500).json({ error: err.message });
  }
});

app.get('/listings/:id', (req, res) => {
  try {
    const listing = db.prepare("SELECT * FROM listings WHERE id = ? AND status = 'approved'").get(req.params.id);
    if (!listing) return res.status(404).json({ error: 'Listing not found' });

    // Increment view count
    db.prepare("UPDATE listings SET view_count = view_count + 1 WHERE id = ?").run(req.params.id);

    listing.photos = JSON.parse(listing.photos || '[]');
    listing.price  = listing.price / 100;
    listing.shipping_estimate = (listing.shipping_estimate || 0) / 100;
    listing.split  = calculateSplit(listing.price * 100, listing.shipping_estimate * 100, listing.listing_type, listing.dropoff_tier);
    res.json({ success: true, listing });
  } catch(err) {
    res.status(500).json({ error: err.message });
  }
});

// ── VIEW TRACKING (lightweight — called when modal opens) ──
app.post('/listings/:id/view', (req, res) => {
  try {
    db.prepare("UPDATE listings SET view_count = view_count + 1 WHERE id = ?").run(req.params.id);
    res.json({ success: true });
  } catch(err) {
    res.status(500).json({ error: err.message });
  }
});

// ── CHECKOUT ──
app.post('/checkout', async (req, res) => {
  try {
    const { listingId, buyerEmail, deliveryType } = req.body;
    const listing = db.prepare("SELECT * FROM listings WHERE id = ? AND status = 'approved'").get(listingId);
    if (!listing) return res.status(404).json({ error: 'Listing not found or no longer available' });

    const shippingCents = listing.shipping_estimate || 0;
    const split         = calculateSplit(listing.price, shippingCents, listing.listing_type, listing.dropoff_tier);
    const totalCharge   = listing.price + shippingCents;
    const sellerTransferCents = Math.round(split.sellerNet * 100);

    const paymentIntent = await stripe.paymentIntents.create({
      amount: totalCharge, currency: 'usd', receipt_email: buyerEmail,
      metadata: { listingId: listing.id, listingTitle: listing.title, sellerEmail: listing.seller_email, deliveryType: deliveryType || 'shipping' },
      transfer_data: { destination: listing.stripe_account_id, amount: sellerTransferCents }
    });

    const saleId = uuidv4();
    db.prepare(`INSERT INTO sales (id, listing_id, buyer_email, payment_intent_id, amount, seller_payout, dbp_payout, devo_payout, delivery_type) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`)
      .run(saleId, listingId, buyerEmail, paymentIntent.id, listing.price,
        Math.round(split.sellerNet * 100), Math.round(split.dbpNet * 100), Math.round(split.devo * 100), deliveryType || 'shipping');

    res.json({
      success: true, clientSecret: paymentIntent.client_secret,
      publishableKey: process.env.STRIPE_PUBLISHABLE_KEY,
      listing: { title: listing.title, price: listing.price / 100, photos: JSON.parse(listing.photos || '[]') },
      split: { seller: split.sellerNet, dbp: split.dbpNet, devo: split.devo }
    });
  } catch(err) {
    console.error('Checkout error:', err);
    res.status(500).json({ error: err.message });
  }
});

// ── OFFERS ──
app.post('/offers', async (req, res) => {
  try {
    const { listingId, buyerName, buyerEmail, amount, message } = req.body;
    if (!listingId || !buyerEmail || !amount) return res.status(400).json({ error: 'Missing required fields' });

    const listing = db.prepare("SELECT * FROM listings WHERE id = ? AND status = 'approved'").get(listingId);
    if (!listing) return res.status(404).json({ error: 'Listing not found' });

    const amountCents = Math.round(parseFloat(amount) * 100);
    if (amountCents < 100) return res.status(400).json({ error: 'Minimum offer is $1.00' });

    const id = uuidv4();
    db.prepare("INSERT INTO offers (id, listing_id, buyer_name, buyer_email, amount, message) VALUES (?, ?, ?, ?, ?, ?)")
      .run(id, listingId, buyerName || buyerEmail, buyerEmail, amountCents, message || '');

    // Email seller
    const sellerName = listing.seller_name;
    await sendEmail(listing.seller_email, `New offer on your ${listing.title}`, emailTemplate(
      'You Have a New Offer',
      `<p style="font-size:15px;color:#2a1f0e;line-height:1.7;margin:0 0 16px;">Hi ${sellerName},</p>
       <p style="font-size:15px;color:#2a1f0e;line-height:1.7;margin:0 0 20px;"><strong>${buyerName || buyerEmail}</strong> has made an offer on your listing <strong>${listing.title}</strong>.</p>
       <div style="background:#e8dcc8;padding:16px 20px;margin-bottom:20px;">
         <p style="font-size:13px;color:#8a7a65;margin:0 0 4px;">Listed price</p>
         <p style="font-size:24px;font-weight:700;color:#1d3a2e;margin:0 0 12px;">$${(listing.price / 100).toFixed(0)}</p>
         <p style="font-size:13px;color:#8a7a65;margin:0 0 4px;">Offer amount</p>
         <p style="font-size:24px;font-weight:700;color:#c0531a;margin:0;">${parseFloat(amount).toFixed(2) > 0 ? '$' + parseFloat(amount).toFixed(2) : 'N/A'}</p>
         ${message ? `<p style="font-size:13px;color:#5a4a35;margin:12px 0 0;font-style:italic;">"${message}"</p>` : ''}
       </div>
       <p style="font-size:14px;color:#5a4a35;line-height:1.7;margin:0 0 20px;">Log in to your seller portal to accept, counter, or decline this offer.</p>
       <a href="${BASE_URL}/marketplace-seller" style="display:inline-block;background:#c0531a;color:white;padding:14px 28px;font-size:14px;font-weight:600;text-decoration:none;">View Offer in Portal →</a>`
    ));

    // Notify DBP
    await sendEmail(NOTIFY_EMAIL, `[DBP Marketplace] New offer on ${listing.title}`,
      `Offer: $${amount} from ${buyerEmail} on listing "${listing.title}" (ID: ${listingId})`);

    res.json({ success: true, offerId: id });
  } catch(err) {
    console.error('Offer error:', err);
    res.status(500).json({ error: err.message });
  }
});

app.post('/offers/:id/respond', async (req, res) => {
  try {
    const { token, action, counterAmount, counterMessage } = req.body;
    // Verify seller session
    const session = db.prepare("SELECT * FROM seller_sessions WHERE token = ? AND used = 1").get(token);
    if (!session) return res.status(401).json({ error: 'Invalid session' });

    const offer = db.prepare(`
      SELECT o.*, l.title, l.seller_email, l.seller_name, l.price as list_price
      FROM offers o JOIN listings l ON o.listing_id = l.id
      WHERE o.id = ?
    `).get(req.params.id);

    if (!offer) return res.status(404).json({ error: 'Offer not found' });
    if (offer.seller_email !== session.email) return res.status(403).json({ error: 'Unauthorized' });
    if (offer.status !== 'pending' && offer.status !== 'countered') return res.status(400).json({ error: 'Offer already resolved' });

    if (action === 'accept') {
      db.prepare("UPDATE offers SET status = 'accepted', updated_at = datetime('now') WHERE id = ?").run(offer.id);

      await sendEmail(offer.buyer_email, `Your offer on ${offer.title} was accepted!`, emailTemplate(
        'Offer Accepted 🎉',
        `<p style="font-size:15px;color:#2a1f0e;line-height:1.7;margin:0 0 16px;">Great news! Your offer of <strong>$${(offer.amount / 100).toFixed(2)}</strong> on <strong>${offer.title}</strong> has been accepted.</p>
         <p style="font-size:15px;color:#2a1f0e;line-height:1.7;margin:0 0 20px;">Visit the marketplace to complete your purchase.</p>
         <a href="${BASE_URL}/marketplace" style="display:inline-block;background:#c0531a;color:white;padding:14px 28px;font-size:14px;font-weight:600;text-decoration:none;">Complete Purchase →</a>`
      ));

    } else if (action === 'counter') {
      if (!counterAmount) return res.status(400).json({ error: 'Counter amount required' });
      const counterCents = Math.round(parseFloat(counterAmount) * 100);
      db.prepare("UPDATE offers SET status = 'countered', counter_amount = ?, counter_message = ?, updated_at = datetime('now') WHERE id = ?")
        .run(counterCents, counterMessage || '', offer.id);

      await sendEmail(offer.buyer_email, `Counter offer on ${offer.title}`, emailTemplate(
        'Counter Offer Received',
        `<p style="font-size:15px;color:#2a1f0e;line-height:1.7;margin:0 0 16px;">The seller has responded to your offer on <strong>${offer.title}</strong> with a counter offer.</p>
         <div style="background:#e8dcc8;padding:16px 20px;margin-bottom:20px;">
           <p style="font-size:13px;color:#8a7a65;margin:0 0 4px;">Your offer</p>
           <p style="font-size:20px;font-weight:700;color:#5a4a35;margin:0 0 12px;">$${(offer.amount / 100).toFixed(2)}</p>
           <p style="font-size:13px;color:#8a7a65;margin:0 0 4px;">Counter offer</p>
           <p style="font-size:24px;font-weight:700;color:#c0531a;margin:0;">$${parseFloat(counterAmount).toFixed(2)}</p>
           ${counterMessage ? `<p style="font-size:13px;color:#5a4a35;margin:12px 0 0;font-style:italic;">"${counterMessage}"</p>` : ''}
         </div>
         <a href="${BASE_URL}/marketplace" style="display:inline-block;background:#c0531a;color:white;padding:14px 28px;font-size:14px;font-weight:600;text-decoration:none;">View Listing →</a>`
      ));

    } else if (action === 'decline') {
      db.prepare("UPDATE offers SET status = 'declined', updated_at = datetime('now') WHERE id = ?").run(offer.id);

      await sendEmail(offer.buyer_email, `Update on your offer for ${offer.title}`, emailTemplate(
        'Offer Update',
        `<p style="font-size:15px;color:#2a1f0e;line-height:1.7;margin:0 0 16px;">The seller has declined your offer on <strong>${offer.title}</strong>.</p>
         <p style="font-size:15px;color:#2a1f0e;line-height:1.7;margin:0 0 20px;">There are plenty more great items in the marketplace — come take a look.</p>
         <a href="${BASE_URL}/marketplace" style="display:inline-block;background:#c0531a;color:white;padding:14px 28px;font-size:14px;font-weight:600;text-decoration:none;">Browse Marketplace →</a>`
      ));
    }

    res.json({ success: true });
  } catch(err) {
    console.error('Offer respond error:', err);
    res.status(500).json({ error: err.message });
  }
});

// ── MESSAGES ──
app.post('/messages', async (req, res) => {
  try {
    const { listingId, offerId, fromEmail, fromName, fromRole, body } = req.body;
    if (!listingId || !fromEmail || !body) return res.status(400).json({ error: 'Missing required fields' });

    const listing = db.prepare("SELECT * FROM listings WHERE id = ?").get(listingId);
    if (!listing) return res.status(404).json({ error: 'Listing not found' });

    const id = uuidv4();
    db.prepare("INSERT INTO messages (id, listing_id, offer_id, from_email, from_name, from_role, body) VALUES (?, ?, ?, ?, ?, ?, ?)")
      .run(id, listingId, offerId || null, fromEmail, fromName || fromEmail, fromRole || 'buyer', body);

    // Notify the other party
    if (fromRole === 'buyer') {
      // Email seller
      await sendEmail(listing.seller_email, `New message about your ${listing.title}`, emailTemplate(
        'New Message',
        `<p style="font-size:15px;color:#2a1f0e;line-height:1.7;margin:0 0 16px;"><strong>${fromName || fromEmail}</strong> sent you a message about <strong>${listing.title}</strong>:</p>
         <div style="background:#e8dcc8;padding:16px 20px;margin-bottom:20px;border-left:3px solid #c0531a;">
           <p style="font-size:15px;color:#2a1f0e;line-height:1.7;margin:0;">"${body}"</p>
         </div>
         <a href="${BASE_URL}/marketplace-seller" style="display:inline-block;background:#c0531a;color:white;padding:14px 28px;font-size:14px;font-weight:600;text-decoration:none;">Reply in Portal →</a>`
      ));
      await sendEmail(NOTIFY_EMAIL, `[DBP] Message on ${listing.title}`, `From: ${fromEmail}\nListing: ${listing.title}\nMessage: ${body}`);
    } else {
      // Email buyer — find most recent message from buyer on this listing
      const buyerMsg = db.prepare("SELECT from_email FROM messages WHERE listing_id = ? AND from_role = 'buyer' ORDER BY created_at ASC LIMIT 1").get(listingId);
      if (buyerMsg) {
        await sendEmail(buyerMsg.from_email, `Reply about ${listing.title}`, emailTemplate(
          'Message from Seller',
          `<p style="font-size:15px;color:#2a1f0e;line-height:1.7;margin:0 0 16px;">The seller replied to your message about <strong>${listing.title}</strong>:</p>
           <div style="background:#e8dcc8;padding:16px 20px;margin-bottom:20px;border-left:3px solid #1d3a2e;">
             <p style="font-size:15px;color:#2a1f0e;line-height:1.7;margin:0;">"${body}"</p>
           </div>
           <a href="${BASE_URL}/marketplace" style="display:inline-block;background:#c0531a;color:white;padding:14px 28px;font-size:14px;font-weight:600;text-decoration:none;">View Listing →</a>`
        ));
      }
    }

    res.json({ success: true, messageId: id });
  } catch(err) {
    console.error('Message error:', err);
    res.status(500).json({ error: err.message });
  }
});

app.get('/messages/:listingId', (req, res) => {
  try {
    const messages = db.prepare("SELECT * FROM messages WHERE listing_id = ? ORDER BY created_at ASC").all(req.params.listingId);
    res.json({ success: true, messages });
  } catch(err) {
    res.status(500).json({ error: err.message });
  }
});

// ── STRIPE WEBHOOK ──
app.post('/webhook', async (req, res) => {
  const sig = req.headers['stripe-signature'];
  let event;
  try {
    event = stripe.webhooks.constructEvent(req.body, sig, process.env.STRIPE_WEBHOOK_SECRET);
  } catch(err) {
    return res.status(400).send('Webhook error: ' + err.message);
  }

  if (event.type === 'payment_intent.succeeded') {
    const pi = event.data.object;
    const listingId = pi.metadata.listingId;
    db.prepare("UPDATE listings SET status = 'sold', sold_at = datetime('now') WHERE id = ?").run(listingId);
    db.prepare("UPDATE sales SET status = 'delivered' WHERE payment_intent_id = ?").run(pi.id);

    // Email seller that item sold
    const listing = db.prepare("SELECT * FROM listings WHERE id = ?").get(listingId);
    const sale    = db.prepare("SELECT * FROM sales WHERE payment_intent_id = ?").get(pi.id);
    if (listing && sale) {
      await sendEmail(listing.seller_email, `Your ${listing.title} sold! 🎉`, emailTemplate(
        'Item Sold!',
        `<p style="font-size:15px;color:#2a1f0e;line-height:1.7;margin:0 0 16px;">Hi ${listing.seller_name},</p>
         <p style="font-size:15px;color:#2a1f0e;line-height:1.7;margin:0 0 20px;">Your <strong>${listing.title}</strong> just sold!</p>
         <div style="background:#e8dcc8;padding:16px 20px;margin-bottom:20px;">
           <p style="font-size:13px;color:#8a7a65;margin:0 0 4px;">Your payout</p>
           <p style="font-size:28px;font-weight:700;color:#1d3a2e;margin:0;">$${(sale.seller_payout / 100).toFixed(2)}</p>
           <p style="font-size:12px;color:#8a7a65;margin:8px 0 0;">Transferred automatically 72 hours after delivery confirmation.</p>
         </div>
         ${listing.dropoff_tier && listing.dropoff_tier !== 'self'
           ? `<p style="font-size:14px;color:#c0531a;font-weight:600;margin:0 0 16px;">📦 Please drop off your item at DBP (225 E 8th Ave) within 5 days.</p>`
           : `<p style="font-size:14px;color:#5a4a35;line-height:1.7;margin:0 0 16px;">Please ship your item promptly and mark it shipped.</p>`}
         <a href="${BASE_URL}/marketplace-seller" style="display:inline-block;background:#c0531a;color:white;padding:14px 28px;font-size:14px;font-weight:600;text-decoration:none;">View in Portal →</a>`
      ));
    }
    console.log('Payment confirmed, listing marked sold:', listingId);
  }

  res.json({ received: true });
});

// ── ADMIN ──
app.post('/admin/release-payouts', async (req, res) => {
  const { adminKey } = req.body;
  if (adminKey !== process.env.ADMIN_KEY) return res.status(401).json({ error: 'Unauthorized' });
  try {
    const cutoff = new Date(Date.now() - 72 * 60 * 60 * 1000).toISOString();
    const pending = db.prepare(
      "SELECT s.*, l.stripe_account_id, l.title FROM sales s JOIN listings l ON s.listing_id = l.id WHERE s.status = 'delivered' AND s.created_at < ?"
    ).all(cutoff);
    let released = 0;
    for (const sale of pending) {
      try {
        if (process.env.DEVO_ACCOUNT_ID && sale.devo_payout > 0) {
          await stripe.transfers.create({
            amount: sale.devo_payout, currency: 'usd',
            destination: process.env.DEVO_ACCOUNT_ID,
            description: 'Durango Devo — ' + sale.listing_id,
            idempotency_key: `devo-${sale.id}`
          });
        }
        db.prepare("UPDATE sales SET status = 'paid_out' WHERE id = ?").run(sale.id);
        released++;
      } catch(err) {
        console.error('Payout error for sale', sale.id, err.message);
      }
    }
    res.json({ success: true, released });
  } catch(err) {
    res.status(500).json({ error: err.message });
  }
});

app.get('/admin/listings', (req, res) => {
  const { adminKey } = req.query;
  if (adminKey !== process.env.ADMIN_KEY) return res.status(401).json({ error: 'Unauthorized' });
  try {
    const listings = db.prepare("SELECT * FROM listings ORDER BY created_at DESC").all()
      .map(l => ({ ...l, photos: JSON.parse(l.photos || '[]'), price: l.price / 100, shipping_estimate: (l.shipping_estimate || 0) / 100 }));
    res.json({ success: true, listings });
  } catch(err) {
    res.status(500).json({ error: err.message });
  }
});

app.patch('/admin/listings/:id', (req, res) => {
  const { adminKey, status } = req.body;
  if (adminKey !== process.env.ADMIN_KEY) return res.status(401).json({ error: 'Unauthorized' });
  try {
    db.prepare("UPDATE listings SET status = ? WHERE id = ?").run(status, req.params.id);
    res.json({ success: true });
  } catch(err) {
    res.status(500).json({ error: err.message });
  }
});

app.get('/split/:price', (req, res) => {
  const priceInCents = Math.round(parseFloat(req.params.price) * 100);
  if (isNaN(priceInCents) || priceInCents < 100) return res.status(400).json({ error: 'Invalid price' });
  const split = calculateSplit(priceInCents, Math.round(parseFloat(req.query.shipping || 0) * 100), req.query.type || 'seller', req.query.dropoff || 'self');
  res.json({ price: split.itemPrice, shipping: split.shipping, total: split.total, seller: split.sellerItem, sellerNet: split.sellerNet, devo: split.devo, dbp: split.dbpNet, stripeFee: split.stripeFee, handlingFee: split.handlingFee, sellerPct: split.sellerPct, dbpPct: split.dbpPct, devoPct: split.devoPct });
});

app.listen(PORT, () => console.log(`DBP Marketplace running on port ${PORT}`));

// ── AUTO PAYOUT ──
async function releasePayouts() {
  try {
    const cutoff = new Date(Date.now() - 72 * 60 * 60 * 1000).toISOString();
    const pending = db.prepare(
      "SELECT s.*, l.stripe_account_id, l.title, l.seller_email, l.seller_name FROM sales s JOIN listings l ON s.listing_id = l.id WHERE s.status = 'delivered' AND s.created_at < ?"
    ).all(cutoff);
    for (const sale of pending) {
      try {
        if (process.env.DEVO_ACCOUNT_ID && sale.devo_payout > 0) {
          await stripe.transfers.create({
            amount: sale.devo_payout, currency: 'usd',
            destination: process.env.DEVO_ACCOUNT_ID,
            description: 'Durango Devo — ' + sale.listing_id,
            idempotency_key: `devo-${sale.id}`
          });
        }
        db.prepare("UPDATE sales SET status = 'paid_out' WHERE id = ?").run(sale.id);
        console.log(`Payout released for sale ${sale.id}`);
      } catch(err) {
        console.error(`Payout failed for sale ${sale.id}:`, err.message);
      }
    }
  } catch(err) {
    console.error('releasePayouts error:', err.message);
  }
}

setTimeout(() => {
  releasePayouts();
  setInterval(releasePayouts, 60 * 60 * 1000);
}, 15000); // 15s delay on startup
