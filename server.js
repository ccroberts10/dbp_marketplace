require('dotenv').config();
const express = require('express');
const cors = require('cors');
const stripe = require('stripe')(process.env.STRIPE_SECRET_KEY);
const Database = require('better-sqlite3');
const multer = require('multer');
const path = require('path');
const fs = require('fs');
const { v4: uuidv4 } = require('uuid');

const app = express();
const PORT = process.env.PORT || 3000;

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
    status TEXT DEFAULT 'pending',
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
`);

// ── SPLITS ──
// Durango Devo always gets 5%
// DBP and seller split the remaining 95% based on price tier
function calculateSplit(priceInCents) {
  const price = priceInCents / 100;
  let sellerPct, dbpPct;

  if (price < 100) {
    sellerPct = 0.60; dbpPct = 0.35;
  } else if (price < 500) {
    sellerPct = 0.65; dbpPct = 0.30;
  } else if (price < 1500) {
    sellerPct = 0.70; dbpPct = 0.25;
  } else {
    sellerPct = 0.75; dbpPct = 0.20;
  }

  const devoPct = 0.05;
  const stripeFee = Math.round(priceInCents * 0.029 + 30); // 2.9% + 30c

  const devo   = Math.round(priceInCents * devoPct);
  const seller = Math.round(priceInCents * sellerPct);
  const dbp    = priceInCents - seller - devo - stripeFee;

  return { seller, dbp: Math.max(dbp, 0), devo, stripeFee,
           sellerPct, dbpPct, devoPct };
}

// ── MIDDLEWARE ──
app.use(cors({
  origin: ['https://www.durangobikeproject.com', 'http://localhost:3000']
}));

// Raw body for Stripe webhooks
app.use('/webhook', express.raw({ type: 'application/json' }));
app.use(express.json());

// Photo uploads — stored in /uploads
const uploadDir = './uploads';
if (!fs.existsSync(uploadDir)) fs.mkdirSync(uploadDir);

const storage = multer.diskStorage({
  destination: uploadDir,
  filename: (req, file, cb) => {
    cb(null, uuidv4() + path.extname(file.originalname));
  }
});
const upload = multer({
  storage,
  limits: { fileSize: 10 * 1024 * 1024 }, // 10MB per file
  fileFilter: (req, file, cb) => {
    if (file.mimetype.startsWith('image/')) cb(null, true);
    else cb(new Error('Images only'));
  }
});

// Serve uploaded photos publicly
app.use('/uploads', express.static('./uploads'));

// ── ROUTES ──

// Health check
app.get('/', (req, res) => res.json({ status: 'DBP Marketplace running' }));

// ── SELLER ONBOARDING ──
// Creates a Stripe Connect Express account and returns onboarding URL
app.post('/seller/onboard', async (req, res) => {
  try {
    const { email, name } = req.body;
    if (!email || !name) return res.status(400).json({ error: 'Email and name required' });

    // Create Express connected account
    const account = await stripe.accounts.create({
      type: 'express',
      email,
      capabilities: { transfers: { requested: true } },
      business_type: 'individual',
      metadata: { seller_name: name }
    });

    // Generate onboarding link
    const accountLink = await stripe.accountLinks.create({
      account: account.id,
      refresh_url: `${process.env.BASE_URL || 'https://www.durangobikeproject.com'}/marketplace/sell?reauth=true`,
      return_url:  `${process.env.BASE_URL || 'https://www.durangobikeproject.com'}/marketplace/sell?onboarded=true&account=${account.id}`,
      type: 'account_onboarding'
    });

    res.json({ url: accountLink.url, accountId: account.id });
  } catch (err) {
    console.error('Onboard error:', err);
    res.status(500).json({ error: err.message });
  }
});

// Check if seller account is fully onboarded
app.get('/seller/status/:accountId', async (req, res) => {
  try {
    const account = await stripe.accounts.retrieve(req.params.accountId);
    res.json({
      ready: account.charges_enabled && account.payouts_enabled,
      detailsSubmitted: account.details_submitted
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ── LISTINGS ──

// Create a new listing (with photo upload)
app.post('/listings', upload.array('photos', 8), (req, res) => {
  try {
    const {
      seller_name, seller_email, stripe_account_id,
      title, category, description, condition, price
    } = req.body;

    if (!seller_name || !seller_email || !title || !price) {
      return res.status(400).json({ error: 'Missing required fields' });
    }

    if (!stripe_account_id) {
      return res.status(400).json({ error: 'Seller must complete Stripe onboarding first' });
    }

    const priceInCents = Math.round(parseFloat(price) * 100);
    if (priceInCents < 100) return res.status(400).json({ error: 'Minimum price is $1' });

    const photos = req.files ? req.files.map(f => '/uploads/' + f.filename) : [];
    const id = uuidv4();
    const split = calculateSplit(priceInCents);

    db.prepare(`
      INSERT INTO listings (id, seller_name, seller_email, stripe_account_id, title, category, description, condition, price, photos, status)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending')
    `).run(id, seller_name, seller_email, stripe_account_id, title, category, description, condition, priceInCents, JSON.stringify(photos));

    res.json({
      success: true,
      listingId: id,
      split: {
        price: priceInCents / 100,
        seller: split.seller / 100,
        dbp: split.dbp / 100,
        devo: split.devo / 100,
        sellerPct: Math.round(split.sellerPct * 100),
        dbpPct: Math.round(split.dbpPct * 100)
      },
      message: 'Listing submitted for approval. We will review and publish within 24 hours.'
    });
  } catch (err) {
    console.error('Listing error:', err);
    res.status(500).json({ error: err.message });
  }
});

// Get all approved listings (public browse page)
app.get('/listings', (req, res) => {
  try {
    const { category, maxPrice, condition } = req.query;
    let query = "SELECT * FROM listings WHERE status = 'approved' ORDER BY created_at DESC";
    const listings = db.prepare(query).all().map(l => ({
      ...l,
      photos: JSON.parse(l.photos || '[]'),
      price: l.price / 100,
      split: calculateSplit(l.price)
    }));

    let filtered = listings;
    if (category) filtered = filtered.filter(l => l.category === category);
    if (maxPrice) filtered = filtered.filter(l => l.price <= parseFloat(maxPrice));
    if (condition) filtered = filtered.filter(l => l.condition === condition);

    res.json({ success: true, listings: filtered });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Get single listing
app.get('/listings/:id', (req, res) => {
  try {
    const listing = db.prepare("SELECT * FROM listings WHERE id = ? AND status = 'approved'").get(req.params.id);
    if (!listing) return res.status(404).json({ error: 'Listing not found' });
    listing.photos = JSON.parse(listing.photos || '[]');
    listing.price = listing.price / 100;
    listing.split = calculateSplit(listing.price * 100);
    res.json({ success: true, listing });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ── ADMIN — approve/reject listings ──
app.post('/admin/listings/:id/approve', (req, res) => {
  const { adminKey } = req.body;
  if (adminKey !== process.env.ADMIN_KEY) return res.status(401).json({ error: 'Unauthorized' });
  db.prepare("UPDATE listings SET status = 'approved' WHERE id = ?").run(req.params.id);
  res.json({ success: true });
});

app.post('/admin/listings/:id/reject', (req, res) => {
  const { adminKey } = req.body;
  if (adminKey !== process.env.ADMIN_KEY) return res.status(401).json({ error: 'Unauthorized' });
  db.prepare("UPDATE listings SET status = 'rejected' WHERE id = ?").run(req.params.id);
  res.json({ success: true });
});

// Get all pending listings for admin review
app.post('/admin/listings/pending', (req, res) => {
  const { adminKey } = req.body;
  if (adminKey !== process.env.ADMIN_KEY) return res.status(401).json({ error: 'Unauthorized' });
  const listings = db.prepare("SELECT * FROM listings WHERE status = 'pending' ORDER BY created_at DESC").all()
    .map(l => ({ ...l, photos: JSON.parse(l.photos || '[]'), price: l.price / 100 }));
  res.json({ success: true, listings });
});

// ── CHECKOUT ──
// Create Stripe payment intent with automatic split
app.post('/checkout', async (req, res) => {
  try {
    const { listingId, buyerEmail, deliveryType } = req.body;

    const listing = db.prepare("SELECT * FROM listings WHERE id = ? AND status = 'approved'").get(listingId);
    if (!listing) return res.status(404).json({ error: 'Listing not found or unavailable' });

    const split = calculateSplit(listing.price);
    const devoPct = process.env.DEVO_ACCOUNT_ID ? split.devo : 0;

    // Create payment intent with automatic transfer to seller
    const paymentIntent = await stripe.paymentIntents.create({
      amount: listing.price,
      currency: 'usd',
      receipt_email: buyerEmail,
      metadata: {
        listingId: listing.id,
        listingTitle: listing.title,
        sellerEmail: listing.seller_email,
        deliveryType: deliveryType || 'shipping'
      },
      transfer_data: {
        destination: listing.stripe_account_id,
        amount: split.seller
      }
    });

    // Record sale
    const saleId = uuidv4();
    db.prepare(`
      INSERT INTO sales (id, listing_id, buyer_email, payment_intent_id, amount, seller_payout, dbp_payout, devo_payout, delivery_type)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    `).run(saleId, listingId, buyerEmail, paymentIntent.id, listing.price, split.seller, split.dbp, split.devo, deliveryType || 'shipping');

    res.json({
      success: true,
      clientSecret: paymentIntent.client_secret,
      publishableKey: process.env.STRIPE_PUBLISHABLE_KEY,
      listing: {
        title: listing.title,
        price: listing.price / 100,
        photos: JSON.parse(listing.photos || '[]')
      },
      split: {
        seller: split.seller / 100,
        dbp: split.dbp / 100,
        devo: split.devo / 100
      }
    });
  } catch (err) {
    console.error('Checkout error:', err);
    res.status(500).json({ error: err.message });
  }
});

// ── STRIPE WEBHOOK ──
app.post('/webhook', async (req, res) => {
  const sig = req.headers['stripe-signature'];
  let event;

  try {
    event = stripe.webhooks.constructEvent(req.body, sig, process.env.STRIPE_WEBHOOK_SECRET);
  } catch (err) {
    return res.status(400).send('Webhook error: ' + err.message);
  }

  if (event.type === 'payment_intent.succeeded') {
    const pi = event.data.object;
    const listingId = pi.metadata.listingId;

    // Mark listing as sold
    db.prepare("UPDATE listings SET status = 'sold', sold_at = datetime('now') WHERE id = ?").run(listingId);

    // Mark sale as complete
    db.prepare("UPDATE sales SET status = 'completed' WHERE payment_intent_id = ?").run(pi.id);

    // If Devo account is set up, send them their share
    if (process.env.DEVO_ACCOUNT_ID) {
      const sale = db.prepare("SELECT * FROM sales WHERE payment_intent_id = ?").get(pi.id);
      if (sale && sale.devo_payout > 0) {
        await stripe.transfers.create({
          amount: sale.devo_payout,
          currency: 'usd',
          destination: process.env.DEVO_ACCOUNT_ID,
          description: 'Durango Devo share — ' + pi.metadata.listingTitle
        });
      }
    }

    console.log('Sale completed:', listingId);
  }

  res.json({ received: true });
});

// ── SPLIT CALCULATOR (public) ──
app.get('/split/:price', (req, res) => {
  const priceInCents = Math.round(parseFloat(req.params.price) * 100);
  if (isNaN(priceInCents) || priceInCents < 100) {
    return res.status(400).json({ error: 'Invalid price' });
  }
  const split = calculateSplit(priceInCents);
  res.json({
    price: priceInCents / 100,
    seller: split.seller / 100,
    dbp: split.dbp / 100,
    devo: split.devo / 100,
    sellerPct: Math.round(split.sellerPct * 100),
    dbpPct: Math.round(split.dbpPct * 100),
    devoPct: 5
  });
});

app.listen(PORT, () => console.log(`DBP Marketplace running on port ${PORT}`));
