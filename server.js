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
    listing_type TEXT DEFAULT 'seller',
    dropoff_tier TEXT DEFAULT 'self',
    shipping_estimate INTEGER DEFAULT 0,
    status TEXT DEFAULT 'approved',
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
// Handling fee tiers for drop-off shipping service (in cents)
const HANDLING_FEES = {
  small:  1500,  // $15 — cassette, stem, small parts
  medium: 2500,  // $25 — wheels, fork, groupset
  large:  5000   // $50 — complete bike
};

// Calculate split on ITEM PRICE ONLY — shipping passes through to seller
// Commission: Seller 80%, DBP ~10% (covers Stripe), Devo 5%
// DBP absorbs Stripe fees (~2.9% + $0.30) out of its ~15% cut
function calculateSplit(itemPriceCents, shippingCents, listingType, dropoffTier) {
  shippingCents = shippingCents || 0;
  const totalCents = itemPriceCents + shippingCents;

  // Stripe fee on full transaction (item + shipping)
  const stripeFee = Math.round(totalCents * 0.029 + 30);

  let sellerItemPct, dbpPct, devoPct;

  if (listingType === 'dbp') {
    // DBP own inventory — no seller, 95% DBP, 5% Devo
    sellerItemPct = 0.00;
    dbpPct        = 0.95;
    devoPct       = 0.05;
  } else {
    // Self-serve: 80% seller, 5% Devo, remainder to DBP (covers Stripe)
    sellerItemPct = 0.80;
    dbpPct        = 0.15; // DBP gets 15% then pays Stripe from it
    devoPct       = 0.05;
  }

  // Handling fee deducted from seller payout if drop-off chosen
  const handlingFee = dropoffTier && dropoffTier !== 'self'
    ? (HANDLING_FEES[dropoffTier] || 0)
    : 0;

  const devo         = Math.round(itemPriceCents * devoPct);
  const sellerItem   = Math.round(itemPriceCents * sellerItemPct);
  const sellerNet    = sellerItem + shippingCents - handlingFee; // seller gets item% + shipping passthrough - handling
  const dbpGross     = itemPriceCents - sellerItem - devo;
  const dbpNet       = dbpGross - stripeFee; // DBP absorbs Stripe

  return {
    itemPrice:    itemPriceCents / 100,
    shipping:     shippingCents / 100,
    total:        totalCents / 100,
    sellerItem:   sellerItem / 100,
    sellerNet:    sellerNet / 100,
    devo:         devo / 100,
    dbpGross:     dbpGross / 100,
    dbpNet:       Math.max(dbpNet, 0) / 100,
    stripeFee:    stripeFee / 100,
    handlingFee:  handlingFee / 100,
    sellerPct:    Math.round(sellerItemPct * 100),
    dbpPct:       Math.round(dbpPct * 100),
    devoPct:      Math.round(devoPct * 100)
  };
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

    const { listing_type, dropoff_tier, shipping_estimate } = req.body;
    const priceInCents = Math.round(parseFloat(price) * 100);
    const shippingCents = Math.round(parseFloat(shipping_estimate || 0) * 100);
    if (priceInCents < 100) return res.status(400).json({ error: 'Minimum price is $1' });

    const photos = req.files ? req.files.map(f => '/uploads/' + f.filename) : [];
    const id = uuidv4();
    const split = calculateSplit(priceInCents, listing_type);

    db.prepare(`
      INSERT INTO listings (id, seller_name, seller_email, stripe_account_id, title, category, description, condition, price, shipping_estimate, photos, listing_type, dropoff_tier, status)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'approved')
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
      shipping_estimate: (l.shipping_estimate || 0) / 100,
      split: calculateSplit(l.price, l.shipping_estimate || 0, l.listing_type, l.dropoff_tier)
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
    listing.shipping_estimate = (listing.shipping_estimate || 0) / 100;
    listing.split = calculateSplit(listing.price * 100, listing.shipping_estimate * 100, listing.listing_type, listing.dropoff_tier);
    res.json({ success: true, listing });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Admin routes removed — listings auto-approved on submission

// ── CHECKOUT ──
// Create Stripe payment intent with automatic split
app.post('/checkout', async (req, res) => {
  try {
    const { listingId, buyerEmail, deliveryType } = req.body;

    const listing = db.prepare("SELECT * FROM listings WHERE id = ? AND status = 'approved'").get(listingId);
    if (!listing) return res.status(404).json({ error: 'Listing not found or unavailable' });

    const shippingCents = listing.shipping_estimate || 0;
    const split = calculateSplit(listing.price, shippingCents, listing.listing_type, listing.dropoff_tier);

    // Total charge = item price + shipping
    const totalCharge = listing.price + shippingCents;

    // Seller transfer = 80% of item price + shipping - handling fee
    const sellerTransferCents = Math.round(split.sellerNet * 100);

    // Create payment intent with automatic transfer to seller
    const paymentIntent = await stripe.paymentIntents.create({
      amount: totalCharge,
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
    db.prepare("UPDATE sales SET status = 'delivered' WHERE payment_intent_id = ?").run(pi.id);
    console.log('Payment confirmed, 72hr buyer window started:', listingId);
  }

  // charge.updated or manual: release payout 72hrs after delivery
  // In production wire this to your shipping carrier webhook or a cron job
  // For now payout is held via Stripe's transfer delay settings

  res.json({ received: true });
});

// ── CRON-STYLE: release payouts 72hrs after delivery ──
// Call this endpoint from a Railway cron or external scheduler every hour
app.post('/admin/release-payouts', async (req, res) => {
  const { adminKey } = req.body;
  if (adminKey !== process.env.ADMIN_KEY) return res.status(401).json({ error: 'Unauthorized' });

  try {
    // Find sales delivered 72+ hours ago that haven't been paid out
    const cutoff = new Date(Date.now() - 72 * 60 * 60 * 1000).toISOString();
    const pending = db.prepare(
      "SELECT s.*, l.stripe_account_id, l.title FROM sales s JOIN listings l ON s.listing_id = l.id WHERE s.status = 'delivered' AND s.created_at < ?"
    ).all(cutoff);

    let released = 0;
    for (const sale of pending) {
      try {
        // Transfer Devo share
        if (process.env.DEVO_ACCOUNT_ID && sale.devo_payout > 0) {
          await stripe.transfers.create({
            amount: sale.devo_payout,
            currency: 'usd',
            destination: process.env.DEVO_ACCOUNT_ID,
            description: 'Durango Devo — ' + sale.listing_id
          });
        }
        // Mark as paid out
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

// ── SPLIT CALCULATOR (public) ──
app.get('/split/:price', (req, res) => {
  const priceInCents = Math.round(parseFloat(req.params.price) * 100);
  if (isNaN(priceInCents) || priceInCents < 100) {
    return res.status(400).json({ error: 'Invalid price' });
  }
  const listingType  = req.query.type     || 'seller';
  const dropoffTier  = req.query.dropoff  || 'self';
  const shippingCents = Math.round(parseFloat(req.query.shipping || 0) * 100);
  const split = calculateSplit(priceInCents, shippingCents, listingType, dropoffTier);
  res.json({
    price:       split.itemPrice,
    shipping:    split.shipping,
    total:       split.total,
    seller:      split.sellerItem,
    sellerNet:   split.sellerNet,
    devo:        split.devo,
    dbp:         split.dbpNet,
    stripeFee:   split.stripeFee,
    handlingFee: split.handlingFee,
    sellerPct:   split.sellerPct,
    dbpPct:      split.dbpPct,
    devoPct:     split.devoPct
  });
});

app.listen(PORT, () => console.log(`DBP Marketplace running on port ${PORT}`));

// ── AUTO PAYOUT — runs every hour inside the server process ──
// Finds sales delivered 72+ hours ago and releases seller payouts
async function releasePayouts() {
  try {
    const cutoff = new Date(Date.now() - 72 * 60 * 60 * 1000).toISOString();
    const pending = db.prepare(
      "SELECT s.*, l.stripe_account_id, l.title FROM sales s JOIN listings l ON s.listing_id = l.id WHERE s.status = 'delivered' AND s.created_at < ?"
    ).all(cutoff);

    for (const sale of pending) {
      try {
        if (process.env.DEVO_ACCOUNT_ID && sale.devo_payout > 0) {
          await stripe.transfers.create({
            amount: sale.devo_payout,
            currency: 'usd',
            destination: process.env.DEVO_ACCOUNT_ID,
            description: 'Durango Devo — ' + sale.listing_id
          });
        }
        db.prepare("UPDATE sales SET status = 'paid_out' WHERE id = ?").run(sale.id);
        console.log(`Payout released for sale ${sale.id}`);
      } catch(err) {
        console.error(`Payout failed for sale ${sale.id}:`, err.message);
      }
    }

    if (pending.length > 0) console.log(`Released ${pending.length} payout(s)`);
  } catch(err) {
    console.error('releasePayouts error:', err.message);
  }
}

// Run once on startup, then every hour
releasePayouts();
setInterval(releasePayouts, 60 * 60 * 1000);
