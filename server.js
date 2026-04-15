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
const HANDLING_FEES = {
  small:  1500,
  medium: 2500,
  large:  5000
};

function calculateSplit(itemPriceCents, shippingCents, listingType, dropoffTier) {
  shippingCents = shippingCents || 0;
  const totalCents = itemPriceCents + shippingCents;
  const stripeFee = Math.round(totalCents * 0.029 + 30);

  let sellerItemPct, dbpPct, devoPct;
  if (listingType === 'dbp') {
    sellerItemPct = 0.00;
    dbpPct        = 0.95;
    devoPct       = 0.05;
  } else {
    sellerItemPct = 0.80;
    dbpPct        = 0.15;
    devoPct       = 0.05;
  }

  const handlingFee = dropoffTier && dropoffTier !== 'self'
    ? (HANDLING_FEES[dropoffTier] || 0)
    : 0;

  const devo       = Math.round(itemPriceCents * devoPct);
  const sellerItem = Math.round(itemPriceCents * sellerItemPct);
  const sellerNet  = sellerItem + shippingCents - handlingFee;
  const dbpGross   = itemPriceCents - sellerItem - devo;
  const dbpNet     = dbpGross - stripeFee;

  return {
    itemPrice:   itemPriceCents / 100,
    shipping:    shippingCents / 100,
    total:       totalCents / 100,
    sellerItem:  sellerItem / 100,
    sellerNet:   sellerNet / 100,
    devo:        devo / 100,
    dbpGross:    dbpGross / 100,
    dbpNet:      Math.max(dbpNet, 0) / 100,
    stripeFee:   stripeFee / 100,
    handlingFee: handlingFee / 100,
    sellerPct:   Math.round(sellerItemPct * 100),
    dbpPct:      Math.round(dbpPct * 100),
    devoPct:     Math.round(devoPct * 100)
  };
}

// ── MIDDLEWARE ──
app.use(cors({
  origin: ['https://www.durangobikeproject.com', 'http://localhost:3000']
}));
app.use('/webhook', express.raw({ type: 'application/json' }));
app.use(express.json());

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
  limits: { fileSize: 10 * 1024 * 1024 },
  fileFilter: (req, file, cb) => {
    if (file.mimetype.startsWith('image/')) cb(null, true);
    else cb(new Error('Images only'));
  }
});
app.use('/uploads', express.static('./uploads'));

// ── ROUTES ──
app.get('/', (req, res) => res.json({ status: 'DBP Marketplace running' }));

// ── SELLER ONBOARDING ──
app.post('/seller/onboard', async (req, res) => {
  try {
    const { email, name } = req.body;
    if (!email || !name) return res.status(400).json({ error: 'Email and name required' });

    const nameParts = name.trim().split(' ');
    const firstName = nameParts[0];
    const lastName  = nameParts.slice(1).join(' ') || '';

    const account = await stripe.accounts.create({
      type: 'express',
      email,
      capabilities: { card_payments: { requested: true }, transfers: { requested: true } },
      business_type: 'individual',
      individual: { first_name: firstName, last_name: lastName, email },
      settings: { payouts: { schedule: { interval: 'manual' } } },
      metadata: { seller_name: name }
    });

    const accountLink = await stripe.accountLinks.create({
      account: account.id,
      refresh_url: `${process.env.BASE_URL || 'https://www.durangobikeproject.com'}/marketplace-sell?reauth=true`,
      return_url:  `${process.env.BASE_URL || 'https://www.durangobikeproject.com'}/marketplace-sell?onboarded=true&account=${account.id}`,
      type: 'account_onboarding',
      collection_options: { fields: 'eventually_due', future_requirements: 'omit' }
    });

    res.json({ url: accountLink.url, accountId: account.id });
  } catch (err) {
    console.error('Onboard error:', err);
    res.status(500).json({ error: err.message });
  }
});

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
app.post('/listings', upload.array('photos', 8), (req, res) => {
  try {
    const {
      seller_name, seller_email, stripe_account_id,
      title, category, description, condition, price,
      listing_type, dropoff_tier, shipping_estimate
    } = req.body;

    // Validate required fields
    if (!seller_name || !seller_email || !title || !price) {
      return res.status(400).json({ error: 'Missing required fields: seller_name, seller_email, title, price' });
    }
    if (!stripe_account_id) {
      return res.status(400).json({ error: 'Seller must complete Stripe onboarding first' });
    }

    const priceInCents    = Math.round(parseFloat(price) * 100);
    const shippingCents   = Math.round(parseFloat(shipping_estimate || 0) * 100);
    const listingTypeSafe = listing_type  || 'seller';
    const dropoffTierSafe = dropoff_tier  || 'self';
    const categorySafe    = category      || 'Other';
    const descriptionSafe = description   || '';
    const conditionSafe   = condition     || 'Good';

    if (priceInCents < 100) return res.status(400).json({ error: 'Minimum price is $1.00' });

    const photos = req.files ? req.files.map(f => '/uploads/' + f.filename) : [];
    const id     = uuidv4();
    const split  = calculateSplit(priceInCents, shippingCents, listingTypeSafe, dropoffTierSafe);

    // 13 ? placeholders + hardcoded 'approved' = 14 columns, 13 bound values
    const stmt = db.prepare(`
      INSERT INTO listings (
        id, seller_name, seller_email, stripe_account_id,
        title, category, description, condition,
        price, shipping_estimate, photos,
        listing_type, dropoff_tier, status
      ) VALUES (
        ?, ?, ?, ?,
        ?, ?, ?, ?,
        ?, ?, ?,
        ?, ?, 'approved'
      )
    `);

    stmt.run(
      id,
      seller_name.trim(),
      seller_email.trim(),
      stripe_account_id.trim(),
      title.trim(),
      categorySafe,
      descriptionSafe,
      conditionSafe,
      priceInCents,
      shippingCents,
      JSON.stringify(photos),
      listingTypeSafe,
      dropoffTierSafe
    );

    res.json({
      success: true,
      listingId: id,
      split: {
        price:     priceInCents / 100,
        seller:    split.sellerNet,
        dbp:       split.dbpNet,
        devo:      split.devo,
        sellerPct: split.sellerPct,
        dbpPct:    split.dbpPct,
        devoPct:   split.devoPct
      },
      message: 'Listing submitted and live.'
    });
  } catch (err) {
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
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

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

// ── CHECKOUT ──
app.post('/checkout', async (req, res) => {
  try {
    const { listingId, buyerEmail, deliveryType } = req.body;
    const listing = db.prepare("SELECT * FROM listings WHERE id = ? AND status = 'approved'").get(listingId);
    if (!listing) return res.status(404).json({ error: 'Listing not found or unavailable' });

    const shippingCents = listing.shipping_estimate || 0;
    const split = calculateSplit(listing.price, shippingCents, listing.listing_type, listing.dropoff_tier);
    const totalCharge = listing.price + shippingCents;
    const sellerTransferCents = Math.round(split.sellerNet * 100);

    const paymentIntent = await stripe.paymentIntents.create({
      amount: totalCharge,
      currency: 'usd',
      receipt_email: buyerEmail,
      metadata: {
        listingId:    listing.id,
        listingTitle: listing.title,
        sellerEmail:  listing.seller_email,
        deliveryType: deliveryType || 'shipping'
      },
      transfer_data: {
        destination: listing.stripe_account_id,
        amount: sellerTransferCents
      }
    });

    const saleId = uuidv4();
    db.prepare(`
      INSERT INTO sales (id, listing_id, buyer_email, payment_intent_id, amount, seller_payout, dbp_payout, devo_payout, delivery_type)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    `).run(
      saleId, listingId, buyerEmail, paymentIntent.id,
      listing.price,
      Math.round(split.sellerNet * 100),
      Math.round(split.dbpNet * 100),
      Math.round(split.devo * 100),
      deliveryType || 'shipping'
    );

    res.json({
      success: true,
      clientSecret: paymentIntent.client_secret,
      publishableKey: process.env.STRIPE_PUBLISHABLE_KEY,
      listing: {
        title:  listing.title,
        price:  listing.price / 100,
        photos: JSON.parse(listing.photos || '[]')
      },
      split: { seller: split.sellerNet, dbp: split.dbpNet, devo: split.devo }
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
    db.prepare("UPDATE listings SET status = 'sold', sold_at = datetime('now') WHERE id = ?").run(listingId);
    db.prepare("UPDATE sales SET status = 'delivered' WHERE payment_intent_id = ?").run(pi.id);
    console.log('Payment confirmed, listing marked sold:', listingId);
  }

  res.json({ received: true });
});

// ── ADMIN: RELEASE PAYOUTS ──
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
            amount: sale.devo_payout,
            currency: 'usd',
            destination: process.env.DEVO_ACCOUNT_ID,
            description: 'Durango Devo — ' + sale.listing_id
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

// ── SPLIT CALCULATOR (public) ──
app.get('/split/:price', (req, res) => {
  const priceInCents = Math.round(parseFloat(req.params.price) * 100);
  if (isNaN(priceInCents) || priceInCents < 100) {
    return res.status(400).json({ error: 'Invalid price' });
  }
  const listingType   = req.query.type    || 'seller';
  const dropoffTier   = req.query.dropoff || 'self';
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

// ── ADMIN: LIST ALL LISTINGS (including sold/pending) ──
app.get('/admin/listings', (req, res) => {
  const { adminKey } = req.query;
  if (adminKey !== process.env.ADMIN_KEY) return res.status(401).json({ error: 'Unauthorized' });
  try {
    const listings = db.prepare("SELECT * FROM listings ORDER BY created_at DESC").all()
      .map(l => ({
        ...l,
        photos: JSON.parse(l.photos || '[]'),
        price: l.price / 100,
        shipping_estimate: (l.shipping_estimate || 0) / 100
      }));
    res.json({ success: true, listings });
  } catch(err) {
    res.status(500).json({ error: err.message });
  }
});

// ── ADMIN: UPDATE LISTING STATUS ──
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

app.listen(PORT, () => console.log(`DBP Marketplace running on port ${PORT}`));

// ── AUTO PAYOUT — runs every hour ──
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

releasePayouts();
setInterval(releasePayouts, 60 * 60 * 1000);
