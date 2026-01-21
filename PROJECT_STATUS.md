# Project Status & Updates - Attio Deduplicator SaaS

## ✅ Completed Updates (Jan 21, 2026)

### 1. **Stripe Updated**
- ✅ Upgraded from `stripe==7.12.0` to `stripe==11.2.0` (latest stable)
- ✅ Installed successfully in virtual environment

### 2. **Pricing Updates**
- ✅ Changed free tier: **100 → 250 contacts**
- ✅ Moved pricing display to **center of page** (large, bold, prominent)
- ✅ Added gradient styling: "Free for 250 contacts, then 1¢ per contact"
- ✅ Removed small text from header

### 3. **Calendar Booking Added**
- ✅ Added **white "Book a Call" button** in top right header
- ✅ Links to: https://calendar.notion.so/meet/nacholafuentemoreno/attio-expert
- ✅ Opens in new tab with calendar icon

### 4. **Environment Configuration**
- ✅ Created `.env` file (you need to add your Stripe keys)

---

## 📋 What You Need to Do

### Stripe Configuration
1. Go to [Stripe Dashboard](https://dashboard.stripe.com/test/apikeys)
2. Copy your **Secret Key** (starts with `sk_test_`)
3. Open `.env` file and replace `STRIPE_SECRET_KEY=sk_test_YOUR_KEY_HERE`
4. For webhooks (optional for testing):
   - Go to Stripe Dashboard → Developers → Webhooks
   - Add endpoint pointing to `https://your-domain.com/webhook/stripe`
   - Copy the signing secret and update `STRIPE_WEBHOOK_SECRET`

---

## 🚀 Current Features

### Core Functionality
- ✅ CSV upload with drag & drop
- ✅ Fuzzy matching for duplicate detection (names & emails)
- ✅ Smart merging:
  - Auto-merge: same company or no company
  - Flagged: different companies (user review needed)
- ✅ Two CSV outputs:
  - `Master` - Clean records with merged data
  - `To Delete` - Duplicates to remove from Attio

### Pricing & Payments
- ✅ Free tier: 250 contacts
- ✅ Paid: €0.01 per contact
- ✅ Stripe Checkout integration
- ✅ Payment verification

### UI/UX
- ✅ Dark theme (Tailwind CSS)
- ✅ Drag & drop upload
- ✅ Real-time progress
- ✅ Results preview with duplicate groups
- ✅ Responsive design
- ✅ Professional styling

---

## 🏃 How to Run

### Local Development
```bash
cd /Users/nacho/Desktop/AttioCursor\ Project/dedupe-csv-saas
source venv/bin/activate
uvicorn app.main:app --reload
```

Visit: http://localhost:8000

### Production Deployment
1. Update `BASE_URL` in `.env` to your production URL
2. Set production Stripe keys
3. Deploy to Railway/Render/Vercel

---

## 📂 Project Structure
```
dedupe-csv-saas/
├── app/
│   ├── main.py           # FastAPI routes & Stripe logic
│   └── deduplicator.py   # CSV deduplication algorithm
├── templates/
│   ├── index.html        # Landing page (updated)
│   ├── results.html      # Analysis results
│   └── download.html     # Download page after payment
├── static/               # Static assets
├── requirements.txt      # Python dependencies (updated)
├── .env                  # Environment variables (NEW - add your keys!)
├── .env.example          # Example configuration
└── Dockerfile            # Container configuration
```

---

## 🐛 Known Issues / Future Improvements

### To Consider:
- [ ] Session storage uses in-memory dict (should use Redis for production)
- [ ] 24-hour session expiry (hardcoded)
- [ ] No user authentication
- [ ] No email confirmations for purchases
- [ ] Consider adding more payment methods

---

## 💰 Pricing Breakdown

| Records | Cost       | Notes                    |
|---------|-----------|--------------------------|
| 1-250   | **FREE**  | No payment required      |
| 251+    | €0.01/ea  | All records charged      |

Example: 500 records = 500 × €0.01 = **€5.00**

---

## 📞 Contact Options

1. **DIY**: Free tool at http://localhost:8000
2. **Done-for-you**: €250 (direct Attio workspace integration) - nacho@5050growth.com
3. **Book a call**: Top right button → Notion calendar

---

Last updated: Jan 21, 2026
