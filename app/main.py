"""
CSV Deduplication SaaS API
- Upload CSV
- Analyze for duplicates
- Stripe payment integration
- Download cleaned CSVs
"""

import os
import uuid
import json
from datetime import datetime, timedelta
from pathlib import Path

import stripe
from fastapi import FastAPI, File, UploadFile, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel

from .deduplicator import process_csv

# Initialize FastAPI
app = FastAPI(
    title="CSV Deduplicator",
    description="Find and merge duplicate records in your CSV files"
)

# Setup paths
BASE_DIR = Path(__file__).resolve().parent.parent
templates = Jinja2Templates(directory=BASE_DIR / "templates")

# Mount static files only if directory exists and has files
static_dir = BASE_DIR / "static"
if static_dir.exists():
    try:
        app.mount("/static", StaticFiles(directory=static_dir), name="static")
    except Exception:
        pass  # Skip if empty or fails

# Stripe configuration
stripe.api_key = os.getenv("STRIPE_SECRET_KEY", "")
STRIPE_WEBHOOK_SECRET = os.getenv("STRIPE_WEBHOOK_SECRET", "")
STRIPE_PRICE_PER_RECORD = 0.01  # €0.01 per record
FREE_TIER_LIMIT = 250
CURRENCY = "eur"

# In-memory session storage (use Redis in production)
sessions = {}

# Base URL for redirects
BASE_URL = os.getenv("BASE_URL", "http://localhost:8000")


class AnalysisResult(BaseModel):
    session_id: str
    total_records: int
    duplicate_groups: list
    auto_merge_count: int
    flagged_count: int
    clean_count: int
    price_cents: int
    is_free_tier: bool


def calculate_price(total_records: int) -> tuple[int, bool]:
    """Calculate price in cents and whether it's free tier."""
    if total_records <= FREE_TIER_LIMIT:
        return 0, True
    billable_records = total_records
    price_cents = int(billable_records * STRIPE_PRICE_PER_RECORD * 100)
    return price_cents, False


@app.get("/", response_class=HTMLResponse)
async def landing_page(request: Request):
    """Serve the landing page."""
    return templates.TemplateResponse("index.html", {"request": request})


@app.post("/upload")
async def upload_csv(file: UploadFile = File(...)):
    """Upload and analyze a CSV file."""
    if not file.filename.endswith('.csv'):
        raise HTTPException(status_code=400, detail="Please upload a CSV file")

    # Read file content
    content = await file.read()
    try:
        csv_content = content.decode('utf-8')
    except UnicodeDecodeError:
        try:
            csv_content = content.decode('latin-1')
        except:
            raise HTTPException(status_code=400, detail="Could not decode CSV file")

    # Process CSV
    try:
        result = process_csv(csv_content)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error processing CSV: {str(e)}")

    # Calculate price
    price_cents, is_free_tier = calculate_price(result['total_records'])

    # Create session
    session_id = str(uuid.uuid4())
    sessions[session_id] = {
        'created_at': datetime.utcnow().isoformat(),
        'filename': file.filename,
        'result': result,
        'price_cents': price_cents,
        'is_free_tier': is_free_tier,
        'paid': is_free_tier,  # Free tier is auto-paid
        'expires_at': (datetime.utcnow() + timedelta(hours=24)).isoformat()
    }

    return {
        'session_id': session_id,
        'filename': file.filename,
        'total_records': result['total_records'],
        'duplicate_groups': result['duplicate_groups'][:20],  # Limit preview
        'total_duplicate_groups': len(result['duplicate_groups']),
        'auto_merge_count': result['auto_merge_count'],
        'flagged_count': result['flagged_count'],
        'clean_count': result['clean_count'],
        'price_cents': price_cents,
        'price_display': f"€{price_cents / 100:.2f}" if price_cents > 0 else "Free",
        'is_free_tier': is_free_tier
    }


@app.get("/results/{session_id}", response_class=HTMLResponse)
async def results_page(request: Request, session_id: str):
    """Show analysis results page."""
    if session_id not in sessions:
        raise HTTPException(status_code=404, detail="Session not found")

    session = sessions[session_id]
    return templates.TemplateResponse("results.html", {
        "request": request,
        "session_id": session_id,
        "session": session
    })


@app.post("/create-checkout/{session_id}")
async def create_checkout(session_id: str):
    """Create a Stripe checkout session."""
    if session_id not in sessions:
        raise HTTPException(status_code=404, detail="Session not found")

    session = sessions[session_id]

    if session['is_free_tier']:
        return {"redirect_url": f"{BASE_URL}/download/{session_id}"}

    if session['paid']:
        return {"redirect_url": f"{BASE_URL}/download/{session_id}"}

    if not stripe.api_key:
        raise HTTPException(status_code=500, detail="Stripe not configured")

    try:
        checkout_session = stripe.checkout.Session.create(
            payment_method_types=['card'],
            line_items=[{
                'price_data': {
                    'currency': CURRENCY,
                    'unit_amount': session['price_cents'],
                    'product_data': {
                        'name': f"CSV Deduplication - {session['result']['total_records']} records",
                        'description': f"Deduplicate {session['filename']}"
                    }
                },
                'quantity': 1
            }],
            mode='payment',
            success_url=f"{BASE_URL}/payment-success/{session_id}",
            cancel_url=f"{BASE_URL}/results/{session_id}",
            metadata={
                'session_id': session_id
            }
        )
        return {"redirect_url": checkout_session.url}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Stripe error: {str(e)}")


@app.get("/payment-success/{session_id}")
async def payment_success(session_id: str):
    """Handle successful payment."""
    if session_id not in sessions:
        raise HTTPException(status_code=404, detail="Session not found")

    # Mark as paid (in production, verify with webhook)
    sessions[session_id]['paid'] = True

    return RedirectResponse(url=f"/download/{session_id}")


@app.post("/webhook/stripe")
async def stripe_webhook(request: Request):
    """Handle Stripe webhooks."""
    payload = await request.body()
    sig_header = request.headers.get('stripe-signature')

    if not STRIPE_WEBHOOK_SECRET:
        # No webhook secret configured, skip verification
        event = json.loads(payload)
    else:
        try:
            event = stripe.Webhook.construct_event(
                payload, sig_header, STRIPE_WEBHOOK_SECRET
            )
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Webhook error: {str(e)}")

    if event['type'] == 'checkout.session.completed':
        checkout_session = event['data']['object']
        session_id = checkout_session.get('metadata', {}).get('session_id')
        if session_id and session_id in sessions:
            sessions[session_id]['paid'] = True

    return {"status": "ok"}


@app.get("/download/{session_id}", response_class=HTMLResponse)
async def download_page(request: Request, session_id: str):
    """Download page after payment."""
    if session_id not in sessions:
        raise HTTPException(status_code=404, detail="Session not found")

    session = sessions[session_id]

    if not session['paid']:
        return RedirectResponse(url=f"/results/{session_id}")

    return templates.TemplateResponse("download.html", {
        "request": request,
        "session_id": session_id,
        "session": session
    })


@app.get("/download/{session_id}/master.csv")
async def download_master_csv(session_id: str):
    """Download the master CSV with merged records."""
    if session_id not in sessions:
        raise HTTPException(status_code=404, detail="Session not found")

    session = sessions[session_id]

    if not session['paid']:
        raise HTTPException(status_code=403, detail="Payment required")

    csv_content = session['result']['master_csv']
    filename = f"master_{session['filename']}"

    return StreamingResponse(
        iter([csv_content]),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"}
    )


@app.get("/download/{session_id}/duplicates.csv")
async def download_duplicates_csv(session_id: str):
    """Download the duplicates CSV (records to delete)."""
    if session_id not in sessions:
        raise HTTPException(status_code=404, detail="Session not found")

    session = sessions[session_id]

    if not session['paid']:
        raise HTTPException(status_code=403, detail="Payment required")

    csv_content = session['result']['duplicates_csv']
    filename = f"duplicates_to_delete_{session['filename']}"

    return StreamingResponse(
        iter([csv_content]),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"}
    )


@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {
        "status": "healthy",
        "stripe_configured": bool(stripe.api_key)
    }


if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
