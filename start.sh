#!/bin/bash
# Quick Start Script for Attio Deduplicator

echo "🚀 Starting Attio Deduplicator..."

# Activate virtual environment
source venv/bin/activate

# Check if .env is configured
if grep -q "YOUR_KEY_HERE" .env; then
    echo "⚠️  WARNING: Please update your Stripe keys in .env file"
    echo "   Visit: https://dashboard.stripe.com/test/apikeys"
    echo ""
fi

# Start the server
echo "✅ Starting server on http://localhost:8000"
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
