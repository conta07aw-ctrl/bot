#!/bin/bash
# ==============================================
# Cloudflare Tunnel Setup for SARBCCODE
# Uses TryCloudflare (free, no domain needed)
# ==============================================

set -e

echo "=== SARBCCODE Cloudflare Tunnel Setup ==="

# 1. Install cloudflared (if not present)
if ! command -v cloudflared &> /dev/null; then
  echo "[1/2] Installing cloudflared..."
  curl -L --output /tmp/cloudflared.deb \
    https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-amd64.deb
  sudo dpkg -i /tmp/cloudflared.deb
  rm /tmp/cloudflared.deb
  echo "  cloudflared installed successfully"
else
  echo "[1/2] cloudflared already installed: $(cloudflared --version)"
fi

# 2. Quick test
echo ""
echo "[2/2] Testing TryCloudflare tunnel..."
echo "  Starting tunnel to localhost:3001..."
echo "  The URL will appear below (look for 'https://xxxxx.trycloudflare.com')"
echo "  Press Ctrl+C to stop."
echo ""

cloudflared tunnel --url http://localhost:3001
