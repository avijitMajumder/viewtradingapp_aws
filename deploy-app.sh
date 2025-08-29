#!/bin/bash
# deploy-app.sh

echo "🚀 Deploying TradingView App to EC2..."

# Copy application files
cd /var/www/trading-app

# Pull from git or copy files manually
# git clone your-repo-url . || echo "Using existing files"

# Stop existing service
sudo systemctl stop trading-app

# Install/update dependencies
source venv/bin/activate
pip install -r requirements.txt

# Set environment variables (you can also set these in the service file)
export S3_BUCKET="mytradeapp-csv-data"
export DHAN_CLIENT_ID="your_dhan_client_id"
export DHAN_ACCESS_TOKEN="your_dhan_access_token"

# Create necessary directories
mkdir -p templates static

# Start application
sudo systemctl daemon-reload
sudo systemctl start trading-app
sudo systemctl enable trading-app

# Restart Nginx
sudo systemctl restart nginx

echo "✅ Deployment complete!"
echo "🌐 Your app is running at: http://$(curl -s http://169.254.169.254/latest/meta-data/public-ipv4)"
echo "📊 Check status: sudo systemctl status trading-app"
echo "📝 View logs: sudo journalctl -u trading-app -f"