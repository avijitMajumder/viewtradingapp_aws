#!/bin/bash
# initial-setup.sh
set -e

echo "Running initial setup..."

# Clone your repository
git clone https://github.com/yourusername/your-repo.git
cd your-repo

# Copy config files to system directories
sudo cp nginx.conf /etc/nginx/sites-available/trading-app
sudo ln -s /etc/nginx/sites-available/trading-app /etc/nginx/sites-enabled/
sudo rm -f /etc/nginx/sites-enabled/default  # Remove default config

sudo cp trading-app.service /etc/systemd/system/

# Test and reload configurations
sudo nginx -t
sudo systemctl daemon-reload

# Install dependencies
pip install -r requirements.txt

# Enable and start services
sudo systemctl enable trading-app.service
sudo systemctl start trading-app.service
sudo systemctl restart nginx

echo "Initial setup completed!"