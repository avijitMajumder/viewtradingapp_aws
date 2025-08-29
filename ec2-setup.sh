#!/bin/bash
# ec2-setup.sh

echo "🚀 Setting up EC2 instance for TradingView App..."

# Update system
sudo yum update -y

# Install Python 3.9 and development tools
sudo yum install -y python3.9 python3.9-pip python3.9-devel
sudo yum install -y gcc-c++ make openssl-devel libffi-devel

# Install Nginx
sudo yum install -y nginx
sudo systemctl start nginx
sudo systemctl enable nginx

# Create application directory
sudo mkdir -p /var/www/trading-app
sudo chown ec2-user:ec2-user /var/www/trading-app

# Install AWS CLI (for S3 access)
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
unzip awscliv2.zip
sudo ./aws/install
rm -rf awscliv2.zip aws/

# Create virtual environment
cd /var/www/trading-app
python3.9 -m venv venv
source venv/bin/activate

# Install Python dependencies
pip install --upgrade pip
pip install flask pandas numpy python-dotenv boto3 dhanhq gunicorn

# Configure AWS credentials (you'll need to set these manually)
mkdir -p ~/.aws
cat > ~/.aws/config <<EOF
[default]
region = ap-south-1
output = json
EOF

echo "✅ EC2 setup complete!"
echo "📝 Next steps:"
echo "1. Set AWS credentials: aws configure"
echo "2. Copy your application files to /var/www/trading-app"
echo "3. Set up environment variables"
echo "4. Configure Nginx and start the application"