#!/bin/bash
# Update OS packages
yum update -y

# Install Python3, Git, and venv support
yum install -y python3 git python3-venv

# Create Python virtual environment
python3 -m venv /home/ec2-user/myenv
source /home/ec2-user/myenv/bin/activate

# Upgrade pip and install boto3
pip install --upgrade pip
pip install boto3
sudo yum install git -y

# Clone the GitHub repo
cd /home/ec2-user
git clone https://github.com/avijitMajumder/viewtradingapp_aws.git
cd viewtradingapp_aws/

# Install Python dependencies
pip install -r requirements.txt

# Optional: auto-activate virtual environment on login
echo "source /home/ec2-user/myenv/bin/activate" >> /home/ec2-user/.bashrc

echo "EC2 setup complete!"
