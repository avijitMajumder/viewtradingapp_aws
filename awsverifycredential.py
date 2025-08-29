# aws_credentials_test.py
import boto3
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def test_aws_credentials():
    print("🔍 Testing AWS Credentials...")
    print("=" * 50)
    
    # Check if environment variables are loaded
    access_key = os.getenv('AWS_ACCESS_KEY_ID')
    secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
    region = os.getenv('AWS_DEFAULT_REGION')
    
    print(f"AWS_ACCESS_KEY_ID: {'✅ Found' if access_key else '❌ Missing'}")
    print(f"AWS_SECRET_ACCESS_KEY: {'✅ Found' if secret_key else '❌ Missing'}")
    print(f"AWS_DEFAULT_REGION: {region if region else '❌ Missing'}")
    
    if not all([access_key, secret_key, region]):
        print("\n❌ Please check your .env file. Some credentials are missing.")
        return False
    
    # Test with STS to validate credentials
    try:
        sts_client = boto3.client(
            'sts',
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name=region
        )
        
        identity = sts_client.get_caller_identity()
        print(f"\n✅ AWS Credentials Valid!")
        print(f"   Account ID: {identity['Account']}")
        print(f"   User ARN: {identity['Arn']}")
        return True
        
    except Exception as e:
        print(f"\n❌ AWS Credentials Error: {e}")
        print("\n💡 Possible solutions:")
        print("1. Check if your AWS Access Key and Secret Key are correct")
        print("2. Verify the keys have proper S3 permissions")
        print("3. Check if the IAM user has S3 full access policy")
        print("4. Ensure the keys haven't expired")
        return False

if __name__ == "__main__":
    test_aws_credentials()