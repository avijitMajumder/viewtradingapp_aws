import boto3
import json
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_dhan_credentials(secret_name="dhan_api_secret", region_name="ap-south-1"):
    import boto3, json
    try:
        session = boto3.session.Session()
        client = session.client("secretsmanager", region_name=region_name)
        response = client.get_secret_value(SecretId=secret_name)
        secret_dict = json.loads(response['SecretString'])
        return secret_dict.get("DHAN_CLIENT_ID"), secret_dict.get("DHAN_ACCESS_TOKEN")
    except Exception as e:
        logger.error(f"Failed to retrieve secret '{secret_name}': {e}")
        return None, None


# Test fetching the secret
client_id, access_token = get_dhan_credentials()

if client_id and access_token:
    print("✅ Secret retrieved successfully!")
    print("DHAN_CLIENT_ID:", client_id[:4] + "****")   # partial print
    print("DHAN_ACCESS_TOKEN:", access_token[:4] + "****")  # partial print
else:
    print("❌ Failed to retrieve secret.")
