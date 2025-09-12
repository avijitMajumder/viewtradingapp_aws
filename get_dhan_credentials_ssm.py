import boto3
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_dhan_credentials_from_ssm(region_name="ap-south-1"):
    """
    Retrieve DHAN credentials from AWS SSM Parameter Store.
    Expects parameters:
      /flask-app/dhan_client_id
      /flask-app/dhan_access_token
    Both stored as SecureString.
    """
    ssm = boto3.client("ssm", region_name=region_name)
    try:
        client_id = ssm.get_parameter(Name="/flask-app/dhan_client_id", WithDecryption=True)["Parameter"]["Value"]
        access_token = ssm.get_parameter(Name="/flask-app/dhan_access_token", WithDecryption=True)["Parameter"]["Value"]
        return client_id, access_token
    except ssm.exceptions.ParameterNotFound as e:
        logger.error(f"❌ SSM Parameter not found: {e}")
        return None, None
    except ssm.exceptions.AccessDeniedException as e:
        logger.error(f"❌ Access denied to SSM Parameter: {e}")
        return None, None
    except Exception as e:
        logger.error(f"❌ Failed to retrieve DHAN credentials from SSM: {e}")
        return None, None


# Test fetching the secret
client_id, access_token = get_dhan_credentials_from_ssm()

if client_id and access_token:
    print("✅ Secret retrieved successfully!")
    print("DHAN_CLIENT_ID:", client_id[:4] + "****")   # partial print
    print("DHAN_ACCESS_TOKEN:", access_token[:4] + "****")  # partial print
else:
    print("❌ Failed to retrieve secret.")

