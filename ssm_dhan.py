import boto3
import logging

ssm = boto3.client("ssm", region_name="ap-south-1")

def get_parameter(name, with_decryption=False):
    try:
        return ssm.get_parameter(Name=name, WithDecryption=with_decryption)["Parameter"]["Value"]
    except ssm.exceptions.ParameterNotFound:
        logging.error(f"Parameter {name} not found in SSM")
        return None

DHAN_CLIENT_ID = get_parameter("/flask-app/dhan_client_id")
DHAN_ACCESS_TOKEN = get_parameter("/flask-app/dhan_access_token", with_decryption=True)
REPO_URL = get_parameter("/flask-app/repo_url")

print("DHAN_CLIENT_ID:", DHAN_CLIENT_ID)
print("DHAN_ACCESS_TOKEN:", DHAN_ACCESS_TOKEN)
print("REPO_URL:", REPO_URL)

