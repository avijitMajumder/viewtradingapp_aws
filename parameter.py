import boto3

ssm = boto3.client("ssm", region_name="ap-south-1")

def get_parameter(name, with_decryption=False):
    return ssm.get_parameter(Name=name, WithDecryption=with_decryption)["Parameter"]["Value"]

# Fetch parameters
client_id = get_parameter("/flask-app/client_id")
client_secret = get_parameter("/flask-app/client_secret", with_decryption=True)
user_pool_id = get_parameter("/flask-app/user_pool_id")

# Construct full URL
server_metadata_url = f"https://cognito-idp.ap-south-1.amazonaws.com/{user_pool_id}/.well-known/openid-configuration"

# Print all values
print("client_id:", client_id)
print("client_secret:", client_secret)
print("user_pool_id:", user_pool_id)
print("server_metadata_url:", server_metadata_url)

