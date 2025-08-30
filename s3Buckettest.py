import boto3

def list_buckets():
    s3 = boto3.client('s3')
    try:
        response = s3.list_buckets()
        buckets = [bucket['Name'] for bucket in response.get('Buckets', [])]
        print("✅ Accessible Buckets:")
        for b in buckets:
            print(f" - {b}")
        return buckets
    except Exception as e:
        print(f"❌ Failed to list buckets: {e}")
        return []

# Run it
if __name__ == "__main__":
    bucket_list = list_buckets()
    if not bucket_list:
        print("No buckets accessible. Check IAM Role or Bucket Policy.")

