# s3_uploader.py
import boto3
import os
import logging
from dotenv import load_dotenv
from botocore.exceptions import ClientError, NoCredentialsError

# Load environment variables
load_dotenv()

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class S3Uploader:
    def __init__(self):
        # Get configuration from environment
        self.aws_access_key = os.getenv('AWS_ACCESS_KEY_ID')
        self.aws_secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
        self.region = os.getenv('AWS_DEFAULT_REGION', 'ap-south-1')
        self.bucket_name = os.getenv('S3_BUCKET_NAME', 'mytradeapp-csv-data')
        self.local_folder = os.getenv('LOCAL_UPLOAD_FOLDER', 'D:\\2025 BACKUP FEB13\\Python\\Trade\\uploads')
        self.s3_folder = os.getenv('S3_FOLDER_NAME', 'daily-csv-data')
        
        # Validate credentials
        if not all([self.aws_access_key, self.aws_secret_key]):
            raise ValueError("AWS credentials not found in .env file")
        
        # Initialize S3 client
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=self.aws_access_key,
            aws_secret_access_key=self.aws_secret_key,
            region_name=self.region
        )
        
        logger.info(f"S3 Uploader initialized for bucket: {self.bucket_name}")
    
    def ensure_folder_exists(self, folder_path):
        """
        Ensure that a folder exists in S3 (creates it if it doesn't)
        """
        try:
            # S3 doesn't have real folders, so we create a placeholder object
            folder_key = folder_path.rstrip('/') + '/'
            
            # Check if folder already exists by listing objects with the prefix
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=folder_key,
                MaxKeys=1
            )
            
            # If no objects found with this prefix, create the folder
            if 'Contents' not in response or len(response['Contents']) == 0:
                self.s3_client.put_object(
                    Bucket=self.bucket_name,
                    Key=folder_key,
                    Body=b''
                )
                logger.info(f"✅ Created folder: {folder_path}")
            else:
                logger.info(f"✅ Folder already exists: {folder_path}")
                
            return True
            
        except ClientError as e:
            logger.error(f"❌ Error ensuring folder exists: {e}")
            return False
    
    def upload_file(self, local_file_path, s3_key):
        """
        Upload a single file to S3
        """
        try:
            self.s3_client.upload_file(local_file_path, self.bucket_name, s3_key)
            logger.info(f"✅ Uploaded: {os.path.basename(local_file_path)} → {s3_key}")
            return True
        except FileNotFoundError:
            logger.error(f"❌ File not found: {local_file_path}")
            return False
        except ClientError as e:
            logger.error(f"❌ Error uploading file: {e}")
            return False
    
    def upload_folder_contents(self):
        """
        Upload all files from local folder to S3
        """
        # Check if local folder exists
        if not os.path.exists(self.local_folder):
            logger.error(f"❌ Local folder not found: {self.local_folder}")
            return False
        
        # Ensure S3 folder exists
        if not self.ensure_folder_exists(self.s3_folder):
            return False
        
        # Get all files from local folder
        files = []
        for item in os.listdir(self.local_folder):
            item_path = os.path.join(self.local_folder, item)
            if os.path.isfile(item_path):
                files.append(item_path)
        
        if not files:
            logger.warning(f"⚠️ No files found in: {self.local_folder}")
            return True
        
        logger.info(f"📁 Found {len(files)} files to upload")
        
        # Upload each file
        success_count = 0
        for file_path in files:
            filename = os.path.basename(file_path)
            s3_key = f"{self.s3_folder}/{filename}"
            
            if self.upload_file(file_path, s3_key):
                success_count += 1
        
        logger.info(f"📊 Upload complete: {success_count}/{len(files)} files successful")
        return success_count > 0
    
    def upload_specific_file(self, filename, s3_subfolder=None):
        """
        Upload a specific file to S3 with optional subfolder
        """
        file_path = os.path.join(self.local_folder, filename)
        
        if not os.path.exists(file_path):
            logger.error(f"❌ File not found: {file_path}")
            return False
        
        # Build S3 key path
        if s3_subfolder:
            s3_key = f"{self.s3_folder}/{s3_subfolder}/{filename}"
            # Ensure subfolder exists
            self.ensure_folder_exists(f"{self.s3_folder}/{s3_subfolder}")
        else:
            s3_key = f"{self.s3_folder}/{filename}"
        
        return self.upload_file(file_path, s3_key)
    
    def list_s3_contents(self, prefix=None):
        """
        List all objects in the S3 bucket/folder
        """
        try:
            if prefix is None:
                prefix = self.s3_folder + '/'
            
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=prefix
            )
            
            if 'Contents' in response:
                print(f"\n📦 Contents of s3://{self.bucket_name}/{prefix}:")
                print("-" * 60)
                for obj in response['Contents']:
                    size_mb = obj['Size'] / (1024 * 1024)
                    if not obj['Key'].endswith('/'):  # Skip folder markers
                        print(f"📄 {obj['Key']}")
                        print(f"   Size: {size_mb:.2f} MB, Modified: {obj['LastModified']}")
                        print()
            else:
                print(f"No files found in s3://{self.bucket_name}/{prefix}")
                
        except ClientError as e:
            logger.error(f"Error listing S3 contents: {e}")

def main():
    """
    Main function to demonstrate the uploader
    """
    print("🚀 S3 File Uploader")
    print("=" * 50)
    
    try:
        # Initialize uploader
        uploader = S3Uploader()
        
        print(f"📁 Local folder: {uploader.local_folder}")
        print(f"☁️  S3 bucket: {uploader.bucket_name}")
        print(f"📂 S3 folder: {uploader.s3_folder}")
        print("-" * 50)
        
        # Option 1: Upload all files from local folder
        print("\n1. 📤 Uploading all files from local folder...")
        if uploader.upload_folder_contents():
            print("✅ All files uploaded successfully!")
        else:
            print("❌ File upload failed")
        
        # Option 2: List uploaded files
        print("\n2. 📋 Listing uploaded files...")
        uploader.list_s3_contents()
        
        # Example: Upload a specific file to subfolder
        # print("\n3. 📤 Uploading specific file to subfolder...")
        # uploader.upload_specific_file("example.csv", "subfolder-name")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        print("\n💡 Make sure your .env file has correct AWS credentials")

if __name__ == "__main__":
    main()