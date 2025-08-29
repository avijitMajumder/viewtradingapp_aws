# s3_manager.py
import boto3
import os
import logging
from dotenv import load_dotenv
from botocore.exceptions import ClientError, NoCredentialsError
import uuid

# Load environment variables
load_dotenv()

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class S3TradeBucketManager:
    def __init__(self):
        # Load configuration from .env
        self.region_name = os.getenv('AWS_DEFAULT_REGION', 'ap-south-1')
        self.bucket_name = os.getenv('S3_BUCKET_NAME', f"mytradeapp-{uuid.uuid4().hex[:8]}")
        self.local_upload_folder = os.getenv('LOCAL_UPLOAD_FOLDER')
        self.s3_folder_name = os.getenv('S3_FOLDER_NAME', 'daily-csv-data')
        
        # Validate local folder
        if not self.local_upload_folder or not os.path.exists(self.local_upload_folder):
            logger.warning(f"Local upload folder not found: {self.local_upload_folder}")
            logger.info("Please create the folder: D:\\2025 BACKUP FEB13\\Python\\Trade\\uploads")
        
        # Initialize S3 client
        try:
            self.s3_client = boto3.client(
                's3',
                region_name=self.region_name
            )
            logger.info(f"S3 client initialized for region: {self.region_name}")
        except Exception as e:
            logger.error(f"Failed to initialize S3 client: {e}")
            raise
    
    def create_bucket(self):
        """
        Create S3 bucket with no public access
        """
        try:
            # Check if bucket already exists
            try:
                self.s3_client.head_bucket(Bucket=self.bucket_name)
                logger.info(f"Bucket {self.bucket_name} already exists")
                return True
            except ClientError:
                # Bucket doesn't exist, create it
                pass
            
            # Create the bucket
            logger.info(f"Creating bucket: {self.bucket_name} in {self.region_name}")
            
            if self.region_name == 'us-east-1':
                response = self.s3_client.create_bucket(Bucket=self.bucket_name)
            else:
                response = self.s3_client.create_bucket(
                    Bucket=self.bucket_name,
                    CreateBucketConfiguration={
                        'LocationConstraint': self.region_name
                    }
                )
            
            logger.info(f"Bucket {self.bucket_name} created successfully")
            
            # Configure bucket settings
            self._configure_bucket()
            
            return True
            
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == 'BucketAlreadyExists':
                logger.error("Bucket name already exists. Choose a different name.")
            elif error_code == 'BucketAlreadyOwnedByYou':
                logger.info("Bucket already owned by you")
                return True
            else:
                logger.error(f"Error creating bucket: {e}")
            return False
        except NoCredentialsError:
            logger.error("AWS credentials not found. Please check your .env file")
            return False
    
    def _configure_bucket(self):
        """
        Configure bucket settings: block public access, enable versioning
        """
        try:
            # Block all public access
            self.s3_client.put_public_access_block(
                Bucket=self.bucket_name,
                PublicAccessBlockConfiguration={
                    'BlockPublicAcls': True,
                    'IgnorePublicAcls': True,
                    'BlockPublicPolicy': True,
                    'RestrictPublicBuckets': True
                }
            )
            logger.info("Public access blocked")
            
            # Enable versioning
            self.s3_client.put_bucket_versioning(
                Bucket=self.bucket_name,
                VersioningConfiguration={'Status': 'Enabled'}
            )
            logger.info("Versioning enabled")
            
            # Create folder structure in S3
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=f"{self.s3_folder_name}/"
            )
            logger.info(f"Folder {self.s3_folder_name} created in S3")
            
        except ClientError as e:
            logger.error(f"Error configuring bucket: {e}")
    
    def upload_all_csv_files(self):
        """
        Upload all CSV files from local uploads folder to S3
        """
        if not os.path.exists(self.local_upload_folder):
            logger.error(f"Local folder not found: {self.local_upload_folder}")
            return False
        
        csv_files = [f for f in os.listdir(self.local_upload_folder) 
                    if f.endswith('.csv') and os.path.isfile(os.path.join(self.local_upload_folder, f))]
        
        if not csv_files:
            logger.warning(f"No CSV files found in {self.local_upload_folder}")
            return False
        
        success_count = 0
        for csv_file in csv_files:
            local_file_path = os.path.join(self.local_upload_folder, csv_file)
            s3_key = f"{self.s3_folder_name}/{csv_file}"
            
            if self.upload_file(local_file_path, s3_key):
                success_count += 1
        
        logger.info(f"Uploaded {success_count}/{len(csv_files)} files successfully")
        return success_count > 0
    
    def upload_file(self, local_file_path, s3_key):
        """
        Upload a single file to S3
        """
        try:
            self.s3_client.upload_file(local_file_path, self.bucket_name, s3_key)
            logger.info(f"Uploaded {local_file_path} to s3://{self.bucket_name}/{s3_key}")
            return True
        except FileNotFoundError:
            logger.error(f"File not found: {local_file_path}")
            return False
        except ClientError as e:
            logger.error(f"Error uploading file {local_file_path}: {e}")
            return False
    
    def list_bucket_contents(self):
        """
        List all objects in the bucket
        """
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=self.s3_folder_name
            )
            
            if 'Contents' in response:
                print(f"\nFiles in bucket {self.bucket_name}:")
                print("-" * 50)
                for obj in response['Contents']:
                    size_mb = obj['Size'] / (1024 * 1024)
                    print(f"📁 {obj['Key']}")
                    print(f"   Size: {size_mb:.2f} MB, Modified: {obj['LastModified']}")
                    print()
            else:
                print("No files found in the bucket")
                
        except ClientError as e:
            logger.error(f"Error listing bucket contents: {e}")
    
    def get_bucket_info(self):
        """
        Display bucket information
        """
        print("=" * 60)
        print("S3 BUCKET CONFIGURATION")
        print("=" * 60)
        print(f"Bucket Name: {self.bucket_name}")
        print(f"Region: {self.region_name}")
        print(f"Local Folder: {self.local_upload_folder}")
        print(f"S3 Folder: {self.s3_folder_name}")
        print("=" * 60)

def main():
    """
    Main function to demonstrate the S3 manager
    """
    # Initialize the S3 manager
    manager = S3TradeBucketManager()
    
    # Display configuration
    manager.get_bucket_info()
    
    # Create the bucket
    print("\n🚀 Creating S3 bucket...")
    if manager.create_bucket():
        print("✅ Bucket created successfully!")
        
        # Upload CSV files
        print("\n📤 Uploading CSV files...")
        if manager.upload_all_csv_files():
            print("✅ Files uploaded successfully!")
        else:
            print("❌ File upload failed")
        
        # List bucket contents
        print("\n📋 Listing bucket contents...")
        manager.list_bucket_contents()
        
    else:
        print("❌ Failed to create bucket")

if __name__ == "__main__":
    main()