# s3_cleanup.py
import boto3
import os
import logging
from dotenv import load_dotenv
from botocore.exceptions import ClientError

# Load environment variables
load_dotenv()

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class S3Cleanup:
    def __init__(self):
        # Get configuration from environment
        self.aws_access_key = os.getenv('AWS_ACCESS_KEY_ID')
        self.aws_secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
        self.region = os.getenv('AWS_DEFAULT_REGION', 'ap-south-1')
        self.bucket_name = os.getenv('S3_BUCKET_NAME', 'mytradeapp-csv-data')
        self.s3_folder = os.getenv('S3_FOLDER_NAME', 'daily-csv-data')
        
        # Initialize S3 client
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=self.aws_access_key,
            aws_secret_access_key=self.aws_secret_key,
            region_name=self.region
        )
        
        logger.info(f"S3 Cleanup initialized for bucket: {self.bucket_name}")
    
    def list_files(self, prefix=None):
        """
        List all files in the bucket or specific folder
        """
        if prefix is None:
            prefix = self.s3_folder + '/' if self.s3_folder else ''
        
        try:
            files = []
            continuation_token = None
            
            while True:
                list_kwargs = {
                    'Bucket': self.bucket_name,
                    'Prefix': prefix
                }
                
                if continuation_token:
                    list_kwargs['ContinuationToken'] = continuation_token
                
                response = self.s3_client.list_objects_v2(**list_kwargs)
                
                if 'Contents' in response:
                    for obj in response['Contents']:
                        if not obj['Key'].endswith('/'):  # Skip folder markers
                            files.append({
                                'Key': obj['Key'],
                                'Size': obj['Size'],
                                'LastModified': obj['LastModified']
                            })
                
                if not response.get('IsTruncated'):
                    break
                    
                continuation_token = response.get('NextContinuationToken')
            
            return files
            
        except ClientError as e:
            logger.error(f"Error listing files: {e}")
            return []
    
    def delete_files(self, file_keys):
        """
        Delete specific files from S3
        """
        if not file_keys:
            logger.info("No files to delete")
            return True
        
        try:
            # Convert single file key to list
            if isinstance(file_keys, str):
                file_keys = [file_keys]
            
            # Delete objects
            response = self.s3_client.delete_objects(
                Bucket=self.bucket_name,
                Delete={
                    'Objects': [{'Key': key} for key in file_keys],
                    'Quiet': True
                }
            )
            
            logger.info(f"✅ Deleted {len(file_keys)} files")
            return True
            
        except ClientError as e:
            logger.error(f"Error deleting files: {e}")
            return False
    
    def delete_folder(self, folder_path):
        """
        Delete entire folder and its contents
        """
        try:
            # List all files in the folder
            files = self.list_files(folder_path)
            file_keys = [file['Key'] for file in files]
            
            if not file_keys:
                logger.info(f"Folder {folder_path} is already empty")
                return True
            
            # Delete all files
            return self.delete_files(file_keys)
            
        except ClientError as e:
            logger.error(f"Error deleting folder: {e}")
            return False
    
    def cleanup_old_files(self, days_old=30):
        """
        Delete files older than specified days
        """
        from datetime import datetime, timedelta
        
        try:
            cutoff_date = datetime.now().replace(tzinfo=None) - timedelta(days=days_old)
            files = self.list_files()
            
            old_files = []
            for file in files:
                if file['LastModified'].replace(tzinfo=None) < cutoff_date:
                    old_files.append(file['Key'])
            
            if old_files:
                logger.info(f"Found {len(old_files)} files older than {days_old} days")
                return self.delete_files(old_files)
            else:
                logger.info("No old files found to delete")
                return True
                
        except Exception as e:
            logger.error(f"Error in cleanup_old_files: {e}")
            return False
    
    def empty_bucket(self, confirm=False):
        """
        Empty the entire bucket (use with caution!)
        """
        if not confirm:
            logger.warning("This will delete ALL files in the bucket. Call with confirm=True to proceed")
            return False
        
        try:
            # List all files
            files = self.list_files('')
            file_keys = [file['Key'] for file in files]
            
            if not file_keys:
                logger.info("Bucket is already empty")
                return True
            
            # Delete in batches of 1000 (S3 limit)
            for i in range(0, len(file_keys), 1000):
                batch = file_keys[i:i + 1000]
                self.s3_client.delete_objects(
                    Bucket=self.bucket_name,
                    Delete={
                        'Objects': [{'Key': key} for key in batch],
                        'Quiet': True
                    }
                )
            
            logger.info(f"✅ Emptied bucket: {len(file_keys)} files deleted")
            return True
            
        except ClientError as e:
            logger.error(f"Error emptying bucket: {e}")
            return False

def main():
    """
    Interactive cleanup menu
    """
    print("🧹 S3 Cleanup Utility")
    print("=" * 50)
    
    try:
        cleanup = S3Cleanup()
        
        while True:
            print(f"\nBucket: {cleanup.bucket_name}")
            print("1. 📋 List files")
            print("2. 🗑️  Delete specific files")
            print("3. 📁 Delete entire folder contents")
            print("4. ⏰ Cleanup old files (older than 30 days)")
            print("5. 🗂️  Empty entire bucket")
            print("6. ❌ Exit")
            
            choice = input("\nChoose an option (1-6): ").strip()
            
            if choice == '1':
                # List files
                files = cleanup.list_files()
                if files:
                    print(f"\n📁 Found {len(files)} files:")
                    for i, file in enumerate(files, 1):
                        size_mb = file['Size'] / (1024 * 1024)
                        print(f"{i}. {file['Key']} ({size_mb:.2f} MB, {file['LastModified']})")
                else:
                    print("No files found")
            
            elif choice == '2':
                # Delete specific files
                files = cleanup.list_files()
                if files:
                    print("\nSelect files to delete (comma-separated numbers):")
                    for i, file in enumerate(files, 1):
                        print(f"{i}. {file['Key']}")
                    
                    try:
                        selections = input("Enter numbers: ").split(',')
                        indices = [int(s.strip()) - 1 for s in selections if s.strip().isdigit()]
                        
                        files_to_delete = [files[i]['Key'] for i in indices if 0 <= i < len(files)]
                        
                        if files_to_delete:
                            confirm = input(f"Delete {len(files_to_delete)} files? (y/N): ").lower() == 'y'
                            if confirm:
                                cleanup.delete_files(files_to_delete)
                            else:
                                print("Cancelled")
                        else:
                            print("No valid files selected")
                    except ValueError:
                        print("Invalid input")
                else:
                    print("No files to delete")
            
            elif choice == '3':
                # Delete folder contents
                folder = input("Enter folder path to delete (press Enter for default): ").strip()
                if not folder:
                    folder = cleanup.s3_folder
                
                confirm = input(f"Delete ALL files in '{folder}'? (y/N): ").lower() == 'y'
                if confirm:
                    cleanup.delete_folder(folder)
                else:
                    print("Cancelled")
            
            elif choice == '4':
                # Cleanup old files
                try:
                    days = int(input("Delete files older than (days, default 30): ") or "30")
                    confirm = input(f"Delete files older than {days} days? (y/N): ").lower() == 'y'
                    if confirm:
                        cleanup.cleanup_old_files(days)
                    else:
                        print("Cancelled")
                except ValueError:
                    print("Invalid number")
            
            elif choice == '5':
                # Empty entire bucket
                confirm1 = input("⚠️  This will delete ALL files in the bucket. Continue? (y/N): ").lower() == 'y'
                if confirm1:
                    confirm2 = input("⚠️  ARE YOU ABSOLUTELY SURE? (type 'DELETE' to confirm): ")
                    if confirm2 == 'DELETE':
                        cleanup.empty_bucket(confirm=True)
                    else:
                        print("Cancelled")
                else:
                    print("Cancelled")
            
            elif choice == '6':
                print("Goodbye! 👋")
                break
            
            else:
                print("Invalid option")
                
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    main()