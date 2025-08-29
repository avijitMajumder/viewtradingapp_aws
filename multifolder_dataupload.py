# s3_uploader.py
import boto3
import os
import logging
from dotenv import load_dotenv
from botocore.exceptions import ClientError, NoCredentialsError
import glob

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
        self.bucket_name = os.getenv('S3_BUCKET_NAME', 'mytradeapp-csv-bucket')
        self.local_folder = os.getenv('LOCAL_UPLOAD_FOLDER', 'D:\\2025 BACKUP FEB13\\Python\\Trade\\dhanhq_flask_app_updated\\eod_data')
        self.s3_folder = os.getenv('S3_FOLDER_NAME', 'eod_data')
        
        # Get additional folders from .env (optional)
        self.additional_folders = self._load_additional_folders()
        
        # Get file patterns from .env (optional)
        patterns_str = os.getenv('FILE_PATTERNS', '*.csv,*.txt')
        self.file_patterns = [pattern.strip() for pattern in patterns_str.split(',')]
        
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
        self._print_configuration()
    
    def _load_additional_folders(self):
        """
        Load additional folder mappings from .env if they exist
        """
        folders = []
        
        # Check for additional folder configurations
        folder_index = 1
        while True:
            local_key = f'FOLDER{folder_index}_LOCAL'
            s3_key = f'FOLDER{folder_index}_S3'
            
            local_path = os.getenv(local_key)
            s3_folder_name = os.getenv(s3_key)
            
            if local_path and s3_folder_name:
                folders.append((local_path, s3_folder_name))
                folder_index += 1
            else:
                break
        
        return folders
    
    def _print_configuration(self):
        """Print the current configuration"""
        print("🔧 Configuration:")
        print("=" * 50)
        print(f"AWS Region: {self.region}")
        print(f"S3 Bucket: {self.bucket_name}")
        print(f"Main Local Folder: {self.local_folder}")
        print(f"Main S3 Folder: {self.s3_folder}")
        print(f"File Patterns: {', '.join(self.file_patterns)}")
        
        if self.additional_folders:
            print(f"\n📁 Additional Folders:")
            for i, (local_path, s3_folder) in enumerate(self.additional_folders, 1):
                print(f"  {i}. {local_path} → {s3_folder}")
        print("=" * 50)
    
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
    
    def upload_single_folder(self, local_folder_path, s3_folder_name):
        """
        Upload all matching files from a single local folder to S3
        """
        if not os.path.exists(local_folder_path):
            logger.error(f"❌ Local folder not found: {local_folder_path}")
            return {'total': 0, 'success': 0, 'failed': 0}
        
        # Ensure S3 folder exists
        if not self.ensure_folder_exists(s3_folder_name):
            return {'total': 0, 'success': 0, 'failed': 0}
        
        # Find all matching files
        all_files = []
        for pattern in self.file_patterns:
            pattern_files = glob.glob(os.path.join(local_folder_path, pattern))
            all_files.extend([f for f in pattern_files if os.path.isfile(f)])
        
        # Remove duplicates
        all_files = list(set(all_files))
        
        if not all_files:
            logger.warning(f"⚠️ No matching files found in: {local_folder_path}")
            return {'total': 0, 'success': 0, 'failed': 0}
        
        logger.info(f"📁 Found {len(all_files)} files in {local_folder_path}")
        
        # Upload files
        success_count = 0
        failed_count = 0
        
        for file_path in all_files:
            filename = os.path.basename(file_path)
            s3_key = f"{s3_folder_name}/{filename}"
            
            if self.upload_file(file_path, s3_key):
                success_count += 1
            else:
                failed_count += 1
        
        return {
            'total': len(all_files),
            'success': success_count,
            'failed': failed_count
        }
    
    def upload_main_folder(self):
        """
        Upload files from the main configured folder
        """
        return self.upload_single_folder(self.local_folder, self.s3_folder)
    
    def upload_all_folders(self):
        """
        Upload files from all configured folders (main + additional)
        """
        all_folders = [(self.local_folder, self.s3_folder)] + self.additional_folders
        
        if not all_folders:
            logger.error("❌ No folders configured")
            return False
        
        total_stats = {'total': 0, 'success': 0, 'failed': 0}
        all_success = True
        
        logger.info("🚀 Starting multi-folder upload...")
        
        for local_path, s3_folder in all_folders:
            logger.info(f"\n📤 Uploading from: {local_path} → {s3_folder}")
            
            stats = self.upload_single_folder(local_path, s3_folder)
            
            # Update totals
            total_stats['total'] += stats['total']
            total_stats['success'] += stats['success']
            total_stats['failed'] += stats['failed']
            
            if stats['failed'] > 0:
                all_success = False
        
        # Print summary
        print(f"\n{'='*50}")
        print("📊 UPLOAD SUMMARY")
        print(f"{'='*50}")
        print(f"Total folders processed: {len(all_folders)}")
        print(f"Total files found: {total_stats['total']}")
        print(f"✅ Successful uploads: {total_stats['success']}")
        print(f"❌ Failed uploads: {total_stats['failed']}")
        print(f"{'='*50}")
        
        return all_success and total_stats['failed'] == 0
    
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
    
    def list_s3_contents(self, s3_folder_name=None):
        """
        List all objects in the S3 bucket/folder
        """
        try:
            if s3_folder_name:
                prefix = f"{s3_folder_name}/"
            else:
                prefix = f"{self.s3_folder}/"
            
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
    print("🚀 S3 Uploader for MyTradeApp")
    print("=" * 50)
    
    try:
        # Initialize uploader
        uploader = S3Uploader()
        
        while True:
            print("\nChoose an option:")
            print("1. 📤 Upload main folder only")
            print("2. 📤 Upload all folders (main + additional)")
            print("3. 📋 List uploaded files")
            print("4. 🔧 Show configuration")
            print("5. ❌ Exit")
            
            choice = input("\nEnter your choice (1-5): ").strip()
            
            if choice == '1':
                print(f"\nUploading main folder: {uploader.local_folder}")
                if uploader.upload_main_folder():
                    print("\n✅ Main folder uploaded successfully!")
                else:
                    print("\n❌ Main folder upload failed")
            
            elif choice == '2':
                print("\nUploading all configured folders...")
                if uploader.upload_all_folders():
                    print("\n✅ All folders uploaded successfully!")
                else:
                    print("\n❌ Some folders failed to upload")
            
            elif choice == '3':
                folder_name = input("Enter S3 folder name to list (press Enter for main folder): ").strip()
                if not folder_name:
                    folder_name = uploader.s3_folder
                uploader.list_s3_contents(folder_name)
            
            elif choice == '4':
                uploader._print_configuration()
            
            elif choice == '5':
                print("👋 Goodbye!")
                break
            
            else:
                print("❌ Invalid choice. Please try again.")
                
    except Exception as e:
        print(f"❌ Error: {e}")
        print("\n💡 Make sure your .env file has correct AWS credentials")

if __name__ == "__main__":
    main()