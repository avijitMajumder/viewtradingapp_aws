# tradingview_helper.py
import pandas as pd
import os
import logging
import time
from typing import Dict, Optional, List, Any
from dotenv import load_dotenv
import boto3
import json  # <-- add this
from io import StringIO
import tempfile
from botocore.exceptions import ClientError, NoCredentialsError



# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
# Load environment variables
load_dotenv()

def get_dhan_credentials(secret_name="dhan_api_secret", region_name="ap-south-1"):
    """Retrieve DHAN credentials from AWS Secrets Manager"""
    try:
        client = boto3.client("secretsmanager", region_name=region_name)
        response = client.get_secret_value(SecretId=secret_name)
        secret_dict = json.loads(response['SecretString'])
        return secret_dict.get("DHAN_CLIENT_ID"), secret_dict.get("DHAN_ACCESS_TOKEN")
    except Exception as e:
        logger.error(f"❌ Failed to retrieve secret '{secret_name}': {e}")
        return None, None

# Get credentials securely from Secrets Manager
DHAN_CLIENT_ID, DHAN_ACCESS_TOKEN = get_dhan_credentials()



# S3 Configuration - CORRECTED BUCKET NAME
#S3_BUCKET = os.getenv("S3_BUCKET", "mytradeapp-csv-bucket")  # Changed to your actual bucket name
S3_BUCKET = "mytradeapp-csv-bucket"  # Hard-coded or configured via IAM role only
S3_MAPPING_KEY = "uploads/mapping.csv"
S3_EOD_DIR = "eod_data"
S3_DROP_DIR = "stock_dump_eod"

# Initialize S3 client with error handling


def init_s3_client():
    try:
        client = boto3.client('s3', region_name='ap-south-1')
        # Quick test: list buckets to ensure role works
        client.list_buckets()
        logger.info("✅ S3 client initialized with IAM Role")
        return client
    except NoCredentialsError:
        logger.error("❌ No credentials found for S3. Ensure EC2 IAM Role is attached.")
        return None
    except ClientError as e:
        logger.error(f"❌ Failed to initialize S3 client: {e}")
        return None

s3_client = init_s3_client()

def get_accessible_buckets():
    if not s3_client:
        return []
    try:
        response = s3_client.list_buckets()
        return [b['Name'] for b in response.get('Buckets', [])]
    except Exception as e:
        logger.error(f"❌ Could not list buckets: {e}")
        return []

logger.info(f"Accessible buckets: {get_accessible_buckets()}")


# Initialize Dhan SDK
dhan = None
dhan_context = None

if DHAN_CLIENT_ID and DHAN_ACCESS_TOKEN:
    try:
        from dhanhq import DhanContext, dhanhq
        dhan_context = DhanContext(DHAN_CLIENT_ID, DHAN_ACCESS_TOKEN)
        dhan = dhanhq(dhan_context)
        logger.info("✅ Dhan SDK initialized successfully")
    except ImportError:
        logger.warning("⚠️ Dhan SDK not available. Live data will be disabled.")
    except Exception as e:
        logger.error(f"❌ Failed to initialize Dhan SDK: {e}")
else:
    logger.warning("⚠️ Dhan credentials not found. Live data will be disabled.")

# Global cache for live data and mapping
_live_data_cache = {}
_last_live_fetch_time = 0
LIVE_DATA_CACHE_DURATION = 600
_df_map = None

def check_s3_bucket_exists():
    """Check if S3 bucket exists and is accessible"""
    if not s3_client:
        logger.error("S3 client not initialized")
        return False
    
    try:
        s3_client.head_bucket(Bucket=S3_BUCKET)
        logger.info(f"✅ S3 bucket '{S3_BUCKET}' exists and is accessible")
        return True
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == '404':
            logger.error(f"❌ S3 bucket '{S3_BUCKET}' not found")
        elif error_code == '403':
            logger.error(f"❌ Access denied to S3 bucket '{S3_BUCKET}'")
        else:
            logger.error(f"❌ S3 connection error: {e}")
        return False
    except Exception as e:
        logger.error(f"❌ Unexpected S3 error: {e}")
        return False

def load_mapping_from_s3():
    """Load mapping data from S3 with enhanced error handling"""
    global _df_map
    try:
        if not check_s3_bucket_exists():
            logger.error("Cannot load mapping - S3 bucket not accessible")
            return pd.DataFrame(columns=["Stock Name", "Instrument ID", "Market Cap", "Setup_Case"])
        
        response = s3_client.get_object(Bucket=S3_BUCKET, Key=S3_MAPPING_KEY)
        csv_content = response['Body'].read().decode('utf-8')
        _df_map = pd.read_csv(StringIO(csv_content))
        
        # Validate required columns
        required_columns = ["Stock Name", "Instrument ID", "Market Cap", "Setup_Case"]
        missing_columns = [col for col in required_columns if col not in _df_map.columns]
        
        if missing_columns:
            logger.error(f"Missing columns in mapping file: {missing_columns}")
            return pd.DataFrame(columns=required_columns)
        
        _df_map = _df_map[required_columns].dropna()
        _df_map["Instrument ID"] = pd.to_numeric(_df_map["Instrument ID"], errors='coerce').astype('Int64')
        _df_map = _df_map.dropna(subset=["Instrument ID"])
        
        logger.info(f"✅ Loaded mapping from S3 with {len(_df_map)} instruments")
        return _df_map
        
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            logger.error(f"❌ Mapping file not found at s3://{S3_BUCKET}/{S3_MAPPING_KEY}")
            # Create a sample mapping file
            return create_sample_mapping_file()
        else:
            logger.error(f"❌ S3 client error loading mapping: {e}")
        return pd.DataFrame(columns=["Stock Name", "Instrument ID", "Market Cap", "Setup_Case"])
    except Exception as e:
        logger.error(f"❌ Failed to load mapping from S3: {e}")
        return pd.DataFrame(columns=["Stock Name", "Instrument ID", "Market Cap", "Setup_Case"])

def create_sample_mapping_file():
    """Create a sample mapping file if it doesn't exist"""
    try:
        logger.info("Creating sample mapping file...")
        
        # Sample mapping data for popular Indian stocks
        sample_data = {
            "Stock Name": ["RELIANCE", "TATASTEEL", "INFY", "HDFCBANK", "ICICIBANK", "SBIN"],
            "Instrument ID": [2885, 3499, 1594, 1333, 4963, 3045],
            "Market Cap": [1000000, 500000, 800000, 1200000, 900000, 700000],
            "Setup_Case": ["Case1", "Case2", "Case1", "Case3", "Case2", "Case1"]
        }
        
        sample_df = pd.DataFrame(sample_data)
        
        # Upload to S3
        csv_buffer = StringIO()
        sample_df.to_csv(csv_buffer, index=False)
        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=S3_MAPPING_KEY,
            Body=csv_buffer.getvalue()
        )
        
        logger.info(f"✅ Sample mapping file created at s3://{S3_BUCKET}/{S3_MAPPING_KEY}")
        return sample_df
        
    except Exception as e:
        logger.error(f"❌ Failed to create sample mapping file: {e}")
        return pd.DataFrame(columns=["Stock Name", "Instrument ID", "Market Cap", "Setup_Case"])

def get_df_map():
    """Get mapping DataFrame (lazy loading)"""
    global _df_map
    if _df_map is None or _df_map.empty:
        _df_map = load_mapping_from_s3()
    return _df_map

def batch_list(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

def fetch_all_live_data_bulk():
    global _live_data_cache, _last_live_fetch_time
    
    current_time = time.time()
    if current_time - _last_live_fetch_time < LIVE_DATA_CACHE_DURATION and _live_data_cache:
        return _live_data_cache

    if dhan is None:
        logger.warning("⚠️ Dhan SDK not available for live data")
        return {}

    df_map = get_df_map()
    if df_map.empty:
        logger.warning("⚠️ No instruments found in mapping for live data")
        return {}

    instrument_ids = df_map["Instrument ID"].dropna().astype(int).tolist()
    logger.info(f"🔄 Fetching live data for {len(instrument_ids)} instruments")

    live_data = {}
    
    for batch_num, batch in enumerate(batch_list(instrument_ids, 1000), start=1):
        try:
            logger.info(f"Processing batch {batch_num} with {len(batch)} instruments")
            response = dhan.quote_data(securities={"NSE_EQ": batch})
            
            if isinstance(response, dict) and "data" in response:
                batch_data = response["data"].get("data", {}).get("NSE_EQ", {})
                live_data.update(batch_data)
                logger.info(f"✅ Batch {batch_num}: {len(batch_data)} instruments")
            else:
                logger.warning(f"⚠️ Invalid response in batch {batch_num}")
        
        except Exception as e:
            logger.error(f"❌ API error in batch {batch_num}: {e}")
        
        time.sleep(1)

    _live_data_cache = live_data
    _last_live_fetch_time = current_time
    logger.info(f"✅ Total live instruments cached: {len(live_data)}")
    
    return _live_data_cache

def get_stock_list():
    df_map = get_df_map()
    stocks = []
    for _, row in df_map.iterrows():
        try:
            stocks.append({
                "stock_name": str(row["Stock Name"]),
                "instrument_id": int(row["Instrument ID"]),
                "market_cap": float(row["Market Cap"]) if pd.notna(row["Market Cap"]) else 0.0,
                "setup_case": str(row["Setup_Case"]) if pd.notna(row["Setup_Case"]) else "Unknown"
            })
        except (ValueError, TypeError) as e:
            logger.warning(f"Skipping invalid row in mapping: {e}")
    return stocks

def load_csv_from_s3(instrument_id):
    """Load CSV from S3 with fallback to backup directory"""
    if not s3_client:
        logger.error("S3 client not available")
        return None

    locations = [S3_EOD_DIR, S3_DROP_DIR]
    
    for location in locations:
        try:
            key = f"{location}/{instrument_id}.csv"
            response = s3_client.get_object(Bucket=S3_BUCKET, Key=key)
            logger.info(f"✅ Loaded {instrument_id}.csv from {location}")
            
            csv_content = response['Body'].read().decode('utf-8')
            return pd.read_csv(StringIO(csv_content))
            
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey':
                continue  # Try next location
            else:
                logger.error(f"❌ S3 error loading {instrument_id}.csv: {e}")
                return None
        except Exception as e:
            logger.error(f"❌ Failed to load {instrument_id}.csv: {e}")
            return None
    
    logger.error(f"❌ CSV for instrument {instrument_id} not found in any location")
    return None

def load_stock_data(instrument_id):
    df = load_csv_from_s3(instrument_id)
    if df is None:
        return None

    try:
        df = df.rename(columns=str.lower)
        required_columns = ["date", "open", "high", "low", "close", "volume"]
        
        # Check for required columns
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            logger.error(f"Missing columns in data for {instrument_id}: {missing_columns}")
            return None
        
        df = df[required_columns].dropna()
        df["date"] = pd.to_datetime(df["date"], errors='coerce')
        df = df.dropna(subset=["date"])

        data = [
            {
                "time": int(row["date"].timestamp()),
                "open": float(row["open"]),
                "high": float(row["high"]),
                "low": float(row["low"]),
                "close": float(row["close"]),
                "volume": float(row["volume"])
            }
            for _, row in df.iterrows()
        ]
        
        # Add live data if available
        if dhan is not None:
            if not _live_data_cache or time.time() - _last_live_fetch_time >= LIVE_DATA_CACHE_DURATION:
                fetch_all_live_data_bulk()
            
            instrument_str = str(instrument_id)
            if instrument_str in _live_data_cache:
                live = _live_data_cache[instrument_str]
                ohlc = live.get("ohlc", {})
                last_price = live.get("last_price", ohlc.get("close", 0))
                
                live_bar = {
                    "time": int(time.time()),
                    "open": float(ohlc.get("open", last_price)),
                    "high": float(ohlc.get("high", last_price)),
                    "low": float(ohlc.get("low", last_price)),
                    "close": float(last_price),
                    "volume": float(live.get("volume", 0))
                }
                data.append(live_bar)
        
        return data
        
    except Exception as e:
        logger.error(f"❌ Error processing data for {instrument_id}: {e}")
        return None

def refresh_live_data():
    global _last_live_fetch_time
    _last_live_fetch_time = 0
    logger.info("🔄 Forcing live data refresh")
    return fetch_all_live_data_bulk()

def get_cache_status():
    current_time = time.time()
    cache_age = current_time - _last_live_fetch_time
    cache_valid = cache_age < LIVE_DATA_CACHE_DURATION
    df_map = get_df_map()
    
    return {
        "dhan_available": dhan is not None,
        "cache_age_seconds": cache_age,
        "cache_valid": cache_valid,
        "cached_instruments": len(_live_data_cache),
        "total_instruments": len(df_map),
        "s3_bucket": S3_BUCKET,
        "s3_accessible": check_s3_bucket_exists()
    }

def upload_csv_to_s3(file_path, s3_key):
    """Upload a CSV file to S3"""
    if not s3_client:
        logger.error("S3 client not available for upload")
        return False
        
    try:
        s3_client.upload_file(file_path, S3_BUCKET, s3_key)
        logger.info(f"✅ Uploaded {file_path} to s3://{S3_BUCKET}/{s3_key}")
        return True
    except Exception as e:
        logger.error(f"❌ Failed to upload to S3: {e}")
        return False


# tradingview_helper.py
def get_ec2_public_ip(tag_name="FlaskTradingApp", region_name="ap-south-1"):
    """
    Get the public IP of a running EC2 instance with the given tag.
    Uses IAM Role attached to EC2, no credentials needed in .env.
    """
    try:
        ec2_client = boto3.client('ec2', region_name=region_name)
        response = ec2_client.describe_instances(
            Filters=[
                {'Name': 'tag:Name', 'Values': [tag_name]},
                {'Name': 'instance-state-name', 'Values': ['running']}
            ]
        )

        for reservation in response.get('Reservations', []):
            for instance in reservation.get('Instances', []):
                public_ip = instance.get('PublicIpAddress')
                if public_ip:
                    return public_ip

        logger.warning(f"No running EC2 instance found with tag '{tag_name}'")
        return None

    except Exception as e:
        logger.error(f"❌ Error fetching EC2 public IP: {e}")
        return None



# Initialize on import
logger.info("TradingView helper initialized")
check_s3_bucket_exists()
get_df_map()  # Pre-load mapping
