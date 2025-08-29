# tradingview_helper.py
import pandas as pd
import os
import logging
import time
from typing import Dict, Optional
from dotenv import load_dotenv
import boto3
from io import StringIO
import tempfile

# Load environment variables
load_dotenv()
DHAN_CLIENT_ID = os.getenv("DHAN_CLIENT_ID")
DHAN_ACCESS_TOKEN = os.getenv("DHAN_ACCESS_TOKEN")

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# S3 Configuration
S3_BUCKET = os.getenv("S3_BUCKET", "mytradeapp-csv-data")
S3_MAPPING_KEY = "uploads/mapping.csv"
S3_EOD_DIR = "eod_data"
S3_DROP_DIR = "stock_dump_eod"

# Initialize S3 client
s3_client = boto3.client('s3', region_name='ap-south-1')

# Initialize Dhan SDK
dhan = None
dhan_context = None

if DHAN_CLIENT_ID and DHAN_ACCESS_TOKEN:
    try:
        from dhanhq import DhanContext, dhanhq
        dhan_context = DhanContext(DHAN_CLIENT_ID, DHAN_ACCESS_TOKEN)
        dhan = dhanhq(dhan_context)
        logging.info("✅ Dhan SDK initialized successfully")
    except ImportError:
        logging.warning("⚠️ Dhan SDK not available. Live data will be disabled.")
    except Exception as e:
        logging.error(f"❌ Failed to initialize Dhan SDK: {e}")
else:
    logging.warning("⚠️ Dhan credentials not found. Live data will be disabled.")

# Global cache for live data and mapping
_live_data_cache = {}
_last_live_fetch_time = 0
LIVE_DATA_CACHE_DURATION = 600
_df_map = None

def load_mapping_from_s3():
    """Load mapping data from S3"""
    global _df_map
    try:
        response = s3_client.get_object(Bucket=S3_BUCKET, Key=S3_MAPPING_KEY)
        csv_content = response['Body'].read().decode('utf-8')
        _df_map = pd.read_csv(StringIO(csv_content))
        _df_map = _df_map[["Stock Name", "Instrument ID", "Market Cap", "Setup_Case"]].dropna()
        _df_map["Instrument ID"] = _df_map["Instrument ID"].astype(int)
        logging.info(f"✅ Loaded mapping from S3 with {len(_df_map)} instruments")
        return _df_map
    except Exception as e:
        logging.error(f"❌ Failed to load mapping from S3: {e}")
        return pd.DataFrame(columns=["Stock Name", "Instrument ID", "Market Cap", "Setup_Case"])

def get_df_map():
    """Get mapping DataFrame (lazy loading)"""
    global _df_map
    if _df_map is None:
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
        logging.warning("⚠️ Dhan SDK not available for live data")
        return {}

    df_map = get_df_map()
    instrument_ids = df_map["Instrument ID"].tolist()
    logging.info(f"🔄 Fetching live data for {len(instrument_ids)} instruments")

    live_data = {}
    
    for batch_num, batch in enumerate(batch_list(instrument_ids, 1000), start=1):
        try:
            response = dhan.quote_data(securities={"NSE_EQ": batch})
            
            if isinstance(response, dict) and "data" in response:
                batch_data = response["data"].get("data", {}).get("NSE_EQ", {})
                live_data.update(batch_data)
                logging.info(f"✅ Batch {batch_num}: {len(batch_data)} instruments")
            else:
                logging.warning(f"⚠️ Invalid response in batch {batch_num}")
        
        except Exception as e:
            logging.error(f"❌ API error in batch {batch_num}: {e}")
        
        time.sleep(1)

    _live_data_cache = live_data
    _last_live_fetch_time = current_time
    logging.info(f"✅ Total live instruments cached: {len(live_data)}")
    
    return _live_data_cache

def get_stock_list():
    df_map = get_df_map()
    stocks = []
    for _, row in df_map.iterrows():
        stocks.append({
            "stock_name": row["Stock Name"],
            "instrument_id": int(row["Instrument ID"]),
            "market_cap": float(row["Market Cap"]),
            "setup_case": row["Setup_Case"]
        })
    return stocks

def load_csv_from_s3(instrument_id):
    """Load CSV from S3 with fallback to backup directory"""
    try:
        # Try main directory first
        try:
            key = f"{S3_EOD_DIR}/{instrument_id}.csv"
            response = s3_client.get_object(Bucket=S3_BUCKET, Key=key)
            logging.info(f"✅ Loaded {instrument_id}.csv from {S3_EOD_DIR}")
        except:
            # Fallback to backup directory
            key = f"{S3_DROP_DIR}/{instrument_id}.csv"
            response = s3_client.get_object(Bucket=S3_BUCKET, Key=key)
            logging.info(f"✅ Loaded {instrument_id}.csv from {S3_DROP_DIR}")
        
        csv_content = response['Body'].read().decode('utf-8')
        return pd.read_csv(StringIO(csv_content))
        
    except Exception as e:
        logging.error(f"❌ Failed to load {instrument_id}.csv from S3: {e}")
        return None

def load_stock_data(instrument_id):
    df = load_csv_from_s3(instrument_id)
    if df is None:
        return None

    try:
        df = df.rename(columns=str.lower)
        df = df[["date", "open", "high", "low", "close", "volume"]].dropna()
        df["date"] = pd.to_datetime(df["date"])

        data = [
            {
                "time": int(row["date"].timestamp()),
                "open": row["open"],
                "high": row["high"],
                "low": row["low"],
                "close": row["close"],
                "volume": row["volume"]
            }
            for _, row in df.iterrows()
        ]
        
        # Add live data if available
        if dhan is not None:
            if not _live_data_cache or time.time() - _last_live_fetch_time >= LIVE_DATA_CACHE_DURATION:
                fetch_all_live_data_bulk()
            
            if str(instrument_id) in _live_data_cache:
                live = _live_data_cache[str(instrument_id)]
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
        logging.error(f"❌ Error processing data for {instrument_id}: {e}")
        return None

def refresh_live_data():
    global _last_live_fetch_time
    _last_live_fetch_time = 0
    logging.info("🔄 Forcing live data refresh")
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
        "s3_bucket": S3_BUCKET
    }

def upload_csv_to_s3(file_path, s3_key):
    """Upload a CSV file to S3"""
    try:
        s3_client.upload_file(file_path, S3_BUCKET, s3_key)
        logging.info(f"✅ Uploaded {file_path} to s3://{S3_BUCKET}/{s3_key}")
        return True
    except Exception as e:
        logging.error(f"❌ Failed to upload to S3: {e}")
        return False