import os
import time
import logging
from datetime import datetime
from typing import List, Dict
from io import StringIO
import pandas as pd
import boto3
import json
from dhanhq import DhanContext, dhanhq

# ===========================
# Logging
# ===========================
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

# ===========================
# AWS / S3 Config
# ===========================
S3_BUCKET = "mytradeapp-csv-bucket"
S3_WATCHLIST_KEY = "uploads/momentum_watchlist.csv"
S3_MAPPING_KEY = "uploads/master_marketsmithindia_data_marketcap_gt500cr.csv"

def init_s3_client():
    try:
        client = boto3.client("s3", region_name="ap-south-1")
        logger.info("✅ S3 client initialized")
        return client
    except Exception as e:
        logger.error(f"❌ Failed to initialize S3 client: {e}")
        return None

s3_client = init_s3_client()

# ===========================
# Get Dhan Credentials from AWS Secrets Manager
# ===========================
def get_dhan_credentials(secret_name="dhan_api_secret", region_name="ap-south-1"):
    try:
        client = boto3.client("secretsmanager", region_name=region_name)
        response = client.get_secret_value(SecretId=secret_name)
        secret_dict = json.loads(response['SecretString'])
        return secret_dict.get("DHAN_CLIENT_ID"), secret_dict.get("DHAN_ACCESS_TOKEN")
    except Exception as e:
        logger.error(f"❌ Failed to retrieve secret '{secret_name}': {e}")
        return None, None

DHAN_CLIENT_ID, DHAN_ACCESS_TOKEN = get_dhan_credentials()

# ===========================
# Dhan SDK Init
# ===========================
dhan = None
if DHAN_CLIENT_ID and DHAN_ACCESS_TOKEN:
    try:
        dhan_context = DhanContext(DHAN_CLIENT_ID, DHAN_ACCESS_TOKEN)
        dhan = dhanhq(dhan_context)
        logger.info("✅ Dhan SDK initialized")
    except Exception as e:
        logger.error(f"❌ Failed to init Dhan SDK: {e}")

# ===========================
# S3 CSV Utilities
# ===========================
def load_csv_from_s3(key: str) -> pd.DataFrame:
    try:
        response = s3_client.get_object(Bucket=S3_BUCKET, Key=key)
        csv_content = response['Body'].read().decode('utf-8')
        df = pd.read_csv(StringIO(csv_content))
        return df
    except s3_client.exceptions.NoSuchKey:
        logger.warning(f"⚠️ File not found in S3: {key}")
        return pd.DataFrame()
    except Exception as e:
        logger.error(f"❌ Error reading {key} from S3: {e}")
        return pd.DataFrame()

def save_csv_to_s3(df: pd.DataFrame, key: str):
    try:
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)
        s3_client.put_object(Bucket=S3_BUCKET, Key=key, Body=csv_buffer.getvalue())
        logger.info(f"✅ Saved CSV to S3: {key}")
    except Exception as e:
        logger.error(f"❌ Failed to save CSV to S3: {e}")

# ===========================
# Mapping
# ===========================
def load_mapping() -> Dict[str, Dict]:
    df = load_csv_from_s3(S3_MAPPING_KEY)
    df = df.dropna(subset=["Stock Name"])
    mapping = {row["Stock Name"]: row.to_dict() for _, row in df.iterrows()}
    logger.info(f"✅ Loaded {len(mapping)} mapping rows from S3")
    return mapping

# ===========================
# Watchlist CRUD
# ===========================
def load_watchlist() -> List[Dict]:
    df = load_csv_from_s3(S3_WATCHLIST_KEY)
    if df.empty:
        return []
    return df.to_dict(orient="records")

def save_watchlist(data: List[Dict]):
    df = pd.DataFrame(data)
    save_csv_to_s3(df, S3_WATCHLIST_KEY)

def add_stock(stock_name: str, entry_price: str):
    mapping = load_mapping()
    row = {
        "Stock Name": stock_name,
        "Instrument ID": mapping.get(stock_name, {}).get("Instrument ID", ""),
        "MTF_LEVERAGE": mapping.get(stock_name, {}).get("MTF_LEVERAGE", ""),
        "MIS_LEVERAGE": mapping.get(stock_name, {}).get("MIS_LEVERAGE", ""),
        "Entry Price": entry_price,
        "LTP": "",
        "High": "",
        "Low": "",
        "% Change": "",
        "Time": "",
        "Breakout": "",
        "Action": ""
    }
    data = load_watchlist()
    data.append(row)
    save_watchlist(data)

def delete_stock(index: int):
    data = load_watchlist()
    if 0 <= index < len(data):
        data.pop(index)
        save_watchlist(data)

def update_stock(index: int, field: str, value: str):
    data = load_watchlist()
    if 0 <= index < len(data) and field in data[index]:
        data[index][field] = value
        save_watchlist(data)

def mark_auto_buy(symbol: str):
    data = load_watchlist()
    for row in data:
        if row["Stock Name"] == symbol:
            row["Action"] = "AUTO_BUYED"
            break
    save_watchlist(data)
    logger.info(f"✅ Marked {symbol} as AUTO_BUYED")

# ===========================
# Live Quotes
# ===========================
def batch_list(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i+n]

def fetch_live_quote(symbol: str, live_data=None):
    mapping = load_mapping()
    instrument_id = mapping.get(symbol, {}).get("Instrument ID")
    if not instrument_id:
        return {"LTP": 0, "High": 0, "Low": 0, "% Change": 0}
    instrument_id = int(instrument_id)
    if live_data and instrument_id in live_data:
        data = live_data[instrument_id]
        ltp = float(data.get("last_price", 0))
        ohlc = data.get("ohlc", {})
        high = float(ohlc.get("high", ltp))
        low = float(ohlc.get("low", ltp))
        prev_close = float(ohlc.get("close", ltp))
        pct_change = round(((ltp - prev_close)/prev_close)*100,2) if prev_close else 0
        return {"LTP": ltp, "High": high, "Low": low, "% Change": pct_change}
    return {"LTP": 0, "High": 0, "Low": 0, "% Change": 0}

def fetch_live_data(instrument_ids):
    live_data = {}
    if dhan is None:
        logger.warning("⚠️ Dhan SDK not initialized")
        return {}
    for batch in batch_list(instrument_ids, 1000):
        try:
            response = dhan.quote_data(securities={"NSE_EQ": batch})
            batch_data = response.get("data", {}).get("data", {}).get("NSE_EQ", {})
            live_data.update({int(k): v for k,v in batch_data.items()})
        except Exception as e:
            logger.warning(f"⚠️ Batch fetch error: {e}")
        time.sleep(0.5)
    return live_data

def update_quotes_and_breakouts(live_data=None):
    data = load_watchlist()
    mapping = load_mapping()
    for row in data:
        symbol = row["Stock Name"]
        quote = fetch_live_quote(symbol, live_data)
        row.update(quote)
        breakout = "YES" if float(row["LTP"] or 0) > float(row["Entry Price"] or 0) else "NO"
        row["Breakout"] = breakout
        if row.get("Action") != "AUTO_BUYED":
            row["Action"] = "BUY" if breakout=="YES" else ""
        if breakout=="YES" and row.get("Action") != "AUTO_BUYED":
            row["Time"] = datetime.now().strftime("%H:%M:%S")
    save_watchlist(data)
    return data

def get_today_pnl(dhan_instance):
    """
    Fetch today's PnL (realized and unrealized) from DHAN positions.
    Returns a dict with realized, unrealized, and total PnL.
    """
    try:
        positions_response = dhan_instance.get_positions()
        if positions_response.get("status") != "success":
            return {"success": False, "message": "Failed to fetch positions"}

        realized = 0.0
        unrealized = 0.0

        for pos in positions_response.get("data", []):
            realized += pos.get("realizedProfit", 0.0)
            if pos.get("netQty", 0) != 0:
                unrealized += pos.get("unrealizedProfit", 0.0)

        return {
            "success": True,
            "pnl": {
                "realized_pnl": realized,
                "unrealized_pnl": unrealized,
                "total_pnl": realized + unrealized
            }
        }
    except Exception as e:
        return {"success": False, "message": str(e)}

