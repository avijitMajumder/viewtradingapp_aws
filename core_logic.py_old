# core_logic.py
import os
import json
import time
import logging
from typing import Tuple, Optional, Dict

import boto3
import pandas as pd
from botocore.exceptions import NoCredentialsError, ClientError
from dotenv import load_dotenv

# Dhan imports
from dhanhq import DhanContext, dhanhq

# -------------------------
# Logging
# -------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("core_logic")

# -------------------------
# Load .env (optional)
# -------------------------
load_dotenv()

# -------------------------
# AWS Config
# -------------------------
AWS_REGION = os.getenv("AWS_REGION", "ap-south-1")
SECRETS_NAME = os.getenv("DHAN_SECRETS_NAME", "dhan_api_secret")
S3_BUCKET = os.getenv("S3_BUCKET", "mytradeapp-csv-bucket")
S3_MAPPING_KEY = os.getenv("S3_MAPPING_KEY", "uploads/mapping.csv")
S3_STOCKLIST_KEY = os.getenv(
    "S3_STOCKLIST_KEY",
    "uploads/master_marketsmithindia_data_marketcap_gt500cr.csv"
)

# -------------------------
# Init AWS Clients
# -------------------------
def init_s3_client(region_name: str = AWS_REGION):
    try:
        s3 = boto3.client("s3", region_name=region_name)
        s3.list_buckets()
        logger.info("✅ S3 client initialized")
        return s3
    except NoCredentialsError:
        logger.error("❌ No AWS credentials found")
        return None
    except ClientError as e:
        logger.error(f"❌ Failed to init S3 client: {e}")
        return None

def get_dhan_credentials_from_secrets(secret_name: str = SECRETS_NAME, region_name: str = AWS_REGION) -> Tuple[Optional[str], Optional[str]]:
    try:
        sm = boto3.client("secretsmanager", region_name=region_name)
        resp = sm.get_secret_value(SecretId=secret_name)
        secret = json.loads(resp.get("SecretString", "{}"))
        return (
            secret.get("DHAN_CLIENT_ID") or secret.get("client_id"),
            secret.get("DHAN_ACCESS_TOKEN") or secret.get("access_token")
        )
    except Exception as e:
        logger.error(f"❌ Could not fetch Dhan credentials: {e}")
        return None, None

s3_client = init_s3_client()
DHAN_CLIENT_ID, DHAN_ACCESS_TOKEN = get_dhan_credentials_from_secrets()

# -------------------------
# Init Dhan Client
# -------------------------
dhan = None
if DHAN_CLIENT_ID and DHAN_ACCESS_TOKEN:
    try:
        try:
            context = DhanContext(DHAN_CLIENT_ID, DHAN_ACCESS_TOKEN)
            dhan = dhanhq(context)
        except Exception:
            dhan = dhanhq(DHAN_CLIENT_ID, DHAN_ACCESS_TOKEN)
        logger.info("✅ Dhan client initialized")
    except Exception as e:
        logger.error(f"❌ Failed to init Dhan client: {e}")

# -------------------------
# Mapping Caches
# -------------------------
_mapping_cache: Dict[str, int] = {}
_leverage_cache: Dict[int, float] = {}
_mtf_leverage_cache: Dict[int, float] = {}
_security_id_cache: Dict[str, int] = {}

def load_mapping_from_s3(bucket: str = S3_BUCKET, key: str = S3_MAPPING_KEY) -> Optional[pd.DataFrame]:
    if not s3_client:
        return None
    try:
        obj = s3_client.get_object(Bucket=bucket, Key=key)
        return pd.read_csv(obj["Body"])
    except Exception as e:
        logger.error(f"❌ Failed to load mapping from S3: {e}")
        return None

def build_mapping_caches(force_reload: bool = False):
    global _mapping_cache, _leverage_cache, _mtf_leverage_cache

    if _mapping_cache and not force_reload:
        return

    # 1️⃣ Load mapping.csv (primary)
    df_map = load_mapping_from_s3(bucket=S3_BUCKET, key=S3_MAPPING_KEY)
    if df_map is not None:
        for _, row in df_map.iterrows():
            try:
                stock = str(row.get("Stock Name", "")).strip().upper()
                inst = int(row["Instrument ID"])
                _mapping_cache[stock] = inst
                _leverage_cache[inst] = float(row.get("MIS_LEVERAGE", 1.0))
                _mtf_leverage_cache[inst] = float(row.get("MTF_LEVERAGE", _leverage_cache[inst]))
            except Exception:
                continue

    # 2️⃣ Load master_marketsmithindia_data_marketcap_gt500cr.csv (for any missing symbols like INFY)
    STOCKLIST_KEY = "uploads/master_marketsmithindia_data_marketcap_gt500cr.csv"
    try:
        obj = s3_client.get_object(Bucket=S3_BUCKET, Key=STOCKLIST_KEY)
        df_master = pd.read_csv(obj["Body"])
        for _, row in df_master.iterrows():
            try:
                stock = str(row.get("Stock Name", "")).strip().upper()
                if stock in _mapping_cache:
                    continue  # already loaded
                inst = int(row["Instrument ID"])
                _mapping_cache[stock] = inst
                _leverage_cache[inst] = float(row.get("MIS_LEVERAGE", 1.0))      # default
                _mtf_leverage_cache[inst] = float(row.get("MTF_LEVERAGE", _leverage_cache[inst]))  # default
            except Exception:
                continue
        logger.info(f"✅ Mapping caches updated with master list ({len(_mapping_cache)} symbols total)")
    except Exception as e:
        logger.warning(f"⚠️ Could not load master_marketsmithindia_data_marketcap_gt500cr.csv: {e}")


build_mapping_caches()

# -------------------------
# Security ID Resolver
# -------------------------
def get_cached_security_id(symbol: str) -> Optional[int]:
    symbol_u = symbol.strip().upper()

    if symbol_u in _security_id_cache:
        return _security_id_cache[symbol_u]
    if symbol_u in _mapping_cache:
        sec_id = _mapping_cache[symbol_u]
        _security_id_cache[symbol_u] = sec_id
        return sec_id

    build_mapping_caches(force_reload=True)
    if symbol_u in _mapping_cache:
        sec_id = _mapping_cache[symbol_u]
        _security_id_cache[symbol_u] = sec_id
        return sec_id

    if not s3_client:
        raise ValueError("S3 client not initialized")

    try:
        obj = s3_client.get_object(Bucket=S3_BUCKET, Key=S3_STOCKLIST_KEY)
        df = pd.read_csv(obj["Body"])
        row = df[df["Stock Name"].str.upper() == symbol_u]
        if row.empty:
            raise ValueError(f"❌ Symbol '{symbol_u}' not found in stocklist")
        sec_id = int(row.iloc[0]["Instrument ID"])
        _security_id_cache[symbol_u] = sec_id
        return sec_id
    except Exception as e:
        raise ValueError(f"❌ Error resolving security ID for {symbol_u}: {e}")

# -------------------------
# Dhan Helpers
# -------------------------
def get_available_balance() -> float:
    if dhan is None:
        return 0.0
    try:
        resp = dhan.get_fund_limits()
        if isinstance(resp, dict):
            data = resp.get("data", {})
            bal = data.get("availableBalance") or data.get("availabelBalance")
            return float(bal) if bal else 0.0
    except Exception as e:
        logger.error(f"Error fetching balance: {e}")
    return 0.0

# -------------------------
# Position Sizing
# -------------------------
def calculate_position_size_mixed(price: float, entry: float, sl_price: float, sec_id: int, productType: str) -> Tuple[int, float, float, float, float]:
    try:
        price, entry, sl_price = float(price), float(entry), float(sl_price)
    except Exception:
        return 0, 0.0, 0.0, 1.0, 0.0

    sl_point = abs(entry - sl_price)
    if sl_point == 0:
        return 0, 0.0, 0.0, 1.0, get_available_balance()

    ptype = (productType or "INTRADAY").upper()
    max_loss = 1000.0 if ptype == "INTRADAY" else 1500.0
    qty_by_risk = int(max_loss / sl_point)

    leverage = 1.0
    if ptype == "INTRADAY":
        leverage = _leverage_cache.get(sec_id, 1.0)
    elif ptype == "MARGIN":
        leverage = _mtf_leverage_cache.get(sec_id, 1.0)

    fund = get_available_balance()
    eff_fund = fund * leverage
    qty_by_fund = int(eff_fund / price)

    quantity = min(qty_by_risk, qty_by_fund)
    expected_loss = quantity * sl_point
    exposure = quantity * price

    return int(quantity), expected_loss, exposure, leverage, fund

# -------------------------
# Exports
# -------------------------
__all__ = [
    "dhan",
    "get_available_balance",
    "get_cached_security_id",
    "calculate_position_size_mixed",
    "build_mapping_caches",
    "load_mapping_from_s3",
]

# -------------------------
# Order Payload Builder
import logging

# -------------------------
# Logging Setup
# -------------------------
logging.basicConfig(level=logging.DEBUG, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("order_logic")

# -------------------------
# Order Payload Builder
# -------------------------
def create_order_payload(form_data: dict, security_id: int) -> dict:
    """
    Build Dhan order payload from form input and security ID.
    """
    symbol = form_data['symbol'].strip().upper()
    order_type = form_data['order_type'].upper()
    transaction_type = form_data['transaction_type'].upper()
    product_type = form_data['productType'].upper()
    qty = int(form_data['qty'])
    if transaction_type == 'BUY':
        qty = abs(qty)

    limit_price = float(form_data.get('limit_price') or 0)
    trigger_price = float(form_data.get('trigger_price') or 0)

    if not security_id:
        raise ValueError(f"Symbol {symbol} not found")

    transaction_map = {'BUY': dhan.BUY, 'SELL': dhan.SELL}
    order_type_map = {'MARKET': dhan.MARKET, 'LIMIT': dhan.LIMIT, 'SL': dhan.SL}
    product_type_map = {'INTRADAY': dhan.INTRA, 'CNC': dhan.CNC, 'MARGIN': dhan.MARGIN}

    payload = {
        'security_id': security_id,
        'exchange_segment': dhan.NSE,
        'transaction_type': transaction_map.get(transaction_type, dhan.BUY),
        'order_type': order_type_map.get(order_type, dhan.MARKET),
        'quantity': qty,
        'product_type': product_type_map.get(product_type, dhan.INTRA),
        'price': limit_price if order_type in ['LIMIT', 'SL'] else 0,
        'after_market_order': False,
        'disclosed_quantity': 0,
        'validity': 'DAY'
    }

    if order_type == 'SL':
        payload['trigger_price'] = trigger_price

    logger.debug(f"📦 Order Payload for {symbol}: {payload}")
    return payload


# -------------------------
# Place Order Function
# -------------------------
def place_order(symbol: str, action: str, qty: int, price: float,
                productType: str = "INTRADAY", order_type: str = "LIMIT",
                trigger_price: float = 0) -> dict:
    """
    Places an order via Dhan API.
    Returns a dict with success status and order details or error.
    """
    try:
        # Resolve security ID
        sec_id = get_cached_security_id(symbol)
        if not sec_id:
            return {"success": False, "message": f"Symbol '{symbol}' not found."}

        # Prepare payload
        form_data = {
            "symbol": symbol,
            "transaction_type": action,
            "qty": qty,
            "productType": productType,
            "order_type": order_type,
            "limit_price": price,
            "trigger_price": trigger_price
        }

        payload = create_order_payload(form_data, sec_id)

        # Place order
        logger.info(f"🚀 Placing order for {symbol}: {payload}")
        response = dhan.place_order(**payload)
        logger.info(f"📬 Dhan API Response: {response}")

        if response.get("status") == "success":
            order_id = response.get("data", {}).get("orderId", "")
            logger.info(f"✅ Order placed successfully. Order ID: {order_id}")
            return {"success": True, "order_id": order_id}
        else:
            remarks = response.get("remarks") or response
            if isinstance(remarks, dict):
                remarks = str(remarks)
            logger.warning(f"❌ Order Failed: {remarks}")
            return {"success": False, "message": f"Order Failed: {remarks}"}

    except Exception as e:
        logger.error(f"❌ Exception placing order: {e}", exc_info=True)
        return {"success": False, "message": str(e)}


if __name__ == "__main__":
    # -------------------------
    # Load mapping caches from S3
    # -------------------------
    build_mapping_caches(force_reload=True)

    # -------------------------
    # Test parameters
    # -------------------------
    symbol = "INFY"           # symbol to test
    price = 3500.0            # current market price
    entry = 3480.0             # your entry price
    sl_price = 3450.0          # stop loss
    productType = "INTRADAY"   # product type

    # -------------------------
    # Resolve security ID
    # -------------------------
    sec_id = get_cached_security_id(symbol)
    if not sec_id:
        raise ValueError(f"Symbol '{symbol}' not found in mapping cache!")

    # -------------------------
    # Run calculation
    # -------------------------
    quantity, expected_loss, exposure, leverage, fund = calculate_position_size_mixed(
        price, entry, sl_price, sec_id, productType
    )

    # -------------------------
    # Print results
    # -------------------------
    print("------ POSITION SIZE TEST ------")
    print(f"Symbol: {symbol}")
    print(f"SECURITY_ID: {sec_id}")
    print(f"Quantity: {quantity}")
    print(f"Expected Loss: {expected_loss}")
    print(f"Exposure: {exposure}")
    print(f"Leverage: {leverage}")
    print(f"Available Fund: {fund}")


