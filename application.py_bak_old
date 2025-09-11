# application.py
import os
import logging
from flask import Flask, render_template, request, jsonify

# ===========================
# Import from your helpers
# ===========================
from tradingview_helper import (
    get_stock_list,
    load_stock_data,
    refresh_live_data,
    get_ec2_public_ip,
)

from watchlist_helpers import (
    load_watchlist,
    save_watchlist,
    add_stock,
    delete_stock,
    update_stock,
    load_mapping,
    fetch_live_data,
    update_quotes_and_breakouts,
    mark_auto_buy,
    get_today_pnl,
    dhan
)

from core_logic import (
    get_cached_security_id,
    calculate_position_size_mixed,
    build_mapping_caches,
    place_order
)

# ===========================
# Flask app setup
# ===========================
app = Flask(__name__)
build_mapping_caches(force_reload=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger(__name__)

# ===========================
# Health check
# ===========================
@app.route('/')
def health_check():
    return jsonify({"status": "healthy", "message": "TradingView App is running"}), 200

# ===========================
# TradingView chart page
# ===========================
@app.route('/tradingview/chart')
def tradingview_chart():
    stocks = get_stock_list()
    return render_template('tradingviewchart.html', stocks=stocks)

@app.route('/tradingview/api/stock_data')
def tradingview_api_stock_data():
    instrument_id = request.args.get('instrument_id')
    if not instrument_id:
        return jsonify({"error": "Instrument ID not provided"}), 400

    data = load_stock_data(instrument_id)
    if data is None:
        return jsonify({"error": "File not found"}), 404
    return jsonify(data)

@app.route('/tradingview/refresh')
def refresh_data():
    result = refresh_live_data()
    return jsonify({"message": "Live data refreshed", "instruments_updated": len(result)})

@app.route("/get-ec2-ip")
def get_ec2_ip():
    public_ip = get_ec2_public_ip()
    if public_ip:
        return public_ip, 200
    else:
        return "No running EC2 instance found", 404

# ===========================
# Momentum Watchlist Page
# ===========================
@app.route("/momentum_watchlist")
def momentum_watchlist():
    try:
        watchlist = load_watchlist()  # from S3
        instrument_ids = [
            int(row["Instrument ID"]) for row in watchlist if row.get("Instrument ID")
        ]
        live_data = fetch_live_data(instrument_ids)
        data = update_quotes_and_breakouts(live_data)
        return render_template("momentum_watchlist.html", data=data)
    except Exception as e:
        logger.error(f"Error in momentum_watchlist: {e}")
        return jsonify({"success": False, "error": str(e)}), 500

# ===========================
# Watchlist CRUD APIs
# ===========================
@app.route("/add_stock", methods=["POST"])
def add_stock_route():
    try:
        stock = request.form.get("stock")
        entry = request.form.get("entry_price")
        if not stock or not entry:
            return jsonify({"status": "error", "message": "Missing stock or entry_price"}), 400
        add_stock(stock, entry)
        return jsonify({"status": "success"})
    except Exception as e:
        logger.error(f"Error adding stock: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route("/delete_stock/<int:index>", methods=["POST"])
def delete_stock_route(index):
    try:
        delete_stock(index)
        return jsonify({"status": "success"})
    except Exception as e:
        logger.error(f"Error deleting stock: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route("/update_stock/<int:index>", methods=["POST"])
def update_stock_route(index):
    try:
        field = request.form.get("field")
        value = request.form.get("value")
        update_stock(index, field, value)
        return jsonify({"status": "success"})
    except Exception as e:
        logger.error(f"Error updating stock: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

# ===========================
# API to update watchlist live
# ===========================
@app.route("/api/update_watchlist", methods=["GET"])
def update_watchlist_api():
    try:
        data = load_watchlist()
        mapping = load_mapping()
        instrument_ids = [
            int(mapping.get(row["Stock Name"], {}).get("Instrument ID"))
            for row in data
            if row.get("Stock Name") and mapping.get(row["Stock Name"], {}).get("Instrument ID")
        ]
        live_data = fetch_live_data(instrument_ids)
        updated_data = update_quotes_and_breakouts(live_data)
        return jsonify({"success": True, "data": updated_data})
    except Exception as e:
        logger.error(f"Error updating watchlist: {e}")
        return jsonify({"success": False, "error": str(e)}), 500

# ===========================
# Mark AUTO_BUYED API
# ===========================
@app.route("/api/mark_auto_buy", methods=["POST"])
def api_mark_auto_buy():
    try:
        data = request.get_json()
        symbol = data.get("symbol")
        if not symbol:
            return jsonify({"success": False, "error": "Missing symbol"}), 400
        mark_auto_buy(symbol)
        return jsonify({"success": True, "message": f"{symbol} marked as AUTO_BUYED"})
    except Exception as e:
        logger.error(f"Error in mark_auto_buy API: {e}")
        return jsonify({"success": False, "error": str(e)}), 500

# ===========================
# Get AUTO_BUYED stocks
# ===========================
@app.route("/api/get_auto_buy_status", methods=["GET"])
def get_auto_buy_status():
    try:
        data = load_watchlist()
        auto_bought_stocks = [
            row["Stock Name"] for row in data
            if row.get("Action") and "AUTO_BUYED" in str(row.get("Action")).upper()
        ]
        return jsonify({"success": True, "auto_bought_stocks": auto_bought_stocks})
    except Exception as e:
        logger.error(f"Error getting auto-buy status: {e}")
        return jsonify({"success": False, "error": str(e)}), 500


@app.route('/api/today_pnl', methods=["GET"])
def today_pnl_route():
    result = get_today_pnl(dhan)  # pass your DHAN client instance
    status_code = 200 if result.get("success") else 500
    return jsonify(result), status_code


@app.route("/api/position_sizing", methods=["POST"])
def position_sizing():
    data = request.get_json()
    try:
        symbol = data.get("symbol", "").strip().upper()
        entry = float(data.get("entry", 0))
        sl_price = float(data.get("sl_price", 0))
        productType = data.get("productType", "INTRADAY").strip().upper()

        # Resolve security ID
        sec_id = get_cached_security_id(symbol)
        if not sec_id:
            return jsonify({"success": False, "message": f"Symbol '{symbol}' not found"}), 404

        # Calculate position size
        quantity, expected_loss, exposure, leverage, fund = calculate_position_size_mixed(
            price=entry,  # Using entry price as LTP for simplicity
            entry=entry,
            sl_price=sl_price,
            sec_id=sec_id,
            productType=productType
        )

        response = {
            "success": True,
            "quantity": quantity,
            "expected_loss": round(expected_loss, 2),
            "exposure": round(exposure, 2),
            "leverage": round(leverage, 2),
            "fund": round(fund, 2)
        }
        return jsonify(response)
    except Exception as e:
        return jsonify({"success": False, "message": str(e)}), 500
    
@app.route('/api/place_order_custom', methods=['POST'])
def place_order_custom():
    try:
        data = request.get_json(force=True)

        symbol = data.get("symbol", "").strip().upper()
        action = data.get("action", "").upper()
        qty = int(data.get("qty", 0))
        limit_price = float(data.get("limit_price", 0))
        productType = data.get("productType", "INTRADAY").upper()
        order_type = data.get("order_type", "LIMIT").upper()
        trigger_price = float(data.get("trigger_price", 0))

        if not symbol or action not in {"BUY", "SELL"} or qty <= 0 or limit_price <= 0:
            return jsonify({"success": False, "message": "Invalid input"}), 400

        # Call core logic function
        result = place_order(symbol, action, qty, limit_price, productType, order_type, trigger_price)
        return jsonify(result)

    except Exception as e:
        return jsonify({"success": False, "message": str(e)}), 500


# ===========================
# Run Flask app
# ===========================
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)


