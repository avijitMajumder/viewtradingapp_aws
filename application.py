import os
import logging
from flask import Flask, render_template, request, jsonify, session, g, redirect, url_for
from werkzeug.middleware.proxy_fix import ProxyFix
from authlib.integrations.flask_client import OAuth
from authlib.common.security import generate_token
from functools import wraps

# ===========================
# Helpers and Business Logic
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
# Flask App Setup
# ===========================
app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET_KEY", "supersecretkey123")
app.wsgi_app = ProxyFix(app.wsgi_app, x_proto=1, x_host=1)
build_mapping_caches(force_reload=True)

# ===========================
# Logging
# ===========================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)

# ===========================
# Cognito OAuth
# ===========================
oauth = OAuth(app)
oauth.register(
    name='cognito',
    client_id='124apijpo019p2uc1a83k5ir5b',
    client_secret='v2ahb2kt12b43k33b03lgpi63foq3cvhhtd4n31vpkon9clt1jk',
    server_metadata_url='https://cognito-idp.ap-south-1.amazonaws.com/ap-south-1_kPo2CEj3U/.well-known/openid-configuration',
    client_kwargs={'scope': 'openid email phone'}
)

# ===========================
# Role & UI Management
# ===========================
UI_PAGES = [
    {"route": "/home", "name": "Home"},
    {"route": "/tradingview/chart", "name": "TradingView Chart"},
    {"route": "/momentum_watchlist", "name": "Momentum Watchlist"},
    {"route": "/admin/ui-management", "name": "Admin Management"}
]

ROLE_UI_ACCESS = {
    "viewer": ["/home", "/tradingview/chart"],
    "trader": ["/home", "/tradingview/chart", "/momentum_watchlist"],
    "admin": ["/home", "/tradingview/chart", "/momentum_watchlist", "/admin/ui-management"]
}

def get_user_role(user_info):
    """Return role based on Cognito groups."""
    groups = user_info.get("cognito:groups", [])
    if "admin" in groups:
        return "admin"
    if "trader" in groups:
        return "trader"
    return "viewer"

@app.before_request
def load_user():
    """Load user and role before each request."""
    g.user = session.get("user")
    g.role = get_user_role(g.user) if g.user else "viewer"
    g.ui_pages = [p for p in UI_PAGES if p["route"] in ROLE_UI_ACCESS.get(g.role, ["/home"])]
    logger.info(f"[LOAD_USER] User: {g.user.get('email', 'Guest') if g.user else 'Guest'}, Role: {g.role}")

# ===========================
# Decorators
# ===========================
def require_role(allowed_roles):
    """Decorator to restrict access to roles."""
    def decorator(f):
        @wraps(f)
        def wrapped(*args, **kwargs):
            if g.role not in allowed_roles:
                logger.warning(f"[ACCESS_DENIED] Role '{g.role}' attempted to access {request.path}")
                return redirect(url_for("unauthorized"))
            return f(*args, **kwargs)
        return wrapped
    return decorator

def api_error_handler(f):
    """Decorator to handle API errors gracefully."""
    @wraps(f)
    def wrapper(*args, **kwargs):
        try:
            return f(*args, **kwargs)
        except Exception as e:
            logger.exception(f"API Error: {e}")
            return jsonify({"success": False, "error": str(e)}), 500
    return wrapper

# ===========================
# Helper for JSON responses
# ===========================
def json_response(success=True, **kwargs):
    resp = {"success": success, **kwargs}
    return jsonify(resp)

# ===========================
# Auth Routes
# ===========================
@app.route('/login')
def login():
    redirect_uri = url_for('callback', _external=True, _scheme='https')
    session['nonce'] = generate_token()
    return oauth.cognito.authorize_redirect(redirect_uri, nonce=session['nonce'])

@app.route('/callback')
def callback():
    token = oauth.cognito.authorize_access_token()
    nonce = session.pop('nonce', None)
    g.user = oauth.cognito.parse_id_token(token, nonce=nonce)
    session['user'] = g.user
    logger.info(f"[LOGIN_SUCCESS] {g.user.get('email', 'N/A')}")
    return redirect('/home')

@app.route('/logout')
def logout():
    session.pop('user', None)
    return redirect('/home')

@app.route('/unauthorized')
def unauthorized():
    return "🚫 You are not authorized", 403

# ===========================
# Health Check
# ===========================
@app.route('/')
def health_check():
    return jsonify({"status": "healthy", "message": "TradingView App is running"}), 200

# ===========================
# Pages
# ===========================
@app.route('/home')
def home():
    return render_template('home.html', pages=g.ui_pages, role=g.role, user=g.user)

@app.route('/tradingview/chart')
@require_role(["trader", "admin","viewer"])
def tradingview_chart():
    stocks = get_stock_list()
    return render_template('tradingviewchart.html', stocks=stocks, pages=g.ui_pages, role=g.role, user=g.user)

# ===========================
# TradingView API: Stock Data
# ===========================
@app.route('/tradingview/api/stock_data')
@require_role(["trader", "admin", "viewer"])
def tradingview_api_stock_data():
    instrument_id = request.args.get('instrument_id')
    if not instrument_id:
        return jsonify({"error": "Instrument ID not provided"}), 400

    data = load_stock_data(instrument_id)
    if data is None:
        return jsonify({"error": "File not found"}), 404
    return jsonify(data)

# ===========================
# Refresh Live Data
# ===========================
@app.route('/tradingview/refresh')
@require_role(["trader", "admin", "viewer"])
def refresh_data():
    try:
        result = refresh_live_data()
        return jsonify({
            "success": True,
            "message": "Live data refreshed successfully",
            "instruments_updated": len(result)
        })
    except Exception as e:
        logger.error(f"[TradingView Refresh] Error refreshing data: {e}")
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/momentum_watchlist")
@require_role(["trader", "admin"])
@api_error_handler
def momentum_watchlist():
    watchlist = load_watchlist()
    instrument_ids = [int(row["Instrument ID"]) for row in watchlist if row.get("Instrument ID")]
    live_data = fetch_live_data(instrument_ids)
    data = update_quotes_and_breakouts(live_data)
    return render_template("momentum_watchlist.html", data=data, pages=g.ui_pages, role=g.role, user=g.user)

# ===========================
# Watchlist APIs
# ===========================
@app.route("/add_stock", methods=["POST"])
@require_role(["trader", "admin"])
@api_error_handler
def add_stock_route():
    stock = request.form.get("stock")
    entry = request.form.get("entry_price")
    if not stock or not entry:
        return json_response(success=False, message="Missing stock or entry_price"), 400
    add_stock(stock, entry)
    return json_response(message=f"{stock} added successfully")

@app.route("/delete_stock/<int:index>", methods=["POST"])
@require_role(["trader", "admin"])
@api_error_handler
def delete_stock_route(index):
    delete_stock(index)
    return json_response(message=f"Stock at index {index} deleted")

@app.route("/update_stock/<int:index>", methods=["POST"])
@require_role(["trader", "admin"])
@api_error_handler
def update_stock_route(index):
    field = request.form.get("field")
    value = request.form.get("value")
    update_stock(index, field, value)
    return json_response(message=f"Stock at index {index} updated")

@app.route("/api/update_watchlist", methods=["GET"])
@require_role(["trader", "admin"])
@api_error_handler
def update_watchlist_api():
    data = load_watchlist()
    mapping = load_mapping()
    instrument_ids = [
        int(mapping.get(row["Stock Name"], {}).get("Instrument ID"))
        for row in data
        if row.get("Stock Name") and mapping.get(row["Stock Name"], {}).get("Instrument ID")
    ]
    live_data = fetch_live_data(instrument_ids)
    updated_data = update_quotes_and_breakouts(live_data)
    return json_response(data=updated_data)


# ===========================
# Today's PnL API
# ===========================

@app.route('/api/today_pnl', methods=["GET"])
@require_role(["trader", "admin"])
def today_pnl_route():
    try:
        result = get_today_pnl(dhan)
        if result is None:
            logger.warning("[TODAY_PNL] get_today_pnl returned None")
            return jsonify({"success": False, "data": None, "message": "No PnL data found"}), 404

        logger.info(f"[TODAY_PNL] Result: {result}")
        status_code = 200 if result.get("success") else 500
        return jsonify(result), status_code
    except Exception as e:
        logger.error(f"[TODAY_PNL] Error: {e}")
        return jsonify({"success": False, "error": str(e)}), 500


# ===========================
# Mark AUTO_BUYED API
# ===========================
@app.route("/api/mark_auto_buy", methods=["POST"])
@require_role(["trader", "admin"])
def api_mark_auto_buy():
    try:
        data = request.get_json()
        symbol = data.get("symbol", "").strip().upper()
        if not symbol:
            logger.warning("[MARK_AUTO_BUY] Missing symbol in request")
            return jsonify({"success": False, "error": "Missing symbol"}), 400

        # Call your helper to mark the stock as AUTO_BUYED
        mark_auto_buy(symbol)
        logger.info(f"[MARK_AUTO_BUY] {symbol} marked as AUTO_BUYED")
        return jsonify({"success": True, "message": f"{symbol} marked as AUTO_BUYED"})
    except Exception as e:
        logger.error(f"[MARK_AUTO_BUY] Error: {e}")
        return jsonify({"success": False, "error": str(e)}), 500


# ===========================
# Get AUTO_BUYED stocks
# ===========================
@app.route("/api/get_auto_buy_status", methods=["GET"])
@require_role(["trader", "admin", "viewer"])
def get_auto_buy_status():
    try:
        watchlist_data = load_watchlist()
        auto_bought_stocks = [
            row["Stock Name"] for row in watchlist_data
            if row.get("Action") and "AUTO_BUYED" in str(row.get("Action")).upper()
        ]
        logger.info(f"[AUTO_BUY_STATUS] Auto-bought stocks: {auto_bought_stocks}")
        return jsonify({"success": True, "auto_bought_stocks": auto_bought_stocks})
    except Exception as e:
        logger.error(f"[AUTO_BUY_STATUS] Error fetching auto-buy status: {e}")
        return jsonify({"success": False, "error": str(e)}), 500

# ===========================
# Position Sizing & Orders
# ===========================
@app.route("/api/position_sizing", methods=["POST"])
@require_role(["trader", "admin"])
@api_error_handler
def position_sizing():
    data = request.get_json()
    symbol = data.get("symbol", "").strip().upper()
    entry = float(data.get("entry", 0))
    sl_price = float(data.get("sl_price", 0))
    productType = data.get("productType", "INTRADAY").strip().upper()
    sec_id = get_cached_security_id(symbol)
    if not sec_id:
        return json_response(success=False, message=f"Symbol '{symbol}' not found"), 404
    quantity, expected_loss, exposure, leverage, fund = calculate_position_size_mixed(
        price=entry,
        entry=entry,
        sl_price=sl_price,
        sec_id=sec_id,
        productType=productType
    )
    return json_response(
        quantity=quantity,
        expected_loss=round(expected_loss, 2),
        exposure=round(exposure, 2),
        leverage=round(leverage, 2),
        fund=round(fund, 2)
    )

@app.route('/api/place_order_custom', methods=['POST'])
@require_role(["trader", "admin"])
@api_error_handler
def place_order_custom():
    data = request.get_json(force=True)
    symbol = data.get("symbol", "").strip().upper()
    action = data.get("action", "").upper()
    qty = int(data.get("qty", 0))
    limit_price = float(data.get("limit_price", 0))
    productType = data.get("productType", "INTRADAY").upper()
    order_type = data.get("order_type", "LIMIT").upper()
    trigger_price = float(data.get("trigger_price", 0))
    if not symbol or action not in {"BUY", "SELL"} or qty <= 0 or limit_price <= 0:
        return json_response(success=False, message="Invalid input"), 400
    result = place_order(symbol, action, qty, limit_price, productType, order_type, trigger_price)
    return json_response(**result)

# ===========================
# Admin UI Management
# ===========================
@app.route("/admin/ui-management", methods=["GET", "POST"])
@require_role(["admin"])
@api_error_handler
def ui_management():
    global UI_PAGES, ROLE_UI_ACCESS
    if request.method == "POST":
        # Update role-page access
        for role in ROLE_UI_ACCESS:
            ROLE_UI_ACCESS[role] = request.form.getlist(role)
        # Add new page
        new_page = request.form.get("new_page", "").strip()
        if new_page and not any(p["route"] == new_page for p in UI_PAGES):
            UI_PAGES.append({"route": new_page, "name": new_page.replace("/", "").replace("-", " ").title()})
    return render_template(
        "ui_management.html",
        roles=ROLE_UI_ACCESS,
        pages=UI_PAGES,
        pages_user=g.ui_pages,
        role=g.role,
        user=g.user
    )

# ===========================
# Dynamic Pages
# ===========================
@app.route("/<page_name>")
@api_error_handler
def dynamic_page(page_name):
    route_path = f"/{page_name}"
    page_entry = next((p for p in UI_PAGES if p["route"] == route_path), None)
    if not page_entry:
        return "Page not found", 404
    return render_template("dynamic_page.html", page=page_entry["name"], pages=g.ui_pages, role=g.role, user=g.user)

# ===========================
# Run App
# ===========================
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)

