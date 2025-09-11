from flask import Flask, redirect, url_for, session, g, render_template
from authlib.integrations.flask_client import OAuth
import os
from werkzeug.middleware.proxy_fix import ProxyFix
from authlib.common.security import generate_token
from dotenv import load_dotenv

# Load .env file
load_dotenv()

app = Flask(__name__)
app.secret_key = os.urandom(24)  # Use a fixed secret in production
# --- Fix for HTTPS behind Nginx ---
# This ensures Flask generates https:// URLs for Cognito redirects
app.wsgi_app = ProxyFix(app.wsgi_app, x_proto=1, x_host=1)

oauth = OAuth(app)

# Cognito OIDC registration
oauth.register(
    name='cognito',
    client_id='7q9j',
    client_secret='1',
    server_metadata_url='https://cognito-idp.ap-south-1.amazonaws.com/ap-south-<>/.well-known/openid-configuration',
    client_kwargs={'scope': 'openid email phone'}
)

# Default role → accessible UI pages mapping
ROLE_UI_ACCESS = {
    "viewer": ["Home"],
    "trader": ["Home", "Trading"],
    "admin": ["Home", "Trading", "Admin"]
}

UI_PAGES = ["Home", "Trading", "Admin"]

# Helper: extract role from Cognito groups
def get_user_role(user_info):
    groups = user_info.get("cognito:groups", [])
    if "admin" in groups:
        return "admin"
    if "trader" in groups:
        return "trader"
    return "viewer"

# Before request: set role and allowed pages
@app.before_request
def load_user():
    user = session.get("user")
    if user:
        g.role = get_user_role(user)
    else:
        g.role = "viewer"
    g.ui_pages = [page for page in UI_PAGES if page in ROLE_UI_ACCESS.get(g.role, ["Home"])]

# Role-based page access decorator
def require_role(allowed_roles):
    def wrapper(f):
        def decorated_function(*args, **kwargs):
            if g.role not in allowed_roles:
                return redirect(url_for("unauthorized"))
            return f(*args, **kwargs)
        decorated_function.__name__ = f.__name__
        return decorated_function
    return wrapper

# Routes
@app.route('/')
def home():
    return render_template("home.html", pages=g.ui_pages, role=g.role)

@app.route('/trading')
@require_role(["trader", "admin"])
def trading():
    return render_template("trading.html", pages=g.ui_pages, role=g.role)

@app.route('/admin')
@require_role(["admin"])
def admin():
    return render_template("admin.html", pages=g.ui_pages, role=g.role)

@app.route('/unauthorized')
def unauthorized():
    return "🚫 You are not authorized", 403

# Cognito login/logout
@app.route('/login')
def login():
    redirect_uri = url_for('callback', _external=True, _scheme='https')
    session['nonce'] = generate_token()  # Store nonce in session
    return oauth.cognito.authorize_redirect(redirect_uri, nonce=session['nonce'])

@app.route('/callback')
def callback():
    token = oauth.cognito.authorize_access_token()
    nonce = session.pop('nonce', None)  # Retrieve and remove nonce
    user_info = oauth.cognito.parse_id_token(token, nonce=nonce)
    session['user'] = user_info
    return redirect('/')

@app.route('/logout')
def logout():
    session.pop('user', None)
    return redirect('/')


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)

