# application.py
from flask import Flask, render_template, request, jsonify
from tradingview_helper import get_stock_list, load_stock_data, refresh_live_data,get_ec2_public_ip
import os

app = Flask(__name__)

# Health check route (required for Elastic Beanstalk)
@app.route('/')
def health_check():
    return jsonify({"status": "healthy", "message": "TradingView App is running"}), 200

# Page route for TradingView chart
@app.route('/tradingview/chart')
def tradingview_chart():
    stocks = get_stock_list()  # Get all mapped stocks
    return render_template('tradingviewchart.html', stocks=stocks)

# API route to fetch stock data
@app.route('/tradingview/api/stock_data')
def tradingview_api_stock_data():
    instrument_id = request.args.get('instrument_id')
    if not instrument_id:
        return jsonify({"error": "Instrument ID not provided"}), 400

    data = load_stock_data(instrument_id)
    if data is None:
        return jsonify({"error": "File not found"}), 404
    return jsonify(data)

# Route to refresh live data manually
@app.route('/tradingview/refresh')
def refresh_data():
    result = refresh_live_data()
    return jsonify({"message": "Live data refreshed", "instruments_updated": len(result)})

@app.route("/get-ec2-ip")
def get_ec2_ip():
    public_ip = get_ec2_public_ip()
    if public_ip:
        return public_ip, 200  # Return plain text, not JSON
    else:
        return "No running EC2 instance found", 404

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
