"""
webserver.py — Lightweight Flask keep-alive server
Run alongside main.py so UptimeRobot can ping it every 5 minutes
and keep the Render free plan from sleeping.

Start both together using:  python webserver.py &  python main.py
Or use the start.sh script.
"""

from flask import Flask
from datetime import datetime

app = Flask(__name__)

START_TIME = datetime.now()

@app.route("/")
def home():
    uptime = datetime.now() - START_TIME
    hours, remainder = divmod(int(uptime.total_seconds()), 3600)
    minutes, seconds = divmod(remainder, 60)
    return f"""
    <html>
    <head><title>Felix Bypass Bot</title></head>
    <body style="font-family:monospace; background:#0f0f0f; color:#00ff00; padding:40px;">
        <h2>🤖 esh Bypass Bot</h2>
        <p>✅ Status: <b>ONLINE</b></p>
        <p>⏱ Uptime: <b>{hours}h {minutes}m {seconds}s</b></p>
        <p>🕒 Started: <b>{START_TIME.strftime('%d %b %Y %I:%M %p')}</b></p>
        <p>🔗 Ping this URL with UptimeRobot every 5 minutes to keep alive.</p>
    </body>
    </html>
    """

@app.route("/ping")
def ping():
    return "pong", 200

@app.route("/health")
def health():
    return {"status": "ok", "bot": "Felix Bypass Bot"}, 200

def run():
    app.run(host="0.0.0.0", port=8080)

if __name__ == "__main__":
    run()
