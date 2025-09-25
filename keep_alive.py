from flask import Flask
from threading import Thread
import logging

log = logging.getLogger('werkzeug')
log.setLevel(logging.ERROR)

app = Flask('')


@app.route('/')
def home():
    return '''
    <html>
    <head><title>Reply Tracker Bot</title></head>
    <body>
        <h1>Discord Reply Tracker Bot</h1>
        <p>Status: <span style="color: green;">Online</span></p>
        <p>This bot is running on Replit.</p>
        <hr>
        <p><small>Keep-alive endpoint active</small></p>
    </body>
    </html>
    '''


@app.route('/health')
def health_check():
    return {"status": "healthy", "service": "discord-reply-tracker"}


def run():
    app.run(host='0.0.0.0', port=8080, debug=False)


def keep_alive():
    t = Thread(target=run)
    t.daemon = True
    t.start()
    logging.info("Keep-alive server started on port 8080")
