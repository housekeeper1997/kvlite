#! /usr/bin/env python3

from flask import Flask, jsonify, request
from api import data as db, timepoint
from time import time

from api.utility import env_float, env_int, env_str, failed, strExceedLimit, succeed

# DB_NAME = env_str('DB_NAME','KVData.db')
DB_NAME = env_str('DB_NAME','sqlite:///KVData.db')
TZ_HOUR_OFFSET = env_float('TZ_HOUR_OFFSET', 8.0)    # Bejing Standard Time (UTC+08:00)
ROW_COUNT_LIMIT = env_int('ROW_COUNT_LIMIT',1000)
KEY_LENGTH_LIMIT = env_int('KEY_LENGTH_LIMIT', 64)
VALUE_LENGTH_LIMIT = env_int('VALUE_LENGTH_LIMIT', 1024 * 8)                   # bytes
MINIMUM_SET_INTERVAL_PER_USER = env_int('MINIMUM_SET_INTERVAL_PER_USER', 60)    # seconds
MINIMUM_SET_INTERVAL_SYSTEM = env_int('MINIMUM_SET_INTERVAL_SYSTEM', 1)         # seconds

KKEY = 'key'
VKEY = 'value'
TKEY = "time"

LastWriteTime = timepoint.timepoint()

app = Flask(__name__)

DbEngine = None

def get_engine():
    global DbEngine
    if DbEngine is None:
        DbEngine = db.connect_db(DB_NAME)
    return DbEngine


@app.route('/')
def hello():
    return '''<html>
            <body style="background-color:#f7f7f7;color:#555;font-family: Söhne,ui-sans-serif,system-ui,-apple-system,Segoe UI,Roboto,Ubuntu,Cantarell,Noto Sans,sans-serif,Helvetica Neue,Arial,Apple Color Emoji,Segoe UI Emoji,Segoe UI Symbol,Noto Color Emoji;">
            <div style="height:100%;display:flex;flex-direction:column;align-items:center;justify-content:center;margin-left:-5em;">
                <div style="margin:.8em;margin-top:-3em;font-size:2rem;text-align:center;">
                    <p style="margin:auto;"><strong>KVLite</strong></p>
                    <p style="margin:auto;margin-top:.3em;padding-left:12em;font-size:.65em;">-- A key-value storage service.</p>
                </div>
                <div style="margin:0;font-size:1.3em;">
                    Created with:
                    <ul style="margin:auto;">
                        <li><code>python</code></li>
                        <li><code>Flask</code></li>
                        <li><code>SQLAlchemy</code></li>
                    </ul>
                </div>
            </div>
            </body>
            </html>\n'''

@app.route('/kv')
def get_data():
    rows = db.selectAll(get_engine())
    rows = [{KKEY: r[0],VKEY: r[1],TKEY: r[2]} for r in rows]
    return jsonify(rows)

@app.route('/kv/<key>')
def get_kv(key):
    # print(f"got request for key: {key}")
    row = db.select(get_engine(), key)
    if row is None:
        return failed( 'data not exist')
    value = {KKEY:row[0], VKEY:row[1], TKEY:row[2]}
    # print(value)
    return value

@app.route('/kv', methods=['POST'])
def set_kv():
    if LastWriteTime.elapsed() < MINIMUM_SET_INTERVAL_SYSTEM:
        return failed('one set request per second')
    kv = request.get_json()
    # print(kv)
    if strExceedLimit(kv[KKEY], KEY_LENGTH_LIMIT):
        return failed(f'key length exceed {KEY_LENGTH_LIMIT} bytes')
    if strExceedLimit(kv[VKEY], VALUE_LENGTH_LIMIT):
        return failed(f'data length exceed limit: {VALUE_LENGTH_LIMIT} bytes')
    oldVal = db.select(get_engine(),kv[KKEY])
    # print(oldVal)
    if oldVal is not None:
        userLastWriteTime = timepoint.timepoint(float(oldVal[2]))
        if userLastWriteTime.elapsed() < MINIMUM_SET_INTERVAL_PER_USER:
            return failed('wait one minute')
        db.update(get_engine(), kv[KKEY], kv[VKEY], str(time()))
        LastWriteTime.set()
        return succeed('updated')
    if db.rowCount(get_engine()) > ROW_COUNT_LIMIT:
        print(f"table row count reach limit: {ROW_COUNT_LIMIT}")
        return failed('can not create more rows')
    LastWriteTime.set()
    db.insert(get_engine(), kv[KKEY], kv[VKEY], str(time()))
    return succeed('created')

if __name__ == "__main__":
    from waitress import serve
    port = 5000
    host = "0.0.0.0"
    print(f"running on http://{host}:{port}")
    serve(app, host=host, port=port)
