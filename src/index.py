#! /usr/bin/env python3

from flask import Flask, jsonify, request
from src import data as db, timepoint
from time import time

from src.utility import env_float, env_int, env_str, failed, strExceedLimit, succeed

DB_NAME = env_str('DB_NAME','KVData.db')
TZ_HOUR_OFFSET = env_float('TZ_HOUR_OFFSET', 8.0)    # Bejing Standard Time (UTC+08:00)
ROW_COUNT_LIMIT = env_int('ROW_COUNT_LIMIT',1000)
KEY_LENGTH_LIMIT = env_int('KEY_LENGTH_LIMIT', 64)
VALUE_LENGTH_LIMIT = env_int('VALUE_LENGTH_LIMIT', 1024 * 10)                   # bytes
MINIMUM_SET_INTERVAL_PER_USER = env_int('MINIMUM_SET_INTERVAL_PER_USER', 60)    # seconds
MINIMUM_SET_INTERVAL_SYSTEM = env_int('MINIMUM_SET_INTERVAL_SYSTEM', 1)         # seconds

KKEY = 'key'
VKEY = 'value'
TKEY = "time"

LastWriteTime = timepoint.timepoint()

app = Flask(__name__)

@app.route('/')
def hello():
    return '''<div style="height:100%;display:flex;align-items:center;justify-content:center;">
                <div style="margin:0;font-size:2rem;">
                    <em>Hello World!</em> -- with <code>Flask</code> and <code>SQLite</code>
                </div>
            </div>\n'''

@app.route('/kv')
def get_data():
    with db.connect_db(DB_NAME) as conn:
        return jsonify(db.selectAll(conn))

@app.route('/kv/<key>')
def get_kv(key):
    # print(f"got request for key: {key}")
    with db.connect_db(DB_NAME) as conn:
        row = db.select(conn, key)
        # print(f"got result: {row}")
        if row is None:
            result = failed( 'data not exist')
            # print(f"gonna return {result}")
            return result
        return {KKEY:row[0], VKEY:row[1], TKEY:row[2]}

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
    with db.connect_db(DB_NAME) as conn:
        oldVal = db.select(conn,kv[KKEY])
        if oldVal is not None:
            userLastWriteTime = timepoint.timepoint(float(oldVal[2]))
            if userLastWriteTime.elapsed() < MINIMUM_SET_INTERVAL_PER_USER:
                return failed('wait one minute')
            db.update(conn, kv[KKEY], kv[VKEY], str(time()))
            LastWriteTime.set()
            return succeed('updated')
        if db.rowCount(conn) > ROW_COUNT_LIMIT:
            print(f"table row count reach limit: {ROW_COUNT_LIMIT}")
            return failed('can not create more rows')
        LastWriteTime.set()
        db.insert(conn, kv[KKEY], kv[VKEY], str(time()))
        return succeed('created')

if __name__ == "__main__":
    from waitress import serve
    port = 5000
    host = "0.0.0.0"
    print(f"running on http://{host}:{port}")
    serve(app, host=host, port=port)
