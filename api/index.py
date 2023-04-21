#! /usr/bin/env python3

from flask import Flask, request, render_template, url_for
from api import data as db, timepoint
from time import time
from datetime import datetime
from api.utility import env_float, env_int, env_str, failed, strExceedLimit, succeed

# DB_NAME = env_str('DB_NAME','KVData.db')
DB_NAME = env_str('DB_NAME','sqlite:///KVData.db')
TZ_HOUR_OFFSET = env_float('TZ_HOUR_OFFSET', 8.0)    # Bejing Standard Time (UTC+08:00)
ROW_COUNT_LIMIT = env_int('ROW_COUNT_LIMIT',1000)
KEY_LENGTH_LIMIT = env_int('KEY_LENGTH_LIMIT', 64)
VALUE_LENGTH_LIMIT = env_int('VALUE_LENGTH_LIMIT', 1024 * 8)                   # bytes
MINIMUM_SET_INTERVAL_PER_USER = env_int('MINIMUM_SET_INTERVAL_PER_USER', 60)    # seconds
MINIMUM_SET_INTERVAL_SYSTEM = env_int('MINIMUM_SET_INTERVAL_SYSTEM', 1)         # seconds
PER_PAGE_DEFAULT = env_int('PER_PAGE_DEFAULT', 10)      # Rows per page
PAGE_DEFAULT = env_int('PAGE_DEFAULT', 1)               # Page number

KKEY = 'key'
VKEY = 'value'
TKEY = "time"

LastWriteTime = timepoint.timepoint(time()+MINIMUM_SET_INTERVAL_SYSTEM * 2)

app = Flask(__name__)

DbEngine = None

def get_engine():
    global DbEngine
    if DbEngine is None:
        DbEngine = db.connect_db(DB_NAME)
    return DbEngine


@app.route('/')
def hello():
    return render_template('index.html')

@app.route('/kv')
def get_data():
    page = request.args.get('page', PAGE_DEFAULT, type=int)
    per_page = request.args.get('per_page', PER_PAGE_DEFAULT, type=int)
    records = db.selectAll(get_engine(), page, per_page)
    rows = [{"id": r.id,KKEY: r.key,VKEY: r.value,TKEY: datetime.fromtimestamp(int(float(r.update)))} for r in records]
    prev_page_url = url_for('get_data', page=page-1, per_page=per_page) if page > 1 else None
    next_page_url = url_for('get_data', page=page+1, per_page=per_page) if len(records) == per_page else None
    return render_template('kv.html', kv_list=rows, prev_page_url=prev_page_url, next_page_url=next_page_url)

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
