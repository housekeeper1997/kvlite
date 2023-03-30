#! /usr/bin/env python3

import sqlite3
import unittest
import os

KVTABLE_NAME = os.getenv('KVTABLE_NAME','BanData')
COLUMN_PREFIX = os.getenv('COLUMN_PREFIX','tbt')
KCOLNAME = f'{COLUMN_PREFIX}_key'
VCOLNAME = f'{COLUMN_PREFIX}_value'
UCOLNAME = f'{COLUMN_PREFIX}_update'

def connect_db(dbname, clean=False):
    conn = sqlite3.connect(dbname)
    if clean:
        conn.execute(f'DROP TABLE IF EXISTS {KVTABLE_NAME};')
    query = f'''CREATE TABLE IF NOT EXISTS {KVTABLE_NAME} (
                {KCOLNAME} text NOT NULL UNIQUE,
                {VCOLNAME} text NOT NULL,
                {UCOLNAME} text NOT NULL
            );'''
    conn.execute(query)
    # print("Table created successfully")
    return conn

def insert(conn, k, v, now, tn=KVTABLE_NAME, kcn=KCOLNAME, vcn=VCOLNAME, ucn=UCOLNAME):
    cur = conn.cursor()
    qinsert = f"INSERT INTO {tn}({kcn},{vcn},{ucn}) values(?, ?, ?)"
    cur.execute(qinsert,(k,v,now))
    cur.close()
    conn.commit()
    # print(f"lastrowid: {cur.lastrowid}")
    return cur.lastrowid != 0, cur

def update(conn, k, v, now, tn=KVTABLE_NAME, kcn=KCOLNAME, vcn=VCOLNAME, ucn=UCOLNAME):
    # print(f'gonna update key="{k}" with value="{v}"')
    qupdate = f"update {tn} set {vcn}=?,{ucn}=? where {kcn}=?"
    cur = conn.cursor()
    cur.execute(qupdate,[v,now,k])
    # print(cur.rowcount)
    cur.close()
    conn.commit()
    return cur.rowcount == 1, cur

def select(conn, k, tn=KVTABLE_NAME, kcn=KCOLNAME, vcn=VCOLNAME, ucn=UCOLNAME):
    qselect = f"select {kcn}, {vcn}, {ucn} from {tn} where {kcn}=?"
    qselect = f"select * from {tn} where {kcn}=?"
    cur = conn.cursor()
    row = cur.execute(qselect, (k,)).fetchone()
    # print(row)
    return row

def selectAll(conn, tn=KVTABLE_NAME):
    qselect = f"select * from {tn}"
    cur = conn.cursor()
    cur.execute(qselect)
    rows = cur.fetchall()
    # print(rows)
    cur.close()
    return rows

def rowCount(conn, tn=KVTABLE_NAME):
    qselect = f"select count(rowid) from {tn}"
    cur = conn.cursor()
    cur.execute(qselect)
    count = cur.fetchone()[0]
    # print(f"count is: {count}")
    return count

def delete(conn, k=None, tn=KVTABLE_NAME, kcn=KCOLNAME):
    if k is None:
        qdelete = f"delete from {tn}"
        conn.execute(qdelete)
    else:
        qdelete = f"delete from {tn} where {kcn}=?"
        conn.execute(qdelete, (k,))
    conn.commit()

def getRowCount(conn, tn=KVTABLE_NAME, kcn=KCOLNAME):
    qselect = f"select count({kcn}) from {tn}"
    cur = conn.cursor()
    cur.execute(qselect)
    return cur.fetchone()[0]

def getTableinfo(connection, tn=KVTABLE_NAME):
    qtableinfo = f"PRAGMA table_info('{tn}');"
    cur = connection.cursor()
    tableinfo = cur.execute(qtableinfo).fetchall()
    cur.close()
    print(tableinfo)

TESTDBNAME = ":memory:"
TESTNOW = "now"
class TestDbOperations(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.conn = connect_db(TESTDBNAME, True)

    @classmethod
    def tearDownClass(cls):
        cls.conn.close()

    def test_empty(self):
        rows = selectAll(self.conn)
        self.assertEqual(len(rows), 0, "Got content from empty table")

    def test_insert(self):
        _, cur = insert(self.conn,'testkey01','["testjsondata1"]', TESTNOW)
        self.assertNotEqual(cur.lastrowid, 0, "Insert operation failed")
    
    def test_inserted_select(self):
        value_inserted = '["new testjsondata2"]'
        insert(self.conn,'testkey02', value_inserted, TESTNOW)
        data = select(self.conn,'testkey02')
        # print(data)
        self.assertEqual(data[1], value_inserted, "Got wrong value")

    def test_inserted_update(self):
        _, cur = update(self.conn,'testkey01',TestJson, TESTNOW)
        self.assertEqual(cur.rowcount, 1, f"Update opreation failed with rowcount: {cur.rowcount}")

    def test_nonexist_select(self):
        row = select(self.conn,'testkey05')
        # print(f"row: {row}")
        self.assertEqual(row, None, "Got value should be empty")

    def test_row_delete(self):
        # rows = selectAll(self.conn)
        # print(f"rows before delete: {rows}")
        rowCountBefore = getRowCount(self.conn)
        delete(self.conn, 'testkey01')
        rowCountAfter = getRowCount(self.conn)
        self.assertEqual(rowCountAfter, rowCountBefore-1, "row count wrong after delete")
    
    def test_total_clean(self):
        delete(self.conn)
        rowCountAfter = getRowCount(self.conn)
        self.assertEqual(rowCountAfter, 0, "table should be empty after delete all")

TestJson = '{"BanList":"11点下线,psi,qazqaz,SRX-ATX,纣王","ShowBanTip":true,"BanTip":"Blocked!!!!!","BanNegJisao":false,"JisaoMin":0,"BanQuote":true}'
if __name__ == "__main__":
    # connection = connect_db(TESTDBNAME)
    # print(f"""first insert result: {insert(connection,'testkey01','["testjsondata1"]', TESTNOW)}""")
    # print(f"second update result: {update(connection,'testkey01',TestJson,TESTNOW)}")
    # print(f"third insert result: {insert(connection,'testkey02','testjsondata2', TESTNOW)}")
    # print(f"""second update result: {update(connection,'testkey02','["new testjsondata2"]',TESTNOW)}""")
    # print(f"select testkey01 result: {select(connection, 'testkey01')}")
    # print(f"select testkey01 result: {select(connection, 'testkey02')}")
    # print(f"select testkey05 result: {select(connection, 'testkey05')}")
    # print(f"row count: {getRowCount(connection)}")
    # print(f"rows: {selectAll(connection)}")
    # print(f"gonna delete key={'testkey01'}")
    # delete(connection, 'testkey01')
    # print(f"row count after delete key={'testkey01'}: {getRowCount(connection)}")
    # print("gonna delete all rows")
    # delete(connection)
    # print(f"final row count: {getRowCount(connection)}")
    # connection.close()
    unittest.main()
