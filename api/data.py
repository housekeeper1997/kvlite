#! /usr/bin/env python3

import unittest
import sqlalchemy as db
from sqlalchemy import Column, String, Integer
from sqlalchemy.orm import Session, declarative_base
from api.utility import env_str, env_int

KVTABLE_NAME = env_str('KVTABLE_NAME','BanData')
COLUMN_PREFIX = env_str('COLUMN_PREFIX','tbt')
KEY_COLUMN_LENGTH = env_int('KEY_COLUMN_LENGTH', 256)
VALUE_COLUMN_LENGTH = env_int('VALUE_COLUMN_LENGTH', 1024*8)
UPDATE_COLUMN_LENGTH = env_int('UPDATE_COLUMN_LENGTH', 256)
KCOLNAME = f'{COLUMN_PREFIX}_key'
VCOLNAME = f'{COLUMN_PREFIX}_value'
UCOLNAME = f'{COLUMN_PREFIX}_update'

Base = declarative_base()

class Kv(Base):
    __tablename__ = KVTABLE_NAME
    id = Column(Integer, primary_key=True, name='rowid')
    key = Column(String(KEY_COLUMN_LENGTH), nullable=False, name=KCOLNAME)
    value = Column(String(VALUE_COLUMN_LENGTH), nullable=False, name=VCOLNAME)
    update = Column(String(UPDATE_COLUMN_LENGTH), nullable=False, name=UCOLNAME)

def connect_db(dbname, clean=False):
    # conn = sqlite3.connect(dbname)
    engine = db.create_engine(dbname, echo=False)
    with engine.connect() as conn:
        if clean:
            qdrop = f'DROP TABLE IF EXISTS {KVTABLE_NAME};'
            conn.execute(db.text(qdrop))
            conn.commit()
        meta = db.MetaData()
        table = db.Table(
                KVTABLE_NAME, meta, 
                Column('rowid', Integer, primary_key = True), 
                Column(KCOLNAME, String(KEY_COLUMN_LENGTH)), 
                Column(VCOLNAME, String(VALUE_COLUMN_LENGTH)), 
                Column(UCOLNAME, String(UPDATE_COLUMN_LENGTH)), 
                mysql_charset='utf8mb4',
            )
        meta.create_all(engine)
        # query = f'''CREATE TABLE IF NOT EXISTS {KVTABLE_NAME} (
        #             {KCOLNAME} text NOT NULL UNIQUE,
        #             {VCOLNAME} text NOT NULL,
        #             {UCOLNAME} text NOT NULL
        #         );'''
        # conn.execute(db.text(query))
        return engine

def insert(engine, k, v, now, tn=KVTABLE_NAME, kcn=KCOLNAME, vcn=VCOLNAME, ucn=UCOLNAME):
    record = Kv(key=k, value=v, update=now)
    with Session(engine) as session:
        session.add(record)
        session.commit()
        lastrowid = record.id
        return lastrowid != 0, lastrowid

def update(engine, k, v, now, tn=KVTABLE_NAME, kcn=KCOLNAME, vcn=VCOLNAME, ucn=UCOLNAME):
    stmt = f'UPDATE {tn} SET {vcn} = :v, {ucn} = :now WHERE {kcn}  = :k'
    values = {'v': v,'now': now,'k': k}
    with engine.begin() as conn:
        conn.execute(db.text(stmt), values)

def select(engine, k, tn=KVTABLE_NAME, kcn=KCOLNAME, vcn=VCOLNAME, ucn=UCOLNAME):
    stmt = f"select {kcn}, {vcn}, {ucn} from {tn} where {kcn}= :k"
    values = {'k': k}
    with engine.begin() as conn:
        result = conn.execute(db.text(stmt), values)
        row = result.fetchone()
        # print(row)
        if row is None:
            return None
        return row

def selectAll(engine, page=1, per_page=10):
    with Session(engine) as session:
        return session.query(Kv).offset((page-1)*per_page).limit(per_page).all()

def rowCount(engine, tn=KVTABLE_NAME):
    stmt = f"select count(rowid) from {tn}"
    with engine.begin() as conn:
        result = conn.execute(db.text(stmt)).fetchone()[0]
        # print(result)
        return result

def delete(engine, k=None, tn=KVTABLE_NAME, kcn=KCOLNAME):
    stmt = f"delete from {tn} where {kcn}= :k"
    if k is None:
        stmt = f"delete from {tn}"
    values = {'k': k}
    with engine.begin() as conn:
        conn.execute(db.text(stmt), values)

def getRowCount(engine, tn=KVTABLE_NAME, kcn=KCOLNAME):
    qselect = f"select count({kcn}) from {tn}"
    with engine.begin() as conn:
        result = conn.execute(db.text(qselect)).fetchone()[0]
        # print(result)
        return result

def getTableinfo(connection, tn=KVTABLE_NAME):
    qtableinfo = f"PRAGMA table_info('{tn}');"
    cur = connection.cursor()
    tableinfo = cur.execute(qtableinfo).fetchall()
    cur.close()
    print(tableinfo)

TESTDBNAME = "sqlite:///:memory:"
# TESTDBNAME = 'sqlite:///KVData.db'
TESTNOW = "now"
class TestDbOperations(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.engine = connect_db(TESTDBNAME, True)

    @classmethod
    def tearDownClass(cls):
        pass
        # cls.engine.close()

    def test_empty(self):
        rows = selectAll(self.engine)
        self.assertEqual(len(rows), 0, "Got content from empty table")

    def test_insert(self):
        _, lastrowid = insert(self.engine,'testkey01','["testjsondata1"]', TESTNOW)
        self.assertNotEqual(lastrowid, 0, "Insert operation failed")
    
    def test_inserted_select(self):
        value_inserted = '["new testjsondata2"]'
        insert(self.engine,'testkey02', value_inserted, TESTNOW)
        data = select(self.engine,'testkey02')
        # print(data)
        self.assertEqual(data[1], value_inserted, "Got wrong value")

    def test_inserted_update(self):
        k = 'testkey01'
        update(self.engine, k,TestJson, TESTNOW)
        row = select(self.engine, k)
        self.assertEqual(row[1], TestJson, f"value is not we set")

    def test_nonexist_select(self):
        row = select(self.engine,'testkey05')
        # print(f"row: {row}")
        self.assertEqual(row, None, "Got value should be empty")

    def test_row_delete(self):
        # rows = selectAll(self.engine)
        # print(f"rows before delete: {rows}")
        rowCountBefore = getRowCount(self.engine)
        delete(self.engine, 'testkey01')
        rowCountAfter = getRowCount(self.engine)
        self.assertEqual(rowCountAfter, rowCountBefore-1, "row count wrong after delete")
    
    def test_total_clean(self):
        delete(self.engine)
        rowCountAfter = getRowCount(self.engine)
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
    # # connection.close()
    unittest.main()
