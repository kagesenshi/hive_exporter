"""
    Export Hive create table statements into SQLite file
"""
import glob
import jaydebeapi
import json
import sqlite3
import time
import logging

logging.basicConfig(level=logging.INFO)
log = logging.getLogger('hive-metadata-exporter')

jar_files=glob.glob('/usr/hdp/current/hive-client/jdbc/*.jar')

host='10.10.10.31'
port='10000'
database='default'
driver='org.apache.hive.jdbc.HiveDriver'


conn_hive = jaydebeapi.connect(driver,
    'jdbc:hive2://10.10.10.31:10000/default;',jars=jar_files)


def get_createstmt(cur, db, tbl):
    cur.execute('show create table %s.%s' % (db,tbl))
    
    return '\n'.join([c[0] for c in cur.fetchall()])

cur = conn_hive.cursor()
cur.execute('show databases')

databases = [c[0] for c in cur.fetchall()]

all_tables = []

outdb = sqlite3.connect('output.sqlite')

outdb.execute('''
    create table if not exists exported_tables (
	db_name string,
        tbl_name string,
        createstmt string,
        extract_ts int
    );
''')

outdb.execute('delete from exported_tables where 1=1')

log.info('Exporting list of tables')
db_total = len(databases)
for idx, db in enumerate(databases):
    cur.execute('use %s' % db)
    cur.execute('show tables')
    now = int(time.time())
    for c in cur.fetchall():
        tbl_name = c[0]

        outdb.execute('''
          insert into exported_tables (db_name, tbl_name)
          values (?,?)
        ''', [db, tbl_name])
        outdb.commit()
    log.info('%s/%s' % (idx+1, db_total))

log.info('Done')

log.info('Exporting create table statements')
res = outdb.execute('select db_name, tbl_name from exported_tables')
tables = res.fetchall()
total = len(tables)
idx = 0
for db, tbl in tables:
    idx+=1
    createstmt = get_createstmt(cur, db, tbl)
    outdb.execute('update exported_tables set createstmt=? where db_name=? and tbl_name=?',
        [createstmt, db, tbl])
    log.info('%s/%s' % (idx, total))
    outdb.commit()

log.info('Done')
