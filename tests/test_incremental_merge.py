import MySQLdb
import random
import string
import time
import subprocess
import os
import sys

SPARK_SUBMIT='spark2-submit'
BEELINE = 'spark2-beeline'

host = '10.210.14.99'
db = 'ingest_test'
user = 'root'
password = 'myr00t'

db = MySQLdb.connect(
    host=host,
    user=user,
    password=password,
    database='mysql',
)

here = os.path.dirname(__file__)

def delete_table():
    clean_cmd = [BEELINE, '-u',
            'jdbc:hive2://localhost:10000',
            '--outputformat=csv',
            '--showHeader=false',
            '-e', "drop table if exists ingest_test.data purge"]
    p = subprocess.Popen(clean_cmd, stdout=subprocess.PIPE)
    if p.wait():
        sys.exit(1)

    clean_cmd = [BEELINE, '-u',
            'jdbc:hive2://localhost:10000',
            '--outputformat=csv',
            '--showHeader=false',
            '-e', "drop table if exists ingest_test.data_incremental purge"]
    p = subprocess.Popen(clean_cmd, stdout=subprocess.PIPE)
    if p.wait():
        sys.exit(1)


def ingest_data():
    cmd = [SPARK_SUBMIT, os.path.join(here,'..',
        'jdbc_loader_incremental_merge_spark2.py'), 
        '-u', 'jdbc:mysql://%s/ingest_test' % (host), 
        '-D', 'com.mysql.jdbc.Driver',
        '--user', user,
        '--password', password,
        '-t','ingest_test.data',
        '-p', 'id',
        '-k', 'id',
        '--num-partitions', '2',
        '--output-partition-columns', 'date',
        '--incremental-column', 'id',
        '--last-modified-column', 'last_modified']
    
    p = subprocess.Popen(cmd)
    if p.wait():
        sys.exit(1)
    

def count_hive():

    count_cmd = [BEELINE, '-u',
            'jdbc:hive2://localhost:10000',
            '--outputformat=csv',
            '--showHeader=false',
            '-e', "select count(1) as c from ingest_test.data"]
    
    p = subprocess.Popen(count_cmd, stdout=subprocess.PIPE)
    p.wait()
    
    return int(p.stdout.read().decode('utf8').strip()[1:-1])


delete_table()

cursor = db.cursor()

cursor.execute('create database if not exists ingest_test')

cursor.execute('use ingest_test')
cursor.execute('drop table if exists data')
cursor.execute('''
    create table data(id int, value varchar(30), last_modified
        datetime, created datetime, `date` varchar(10), deleted int)
        ''')
db.commit()

for i in range(10):
    cursor.execute('''
        insert into data(id, value, last_modified, created, `date`) values (%s,
        %s, now(), now(), date_format(now(), '%%Y-%%m-%%d'))''', [i, chr(ord('A') + i)])

db.commit()

ingest_data()

count = count_hive()
print(count)
assert count == 10


cursor.execute(r'''
    insert into data(id, value, last_modified, created, `date`) values
    (11,'L',null,now(), date_format(now(), '%Y-%m-%d'))''')

db.commit()

ingest_data()

count = count_hive()
print(count)
assert count == 11


cursor.execute('''
    update data set last_modified=now(),value='K' where id=3''')

db.commit()

ingest_data()

count = count_hive()
print(count)
assert count == 11
print('DONE')
