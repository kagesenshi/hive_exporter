import MySQLdb
import random
import string
import time
import subprocess
import os

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

clean_cmd = [BEELINE, '-u',
        'jdbc:hive2://localhost:10000',
        '--outputformat=csv',
        '--showHeader=false',
        '-e', "drop table ingest_test.data_append"]
p = subprocess.Popen(clean_cmd, stdout=subprocess.PIPE)
p.wait()

cursor = db.cursor()

cursor.execute('create database if not exists ingest_test')

cursor.execute('use ingest_test')
cursor.execute('drop table if exists data_append')
cursor.execute('create table data_append(id int, value varchar(30), created datetime)')

for i in range(0,10):
    cursor.execute('insert into data_append(id, created) values (%s, now())', [i])
db.commit()

cmd = [SPARK_SUBMIT, os.path.join(here,'..',
    'jdbc_loader_incremental_append_spark2.py'), 
    '-u', 'jdbc:mysql://%s/ingest_test' % (host), 
    '-D', 'com.mysql.jdbc.Driver',
    '--user', user,
    '--password', password,
    '-t','ingest_test.data_append',
    '-p', 'id',
    '--num-partitions', '2',
    '--incremental-column', 'id']

p = subprocess.Popen(cmd)
p.wait()

count_cmd = [BEELINE, '-u',
        'jdbc:hive2://localhost:10000',
        '--outputformat=csv',
        '--showHeader=false',
        '-e', "select count(1) as c from ingest_test.data_append"]

p = subprocess.Popen(count_cmd, stdout=subprocess.PIPE)
p.wait()

assert int(p.stdout.read().decode('utf8').strip()[1:-1]) == 10

for i in range(10,20):
    cursor.execute('insert into data_append(id, created) values (%s, now())', [i])
db.commit()

p = subprocess.Popen(cmd)
p.wait()

p = subprocess.Popen(count_cmd, stdout=subprocess.PIPE)
p.wait()

assert int(p.stdout.read().decode('utf8').strip()[1:-1]) == 20
print('DONE')
