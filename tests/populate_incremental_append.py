import MySQLdb
import random
import string
import time

db = MySQLdb.connect(
    host='10.210.14.99',
    user='root',
    password='myr00t',
    database='mysql',
)


cursor = db.cursor()

cursor.execute('create database if not exists ingest_test')

cursor.execute('use ingest_test')
cursor.execute('drop table if exists data_append')
cursor.execute('create table data_append(id int, value varchar(30), created datetime)')

i = 0
while True:
    i += 1
    cursor.execute('insert into data_append(id, created) values (%s, now())', [i])
    db.commit()
    print('inserted %s' % i)
    time.sleep(2)
