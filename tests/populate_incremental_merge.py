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
cursor.execute('drop table if exists data')
cursor.execute('create table data(id int, value varchar(30), last_modified datetime, created datetime, deleted int)')

for i in range(10):
    cursor.execute('insert into data(id, value, last_modified, created) values (%s, %s, now(), now())', [i, chr(ord('A') + i)])


db.commit()

while True:
    char = random.sample(list(string.ascii_uppercase), 1)[0]
    i = random.randint(0,9)

    cursor.execute('update data set value=%s,last_modified=now() where id=%s', [char, i])
    print('updated %s to %s' % (i, char))
    db.commit()
    time.sleep(2)