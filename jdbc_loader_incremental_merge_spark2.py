"""
    Import table from JDBC into Hive using Spark2

    To execute: 

    spark-submit jdbc_loader_spark2.py
"""
from pyspark.shell import spark
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from argparse import ArgumentParser
import random
import string
import glob
import os
import logging
import sys
import copy

logging.basicConfig(level=logging.INFO)
log = logging.getLogger('jdbc-loader-spark2')

parser = ArgumentParser()
parser.add_argument('-u', '--jdbc', required=True)
parser.add_argument('-D', '--driver')
parser.add_argument('-U', '--username')
parser.add_argument('-P', '--password')
parser.add_argument('-t', '--dbtable')
parser.add_argument('-k', '--key-columns', help='Comma separated list of columns that represent keys', required=True)
parser.add_argument('-l', '--last-modified-column', required=True)
parser.add_argument('-L', '--last-modified')
parser.add_argument('-H', '--hive-table')
parser.add_argument('-q', '--query')
parser.add_argument('-p', '--partition-column')
parser.add_argument('-m', '--num-partitions')
parser.add_argument('-T', '--query-timeout')
parser.add_argument('-F', '--fetch-size')
parser.add_argument('-I', '--init')
parser.add_argument('-s', '--storageformat', default='parquet')
parser.add_argument('-O', '--overwrite', action='store_true', default=False)
args = parser.parse_args()

if args.dbtable and args.query:
    print('Either -t/--dbtable or -q/--query shall be specified, but not both')
    sys.exit(1)
if not args.dbtable and not args.query:
    print('Either -t/--dbtable or -q/--query must be specified')
    sys.exit(1)
if not args.dbtable and not args.hive_table:
    print('-T/--hive-table is required when using with -q/--query')
    sys.exit(1)

if ((args.num_partitions and not args.partition_column) or
   (args.partition_column and not args.num_partitions)):
       print('-m/--num-partitions and -p/--partition-column must '
        'be specified together')
       sys.exit(1)


if ((args.username and  not args.password) or
   (args.password and not args.username)):
       print('-U/--username and -P/--password must '
        'be specified together')
       sys.exit(1)



conn = spark.read.format('jdbc').option('url', args.jdbc)
if args.driver:
    conn = conn.option('driver', args.driver)
if args.username:
    conn = conn.option('user', args.username)
if args.password:
    conn = conn.option('password', args.password)
if args.query_timeout:
    conn = conn.option('queryTimeout', args.query_timeout)
if args.fetch_size:
    conn = conn.option('fetchSize', args.fetch_size)
if args.init:
    conn = conn.option('sessionInitStatement', args.init)

if args.query:
    conn = conn.option('query', args.query)
elif args.dbtable:
    conn = conn.option('dbtable', args.dbtable)
else:
    raise AssertionError('Neither dbtable nor query are available')

dfx = copy.copy(conn).option('pushDownAggregate', 'true').load()
if args.partition_column and args.num_partitions:
    lower_bound, upper_bound = dfx.select(F.min(args.partition_column),
        F.max(args.partition_column)).collect()[0]
    conn = (conn.option('partitionColumn', args.partition_column)
            .option('numPartitions', str(args.num_partitions))
            .option('lowerBound', str(lower_bound))
            .option('upperBound', str(upper_bound)))

db, tbl = (args.hive_table or args.dbtable).split('.')

incremental_exists = False
historical_exists = False
incremental_tbl = '%s_incremental' % tbl
if db in [d.name for d in spark.catalog.listDatabases()]:
    tables = [t.name for t in spark.catalog.listTables(db)]
    if tbl in tables:
        historical_exists = True
    if incremental_tbl in tables:
        incremental_exists = True

if args.last_modified_column and not args.last_modified:
    last_modified = None
    if incremental_exists:
        last_modified = spark.sql('select max(%s) from %s.%s' % (args.last_modified_column, db, incremental_tbl)).take(1)[0][0]
else:
    last_modified = args.last_modified

# load data from source
df = conn.load()

if args.last_modified_column:
    if last_modified:
        df = df.where(F.col(args.last_modified_column) > F.lit(last_modified))

df = df.cache()
new_rows = df.count()

df.createOrReplaceTempView('import_tbl')

if not incremental_exists:
    df.createOrReplaceTempView('import_tbl')
    log.info('Importing %s' % tbl)
    spark.sql('create database if not exists %s' % db)
    df.write.mode('overwrite').format(args.storageformat).saveAsTable('%s.%s' % (db, incremental_tbl))
    log.info('.. DONE')
else:
    log.info('Importing incremental %s' % tbl)
    df.write.mode('append').format(args.storageformat).saveAsTable('%s.%s' % (db, incremental_tbl))
    log.info('.. DONE')

key_columns = args.key_columns.split(',')

# get old data in hive
df = spark.sql('select * from %s.%s' % (db, incremental_tbl))

# reconcile and select latest record
row_num_col = 'row_num_%s' % ''.join(random.sample(string.ascii_lowercase, 6))
windowSpec = (
    Window.partitionBy(*key_columns)
          .orderBy(F.col(args.last_modified_column).desc())
)
reconcile_df = df.select(
    F.row_number().over(windowSpec).alias(row_num_col),
    *df.columns
)
reconcile_df = reconcile_df.where(F.col(row_num_col) == F.lit(1)).drop(row_num_col)
reconcile_df.createOrReplaceTempView('import_tbl')

log.info('Importing/Updating %s' % tbl)

df = reconcile_df.cache()
new_total_rows = df.count()
temp_table = 'temp_table_%s' % ''.join(random.sample(string.ascii_lowercase, 6))

# materialize reconciled data
df.createOrReplaceTempView(temp_table)
df.write.mode('overwrite').saveAsTable('default.%s_persist' % (temp_table))

# move materialized data to destination table
dfx = spark.sql('select * from default.%s_persist' % temp_table)
dfx.write.mode('overwrite').saveAsTable('%s.%s' % (db, tbl))

log.info('.. DONE')

