"""
    Import table from JDBC into Hive using Spark2

    To execute: 

    spark-submit jdbc_loader_spark2.py
"""
from pyspark.shell import spark
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import *
from argparse import ArgumentParser
import random
import string
import glob
import os
import time as Y
import logging
import sys
import copy
from datetime import *

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
parser.add_argument('-r', '--incremental-column', required=True)
parser.add_argument('-R', '--last-value')
parser.add_argument('-d', '--deleted-column')
parser.add_argument('-H', '--hive-table')
parser.add_argument('-q', '--query')
parser.add_argument('-p', '--partition-column')
parser.add_argument('-m', '--num-partitions')
parser.add_argument('-T', '--query-timeout')
parser.add_argument('-F', '--fetch-size')
parser.add_argument('-I', '--init')
parser.add_argument('-s', '--storageformat', default='parquet')
parser.add_argument('-O', '--overwrite', action='store_true', default=False)
parser.add_argument('-S', '--scratch-db', default='spark_scratch')
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

# load data from source
df = conn.load()

def incremental_append_ingestion(spark, df, hive_db, hive_tbl, incremental_column, last_value=None, storageformat='parquet'):
    db, tbl = hive_db, hive_tbl
    incremental_exists = False
    if db.lower() in [d.name.lower() for d in spark.catalog.listDatabases()]:
        tables = [t.name.lower() for t in spark.catalog.listTables(db)]
        if tbl.lower() in tables:
            incremental_exists = True

    if incremental_column and not last_value:
        if incremental_exists:
            last_value = spark.sql('select max(%s) from %s.%s' % (incremental_column, db, tbl)).take(1)[0][0]

    if incremental_column and last_value:
        df = df.where(F.col(incremental_column) > F.lit(last_value))

    df = df.withColumn('dl_ingest_date', F.lit(datetime.now().strftime('%Y%m%dT%H%M%S')))
    df = df.cache()
    new_rows = df.count()
    df.show(2)
    print("Total number of records in df:", df.count())

    if not incremental_exists:
        log.info('Importing %s' % tbl)
        spark.sql('create database if not exists %s' % db)
        df.write.mode('overwrite').format(storageformat).partitionBy('dl_ingest_date').saveAsTable('%s.%s' % (db, tbl))
        log.info('.. DONE')
    else:
        log.info('Importing incremental %s' % tbl)
        df.write.mode('append').format(storageformat).partitionBy('dl_ingest_date').saveAsTable('%s.%s' % (db, tbl))
        log.info('.. DONE')


def incremental_merge_ingestion(spark, df, hive_db, hive_tbl, key_columns, last_modified_column, last_modified=None, deleted_column=None, scratch_db='spark_scratch', 
        storageformat='parquet'):
    db, tbl = hive_db, hive_tbl
    incremental_exists = False
    incremental_tbl = '%s_incremental' % tbl
    if db.lower() in [d.name.lower() for d in spark.catalog.listDatabases()]:
        tables = [t.name.lower() for t in spark.catalog.listTables(db)]
        if incremental_tbl.lower() in tables:
            incremental_exists = True
    if last_modified_column and not last_modified:
        if incremental_exists:
            last_modified = spark.sql('select max(%s) from %s.%s' % (last_modified_column, db, incremental_tbl)).take(1)[0][0]

    if last_modified_column and last_modified:
        df = df.where(F.col(last_modified_column) > F.lit(last_modified))

    df = df.withColumn('dl_ingest_date', F.lit(datetime.now().strftime('%Y%m%dT%H%M%S')))
    df = df.cache()
    new_rows = df.count()
    df.show(2)
    print("Total number of records in df:", df.count())

    if not incremental_exists:
        log.info('Importing %s' % tbl)
        spark.sql('create database if not exists %s' % db)
        df.write.mode('overwrite').format(storageformat).partitionBy('dl_ingest_date').saveAsTable('%s.%s' % (db, incremental_tbl))
        log.info('.. DONE')
    else:
        log.info('Importing incremental %s' % tbl)
        df.write.mode('append').format(storageformat).partitionBy('dl_ingest_date').saveAsTable('%s.%s' % (db, incremental_tbl))
        log.info('.. DONE')

    df = spark.sql('select * from %s.%s' % (db, incremental_tbl))

    # reconcile and select latest record
    row_num_col = 'row_num_%s' % ''.join(random.sample(string.ascii_lowercase, 6))
    windowSpec = (
        Window.partitionBy(*key_columns)
              .orderBy(F.col(last_modified_column).desc())
    )
    reconcile_df = df.select(
        F.row_number().over(windowSpec).alias(row_num_col),
        *df.columns
    )
    reconcile_df = reconcile_df.where(F.col(row_num_col) == F.lit(1)).drop(row_num_col)
    if deleted_column:
        reconcile_df = reconcile_df.where(F.col(deleted_column).isNull())
    
    reconcile_df.createOrReplaceTempView('import_tbl')
    
    log.info('Importing/Updating %s' % tbl)
    
    df = reconcile_df.cache()
    new_total_rows = df.count()
    df.show(2)
    print("Total number of new records in df:", df.count())
    temp_table = 'temp_table_%s' % ''.join(random.sample(string.ascii_lowercase, 6))
    
    # materialize reconciled data
    df.createOrReplaceTempView(temp_table)
    spark.sql('create database if not exists %s' % scratch_db)
    df.write.mode('overwrite').format(storageformat).saveAsTable('%s.%s_persist' % (scratch_db, temp_table))
    
    # move materialized data to destination table
    dfx = spark.sql('select * from %s.%s_persist' % (scratch_db, temp_table))
    spark.sql('create table if not exists %s.%s stored as %s as select * from %s.%s_persist limit 0' % (db, tbl, storageformat, scratch_db, temp_table))
    dfx.write.format(storageformat).insertInto('%s.%s' % (db, tbl), overwrite=True)
    
    log.info('.. DONE')

incremental_append_ingestion(spark, df, db, tbl, args.incremental_column, args.last_value, args.storageformat)
incremental_merge_ingestion(spark, df, db, tbl, args.key_columns.split(','), args.last_modified_column, args.last_modified, args.deleted_column)
