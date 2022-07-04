from pyspark.sql import functions as F
from pyspark.sql.window import Window
import random
import string
import glob
import os
import logging
import sys
import copy
from datetime import datetime

logging.basicConfig(level=logging.INFO)
log = logging.getLogger('spark-ingestor')

def add_common_arguments(parser):
    parser.add_argument('-u', '--jdbc', required=True)
    parser.add_argument('-D', '--driver')
    parser.add_argument('-U', '--username')
    parser.add_argument('-P', '--password')
    parser.add_argument('-t', '--dbtable')
    parser.add_argument('-H', '--hive-table')
    parser.add_argument('-q', '--query')
    parser.add_argument('-p', '--partition-column')
    parser.add_argument('-y', '--output-partition-columns')
    parser.add_argument('-m', '--num-partitions')
    parser.add_argument('-T', '--query-timeout')
    parser.add_argument('-F', '--fetch-size')
    parser.add_argument('-I', '--init')
    parser.add_argument('-i', '--ingestion-tag-column',
            default='dl_ingest_date')
    parser.add_argument('-s', '--storageformat', default='parquet')
    parser.add_argument('-O', '--overwrite', action='store_true', default=False)
    parser.add_argument('-v', '--verbose', action='store_true', default=False)
    
def validate_common_args(args):
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
    
def base_conn_from_args(spark, args):
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
    return conn

def conn_from_args(spark, args, query=None, use_partitioning=True):
    if args.verbose:
        spark.sparkContext.setLogLevel("INFO")
    else:
        spark.sparkContext.setLogLevel("WARN")

    conn = base_conn_from_args(spark, args)

    if query or args.query:
        conn = conn.option('query', query or args.query)
    elif args.dbtable:
        conn = conn.option('dbtable', args.dbtable)
    else:
        raise AssertionError('Neither dbtable nor query are available')
    
    if use_partitioning and args.partition_column and args.num_partitions:
        query = 'select min(%s) as lower_bound, max(%s) as upper_bound from %s' % (args.partition_column, args.partition_column, args.dbtable)
        pushdownConn = base_conn_from_args(spark, args)
        dfx = pushdownConn.option('query', query).load()
        log.info('Getting lower and upper bound of %s' % args.partition_column)
        bounds = dfx.take(1)[0]
        lower_bound = bounds.lower_bound
        upper_bound = bounds.upper_bound
        log.info('Lower bound = %s' % lower_bound)
        log.info('Upper bound = %s' % upper_bound)
        conn = (conn.option('partitionColumn', args.partition_column)
                .option('numPartitions', str(args.num_partitions))
                .option('lowerBound', str(lower_bound))
                .option('upperBound', str(upper_bound)))
    
    if args.jdbc.startswith('oracle'):
        conn = (conn
             .option('oracle.jdbc.mapDateToTimestamp', 'false')
             .option('sessionInitStatement', 'ALTER SESSION SET NLS_TIMESTAMP_FORMAT="YYYY-MM-DD HH24:MI:SS.FF"'))
    
    return conn

_marker = []

def full_ingestion(spark, df, hive_db, hive_tbl, drop=False,
        storageformat='parquet', ingestion_tag_column='dl_ingest_date',
        output_partitions=_marker):
    
    output_partitions = output_partitions or []

    db, tbl = hive_db, hive_tbl
    
    df = df.withColumn(ingestion_tag_column, F.lit(datetime.now().strftime('%Y%m%dT%H%M%S')))
    log.info('Ingesting into spark persistence cache')
    df = df.persist()
    log.info('Persistence done')
    df.createOrReplaceTempView('import_tbl')
    new_rows = df.count()
    log.info("Ingesting %s new rows" % new_rows)
    
    log.info('Importing %s' % tbl)
    spark.sql('create database if not exists %s' % db)
    if drop:
       spark.sql('drop table if exists %s.%s' % (db, tbl))
    spark.sql('create table if not exists %s.%s stored as %s as select * from import_tbl limit 0' % (db, tbl, storageformat))
    df.write.format(storageformat).insertInto('%s.%s' % (db, tbl), overwrite=True)    
    log.info('.. DONE')

    return new_rows

def incremental_append_ingestion(spark, df, hive_db, hive_tbl,
        incremental_column, last_value=None, storageformat='parquet',
        ingestion_tag_column='dl_ingest_date',
        output_partitions=_marker):

    output_partitions = output_partitions or ['dl_ingest_date']

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

    df = df.withColumn(ingestion_tag_column, F.lit(datetime.now().strftime('%Y%m%dT%H%M%S')))
    df = df.persist()
    new_rows = df.count()
    log.info("Ingesting %s new rows" % new_rows)

    if not incremental_exists:
        log.info('Importing %s' % tbl)
        spark.sql('create database if not exists %s' % db)
        df.write.mode('overwrite').format(storageformat).partitionBy(*output_partitions).saveAsTable('%s.%s' % (db, tbl))
        log.info('.. DONE')
    else:
        log.info('Importing incremental %s' % tbl)
        df.write.mode('append').format(storageformat).partitionBy(*output_partitions).saveAsTable('%s.%s' % (db, tbl))
        log.info('.. DONE')

    return new_rows

def incremental_merge_ingestion(spark, df, hive_db, hive_tbl, key_columns,
        last_modified_column, last_modified=None, 
        incremental_column=None, last_value=None,
        deleted_column=None, scratch_db='spark_scratch', 
        storageformat='parquet', ingestion_tag_column='dl_ingest_date',
        output_partitions=_marker):

    output_partitions = output_partitions or []

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
    if incremental_column and not last_value:
        if incremental_exists:
            last_value = spark.sql('select max(%s) from %s.%s' %
                    (incremental_column, db, incremental_tbl)).take(1)[0][0]

    if (last_modified_column and last_modified 
            and incremental_column and last_value):
        df = df.where((F.col(incremental_column) > F.lit(last_value)) |
                (F.col(last_modified_column) > F.lit(last_modified)))
    elif last_modified_column and last_modified:
        df = df.where(F.col(last_modified_column) > F.lit(last_modified))
    elif incremental_column and last_value:
        df = df.where(F.col(incremental_column) > F.lit(last_value))

    df = df.withColumn(ingestion_tag_column, F.lit(datetime.now().strftime('%Y%m%dT%H%M%S')))
    df = df.persist()
    new_rows = df.count()
    log.info("Ingesting %s new rows" % new_rows)

    if not incremental_exists:
        log.info('Importing %s' % tbl)
        spark.sql('create database if not exists %s' % db)
        df.write.mode('overwrite').format(storageformat).partitionBy(ingestion_tag_column).saveAsTable('%s.%s' % (db, incremental_tbl))
        log.info('.. DONE')
    else:
        log.info('Importing incremental %s' % tbl)
        df.write.mode('append').format(storageformat).partitionBy(ingestion_tag_column).saveAsTable('%s.%s' % (db, incremental_tbl))
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
    
    df = reconcile_df.persist()
    temp_table = 'temp_table_%s' % ''.join(random.sample(string.ascii_lowercase, 6))
    
    # materialize reconciled data
    df.createOrReplaceTempView(temp_table)
    spark.sql('create database if not exists %s' % scratch_db)
    writer = df.write.mode('overwrite').format(storageformat)
    if output_partitions:
        writer = writer.partitionBy(*output_partitions)
    writer.saveAsTable('%s.%s_persist' % (scratch_db, temp_table))
    
    # move materialized data to destination table
    dfx = spark.sql('select * from %s.%s_persist' % (scratch_db, temp_table))
    spark.sql('create table if not exists %s.%s like %s.%s_persist'
            % (db, tbl, scratch_db, temp_table))
    writer = dfx.write.format(storageformat)
    writer.insertInto('%s.%s' % (db, tbl), overwrite=True)
    spark.sql('drop table %s.%s_persist' %  (scratch_db, temp_table))
    log.info('.. DONE')
    
    return new_rows


