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

def full_ingestion(spark, df, hive_db, hive_tbl, drop=False, storageformat='parquet'):    
    db, tbl = hive_db, hive_tbl
    
    df = df.withColumn('dl_ingest_date', F.lit(datetime.now().strftime('%Y%m%dT%H%M%S')))
    df = df.persist()
    df.createOrReplaceTempView('import_tbl')
    log.info('show count %s' % tbl)
    new_rows = df.count()
    df.show(2)
    print("Total number of records in df:", df.count())
    
    log.info('Importing %s' % tbl)
    spark.sql('create database if not exists %s' % db)
    if drop:
       spark.sql('drop table if exists %s.%s' % (db, tbl))
    spark.sql('create table if not exists %s.%s stored as %s as select * from import_tbl limit 0' % (db, tbl, storageformat))
    df.write.format(storageformat).insertInto('%s.%s' % (db, tbl), overwrite=True)    
    log.info('.. DONE')   

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
    df = df.persist()
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
    df = df.persist()
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
    
    df = reconcile_df.persist()
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
    



