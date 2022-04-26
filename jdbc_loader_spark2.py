"""
    Import table from JDBC into Hive using Spark2

    To execute: 

    spark-submit jdbc_loader_spark2.py
"""
from pyspark.shell import spark
from pyspark.sql import functions as F
from argparse import ArgumentParser
import glob
import os
from argparse import ArgumentParser
import logging
import sys
import copy
from spark_loaders import full_ingestion, conn_from_args, validate_common_args
from spark_loaders import add_common_arguments

logging.basicConfig(level=logging.INFO)
log = logging.getLogger('jdbc-loader-spark2')

parser = ArgumentParser()
add_common_arguments(parser)
args = parser.parse_args()

validate_common_args(args)

conn = conn_from_args(spark, args)
df = conn.load()
db, tbl = (args.hive_table or args.dbtable).split('.')

source_count = copy.copy(conn).option('pushDownAggregate',
        'true').load().count()

output_partitions = []
if args.output_partition_columns:
    output_partitions = args.output_partition_columns.split(',')

ingested_count = full_ingestion(spark, df, db, tbl, args.overwrite,
        args.storageformat, output_partitions=output_partitions)

dest_count = spark.sql('select * from %s.%s' % (db, tbl)).count()

log.info("Source rows = %s" % source_count)
log.info("Ingested rows = %s" % ingested_count)
log.info("Destination rows = %s" % dest_count)
