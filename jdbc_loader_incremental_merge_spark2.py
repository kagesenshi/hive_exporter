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
from datetime import datetime
from spark_loaders import incremental_merge_ingestion, conn_from_args
from spark_loaders import validate_common_args, add_common_arguments

logging.basicConfig(level=logging.INFO)
log = logging.getLogger('jdbc-loader-spark2')

parser = ArgumentParser()
add_common_arguments(parser)
parser.add_argument('-k', '--key-columns', help='Comma separated list of columns that represent keys', required=True)
parser.add_argument('-l', '--last-modified-column', required=True)
parser.add_argument('-L', '--last-modified')
parser.add_argument('-r', '--incremental-column', required=True)
parser.add_argument('-R', '--last-value')
parser.add_argument('-d', '--deleted-column')
parser.add_argument('-S', '--scratch-db', default='spark_scratch')
args = parser.parse_args()

validate_common_args(args)

conn = conn_from_args(spark, args)
db, tbl = (args.hive_table or args.dbtable).split('.')

# load data from source
df = conn.load()

incremental_merge_ingestion(spark, df, db, tbl, 
        args.key_columns.split(','), 
        args.last_modified_column, 
        args.last_modified, 
        args.incremental_column,
        args.last_value,
        args.deleted_column,
        args.scratch_db,
        args.storageformat)

