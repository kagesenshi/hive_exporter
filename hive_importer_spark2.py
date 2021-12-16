"""
    Hive importer for spark2

    To execute: 

    spark-submit hive_importer_spark2.py
"""
from pyspark.shell import spark
from argparse import ArgumentParser
import glob
import os
from argparse import ArgumentParser
import logging

logging.basicConfig(level=logging.INFO)
log = logging.getLogger('hive-importer-spark2')

parser = ArgumentParser()
parser.add_argument('-i', '--importdir', required=True)
parser.add_argument('-I', '--inputformat', default='parquet')
parser.add_argument('-f', '--storageformat', default='parquet')
parser.add_argument('-o', '--overwrite', action='store_true', default=False)
args = parser.parse_args()

for idx, d in enumerate(glob.glob('%s/*' % args.importdir)):
    p = os.path.abspath(d)
    df = spark.read.format(args.inputformat).load('file://%s' % p)
    df.createOrReplaceTempView('import_%s' % idx)
    tbl = os.path.basename(d)
    db = tbl.split('.')[0]
    log.info('Importing %s' % tbl)
    spark.sql('create database if not exists %s' % db)
    if args.overwrite:
        spark.sql('drop table if exists %s' % tbl)
    spark.sql('create table %s stored as %s as select * from import_%s' % (tbl, args.storageformat, idx))
    log.info('.. DONE')
