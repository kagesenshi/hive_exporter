"""
    Export Hive data and metadata from HDP
"""
from pyspark.shell import spark
import json
from argparse import ArgumentParser
import os
import re
import logging

logging.basicConfig(level=logging.INFO)
log = logging.getLogger('hive-exporter-hdp')

sc.setLogLevel("WARN")

parser = ArgumentParser()
parser.add_argument('-l', '--exportlist', help='List of tables to export', required=True)
parser.add_argument('-o', '--outputdir', default='export', 
        help='Output directory')
parser.add_argument('-O', '--outputformat', default='parquet', 
        help='Output format')
args = parser.parse_args()

here = os.path.dirname(__file__)

all_tables = []

with open(args.exportlist) as tables:
    for tbl in tables:
        tbl = tbl.strip()
        if not tbl:
            continue
        if not re.match('^.*\..*$', tbl):
            raise AssertionError(
                'Invalid table name "%s", expected db_name.table_name pattern' %
                tbl)
        all_tables.append(tbl)

if args.outputdir.startswith('/'):
    outputdir = 'file://%s' % args.outputdir
else:
    outputdir = 'file://%s/%s' % (here, args.outputdir)

for tbl in all_tables:
    df = spark.sql('select * from %s' % tbl)
    log.info('Exporting %s' % tbl)
    df.write.format(args.outputformat).mode('overwrite').save('%s/%s' % (outputdir, tbl))
    log.info('.. DONE')
