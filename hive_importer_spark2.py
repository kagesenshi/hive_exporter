from pyspark.shell import spark
from argparse import ArgumentParser
import glob
import os
from argparse import ArgumentParser

logging.basicConfig(level=logging.INFO)
log = logging.getLogger('hive-importer-spark2')

parser = ArgumentParser()
parser.add_argument('-i', '--importdir', required=True)
parser.add_argument('-I', '--inputformat', default='parquet')
parser.add_argument('-f', '--storageformat', default='parquet')
args = parser.parse_args()

for idx, d in enumerate(glob.glob('%s/*' % args.importdir)):
    df = spark.read.format(args.inputformat).load(d)
    df.createOrRegisterTempView('import_%s' % idx)
    tbl = os.path.basename(d)
    log.info('Importing %s' % tbl)
    spark.sql('create table %s stored as %s as select * from import_%s' % (tbl,
        d))
    log.info('.. DONE')
