---------------------------------
Export Hive datasets using Spark
---------------------------------

* hive_exporter_spark1.py - This script exports Hive dataset to local
  client node using Spark1

* hive_exporter_spark2.py - This script exports Hive dataset to local
  client node using Spark2

* hive_importer_spark2.py - This script imports exported dataset to Hive using
  Spark2

Exporting datasets
-------------------

The exporter expects a txt file with a list of tables to be exported. Eg: if
you want to export table `mydb.mytable` and `mydb.mytable2`, create a text file
(eg: `exportlist.txt`) with following contents::

  mydb.mytable
  mydb.mytable2


Then you can export the list of tables using the exporter::

  spark-submit hive_exporter_spark2.py -l exportlist.txt

This will create a directory named `export`, containing exported datasets
stored in parquet format.

Options:

* `-l` - (REQUIRED) to specify file containing list of tables to export

* `-o` - to specify output directory for export (default: `$PWD/export`)

* `-O` - to specify output format of export (default: `parquet`)


Importing datasets
-------------------

To import, you simply have to pass the `export` directory as parameter to the
importer::

  spark-submit hive_importer_spark2.py -i export

Options:

* `-i` - (REQUIRED) to specify the directory containing datasets to import

* `-I` - to specify input format of the datasets (default: `parquet`)

* `-f` - to specify Hive storage format for the tables (default: `parquet`)

* `-o` - set this to drop hive tables before importing 


