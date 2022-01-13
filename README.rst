---------------------------------------
Export/Import Hive datasets using Spark
---------------------------------------

* hive_exporter_spark1.py - This script exports Hive dataset to local
  client node using Spark1

* hive_exporter_spark2.py - This script exports Hive dataset to local
  client node using Spark2

* hive_importer_spark2.py - This script imports exported dataset to Hive using
  Spark2

Exporting datasets
-------------------

The exporter expects a txt file with a list of tables to be exported. Eg: if
you want to export table ``mydb.mytable`` and ``mydb.mytable2``, create a text file
(eg: ``exportlist.txt``) with following contents::

  mydb.mytable
  mydb.mytable2


Then you can export the list of tables using the exporter::

  spark-submit hive_exporter_spark2.py -l exportlist.txt

This will create a directory named ``export``, containing exported datasets
stored in parquet format.

Options:

* ``-l`` - (REQUIRED) to specify file containing list of tables to export

* ``-o`` - to specify output directory for export (default: ``$PWD/export``)

* ``-O`` - to specify output format of export (default: ``parquet``)


Importing datasets
-------------------

To import, you simply have to pass the ``export`` directory as parameter to the
importer::

  spark-submit hive_importer_spark2.py -i export

Options:

* ``-i`` - (REQUIRED) to specify the directory containing datasets to import

* ``-I`` - to specify input format of the datasets (default: ``parquet``)

* ``-f`` - to specify Hive storage format for the tables (default: ``parquet``)

* ``-o`` - set this to drop hive tables before importing 

--------------------------
Ingesting data from RDBMS
--------------------------

Full refresh ingestion
-----------------------

``jdbc_loader_spark2.py`` provides logic for full ingestion of data from RDBMS into Hive using
`Spark JDBC <https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html>`_ connector.

Example for simple loading from MySQL::

   spark-submit --jars /usr/share/java/mysql-connector-java.jar jdbc_loader_spark2.py \
       -u jdbc:mysql://user:password@my.server:3306/db -t db.table -D com.mysql.jdbc.Driver

Example for simple loading from query result from MySQL::

   spark-submit --jars /usr/share/java/mysql-connector-java.jar jdbc_loader_spark2.py \
       -u jdbc:mysql://user:password@my.server:3306/db -D com.mysql.jdbc.Driver \
       -q 'select created_date,text from mytable'

Example for partitioned loading (similar to Sqoop) from MySQL::

   spark-submit --jars /usr/share/java/mysql-connector-java.jar jdbc_loader_spark2.py \
       -u jdbc:mysql://user:password@my.server:3306/db -t db.table -D com.mysql.jdbc.Driver \
       -m 10 --partition-column created_date


Incremental append ingestion
-----------------------------

``jdbc_loader_incremental_append_spark2.py`` provides logic for incremental append ingestion of data from 
RDBMS into Hive using `Spark JDBC <https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html>`_ connector.
Incremental append ingestion is suitable for log-like data where new rows are added, without any modification in
older rows.

Incremental append requires one (1) column that shall be used to identify new values, which latest records will be
selected from. This column  can be a created date column, or an incremental running number id column. Ingested data
are stored in partitions based on ingested date time.

Example for incremental loading from MySQL, where ``created_date`` is the incremental column::

   spark-submit --jars /usr/share/java/mysql-connector-java.jar jdbc_loader_incremental_append_spark2.py \
       -u jdbc:mysql://user:password@my.server:3306/db -t db.table -D com.mysql.jdbc.Driver -l created_date


Incremental merge ingestion
----------------------------

``jdbc_loader_incremental_merge_spark2.py`` provides logic for incremental merge ingestion of data from 
RDBMS into Hive using `Spark JDBC <https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html>`_ connector.
Incremental merge ingestion is suitable for updating tables incrementally, where older rows may get modified. Take 
note that incremental merge ingestion **DOES NOT** delete records, and if there is a requirement to delete records, you
will need to make sure that application implements soft delete, and provide a deletion marker column that is ``null`` when
record is not yet deleted.

Incremental merge requires a uniquely identifiable ID column or composite ID columns and a last modified timestamp or datetime
column. Without the required columns, it is impossible to do incremental merge, and only full refresh can be done. Ingested
data are stored in two tables, one table contains the actual expected data, and another table with `_incremental` suffix on its name
storing non-consolidated, incremental ingested data is stored.

Example for incremental loading from MySQL, where ``id`` is the key column and ``modified_date`` is the last modified column::

   spark-submit --jars /usr/share/java/mysql-connector-java.jar jdbc_loader_incremental_merge_spark2.py \
       -u jdbc:mysql://user:password@my.server:3306/db -t db.table -D com.mysql.jdbc.Driver -k id -l modified_date

Performance Consideration
--------------------------

For best performance of ingestion, it is required that source systems indexes following columns:

* ID column(s)

* Last modified date column

* Created date column

* Partitioned ingest column 
