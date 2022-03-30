
spark2-submit --jars /usr/share/java/mysql-connector-java.jar jdbc_loader_spark2.py \
    -u jdbc:mysql://10.210.14.99/ingest_test -D com.mysql.jdbc.Driver \
    --user root --password myr00t -t ingest_test.data_append -p id --num-partitions 2
