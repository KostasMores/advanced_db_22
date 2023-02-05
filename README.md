# advanced_db_22

## Apache Spark v3.1.3 installation
1. Install python3.8
2. Install pip
3. Install PySpark
4. Install Apache Spark
5. Install Java
6. Setup a Cluster (1 master and 1 worker)

### Deploy Workers
spark-daemon.sh start org.apache.spark.deploy.worker.Worker 1 --webui-port 8080
--port 65509 --cores 4 --memory 8g spark://192.168.0.2:7077

## Hadoop installation
https://sparkbyexamples.com/hadoop/apache-hadoop-installation/

## Download Data / Upload to Hadoop
1. Download data from https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
2. Concat all parquet files to a single parquet file
3. Upload to hadoop with $hdfs dfs -put

## Create Dataframes and RDD
df_taxitrips = spark.read.option("header", "true").option("inferSchema", "true").parquet( "hdfs://192.168.0.1:9000/yellow_tripdata_2022-01_06.parquet")

df_zonelookups = spark.read.option("header", "true").option("inferSchema", "true").parquet( "hdfs://192.168.0.1:9000/taxi+_zone_lookup.csv")

rdd_taxitrips = df_taxitrips.rdd
rdd_zonelookups = df_zonelookups.rdd
