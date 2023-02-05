from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import time

spark = SparkSession.builder.master("spark://192.168.0.2:7077").appName("Query-1").getOrCreate()
hdfs_path = "hdfs://192.168.0.2:9000/"
data = spark.read.option("header", "true").option("inferSchema", "true").parquet(hdfs_path + "yellow_tripdata_2022-01_06.parquet")
data = data.filter((month(col("tpep_pickup_datetime")) >= 1) & (month(col("tpep_pickup_datetime")) <= 6))
data.createOrReplaceTempView("data")

zone = spark.read.option("header", "true").option("inferSchema", "true").csv(hdfs_path + "taxi+_zone_lookup.csv")
zone.createOrReplaceTempView("zone")

start = time.time()
Q2 = spark.sql("""select *
from
  (select LocationID as id
   from zone
   where zone = "Battery Park") inner join data on id = data.DOLocationID
where month(tpep_pickup_datetime) = 3
order by Tip_amount desc
limit 1
""")
Q2.show()
stop = time.time()
print("Q1:total time is ", stop-start)
spark.stop()

