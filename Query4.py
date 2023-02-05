from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import time

spark = SparkSession.builder.master("spark://192.168.0.2:7077").appName("Query-4").getOrCreate()
hdfs_path = "hdfs://192.168.0.2:9000/"
data = spark.read.option("header", "true").option("inferSchema", "true").parquet(hdfs_path + "yellow_tripdata_2022-01_06.parquet")
data = data.filter((month(col("tpep_pickup_datetime")) >= 1) & (month(col("tpep_pickup_datetime")) <= 6))
data.createOrReplaceTempView("data")

start = time.time()
Q4 = spark.sql("""select * from (select *, row_number() over (partition by daily order by avg_passengers desc) as row
from
  (select sum(Passenger_count)/count(distinct tpep_pickup_datetime) as avg_passengers, hour(tpep_pickup_datetime) as hourly, dayofweek(tpep_pickup_datetime) as daily
   from data
   group by hourly, daily))
where row < 4
""")
Q4.show()
stop = time.time()
print("Q4:total time is ", stop-start)
spark.stop()
