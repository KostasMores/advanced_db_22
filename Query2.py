from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import time

spark = SparkSession.builder.master("spark://192.168.0.2:7077").appName("Query-2").getOrCreate()
hdfs_path = "hdfs://192.168.0.2:9000/"
data = spark.read.option("header", "true").option("inferSchema", "true").parquet(hdfs_path + "yellow_tripdata_2022-01_06.parquet")
data = data.filter((month(col("tpep_pickup_datetime")) >= 1) & (month(col("tpep_pickup_datetime")) <= 6))
data.createOrReplaceTempView("data")

start = time.time()
Q2 = spark.sql("""select *
from
  (select *, row_number() over (partition by month(tpep_pickup_datetime) order by Tolls_amount desc) as row
   from data
   where data.Tolls_amount > 0)
where row = 1
""")
Q2.show()
stop = time.time()
print("Q2:total time is ", stop-start)
spark.stop()

