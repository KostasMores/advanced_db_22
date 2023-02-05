from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import time

spark = SparkSession.builder.master("spark://192.168.0.2:7077").appName("Query-5").getOrCreate()
hdfs_path = "hdfs://192.168.0.2:9000/"
data = spark.read.option("header", "true").option("inferSchema", "true").parquet(hdfs_path + "yellow_tripdata_2022-01_06.parquet")
data = data.filter((month(col("tpep_pickup_datetime")) >= 1) & (month(col("tpep_pickup_datetime")) <= 6))
data.createOrReplaceTempView("data")

start = time.time()
Q5 = spark.sql("""select * from (select *, row_number() over (partition by monthly order by avg_tip desc) as row
from
  (select avg(Fare_amount/Tip_amount) as avg_tip, month(tpep_pickup_datetime) as monthly, dayofweek(tpep_pickup_datetime) as daily
   from data
   group by monthly, daily))
where row < 6
""")
Q5.show()
stop = time.time()
print("Q5:total time is ", stop-start)

spark.stop()

