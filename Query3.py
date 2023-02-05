from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import time

spark = SparkSession.builder.master("spark://192.168.0.2:7077").appName("Query-3").getOrCreate()
hdfs_path = "hdfs://192.168.0.2:9000/"
data = spark.read.option("header", "true").option("inferSchema", "true").parquet(hdfs_path + "yellow_tripdata_2022-01_06.parquet")
data = data.filter((month(col("tpep_pickup_datetime")) >= 1) & (month(col("tpep_pickup_datetime")) <= 6))

data = data.withColumn("fortnight", 2*(month("tpep_pickup_datetime")-1) + floor(dayofmonth("tpep_pickup_datetime")/16))
data.createOrReplaceTempView("data")

start = time.time()
Q3 = spark.sql("""select fortnight, avg(Trip_distance), avg(Total_amount)
from data where PULocationID != DOLocationID  group by fortnight
""")
Q3.show()
stop = time.time()
print("Q3:total time is ", stop-start)

data_rdd = data.rdd
start1 = time.time()
Q3_rdd = data_rdd.filter(lambda a: a.PULocationID != a.DOLocationID)\
          .map(lambda a: (str(a.fortnight), (float(a.trip_distance),1, float(a.total_amount))))\
          .reduceByKey(lambda a,b: (a[0]+b[0], a[1]+b[1], a[2]+b[2]))\
          .mapValues(lambda a: ((a[0]/a[1]), (a[2]/a[1])))

for i in Q3_rdd.collect():
        print(i)
stop1 = time.time()
print("Q3 RDD:total time is ", stop1-start1)
spark.stop()

