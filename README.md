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
