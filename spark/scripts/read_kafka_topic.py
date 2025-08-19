from pyspark.sql import SparkSession

kafka_jar = '/opt/bitnami/spark/jars/commons-pool2-2.12.0.jar,/opt/bitnami/spark/jars/spark-sql-kafka-0-10_2.13-4.0.0.jar,/opt/bitnami/spark/jars/spark-token-provider-kafka-0-10_2.13-4.0.0.jar,/opt/bitnami/spark/jars/kafka-clients-3.9.0.jar'

spark = SparkSession.builder \
    .appName("ReadKafkaTopic") \
    .config('spark.jars', kafka_jar) \
    .getOrCreate()

""" df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "host1:port1,host2:port2") \
  .option("subscribe", "topic1") \
  .option("includeHeaders", "true") \
  .load()
"""

df = spark.read \
    .format("kafka") \
    .option("kafka.bootstrap.servers","broker:9093") \
    .option("subscribe","topic-1") \
    .option("startingOffsets","earliest") \
    .option("includeHeaders", "true") \
    .load()

df.printSchema()

df_data = df.selectExpr("CAST(key AS STRING)","CAST(value as STRING)")
df_data.printSchema()
df_data.show(truncate=False)
#df_data.writeStream.format("console").outputMode("append").start().awaitTermination()

#query = df_data.writeStream \
#    .format("console") \
#    .start()

#query.awaitTermination()

spark.stop()
