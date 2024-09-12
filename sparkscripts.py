#tidak dipakai
#sparkscripts ini untuk membaca dari Kafka, memproses data, dan menyimpannya ke PostgreSQL. 

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import TimestampType

spark = SparkSession.builder \
    .appName("ECommerceAnalysis") \
    .getOrCreate()

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "transactions") \
    .load()

df = df.selectExpr("CAST(value AS STRING)")

df = df.select(
    col("value.transaction_id").cast("int"),
    col("value.product"),
    col("value.quantity").cast("int"),
    col("value.payment_method"),
    col("value.timestamp").cast(TimestampType())
)

df.writeStream \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/ecommerce") \
    .option("dbtable", "transactions") \
    .option("user", "user") \
    .option("password", "password") \
    .start() \
    .awaitTermination()
