from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType

#Initializing Spark Session
spark = SparkSession.builder \
    .appName("KafkaSparkStreaming") \
    .config("spark.cassandra.connection.host", "localhost") \
    .config("spark.cassandra.connection.port", "9042") \
    .getOrCreate()

#Definining Kafka parameters
kafka_bootstrap_servers = "localhost:9092"
kafka_topic = "kafka_streaming"

#Defining the schema of the incoming data
schema = StructType([
    StructField('row_number', StringType(), True),
    StructField('Match_ID', StringType(), True),
    StructField('Date', StringType(), True),
    StructField('Venue', StringType(), True),
    StructField('Bat_First', StringType(), True),
    StructField('Bat_Second', StringType(), True),
    StructField('Innings', StringType(), True),
    StructField('Over', StringType(), True),
    StructField('Ball', StringType(), True),
    StructField('Batter', StringType(), True),
    StructField('Non_Striker', StringType(), True),
    StructField('Bowler', StringType(), True),
    StructField('Batter_Runs', StringType(), True),
    StructField('Extra_Runs', StringType(), True),
    StructField('Runs_From_Ball', StringType(), True),
    StructField('Ball_Rebowled', StringType(), True),
    StructField('Extra_Type', StringType(), True),
    StructField('Wicket', StringType(), True),
    StructField('Method', StringType(), True),
    StructField('Player_Out', StringType(), True),
    StructField('Innings_Runs', StringType(), True),
    StructField('Innings_Wickets', StringType(), True),
    StructField('Target_Score', StringType(), True),
    StructField('Runs_to_Get', StringType(), True),
    StructField('Balls_Remaining', StringType(), True),
    StructField('Winner', StringType(), True),
    StructField('Chased_Successfully', StringType(), True),
    StructField('Total_Batter_Runs', StringType(), True),
    StructField('Total_Non_Striker_Runs', StringType(), True),
    StructField('Batter_Balls_Faced', StringType(), True),
    StructField('Non_Striker_Balls_Faced', StringType(), True),
    StructField('Player_Out_Runs', StringType(), True),
    StructField('Player_Out_Balls_Faced', StringType(), True),
    StructField('Bowler_Runs_Conceded', StringType(), True),
    StructField('Valid_Ball', StringType(), True)
])

#Reading from Kafka topic
kafka_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .load()

#Converting the value column from Kafka into JSON format
df = kafka_stream.selectExpr("CAST(value AS STRING)")
df = df.withColumn("value", from_json(col("value"), schema)).select("value.*")

#Writing output to Cassandra
query = df.writeStream \
    .outputMode("append") \
    .format("org.apache.spark.sql.cassandra") \
    .option("keyspace", "test_keyspace") \
    .option("table", "match_info") \
    .start()

query.awaitTermination()