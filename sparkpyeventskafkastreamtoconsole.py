from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, unbase64, base64, split
from pyspark.sql.types import StructField, StructType, StringType, BooleanType, ArrayType, DateType, IntegerType, FloatType

# TO-DO: using the spark application object, read a streaming dataframe from the Kafka topic stedi-events as the source

stedi_events_schema = StructType(
    [
        StructField("customer",StringType()),
        StructField("score",FloatType()),
        StructField("riskDate",StringType())
    ]
)

spark = SparkSession.builder.appName("stedi_app").getOrCreate()

#TO-DO: set the spark log level to WARN

spark.sparkContext.setLogLevel("WARN")

read_stedievents = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers","localhost:9092") \
    .option("subscribe","stedi-events") \
    .option("startingOffsets","earliest") \
    .load()


# Be sure to specify the option that reads all the events from the topic including those that were published before you started the spark stream
                                   
# TO-DO: cast the value column in the streaming dataframe as a STRING 

cast_stedievents = read_stedievents.selectExpr("CAST (key AS string) key","CAST (value AS string) value")

# TO-DO: parse the JSON from the single column "value" with a json object in it, like this:
# +------------+
# | value      |
# +------------+
# |{"custom"...|
# +------------+
#
# and create separated fields like this:
# +------------+-----+-----------+
# |    customer|score| riskDate  |
# +------------+-----+-----------+
# |"sam@tes"...| -1.4| 2020-09...|
# +------------+-----+-----------+
#
#cast_stedievents.writeStream.outputMode("append").format("console").start().awaitTermination()

cast_stedievents.withColumn("value",from_json("value",stedi_events_schema)) \
    .select(col("value.*")) \
    .createOrReplaceTempView("CustomerRisk")

# storing them in a temporary view called CustomerRisk
# TO-DO: execute a sql statement against a temporary view, selecting the customer and the score from the temporary view, creating a dataframe called customerRiskStreamingDF

customerRiskStreamingDF = spark.sql("SELECT customer, score FROM CustomerRisk")

# TO-DO: sink the customerRiskStreamingDF dataframe to the console in append mode
# 

customerRiskStreamingDF.writeStream.outputMode("append").format("console").start().awaitTermination()

# It should output like this:
#
# +--------------------+-----
# |customer           |score|
# +--------------------+-----+
# |Spencer.Davis@tes...| 8.0|
# +--------------------+-----
# Run the python script by running the command from the terminal:
# /home/workspace/submit-event-kafka-streaming.sh
# Verify the data looks correct 