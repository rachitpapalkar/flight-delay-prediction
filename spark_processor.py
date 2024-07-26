from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import DecisionTreeClassificationModel
import tempfile

spark = SparkSession.builder \
    .appName("FlightDelayPredictionLive") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0") \
    .getOrCreate()

kafka_bootstrap_servers = "localhost:9092"
kafka_topics = "KATL,KLAX,KDFW"

schema = StructType([
    StructField("longitude", DoubleType()),
    StructField("latitude", DoubleType()),
    StructField("baro_altitude", DoubleType()),
    StructField("velocity", DoubleType()),
    StructField("true_track", DoubleType()),
    StructField("vertical_rate", DoubleType()),
    StructField("geo_altitude", DoubleType()),
    StructField("spi", IntegerType()),
    StructField("position_source", IntegerType()),
    StructField("category", IntegerType()),
    StructField("icao24", StringType())
])

raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topics) \
    .load()

parsed_df = raw_df.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), schema).alias("data")) \
    .select("data.*")

feature_columns = [col for col in schema.fieldNames() if col not in ['icao24', 'sensors']]
assembler = VectorAssembler(inputCols=feature_columns, outputCol="features", handleInvalid="keep")

model_path = "decision_tree_model"
dt_model = DecisionTreeClassificationModel.load(model_path)

predictions_df = assembler.transform(parsed_df)
predictions = dt_model.transform(predictions_df).select("icao24", "prediction")

def translate_prediction(prediction):
    return "No Delay" if prediction == 0.0 else "Delayed"

translate_prediction_udf = spark.udf.register("translate_prediction", translate_prediction)

results_df = predictions.withColumn("prediction_result", translate_prediction_udf(col("prediction")))

query = results_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("checkpointLocation", tempfile.mkdtemp()) \
    .start()

query.awaitTermination()

