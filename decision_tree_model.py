from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml import Pipeline
from pyspark.sql.functions import col

# Initialize Spark session
spark = SparkSession.builder.appName("FlightDelayPrediction").getOrCreate()

# Load the dataset
file_path = "trained_dataset.csv"
df = spark.read.csv(file_path, header=True, inferSchema=True)

# Drop the specified columns
columns_to_drop = ["icao24", "callsign", "time_position", "last_contact", "on_ground", "sensors", "squawk", "origin_country"]
df = df.drop(*columns_to_drop)

# Handle missing values (if any)
df = df.na.drop()

# Assemble feature columns, including the 'category' column as a feature
feature_columns = [col for col in df.columns if col != 'delayed']
assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")

# Transform the DataFrame using VectorAssembler
df = assembler.transform(df)


# Split the data into training and test sets
train_data, test_data = df.randomSplit([0.7, 0.3], seed=1234)

# Initialize and train the Decision Tree model
dt_classifier = DecisionTreeClassifier(featuresCol="features", labelCol="delayed")


# Train the model
dt_model = dt_classifier.fit(train_data)

# Make predictions on the test set
predictions = dt_model.transform(test_data)

# Evaluate the model
evaluator = MulticlassClassificationEvaluator(labelCol="delayed", predictionCol="prediction", metricName="accuracy")
accuracy = evaluator.evaluate(predictions)

print(f"Test Accuracy: {accuracy:.2f}")

feature_importance = dt_model.featureImportances.toArray()

# Show feature importance
for i, column in enumerate(assembler.getInputCols()):
    print(f"Feature '{column}': {feature_importance[i]:.2f}")
    
    
# Save the entire pipeline model
model_path = "decision_tree_model"
dt_model.write().overwrite().save(model_path)

# Stop Spark session
spark.stop()
