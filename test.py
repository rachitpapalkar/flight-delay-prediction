from pyspark.sql import SparkSession
from pyspark.ml.classification import DecisionTreeClassificationModel
from pyspark.ml.linalg import Vectors

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("DecisionTreeModelTest") \
    .getOrCreate()

# Define the path to your saved model
model_path = "decision_tree_model"  # Update with your actual model path

try:
    # Load the trained Decision Tree model
    dt_model = DecisionTreeClassificationModel.load(model_path)

    # Define the feature values based on your feature importance results
    feature_values = [0.00, 0.0, 0.10, 0.00, 0.0, 0.0, 99999, 0.0, 0.0, 0.0]

    # Create a DenseVector manually
    test_vector = Vectors.dense(feature_values)

    # Make a prediction on the test vector
    prediction = dt_model.predict(test_vector)

    # Print the prediction
    print(f"Prediction: {prediction}")

except Exception as e:
    print(f"Error loading or using the model: {e}")

finally:
    # Stop SparkSession
    spark.stop()

