from __future__ import print_function
import math

from pyspark.ml.regression import DecisionTreeRegressor
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.evaluation import RegressionEvaluator

from pyspark.sql import SparkSession
from pyspark.ml.linalg import Vectors

if __name__ == "__main__":

    # Create a SparkSession (Note, the config section is only for Windows!)
    spark = SparkSession.builder.appName("DecisionTreeRegression").getOrCreate()

    # Load up the realesate.csv data into a dataframe
    data = spark.read.option("header", "true").option("inferSchema", "true")\
    .csv("realestate.csv")

    # Predict based on house age, distance to mrt and number of convenience stores
    assembler = VectorAssembler().setInputCols(["HouseAge", "DistanceToMRT", "NumberConvenienceStores"]).setOutputCol("features")
    
    df = assembler.transform(data).select("PriceOfUnitArea", "features")

    # Let's split our data into training data and testing data
    trainTest = df.randomSplit([0.5, 0.5])
    trainingDF = trainTest[0]
    testDF = trainTest[1]

    # Now create our decision tree regression model
    dtr = DecisionTreeRegressor().setFeaturesCol("features").setLabelCol("PriceOfUnitArea")

    # Train the model using our training data
    model = dtr.fit(trainingDF)

    # Now see if we can predict values in our test data.
    # Generate predictions using our decision tree regression model for all features in our
    # test dataframe:
    fullPredictions = model.transform(testDF).cache()

    # Extract the predictions and the "known" correct labels.
    predictions = fullPredictions.select("prediction").rdd.map(lambda x: x[0])
    labels = fullPredictions.select("PriceOfUnitArea").rdd.map(lambda x: x[0])

    # Zip them together
    predictionAndLabel = predictions.zip(labels).collect()

    # Print out the predicted and actual values for each point
    for prediction in predictionAndLabel:
      print(prediction)


    # Calculate RMSE manually
    if len(predictionAndLabel) > 0:
        # Calculate squared errors
        squared_errors = []
        for pred, actual in predictionAndLabel:
            error = pred - actual
            squared_error = error ** 2
            squared_errors.append(squared_error)
        
        # Calculate Mean Squared Error (MSE)
        mse = sum(squared_errors) / len(squared_errors)
        
        # Calculate Root Mean Squared Error (RMSE)
        rmse = math.sqrt(mse)
        
        print("\nManual RMSE Calculation:")
        print(f"Number of predictions: {len(predictionAndLabel)}")
        print(f"Mean Squared Error (MSE): {mse:.4f}")
        print(f"Root Mean Squared Error (RMSE): {rmse:.4f}")

    # Use PySpark's RegressionEvaluator
    evaluator = RegressionEvaluator(
        labelCol="PriceOfUnitArea", 
        predictionCol="prediction", 
        metricName="rmse"
    )

    if testDF.count() > 0:
        mse = evaluator.evaluate(fullPredictions)
        print("Root Mean Squared Error (using Pyspark's RegressionEvaluator) = " + str(mse))
    else:
        print("No test data available for evaluation")

    # Stop the session
    spark.stop()
