from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.ml.feature import (
    StringIndexer,
    VectorAssembler,
    OneHotEncoder,
)
from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier, LogisticRegression
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
import sys

# =============================================================
# 1. CREATE SPARK SESSION
# =============================================================
spark = SparkSession.builder \
    .appName("Traffic-Model-Training") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")


# =============================================================
# 2. LOAD CLEANED PARQUET FROM HDFS
# =============================================================
INPUT_PATH = "hdfs:///traffic/cleaned/parquet/"

print("📥 Loading cleaned data from:", INPUT_PATH)
df = spark.read.parquet(INPUT_PATH)
print("Total rows:", df.count())
df.printSchema()


# =============================================================
# 3. DEFINE FEATURES AND LABEL
# =============================================================
label_col = "severity_label"

categorical_cols = [
    "primary_contributing_circumstance_description",
    "manner_of_impact_description",
    "road_surface_description",
    "lighting_condition_description",
    "weather_1_description",
    "weather_2_description",
    "work_zone_type_description",
    "work_zone_location_description"
]

numeric_cols = [
    "year", "month", "day", "hour",
    "latitude", "longitude",
    "is_weekend", "season", "risk_score",
    "collision_on_private_property_bin",
    "pedestrian_involved_bin",
    "alcohol_involved_bin",
    "drug_involved_bin",
    "seatbelt_used_bin",
    "motorcycle_involved_bin",
    "motorcycle_helmet_used_bin",
    "bicycled_involved_bin",
    "bicycle_helmet_used_bin",
    "school_bus_involved_code_bin",
    "work_zone_bin",
    "workers_present_bin",
]


# =============================================================
# 4. ENCODERS FOR CATEGORICAL FEATURES
# =============================================================
indexers = [
    StringIndexer(inputCol=c, outputCol=f"{c}_idx", handleInvalid="keep")
    for c in categorical_cols
]

encoders = [
    OneHotEncoder(inputCol=f"{c}_idx", outputCol=f"{c}_oh")
    for c in categorical_cols
]


# =============================================================
# 5. VECTOR ASSEMBLER
# =============================================================
assembler_inputs = [f"{c}_oh" for c in categorical_cols] + numeric_cols

assembler = VectorAssembler(
    inputCols=assembler_inputs,
    outputCol="features"
)


# =============================================================
# 6. MODEL DEFINITIONS
# =============================================================

rf = RandomForestClassifier(
    labelCol=label_col,
    featuresCol="features",
    numTrees=80,
    maxDepth=12
)

lr = LogisticRegression(
    labelCol=label_col,
    featuresCol="features",
    maxIter=50
)


# =============================================================
# 7. PIPELINE (preprocessing + model)
# =============================================================
rf_pipeline = Pipeline(stages=indexers + encoders + [assembler, rf])
lr_pipeline = Pipeline(stages=indexers + encoders + [assembler, lr])


# =============================================================
# 8. TRAIN-TEST SPLIT
# =============================================================
train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)

print("Train rows:", train_df.count())
print("Test rows:", test_df.count())


# =============================================================
# 9. TRAIN BOTH MODELS
# =============================================================
print("\n🚀 Training Random Forest...")
rf_model = rf_pipeline.fit(train_df)

print("\n🚀 Training Logistic Regression...")
lr_model = lr_pipeline.fit(train_df)


# =============================================================
# 10. EVALUATE BOTH MODELS
# =============================================================
evaluator = MulticlassClassificationEvaluator(
    labelCol=label_col,
    predictionCol="prediction",
    metricName="f1"
)

def evaluate(model, test_df, model_name):
    predictions = model.transform(test_df)
    f1 = evaluator.evaluate(predictions)

    acc = MulticlassClassificationEvaluator(
        labelCol=label_col, predictionCol="prediction", metricName="accuracy"
    ).evaluate(predictions)

    print(f"\n📊 **{model_name} PERFORMANCE**")
    print(f"F1 Score: {f1:.4f}")
    print(f"Accuracy: {acc:.4f}")

    return f1, acc


rf_f1, rf_acc = evaluate(rf_model, test_df, "RandomForest")
lr_f1, lr_acc = evaluate(lr_model, test_df, "LogisticRegression")


# =============================================================
# 11. SAVE MODELS TO HDFS
# =============================================================

RF_MODEL_PATH = "hdfs:///traffic/model/random_forest"
LR_MODEL_PATH = "hdfs:///traffic/model/logistic_regression"

print("\n💾 Saving models to HDFS...")

rf_model.write().overwrite().save(RF_MODEL_PATH)
lr_model.write().overwrite().save(LR_MODEL_PATH)

print("✔ RandomForest saved at", RF_MODEL_PATH)
print("✔ LogisticRegression saved at", LR_MODEL_PATH)


# =============================================================
# 12. SAVE METRICS FOR FRONT-END
# =============================================================
METRIC_PATH = "hdfs:///traffic/model/metrics.txt"

content = f"""
RandomForest:
  F1 Score: {rf_f1}
  Accuracy: {rf_acc}

LogisticRegression:
  F1 Score: {lr_f1}
  Accuracy: {lr_acc}
"""

with open("/tmp/metrics.txt", "w") as f:
    f.write(content)

# upload to HDFS
spark.sparkContext._jvm.org.apache.hadoop.fs.FileSystem \
    .get(spark._jsc.hadoopConfiguration()) \
    .copyFromLocalFile(False, True,
                       spark._jsc.hadoopConfiguration().get("fs.defaultFS") + "/tmp/metrics.txt",
                       METRIC_PATH)

print("📈 Metrics written to", METRIC_PATH)

print("🎉 Training completed successfully!")

spark.stop()
