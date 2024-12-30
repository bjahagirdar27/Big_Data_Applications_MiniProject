from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("Feature Engineering").getOrCreate()

input_path = "s3a://bjahagir-processed-data/cleaned_data.parquet"
print(f"Loading data from {input_path}...")
df = spark.read.parquet(input_path)

print("Generating new features...")
df = df.withColumn("processor_performance_index", col("clock_speed") * col("n_cores"))
df = df.withColumn("screen_resolution", col("px_height") * col("px_width"))
df = df.withColumn(
    "feature_support_count",
    col("blue") + col("dual_sim") + col("four_g") + col("three_g") + col("touch_screen") + col("wifi")
)
print("New features added:")
df.show(5)

output_path = "s3a://bjahagir-processed-data/engineered_features.parquet"
print(f"Saving data with engineered features to {output_path}...")
df.write.parquet(output_path, mode="overwrite")
print("Engineered data saved successfully!")
