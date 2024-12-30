from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Data Overview") \
    .config("spark.jars", "/opt/spark/jars/hadoop-aws-3.3.1.jar,/opt/spark/jars/aws-java-sdk-bundle-1.12.376.jar") \
    .getOrCreate()


input_path = "s3a://bjahagir-s3-raw-data/train.csv"

print(f"Reading data from {input_path}...")
df = spark.read.csv(input_path, header=True, inferSchema=True)
print("Data successfully loaded into Spark DataFrame:")
df.show(5)

print(f"Number of Rows: {df.count()}, Number of Columns: {len(df.columns)}")

print("Schema of the DataFrame:")
df.printSchema()

output_path = "s3a://bjahagir-processed-data/data_overview.parquet"
print(f"Saving overview data to {output_path}...")
df.write.parquet(output_path, mode="overwrite")
print("Data overview saved successfully!")
