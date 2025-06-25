"""
Spark utilities - connecting to your cluster WITH Delta Lake support
"""
import os
from pyspark.sql import SparkSession

def create_spark_session(app_name="JupyterNotebook"):
    """Create Spark session with your S3A configuration AND Delta Lake support"""
    minio_endpoint = os.getenv("MINIO_ENDPOINT", "minio:9000")
    minio_access_key = os.getenv("MINIO_ACCESS_KEY")
    minio_secret_key = os.getenv("MINIO_SECRET_KEY")
    
    if not minio_access_key or not minio_secret_key:
        raise ValueError("MINIO_ACCESS_KEY and MINIO_SECRET_KEY must be set!")
    
    print(f"🚀 Creating Spark session: {app_name}")
    print("📡 Connecting to: spark://spark-master:7077")
    print(f"🗄️  MinIO endpoint: http://{minio_endpoint}")
    print("🔺 Delta Lake: ENABLED")
    
    spark = SparkSession.builder \
        .appName(app_name) \
        .master("spark://spark-master:7077") \
        .config("spark.driver.host", "jupyter-spark") \
        .config("spark.dynamicAllocation.enabled", "false") \
        .config("spark.cores.max", "1") \
        .config("spark.executor.cores", "1") \
        .config("spark.executor.memory", "3g") \
        .config("spark.driver.bindAddress", "0.0.0.0") \
        .config("spark.driver.port", "4040") \
        .config("spark.hadoop.fs.s3a.endpoint", f"http://{minio_endpoint}") \
        .config("spark.hadoop.fs.s3a.access.key", minio_access_key) \
        .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .config("spark.hadoop.fs.s3a.threads.keepalivetime", "60") \
        .config("spark.hadoop.fs.s3a.connection.establish.timeout", "30000") \
        .config("spark.hadoop.fs.s3a.connection.timeout", "200000") \
        .config("spark.hadoop.fs.s3a.connection.ttl", "300000") \
        .config("spark.hadoop.fs.s3a.retry.interval", "500") \
        .config("spark.hadoop.fs.s3a.retry.throttle.interval", "100") \
        .config("spark.hadoop.fs.s3a.assumed.role.session.duration", "1800000") \
        .config("spark.hadoop.fs.s3a.multipart.purge.age", "86400000") \
        .config("spark.hadoop.fs.s3a.connection.maximum", "100") \
        .config("spark.hadoop.fs.s3a.fast.upload", "true") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
    
    print(f"✅ Spark {spark.version} session created successfully!")
    print("🔗 Spark UI: http://localhost:4041")
    print("💡 S3A ready for s3a://delta-lake/ operations")
    print("🔺 Delta Lake ready for delta table operations!")
    
    return spark

def quick_start(app_name="JupyterNotebook"):
    return create_spark_session(app_name)

def test_delta_lake(spark):
    """Test Delta Lake functionality"""
    print("🧪 Testing Delta Lake...")
    
    # Test basic S3 connectivity first
    print("1. Testing S3 connectivity...")
    spark.sql("CREATE DATABASE IF NOT EXISTS test_db LOCATION 's3a://delta-lake/test_db/'")
    print("✅ S3 database creation successful")
    
    # Test regular Parquet (should work)
    print("2. Testing Parquet write/read...")
    data = [(1, "Alice", 25), (2, "Bob", 30)]
    df = spark.createDataFrame(data, ["id", "name", "age"])
    df.write.mode("overwrite").parquet("s3a://delta-lake/test_parquet")
    test_df = spark.read.parquet("s3a://delta-lake/test_parquet")
    print("✅ Parquet operations successful")
    test_df.show()
    
    # Test Delta Lake
    print("3. Testing Delta Lake write/read...")
    df.write.format("delta").mode("overwrite").save("s3a://delta-lake/test_delta")
    delta_df = spark.read.format("delta").load("s3a://delta-lake/test_delta")
    print("✅ Delta Lake operations successful")
    delta_df.show()
    
    # Test Delta table creation
    print("4. Testing Delta table creation...")
    df.write.format("delta").mode("overwrite").saveAsTable("test_db.delta_table")
    spark.sql("SELECT * FROM test_db.delta_table").show()
    print("✅ Delta table creation successful")
    
    print("🎉 All tests passed! Delta Lake is working!")
    return True

def load_github_bronze_keyword_extractions_data(spark=None):
    """Quick loader for GitHub keyword data"""
    if spark is None:
        spark = quick_start("GitHubAnalysis")
    
    df = spark.read.format("delta").load("s3a://delta-lake/bronze/github/keyword_extractions")
    df.cache()  # Cache for performance
    
    print(f"✅ Loaded {df.count()} GitHub keyword records")
    return spark, df