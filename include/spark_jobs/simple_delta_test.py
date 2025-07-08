#!/usr/bin/env python3
# /include/spark_jobs/simple_delta_test.py

"""
Simple Delta Lake Creation Script
Writes directly to S3/MinIO with Delta format
"""

import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType

def create_simple_spark_session():
    """Create minimal Spark session for Delta Lake + S3"""

    minio_access_key = os.getenv('MINIO_ACCESS_KEY', 'minioadmin')
    minio_secret_key = os.getenv('MINIO_SECRET_KEY', 'minioadmin123')
    minio_endpoint = os.getenv('MINIO_ENDPOINT', 'minio:9000')

    print("🚀 Creating Spark session with Delta support...")

    spark = SparkSession.builder \
        .appName("SimpleDeltaLakeTest") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
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
        .getOrCreate()

    print(f"✅ Spark {spark.version} session ready")
    return spark

def create_sample_data(spark):
    print("📊 Creating sample data...")

    schema = StructType([
        StructField("id", LongType(), True),
        StructField("keyword", StringType(), True),
        StructField("mentions", LongType(), True),
        StructField("date", StringType(), True),
        StructField("processing_time", TimestampType(), True)
    ])

    data = [
        (1, "python", 100, "2024-12-31", None),
        (2, "javascript", 75, "2024-12-31", None),
        (3, "react", 50, "2024-12-31", None),
        (4, "spark", 25, "2024-12-31", None),
        (5, "delta", 15, "2024-12-31", None)
    ]

    df = spark.createDataFrame(data, schema)
    df = df.withColumn("processing_time", current_timestamp())

    print(f"✅ Created {df.count()} records")
    df.show()
    return df

def write_to_delta_lake(spark, df):
    path = "s3a://delta-lake/bronze/test/simple_delta_table"
    print(f"💾 Writing Delta table to {path}...")

    try:
        df.write \
            .format("delta") \
            .mode("overwrite") \
            .option("compression", "snappy") \
            .save(path)

        print("✅ Delta table written successfully")
        return True
    except Exception as e:
        print(f"❌ Failed to write Delta table: {e}")
        import traceback
        traceback.print_exc()
        return False

def verify_delta_table(spark):
    path = "s3a://delta-lake/bronze/test/simple_delta_table"
    print(f"🔍 Verifying Delta table at {path}...")

    try:
        df = spark.read.format("delta").load(path)
        count = df.count()
        print(f"✅ Verified {count} records")
        df.show()
        return True
    except Exception as e:
        print(f"❌ Failed to read Delta table: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    print("🎯 RUNNING SIMPLE DELTA LAKE TEST")
    print("=" * 50)

    spark = None

    try:
        spark = create_simple_spark_session()
        df = create_sample_data(spark)

        if write_to_delta_lake(spark, df):
            if verify_delta_table(spark):
                print("\n🎉 Delta Lake test SUCCESSFUL")
            else:
                print("\n⚠️ Written but verification failed")
        else:
            print("\n❌ Write failed")
            sys.exit(1)

    except Exception as e:
        print(f"\n💥 Fatal error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        if spark:
            spark.stop()

if __name__ == "__main__":
    main()