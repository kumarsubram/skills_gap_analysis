#!/usr/bin/env python3
"""
Simple S3A Parquet Test Script
Tests S3A connectivity by writing Parquet files (no Delta Lake complexity)
"""

import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType

def create_simple_spark_session():
    """Override ALL problematic time suffix configurations"""
    
    minio_access_key = os.getenv('MINIO_ACCESS_KEY', 'minioadmin')
    minio_secret_key = os.getenv('MINIO_SECRET_KEY', 'minioadmin123') 
    minio_endpoint = os.getenv('MINIO_ENDPOINT', 'minio:9000')
    
    print("🚀 Creating Spark session (Override ALL time suffixes)")
    
    spark = SparkSession.builder \
        .appName("SimpleS3AParquetTest") \
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
    
    print(f"✅ Spark {spark.version} session created (ALL TIME SUFFIXES OVERRIDDEN)")
    return spark


def create_sample_data(spark):
    """Create simple test data"""
    
    print("📊 Creating sample data...")
    
    # Define schema
    schema = StructType([
        StructField("id", LongType(), True),
        StructField("keyword", StringType(), True),
        StructField("mentions", LongType(), True),
        StructField("date", StringType(), True),
        StructField("processing_time", TimestampType(), True)
    ])
    
    # Create sample data
    data = [
        (1, "python", 100, "2024-12-31", None),
        (2, "javascript", 75, "2024-12-31", None),
        (3, "react", 50, "2024-12-31", None),
        (4, "spark", 25, "2024-12-31", None),
        (5, "delta", 15, "2024-12-31", None)
    ]
    
    df = spark.createDataFrame(data, schema)
    df = df.withColumn("processing_time", current_timestamp())
    
    print(f"✅ Created {df.count()} sample records")
    df.show()
    
    return df


def write_to_s3_parquet(spark, df):
    """Write DataFrame to S3 as Parquet files"""
    
    s3_path = "s3a://delta-lake/bronze/test/simple_parquet_test"
    
    print(f"💾 Writing to S3 Parquet: {s3_path}")
    
    try:
        df.write \
            .format("parquet") \
            .mode("overwrite") \
            .save(s3_path)
        
        print("✅ Successfully wrote to S3 Parquet!")
        return True
        
    except Exception as e:
        print(f"❌ Failed to write S3 Parquet: {e}")
        import traceback
        traceback.print_exc()
        return False


def verify_s3_parquet(spark):
    """Verify the Parquet files were created and can be read"""
    
    s3_path = "s3a://delta-lake/bronze/test/simple_parquet_test"
    
    print(f"🔍 Verifying S3 Parquet: {s3_path}")
    
    try:
        # Read the Parquet files
        read_df = spark.read.format("parquet").load(s3_path)
        
        record_count = read_df.count()
        print(f"✅ S3 Parquet verified: {record_count} records")
        
        print("📊 Sample data from S3 Parquet:")
        read_df.show()
        
        # Show the file structure
        print("📁 Files created:")
        file_df = spark.read.text(s3_path)
        print(f"   Files in S3: {file_df.count()} parquet files")
        
        return True
        
    except Exception as e:
        print(f"❌ Failed to read S3 Parquet: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """Main function"""
    
    print("🎯 SIMPLE S3A PARQUET TEST")
    print("=" * 50)
    print("Testing S3A connectivity with simple Parquet files")
    print("(No Delta Lake complexity)")
    
    spark = None
    
    try:
        # Create Spark session
        spark = create_simple_spark_session()
        
        # Create sample data
        sample_df = create_sample_data(spark)
        
        # Write to S3 Parquet
        write_success = write_to_s3_parquet(spark, sample_df)
        
        if write_success:
            # Verify the files
            verify_success = verify_s3_parquet(spark)
            
            if verify_success:
                print("\n🎉 SUCCESS: S3A + Parquet working perfectly!")
                print("✅ Created Parquet files on S3/MinIO")
                print("✅ Verified files can be read")
                print("🎯 S3A configuration is correct!")
                print("💡 Now you can try Delta Lake with confidence")
            else:
                print("\n⚠️ Write succeeded but verification failed")
        else:
            print("\n❌ Failed to write S3 Parquet files")
            print("🔧 Check S3A configuration and credentials")
            sys.exit(1)
            
    except Exception as e:
        print(f"\n💥 Script failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
        
    finally:
        if spark:
            spark.stop()


if __name__ == "__main__":
    main()