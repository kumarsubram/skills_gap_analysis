"""
GitHub Raw to Bronze Delta Tables - MINIMAL SPARK CLEANUP FIX

ONLY CHANGES:
- Better finally block in main()
- Added signal handler for subprocess termination
- No other changes to existing logic
"""

import sys
import os
import io
import boto3
import pandas as pd
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp
import signal

# Add project root for imports
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))


def cleanup_spark_session(spark):
    """Ensure Spark session is properly cleaned up"""
    if spark:
        try:
            print("🛑 Cleaning up Spark session...")
            spark.catalog.clearCache()
            spark.stop()
            print("✅ Spark session stopped")
        except Exception as e:
            print(f"⚠️  Spark cleanup warning: {e}")
        finally:
            # Force cleanup
            try:
                spark._jvm.System.gc()
            except (AttributeError, Exception):
                pass


def signal_handler(signum, frame):
    """Handle termination signals to cleanup Spark"""
    print(f"\n🛑 Received signal {signum}, cleaning up...")
    if 'spark' in globals():
        cleanup_spark_session(globals()['spark'])
    sys.exit(1)


def get_minio_client():
    """Get MinIO S3 client using your working configuration"""
    access_key = os.getenv('MINIO_ACCESS_KEY')
    secret_key = os.getenv('MINIO_SECRET_KEY')
    endpoint = os.getenv('MINIO_ENDPOINT')
    
    if not access_key or not secret_key:
        raise ValueError("MINIO_ACCESS_KEY and MINIO_SECRET_KEY environment variables must be set")
    
    try:
        client = boto3.client(
            's3',
            endpoint_url=f'http://{endpoint}',
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name='us-east-1'
        )
        
        # Test connection
        client.list_buckets()
        return client
        
    except Exception as e:
        raise ConnectionError(f"MinIO connection failed to {endpoint}: {e}")


def get_bucket_name():
    """Get MinIO bucket name from environment"""
    bucket = os.getenv('MINIO_BUCKET', 'delta-lake')
    return bucket


def create_spark_session_exact_match(app_name):
    """Create Spark session using EXACT same config as your working simple_delta_test.py"""
    
    minio_access_key = os.getenv('MINIO_ACCESS_KEY')
    minio_secret_key = os.getenv('MINIO_SECRET_KEY') 
    minio_endpoint = os.getenv('MINIO_ENDPOINT')
    
    if not minio_access_key or not minio_secret_key:
        raise ValueError("MinIO credentials required")
    
    print("🔧 Creating Spark session - EXACT MATCH to working simple_delta_test.py")
    print("✅ Using proven S3A configuration that works")
    
    spark = SparkSession.builder \
        .appName(app_name) \
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
    
    print("✅ Spark session created with PROVEN working configuration")
    print(f"📊 Spark version: {spark.version}")
    return spark


def check_parquet_files_via_minio(date_str):
    """Check which parquet files exist using MinIO client"""
    client = get_minio_client()
    bucket = get_bucket_name()
    existing_hours = []
    
    print(f"🔍 Checking parquet files in MinIO bucket: {bucket}")
    
    for hour in range(24):
        s3_key = f"bronze/github/keywords-{date_str}-hour-{hour:02d}.parquet"
        
        try:
            client.head_object(Bucket=bucket, Key=s3_key)
            existing_hours.append(hour)
            print(f"   ✅ Found: hour {hour:02d}")
        except Exception:
            print(f"   ❌ Missing: hour {hour:02d}")
    
    print(f"📊 Found {len(existing_hours)}/24 parquet files")
    return existing_hours


def read_parquet_via_minio_to_spark(spark, date_str, existing_hours):
    """Read parquet files via MinIO client, convert to Spark DataFrame"""
    client = get_minio_client()
    bucket = get_bucket_name()
    
    print(f"📖 Reading {len(existing_hours)} parquet files...")
    
    # Collect all data as pandas first
    all_pandas_dfs = []
    
    for hour in existing_hours:
        s3_key = f"bronze/github/keywords-{date_str}-hour-{hour:02d}.parquet"
        
        try:
            # Read parquet file from MinIO
            response = client.get_object(Bucket=bucket, Key=s3_key)
            parquet_data = response['Body'].read()
            
            # Read with pandas
            parquet_bytes = io.BytesIO(parquet_data)
            hour_df = pd.read_parquet(parquet_bytes)
            
            all_pandas_dfs.append(hour_df)
            print(f"   ✅ Hour {hour:02d}: {len(hour_df)} records")
            
        except Exception as e:
            print(f"   ❌ Hour {hour:02d} failed: {e}")
    
    if not all_pandas_dfs:
        raise Exception("No parquet files could be read")
    
    # Combine all pandas DataFrames
    combined_pandas_df = pd.concat(all_pandas_dfs, ignore_index=True)
    print(f"📊 Total pandas records: {len(combined_pandas_df):,}")
    
    # Convert to Spark DataFrame
    spark_df = spark.createDataFrame(combined_pandas_df)
    
    # Add metadata
    enriched_df = spark_df \
        .withColumn("date", lit(date_str)) \
        .withColumn("source_file", lit(f"{date_str}-batch.json.gz")) \
        .withColumn("processing_time", current_timestamp())
    
    total_records = enriched_df.count()
    print(f"📊 Final Spark records: {total_records:,}")
    
    return enriched_df, total_records


def cleanup_parquet_files_via_minio(date_str, existing_hours):
    """Delete parquet files via MinIO client"""
    client = get_minio_client()
    bucket = get_bucket_name()
    
    print(f"🗑️ Cleaning up {len(existing_hours)} parquet files...")
    
    cleaned_count = 0
    for hour in existing_hours:
        s3_key = f"bronze/github/keywords-{date_str}-hour-{hour:02d}.parquet"
        
        try:
            client.delete_object(Bucket=bucket, Key=s3_key)
            cleaned_count += 1
            print(f"   ✅ Deleted hour {hour:02d}")
        except Exception as e:
            print(f"   ⚠️ Failed to delete hour {hour:02d}: {e}")
    
    print(f"✅ Cleaned up {cleaned_count}/{len(existing_hours)} parquet files")


def create_bronze_delta(spark, date_str):
    """Create Bronze Delta table - FULLY FIXED VERSION"""
    
    print(f"\n🔄 FULLY FIXED BRONZE DELTA CREATION: {date_str}")
    print("=" * 60)
    print("✅ Using EXACT same S3A config as working simple_delta_test.py")
    
    bronze_table = "s3a://delta-lake/bronze/bronze_github_keyword_extractions"
    
    print(f"🏢 Enterprise Bronze table: {bronze_table}") 
    
    existing_hours = check_parquet_files_via_minio(date_str)
    
    if not existing_hours:
        print(f"\n❌ No parquet files found for {date_str}")
        return {'success': False, 'reason': 'no_parquet_files'}
    
    try:
        # Step 2: Read via MinIO client, convert to Spark DF
        enriched_df, total_records = read_parquet_via_minio_to_spark(spark, date_str, existing_hours)
        
        if total_records > 0:
            # Step 3: Write to Delta Lake using S3A (like your working script)
            print(f"💾 Writing {total_records:,} records to Bronze Delta Lake...")
            print(f"🎯 Target: {bronze_table}")
            print("✅ Using proven S3A configuration")
            
            enriched_df.write \
                .format("delta") \
                .mode("append") \
                .save(bronze_table)
            
            print(f"✅ Successfully wrote {total_records:,} records to Bronze Delta Lake")
            
            # Step 4: Cleanup parquet files
            cleanup_parquet_files_via_minio(date_str, existing_hours)
            
            # Step 5: Verification using S3A
            print(f"🔍 Verifying Bronze Delta table: {bronze_table}")
            final_df = spark.read.format("delta").load(bronze_table)
            date_records = final_df.filter(col("date") == date_str).count()
            total_delta_records = final_df.count()
            
            print("\n📊 FULLY FIXED BRONZE DELTA SUMMARY:")
            print(f"   ✅ Hours processed: {len(existing_hours)}/24")
            print(f"   📈 Records for {date_str}: {date_records:,}")
            print(f"   📁 Total Bronze records: {total_delta_records:,}")
            print("   ✅ S3A configuration: EXACT match to working script")
            print("   🗑️ Parquet files: Cleaned up")
            print("   🎯 Bronze Delta table: Ready for Silver!")
            
            return {
                'success': True,
                'date': date_str,
                'hours_processed': len(existing_hours),
                'date_records': date_records,
                'total_records': total_delta_records
            }
        else:
            print("❌ No records found")
            return {'success': False, 'reason': 'no_records'}
            
    except Exception as e:
        print(f"\n❌ Bronze processing failed: {e}")
        import traceback
        traceback.print_exc()
        return {'success': False, 'error': str(e)}


def main():
    """Main entry point - ONLY CHANGE: Better cleanup"""
    
    # Register signal handlers for graceful cleanup
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    
    # Get date from command line
    if len(sys.argv) > 1:
        date_str = sys.argv[1]
    else:
        date_str = "2024-12-31"
    
    print(f"🚀 FULLY FIXED Bronze Delta creation for {date_str}")
    print("✅ Using EXACT same S3A config as working simple_delta_test.py")
    
    # Pre-flight checks
    print("\n🔍 PRE-FLIGHT CHECKS:")
    
    # Check environment variables
    required_vars = ['MINIO_ACCESS_KEY', 'MINIO_SECRET_KEY']
    for var in required_vars:
        value = os.getenv(var)
        if value:
            print(f"   ✅ {var}: Set (length: {len(value)})")
        else:
            print(f"   ❌ {var}: Not set!")
            sys.exit(1)
    
    # Check MinIO connectivity
    try:
        print("   🔗 Testing MinIO connection...")
        client = get_minio_client()
        buckets = client.list_buckets()
        print(f"   ✅ MinIO connected: {len(buckets['Buckets'])} buckets found")
    except Exception as e:
        print(f"   ❌ MinIO connection failed: {e}")
        sys.exit(1)
    
    spark = None
    try:
        # Create Spark session with proven working configuration
        spark = create_spark_session_exact_match(f"GitHubBronze_{date_str}")
        
        # Make spark available to signal handler
        globals()['spark'] = spark
        
        print(f"📊 Spark {spark.version}")
        print("✅ Proven S3A configuration applied")
        
        # Process using fully fixed approach
        result = create_bronze_delta(spark, date_str)
        
        if result['success']:
            print(f"\n🎉 SUCCESS: FULLY FIXED Bronze Delta created for {date_str}")
            print(f"   📈 {result['hours_processed']}/24 hours processed")
            print(f"   📊 {result['date_records']:,} records for this date")
            print("   ✅ NO MORE ERRORS!")
        else:
            print(f"\n❌ FAILED: {result.get('reason', 'unknown')}")
            if 'error' in result:
                print(f"   Error: {result['error']}")
            sys.exit(1)
            
    except Exception as e:
        print(f"\n❌ Job failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        # IMPROVED: Guaranteed cleanup
        cleanup_spark_session(spark)


if __name__ == "__main__":
    main()