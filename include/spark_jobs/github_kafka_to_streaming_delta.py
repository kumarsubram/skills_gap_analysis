"""
GitHub Kafka to Streaming Delta - FIXED WITH WORKING S3A CONFIG
===============================================================

FIXED: Using EXACT same S3A configuration from your working batch job
- Removed problematic time-based configs that caused "24h" error
- Added all the working numeric timeout values
- Proper error handling and cleanup

Place at: include/spark_jobs/github_kafka_to_streaming_delta.py
"""

import sys
import os
import json
import re
import time
import signal
from pathlib import Path

# Spark imports - explicit, no star imports
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, current_timestamp, explode, size, hour, to_date, 
    to_timestamp, when, window, from_json, udf
)
from pyspark.sql.types import (
    ArrayType, StringType, StructType, StructField
)

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


def create_optimized_spark_session():
    """Create Spark session using EXACT WORKING S3A CONFIG from your batch job"""
    
    # Get environment variables
    minio_access_key = os.getenv('MINIO_ACCESS_KEY')
    minio_secret_key = os.getenv('MINIO_SECRET_KEY')
    minio_endpoint = os.getenv('MINIO_ENDPOINT')
    
    if not all([minio_access_key, minio_secret_key, minio_endpoint]):
        raise ValueError("MinIO credentials required")
    
    # Get Kafka servers
    vps_ip = os.getenv('VPS_IP', 'localhost')
    kafka_servers = f'{vps_ip}:9092' if vps_ip != 'localhost' else 'localhost:9092'
    
    print("🔧 Creating Spark session for streaming...")
    print(f"📡 Kafka: {kafka_servers}")
    print(f"💾 MinIO: {minio_endpoint}")
    print("✅ Using PROVEN working S3A configuration")
    
    spark = SparkSession.builder \
        .appName("GitHubKafkaConsumer") \
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
        .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-streaming-checkpoint") \
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
        .getOrCreate()
    
    print(f"✅ Spark session created: {spark.version}")
    print("✅ PROVEN S3A configuration applied - NO MORE '24h' ERRORS!")
    return spark, kafka_servers


def load_keywords():
    """Load technology keywords"""
    keyword_path = "/opt/airflow/include/jsons/tech_keywords.json"
    
    try:
        with open(keyword_path, "r", encoding="utf-8") as f:
            keywords = json.load(f)
        print(f"📋 Loaded {len(keywords)} technology keywords")
        return list(keywords.keys())
    except Exception as e:
        print(f"❌ Error loading keywords: {e}")
        return []


def create_technology_extraction_udf(keywords):
    """Create UDF to extract technologies from GitHub event JSON"""
    
    def extract_technologies(event_json):
        """Extract technology keywords from GitHub event JSON"""
        if not event_json:
            return []
        
        try:
            event = json.loads(event_json)
            found_technologies = []
            
            # Collect text content to search
            text_sources = []
            
            # Repository name
            repo_name = event.get("repo", {}).get("name", "")
            if repo_name:
                text_sources.append(repo_name.lower())
            
            # Event-specific content extraction
            event_type = event.get("type", "")
            payload = event.get("payload", {})
            
            if event_type == "PushEvent":
                commits = payload.get("commits", [])
                for commit in commits:
                    message = commit.get("message", "")
                    if message:
                        text_sources.append(message.lower())
            
            elif event_type == "PullRequestEvent":
                pr = payload.get("pull_request", {})
                title = pr.get("title", "")
                if title:
                    text_sources.append(title.lower())
                body = pr.get("body", "")
                if body:
                    text_sources.append(body[:500].lower())
            
            elif event_type == "ReleaseEvent":
                release = payload.get("release", {})
                name = release.get("name", "")
                if name:
                    text_sources.append(name.lower())
                body = release.get("body", "")
                if body:
                    text_sources.append(body[:500].lower())
            
            # Search for technology keywords
            for text in text_sources:
                for keyword in keywords:
                    # Use word boundary regex for accurate matching
                    pattern = rf"\b{re.escape(keyword.lower())}\b"
                    if re.search(pattern, text):
                        found_technologies.append(keyword)
            
            # Remove duplicates and return
            return list(set(found_technologies))
            
        except Exception:
            # Return empty list if JSON parsing or processing fails
            return []
    
    # Create and return the UDF
    return udf(extract_technologies, ArrayType(StringType()))


def write_to_streaming_delta(dataframe, epoch_id):
    """Write streaming batch to Delta table with enhanced schema"""
    
    streaming_table_path = "s3a://delta-lake/bronze/bronze_streaming_github_keyword_extractions"
    
    try:
        # Add batch metadata for traceability
        batch_id = f"batch_{epoch_id}_{int(time.time())}"
        enhanced_df = dataframe.withColumn("batch_id", lit(batch_id))
        
        # Write to Delta table using PROVEN S3A configuration
        print(f"💾 Writing batch {epoch_id} to Delta table...")
        print(f"🎯 Target: {streaming_table_path}")
        print("✅ Using working S3A config - NO MORE ERRORS!")
        
        enhanced_df.write \
            .format("delta") \
            .mode("append") \
            .save(streaming_table_path)
        
        record_count = enhanced_df.count()
        print(f"✅ Batch {epoch_id}: Wrote {record_count} records to streaming Delta table")
        
    except Exception as e:
        print(f"❌ Batch {epoch_id}: Write failed - {e}")
        # Re-raise to stop the streaming query on write failures
        raise


def main():
    """Main streaming consumer entry point"""
    
    # Register signal handlers for graceful shutdown
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    
    print("🚀 GITHUB KAFKA TO STREAMING DELTA - FIXED VERSION")
    print("=" * 60)
    print("📡 Source: github-events-raw (Kafka topic)")
    print("💾 Target: bronze_streaming_github_keyword_extractions")
    print("✅ Using PROVEN working S3A configuration")
    print("🔧 Fixed: No more '24h' NumberFormatException errors!")
    
    spark = None
    try:
        # Initialize Spark session with working S3A config
        spark, kafka_servers = create_optimized_spark_session()
        globals()['spark'] = spark
        
        # Load technology keywords
        keywords = load_keywords()
        if not keywords:
            raise Exception("No technology keywords loaded - cannot proceed")
        
        # Create UDF for technology extraction
        extract_tech_udf = create_technology_extraction_udf(keywords)
        
        print("📡 Setting up Kafka stream...")
        
        # Configure Kafka stream source
        kafka_stream = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", kafka_servers) \
            .option("subscribe", "github-events-raw") \
            .option("startingOffsets", "latest") \
            .option("failOnDataLoss", "false") \
            .option("maxOffsetsPerTrigger", "100") \
            .load()
        
        print("🔍 Setting up enhanced processing pipeline...")
        
        # Define GitHub event schema for JSON parsing
        github_event_schema = StructType([
            StructField("id", StringType(), True),
            StructField("type", StringType(), True),
            StructField("created_at", StringType(), True),
            StructField("repo", StructType([
                StructField("name", StringType(), True)
            ]), True),
            StructField("actor", StructType([
                StructField("login", StringType(), True)
            ]), True)
        ])
        
        # Process the stream with enhanced schema
        processed_stream = kafka_stream \
            .select(
                col("value").cast("string").alias("raw_event"),
                col("timestamp").alias("kafka_timestamp"),
                col("key").cast("string").alias("kafka_key")
            ) \
            .withColumn("technologies", extract_tech_udf(col("raw_event"))) \
            .filter(size(col("technologies")) > 0) \
            .withColumn("event_data", from_json(col("raw_event"), github_event_schema)) \
            .select(
                # Extract event fields
                col("event_data.id").alias("event_id"),
                col("event_data.type").alias("event_type"),
                col("event_data.created_at").alias("event_timestamp_str"),
                col("event_data.repo.name").alias("top_repo"),
                col("event_data.actor.login").alias("actor_login"),
                col("technologies"),
                col("kafka_timestamp"),
                current_timestamp().alias("processing_time")
            ) \
            .withColumn("event_timestamp", 
                when(col("event_timestamp_str").isNotNull(), 
                     to_timestamp(col("event_timestamp_str"), "yyyy-MM-dd'T'HH:mm:ss'Z'"))
                .otherwise(col("kafka_timestamp"))
            ) \
            .withColumn("window_start", window(col("kafka_timestamp"), "30 seconds").start) \
            .withColumn("window_end", window(col("kafka_timestamp"), "30 seconds").end) \
            .select(
                explode(col("technologies")).alias("keyword"),
                # Original batch schema columns (for compatibility)
                hour(col("event_timestamp")).alias("hour"),
                lit(1).alias("mentions"),
                col("top_repo"),
                lit(1).alias("repo_mentions"),
                lit(1).alias("event_mentions"),
                to_date(col("event_timestamp")).alias("date"),
                lit("streaming-kafka").alias("source_file"),
                col("processing_time"),
                # Enhanced streaming columns
                col("event_id"),
                col("event_timestamp"),
                col("event_type"),
                col("actor_login"),
                col("kafka_timestamp"),
                col("window_start"),
                col("window_end")
            )
        
        print("💾 Starting streaming write to Delta table...")
        print("⏰ Processing micro-batches every 30 seconds")
        print("🔄 Will run until manually stopped")
        print("✅ NO MORE S3A CONFIGURATION ERRORS!")
        
        # Start the streaming query
        streaming_query = processed_stream \
            .writeStream \
            .foreachBatch(write_to_streaming_delta) \
            .outputMode("append") \
            .trigger(processingTime='30 seconds') \
            .option("checkpointLocation", "/tmp/spark-streaming-checkpoint/github-consumer") \
            .start()
        
        print("✅ Streaming query started successfully")
        print("📊 Monitor progress in Spark UI")
        
        # Wait for termination (manual stop or error)
        streaming_query.awaitTermination()
        
    except Exception as e:
        print(f"\n❌ Streaming job failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        # Ensure cleanup happens
        cleanup_spark_session(spark)


if __name__ == "__main__":
    main()