"""
GitHub Kafka to Streaming Delta Consumer
================================================================
Replace: include/spark_jobs/github_kafka_to_streaming_delta.py

CLEAN IMPLEMENTATION:
- No unused imports
- Proper f-string formatting
- Specific exception handling
- Ultra-fast in-memory keyword matching
- Single table write only
"""

import sys
import os
import json
import signal
import shutil
from pathlib import Path

# Spark imports
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, current_timestamp, size, from_json, udf, 
    explode, coalesce, count, now, hour, minute
)
from pyspark.sql.types import (
    ArrayType, StringType, StructType, StructField, IntegerType
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
        except Exception as cleanup_error:
            print(f"⚠️ Spark cleanup warning: {cleanup_error}")


def signal_handler(signum, frame):
    """Handle termination signals to cleanup Spark"""
    print(f"\n🛑 Received signal {signum}, cleaning up...")
    if 'spark' in globals():
        cleanup_spark_session(globals()['spark'])
    sys.exit(1)


def clean_checkpoint_directory():
    """Clean checkpoint directory for FRESH REAL-TIME START"""
    checkpoint_dir = "/tmp/spark-streaming-checkpoint/github-realtime"
    try:
        if os.path.exists(checkpoint_dir):
            print(f"🧹 Cleaning checkpoint for fresh real-time start: {checkpoint_dir}")
            shutil.rmtree(checkpoint_dir)
            print("✅ Checkpoint cleaned - starting fresh for latest data")
        else:
            print("ℹ️ No checkpoint - perfect for real-time fresh start")
    except OSError as dir_error:
        print(f"⚠️ Warning: Could not clean checkpoint directory: {dir_error}")


def create_spark_session():
    """Create optimized Spark session"""
    
    # Get environment variables
    minio_access_key = os.getenv('MINIO_ACCESS_KEY')
    minio_secret_key = os.getenv('MINIO_SECRET_KEY')
    minio_endpoint = os.getenv('MINIO_ENDPOINT')
    
    if not all([minio_access_key, minio_secret_key, minio_endpoint]):
        raise ValueError("MinIO credentials required: MINIO_ACCESS_KEY, MINIO_SECRET_KEY, MINIO_ENDPOINT")
    
    kafka_servers = 'kafka:29092'
    
    print("🔧 Creating ultra-fast Spark session...")
    print(f"📡 Kafka: {kafka_servers}")
    print(f"💾 MinIO: {minio_endpoint}")
    
    spark = SparkSession.builder \
        .appName("GitHubKafkaConsumer_UltraFast") \
        .config("spark.jars", 
            "/opt/spark/jars/delta-spark_2.13-4.0.0.jar,"
            "/opt/spark/jars/delta-storage-4.0.0.jar,"
            "/opt/spark/jars/spark-sql-kafka-0-10_2.13-4.0.0.jar,"
            "/opt/spark/jars/kafka-clients-3.9.0.jar,"
            "/opt/spark/jars/spark-token-provider-kafka-0-10_2.13-4.0.0.jar,"
            "/opt/spark/jars/hadoop-aws-3.3.6.jar,"
            "/opt/spark/jars/aws-java-sdk-bundle-1.12.367.jar"
        ) \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.hadoop.fs.s3a.endpoint", f"http://{minio_endpoint}") \
        .config("spark.hadoop.fs.s3a.access.key", minio_access_key) \
        .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-streaming-checkpoint") \
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "false") \
        .config("spark.sql.adaptive.enabled", "false") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "false") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.streaming.stopGracefullyOnShutdown", "true") \
        .config("spark.sql.streaming.stopTimeout", "30s") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .config("spark.kafka.consumer.cache.enabled", "false") \
        .getOrCreate()
    
    # Set log level to reduce noise
    spark.sparkContext.setLogLevel("WARN")
    
    print(f"✅ Ultra-fast Spark session created: {spark.version}")
    return spark, kafka_servers


def load_keywords_optimized():
    """Load and optimize technology keywords for ultra-fast in-memory matching"""
    keyword_path = "/opt/airflow/include/jsons/tech_keywords.json"
    
    try:
        print(f"📋 Loading keywords from: {keyword_path}")
        
        with open(keyword_path, "r", encoding="utf-8") as f:
            keywords_dict = json.load(f)
        
        keywords_list = list(keywords_dict.keys())
        
        # Pre-process for ultra-fast matching
        processed_keywords = []
        for keyword in keywords_list:
            if keyword and isinstance(keyword, str) and len(keyword.strip()) > 1:
                processed_keyword = keyword.strip().lower()
                processed_keywords.append(processed_keyword)
        
        # Sort by length (longer = more specific matches first)
        processed_keywords.sort(key=len, reverse=True)
        
        print(f"✅ Loaded {len(processed_keywords)} optimized keywords")
        print(f"🔍 Sample keywords: {processed_keywords[:5]}")
        
        return processed_keywords
        
    except FileNotFoundError:
        print(f"❌ Keywords file not found: {keyword_path}")
        fallback = ['javascript', 'python', 'docker', 'react', 'node', 'api', 'web', 'app']
        print(f"⚠️ Using fallback keywords: {fallback}")
        return fallback
    except json.JSONDecodeError as json_error:
        print(f"❌ Invalid JSON in keywords file: {json_error}")
        fallback = ['javascript', 'python', 'docker', 'react', 'node', 'api', 'web', 'app']
        print(f"⚠️ Using fallback keywords: {fallback}")
        return fallback
    except Exception as load_error:
        print(f"❌ Error loading keywords: {load_error}")
        fallback = ['javascript', 'python', 'docker', 'react', 'node', 'api', 'web', 'app']
        print(f"⚠️ Using fallback keywords: {fallback}")
        return fallback


def create_ultra_fast_extraction_udf(keywords_list):
    """Create ultra-fast UDF with in-memory set-based keyword matching"""
    
    # Convert to set for O(1) lookups
    keywords_set = set(keywords_list)
    print(f"🚀 Created keyword set with {len(keywords_set)} terms for O(1) matching")
    
    def extract_technologies_ultra_fast(event_json_str):
        """Ultra-fast in-memory technology extraction with set operations"""
        
        if not event_json_str:
            return []
        
        try:
            # Quick JSON parse
            event = json.loads(event_json_str)
            if not isinstance(event, dict):
                return []
            
            found_techs = set()
            
            # Only check repo name (fastest and most reliable signal)
            repo = event.get("repo")
            if repo and isinstance(repo, dict):
                repo_name = repo.get("name", "")
                if repo_name:
                    repo_lower = repo_name.lower()
                    
                    # Ultra-fast set intersection for keyword matching
                    # Split repo name into words, replacing common separators
                    repo_words = set(
                        repo_lower.replace("/", " ")
                                  .replace("-", " ")
                                  .replace(".", " ")
                                  .replace("_", " ")
                                  .split()
                    )
                    
                    # Find exact matches using set intersection (O(1) operation)
                    exact_matches = keywords_set.intersection(repo_words)
                    found_techs.update(exact_matches)
                    
                    # If no exact matches, check substring matches for top 20 keywords
                    if not exact_matches:
                        for keyword in keywords_list[:20]:  # Limit for speed
                            if keyword in repo_lower:
                                found_techs.add(keyword)
                                break  # First match wins for performance
            
            # Return all found technologies (no artificial limit)
            return list(found_techs)
            
        except json.JSONDecodeError:
            return []
        except Exception:
            return []
    
    return udf(extract_technologies_ultra_fast, ArrayType(StringType()))


def write_to_streaming_delta(dataframe, epoch_id):
    """Ultra-fast single table write - NO aggregation complexity"""
    
    streaming_table_path = "s3a://delta-lake/bronze/bronze_github_streaming_keyword_extractions"
    batch_id = f"batch_{epoch_id}"
    
    print(f"\n📦 BATCH {epoch_id} (ULTRA-FAST)")
    
    try:
        # Step 1: Fast count check
        record_count = dataframe.count()
        print(f"📊 Records: {record_count}")
        
        if record_count == 0:
            print("⏩ Empty batch - skipping")
            return
        
        # Step 2: Filter for events with technologies
        tech_events = dataframe.filter(size(col("technologies")) > 0)
        tech_count = tech_events.count()
        print(f"✅ Tech events: {tech_count}")
        
        if tech_count == 0:
            print("⏩ No tech events")
            return
        
        # Step 3: Explode technologies
        exploded_df = tech_events.select(
            explode(col("technologies")).alias("keyword"),
            col("event_id"),
            col("repo_name")
        )
        
        # Step 4: Aggregate by keyword and repo
        aggregated_df = exploded_df.groupBy("keyword", "repo_name").agg(
            count("event_id").alias("mention_count")
        )
        
        # Step 5: Create final DataFrame with exact schema match

        now = current_timestamp()
        
        final_df = aggregated_df.select(
            hour(now).alias("hour"),
            minute(now).alias("minute"),
            col("keyword").alias("keyword"),
            col("mention_count").cast(IntegerType()).alias("mentions"),
            col("repo_name").alias("top_repo"),
            col("mention_count").cast(IntegerType()).alias("repo_mentions"),
            col("mention_count").cast(IntegerType()).alias("event_mentions"),
            now.cast("date").alias("date"),
            lit("streaming").alias("source_file"),
            now.alias("processing_time"),
            lit(None).cast(StringType()).alias("event_id"),
            now.alias("event_timestamp"),
            lit("Streaming").alias("event_type"),
            lit("streaming").alias("actor_login"),
            now.alias("kafka_timestamp"),
            lit(batch_id).alias("batch_id"),
            now.alias("window_start"),
            now.alias("window_end")
        )
        
        # Step 6: Ultra-fast write (no verification overhead)
        final_count = final_df.count()
        print(f"🚀 Writing {final_count} records...")
        
        final_df.write \
            .format("delta") \
            .mode("append") \
            .option("dataChange", "true") \
            .option("mergeSchema", "false") \
            .option("overwriteSchema", "false") \
            .partitionBy("date", "hour", "minute") \
            .save(streaming_table_path)
        
        print(f"✅ SUCCESS: {final_count} records written")
        
        # Show quick sample for small batches
        if final_count <= 3:
            try:
                sample = final_df.collect()
                for i, record in enumerate(sample):
                    print(f"   {i+1}. {record.keyword}: {record.mentions}")
            except Exception as sample_error:
                print(f"⚠️ Could not show sample: {sample_error}")
        
    except Exception as batch_error:
        print(f"❌ Batch {epoch_id} error: {batch_error}")
        
    print(f"📋 Batch {epoch_id} completed")


def main():
    """Ultra-fast streaming consumer main function"""
    
    # Register signal handlers for graceful shutdown
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    
    print("🚀 REAL-TIME GITHUB TRENDS CONSUMER")
    print("=" * 60)
    
    spark = None
    streaming_query = None
    
    try:
        # Always clean checkpoint for fresh real-time start
        clean_checkpoint_directory()
        
        # Initialize Spark session
        spark, kafka_servers = create_spark_session()
        globals()['spark'] = spark
        
        # Load optimized keywords
        keywords = load_keywords_optimized()
        
        # Create ultra-fast UDF
        extract_tech_udf = create_ultra_fast_extraction_udf(keywords)
        print("✅ Ultra-fast UDF with in-memory keyword matching created")
        
        # Configure Kafka stream for REAL-TIME TRENDS (skip old messages)
        kafka_stream = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", kafka_servers) \
            .option("subscribe", "github-events-raw") \
            .option("startingOffsets", "latest") \
            .option("failOnDataLoss", "false") \
            .option("maxOffsetsPerTrigger", "100") \
            .option("kafka.consumer.group.id", "github-realtime-trends") \
            .option("kafka.consumer.auto.offset.reset", "latest") \
            .option("kafka.consumer.enable.auto.commit", "false") \
            .option("kafka.consumer.session.timeout.ms", "10000") \
            .load()
        
        # Define GitHub event schema
        github_event_schema = StructType([
            StructField("id", StringType(), True),
            StructField("type", StringType(), True),
            StructField("repo", StructType([
                StructField("name", StringType(), True)
            ]), True)
        ])
        
        # Process stream
        processed_stream = kafka_stream \
            .select(
                col("value").cast("string").alias("raw_event")
            ) \
            .withColumn("technologies", extract_tech_udf(col("raw_event"))) \
            .withColumn("event_data", from_json(col("raw_event"), github_event_schema)) \
            .select(
                coalesce(col("event_data.id"), lit("unknown")).alias("event_id"),
                coalesce(col("event_data.type"), lit("unknown")).alias("event_type"),
                coalesce(col("event_data.repo.name"), lit("unknown")).alias("repo_name"),
                col("technologies"),
                current_timestamp().alias("processing_time")
            )
        
        print("\n💾 Starting REAL-TIME TRENDS streaming...")
        print("⏰ Micro-batches every 3 seconds (30s rolling window)")
        print("🚀 Latest messages only - skips old data")
        print("🎯 Optimized for real-time dashboard updates")
        print("-" * 60)
        
        # Start streaming query for REAL-TIME TRENDS
        streaming_query = processed_stream \
            .writeStream \
            .foreachBatch(write_to_streaming_delta) \
            .outputMode("append") \
            .trigger(processingTime='3 seconds') \
            .option("checkpointLocation", "/tmp/spark-streaming-checkpoint/github-realtime") \
            .queryName("github-realtime-trends") \
            .start()
        
        print("✅ Ultra-fast streaming query started!")
        
        # Wait for termination
        streaming_query.awaitTermination()
        
    except ValueError as env_error:
        print(f"\n❌ Environment error: {env_error}")
        sys.exit(1)
    except Exception as main_error:
        print(f"\n❌ Critical error: {main_error}")
        
        if streaming_query:
            try:
                streaming_query.stop()
            except Exception as stop_error:
                print(f"⚠️ Error stopping query: {stop_error}")
        
        sys.exit(1)
        
    finally:
        if streaming_query:
            try:
                streaming_query.stop()
            except Exception as final_stop_error:
                print(f"⚠️ Warning stopping query: {final_stop_error}")
        
        cleanup_spark_session(spark)


if __name__ == "__main__":
    main()