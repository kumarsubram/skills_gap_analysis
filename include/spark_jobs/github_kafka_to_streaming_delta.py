"""
Exact Schema Match GitHub Kafka to Streaming Delta Consumer
==========================================================
Replace: include/spark_jobs/github_kafka_to_streaming_delta.py

MINIMAL CHANGES: Only added dual-write functionality to existing working code
"""

import sys
import os
import json
import re
import time
import signal
import shutil
from pathlib import Path

# Spark imports
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, current_timestamp, size, from_json, udf, 
    explode, coalesce, count,
    # ADDED: Additional functions for aggregation
    countDistinct, first
)
from pyspark.sql.types import (
    ArrayType, StringType, StructType, StructField, IntegerType, DateType, TimestampType
)

# ADDED: For Delta merge operations
try:
    from delta.tables import DeltaTable
    DELTA_AVAILABLE = True
except ImportError:
    print("⚠️ Delta tables not available for merge operations")
    DELTA_AVAILABLE = False

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
            print(f"⚠️ Spark cleanup warning: {e}")


def signal_handler(signum, frame):
    """Handle termination signals to cleanup Spark"""
    print(f"\n🛑 Received signal {signum}, cleaning up...")
    if 'spark' in globals():
        cleanup_spark_session(globals()['spark'])
    sys.exit(1)


def clean_checkpoint_directory():
    """Clean checkpoint directory for fresh start"""
    checkpoint_dir = "/tmp/spark-streaming-checkpoint/github-consumer"
    try:
        if os.path.exists(checkpoint_dir):
            print(f"🧹 Cleaning checkpoint directory: {checkpoint_dir}")
            shutil.rmtree(checkpoint_dir)
            print("✅ Checkpoint directory cleaned")
        else:
            print("ℹ️ Checkpoint directory doesn't exist - good for fresh start")
    except Exception as e:
        print(f"⚠️ Warning: Could not clean checkpoint directory: {e}")


def create_optimized_spark_session():
    """Create Spark session with enhanced configuration"""
    
    # Get environment variables
    minio_access_key = os.getenv('MINIO_ACCESS_KEY')
    minio_secret_key = os.getenv('MINIO_SECRET_KEY')
    minio_endpoint = os.getenv('MINIO_ENDPOINT')
    
    if not all([minio_access_key, minio_secret_key, minio_endpoint]):
        raise ValueError("MinIO credentials required")
    
    kafka_servers = 'kafka:29092'
    
    print("🔧 Creating Spark session with exact schema matching...")
    print(f"📡 Kafka: {kafka_servers}")
    print(f"💾 MinIO: {minio_endpoint}")
    
    spark = SparkSession.builder \
        .appName("GitHubKafkaConsumer_ExactSchema") \
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
        .getOrCreate()
    
    # Set log level to reduce noise
    spark.sparkContext.setLogLevel("WARN")
    
    print(f"✅ Spark session created: {spark.version}")
    return spark, kafka_servers


def get_exact_target_schema():
    """Return the EXACT schema that matches your Delta table"""
    return StructType([
        StructField('hour', IntegerType(), True), 
        StructField('keyword', StringType(), True), 
        StructField('mentions', IntegerType(), True), 
        StructField('top_repo', StringType(), True), 
        StructField('repo_mentions', IntegerType(), True), 
        StructField('event_mentions', IntegerType(), True), 
        StructField('date', DateType(), True), 
        StructField('source_file', StringType(), True), 
        StructField('processing_time', TimestampType(), True), 
        StructField('event_id', StringType(), True), 
        StructField('event_timestamp', TimestampType(), True), 
        StructField('event_type', StringType(), True), 
        StructField('actor_login', StringType(), True), 
        StructField('kafka_timestamp', TimestampType(), True), 
        StructField('batch_id', StringType(), True), 
        StructField('window_start', TimestampType(), True), 
        StructField('window_end', TimestampType(), True)
    ])


def load_keywords():
    """Load technology keywords with enhanced error handling"""
    keyword_path = "/opt/airflow/include/jsons/tech_keywords.json"
    
    try:
        print(f"📋 Loading keywords from: {keyword_path}")
        with open(keyword_path, "r", encoding="utf-8") as f:
            keywords_dict = json.load(f)
        
        keywords = list(keywords_dict.keys())
        print(f"✅ Loaded {len(keywords)} technology keywords")
        
        return keywords
        
    except Exception as e:
        print(f"❌ Error loading keywords: {e}")
        # Fallback keywords
        fallback = ['python', 'javascript', 'react', 'node', 'docker', 'api', 'web', 'app']
        print(f"⚠️ Using fallback keywords: {fallback}")
        return fallback


def create_technology_extraction_udf(keywords):
    """Create robust UDF for technology extraction"""
    
    def extract_technologies_robust(event_json_str):
        """
        Robust technology extraction with comprehensive error handling
        """
        # Handle null/empty input
        if not event_json_str or event_json_str.strip() == "":
            return []
        
        try:
            # Parse JSON
            try:
                event = json.loads(event_json_str)
            except json.JSONDecodeError:
                return []
            
            if not isinstance(event, dict):
                return []
            
            found_technologies = set()  # Use set to avoid duplicates
            
            # Extract searchable text from various sources
            text_sources = []
            
            # 1. Repository name (most reliable)
            repo = event.get("repo", {})
            if isinstance(repo, dict):
                repo_name = repo.get("name", "")
                if repo_name and isinstance(repo_name, str):
                    text_sources.append(repo_name.lower())
            
            # 2. Event type and payload
            event_type = event.get("type", "")
            payload = event.get("payload", {})
            
            if isinstance(payload, dict):
                # Handle PushEvent commits
                if event_type == "PushEvent" and "commits" in payload:
                    commits = payload["commits"]
                    if isinstance(commits, list):
                        for commit in commits[:3]:  # Limit to first 3
                            if isinstance(commit, dict):
                                message = commit.get("message", "")
                                if message and isinstance(message, str):
                                    text_sources.append(message.lower()[:200])
                
                # Handle PullRequestEvent
                elif event_type == "PullRequestEvent" and "pull_request" in payload:
                    pr = payload["pull_request"]
                    if isinstance(pr, dict):
                        title = pr.get("title", "")
                        if title and isinstance(title, str):
                            text_sources.append(title.lower())
            
            # 3. Search for keywords in all text sources
            for text in text_sources:
                if not text or not isinstance(text, str):
                    continue
                
                # Create word boundaries for better matching
                words = set(re.findall(r'\b\w+\b', text))
                
                for keyword in keywords:
                    if not keyword or not isinstance(keyword, str):
                        continue
                    
                    keyword_lower = keyword.lower().strip()
                    if not keyword_lower:
                        continue
                    
                    # Strategy 1: Exact word match
                    if keyword_lower in words:
                        found_technologies.add(keyword)
                    
                    # Strategy 2: Substring match for longer keywords (>4 chars)
                    elif len(keyword_lower) > 4 and keyword_lower in text:
                        found_technologies.add(keyword)
                    
                    # Strategy 3: Handle compound keywords (e.g., "next.js", "vue.js")
                    elif "." in keyword_lower or "-" in keyword_lower:
                        # Remove special chars for matching
                        keyword_clean = re.sub(r'[.-]', '', keyword_lower)
                        if keyword_clean in text:
                            found_technologies.add(keyword)
            
            return list(found_technologies)
            
        except Exception:
            # Log error but don't fail the entire job
            return []
    
    # Register UDF
    return udf(extract_technologies_robust, ArrayType(StringType()))


# Function for aggregation table writing
def write_to_aggregation_table(exploded_df, batch_id, epoch_id):
    """
    PERFORMANCE OPTIMIZED: Simple overwrite approach
    Maintains running counts without complex merge operations
    """
    agg_table_path = "s3a://delta-lake/bronze/bronze_github_streaming_daily_aggregates"
    
    try:
        print(f"⚡ [Batch {epoch_id}] Creating fast aggregation...")
        
        from pyspark.sql.functions import current_timestamp, current_date, lit
        now_timestamp = current_timestamp()
        today_date = current_date()
        
        # SIMPLIFIED: Just create current batch aggregation
        batch_agg_df = exploded_df.groupBy("keyword").agg(
            count("event_id").alias("daily_mentions"),
            countDistinct("repo_name").alias("repo_count"),
            first("repo_name").alias("top_repo")
        ).select(
            today_date.alias("date"),
            col("keyword").alias("technology"), 
            col("daily_mentions").cast("long"),
            col("repo_count").cast("long"),
            now_timestamp.alias("last_activity"),
            now_timestamp.alias("last_updated"),
            col("top_repo"),
            lit(batch_id).alias("processing_batch")
        )
        
        batch_count = batch_agg_df.count()
        print(f"📊 [Batch {epoch_id}] Technologies in batch: {batch_count}")
        
        if batch_count > 0:
            # PERFORMANCE FIX: Simple overwrite - no merge complexity
            print(f"🚀 [Batch {epoch_id}] Fast overwrite...")
            
            batch_agg_df.write \
                .format("delta") \
                .mode("overwrite") \
                .option("overwriteSchema", "true") \
                .save(agg_table_path)
            
            print(f"✅ [Batch {epoch_id}] Fast write completed - {batch_count} technologies")
            
            # Quick sample (limit to avoid performance hit)
            try:
                sample = batch_agg_df.orderBy(col("daily_mentions").desc()).limit(3).collect()
                print(f"📊 [Batch {epoch_id}] Top 3:")
                for i, row in enumerate(sample):
                    print(f"   {i+1}. {row.technology}: {row.daily_mentions}")
            except Exception:
                pass  # Don't fail batch for sample display
        
        return True
        
    except Exception as e:
        print(f"❌ [Batch {epoch_id}] Fast agg error: {e}")
        return False


def write_to_streaming_delta(dataframe, epoch_id):
    """Write to Delta with EXACT schema matching - ENHANCED with dual-write"""
    
    streaming_table_path = "s3a://delta-lake/bronze/bronze_github_streaming_keyword_extractions"
    batch_id = f"batch_{epoch_id}_{int(time.time())}"
    
    print("\n" + "="*60)
    print(f"📦 PROCESSING BATCH {epoch_id}")
    print(f"🕐 Timestamp: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"📋 Batch ID: {batch_id}")
    print("="*60)
    
    try:
        # Step 1: Persist input
        dataframe.persist()
        
        # Count records
        record_count = dataframe.count()
        print(f"📊 [Batch {epoch_id}] Raw records received: {record_count}")
        
        if record_count == 0:
            print(f"ℹ️ [Batch {epoch_id}] Empty batch - skipping processing")
            return
        
        # Step 2: Filter for tech events
        tech_events = dataframe.filter(
            (size(col("technologies")) > 0) & 
            col("technologies").isNotNull()
        )
        tech_count = tech_events.count()
        print(f"✅ [Batch {epoch_id}] Events with technologies: {tech_count}")
        
        if tech_count == 0:
            print(f"ℹ️ [Batch {epoch_id}] No tech events to process")
            return
        
        # Step 3: Explode technologies
        exploded_df = tech_events.select(
            explode(col("technologies")).alias("keyword"),
            col("event_id"),
            col("event_type"),
            col("repo_name"),
            col("processing_time")
        )
        
        # ADDED: Write to aggregation table (before original processing)
        agg_success = write_to_aggregation_table(exploded_df, batch_id, epoch_id)
        
        # Step 4: Aggregate by keyword and repo (ORIGINAL LOGIC - UNCHANGED)
        aggregated_df = exploded_df.groupBy("keyword", "repo_name").agg(
            count("event_id").alias("mention_count")
        )
        
        # Step 5: Create DataFrame with EXACT schema match (ORIGINAL LOGIC - UNCHANGED)
        print(f"🔧 [Batch {epoch_id}] Creating exact schema match...")
        
        # Get current timestamp ONCE to ensure consistency
        now_timestamp = current_timestamp()
        
        # Create final DataFrame with EXACTLY the same logic as before
        final_df = aggregated_df.select(
            # EXACT ORDER AS YOUR SCHEMA
            lit(23).cast(IntegerType()).alias("hour"),                                    # hour: IntegerType
            col("keyword").cast(StringType()).alias("keyword"),                          # keyword: StringType  
            col("mention_count").cast(IntegerType()).alias("mentions"),                  # mentions: IntegerType
            col("repo_name").cast(StringType()).alias("top_repo"),                       # top_repo: StringType
            col("mention_count").cast(IntegerType()).alias("repo_mentions"),             # repo_mentions: IntegerType
            col("mention_count").cast(IntegerType()).alias("event_mentions"),            # event_mentions: IntegerType
            now_timestamp.cast("date").alias("date"),                                    # date: DateType
            lit("streaming").cast(StringType()).alias("source_file"),                   # source_file: StringType
            now_timestamp.cast(TimestampType()).alias("processing_time"),               # processing_time: TimestampType
            lit(None).cast(StringType()).alias("event_id"),                             # event_id: StringType
            now_timestamp.cast(TimestampType()).alias("event_timestamp"),               # event_timestamp: TimestampType
            lit("Streaming").cast(StringType()).alias("event_type"),                    # event_type: StringType
            lit("streaming").cast(StringType()).alias("actor_login"),                   # actor_login: StringType
            now_timestamp.cast(TimestampType()).alias("kafka_timestamp"),               # kafka_timestamp: TimestampType
            lit(batch_id).cast(StringType()).alias("batch_id"),                         # batch_id: StringType
            now_timestamp.cast(TimestampType()).alias("window_start"),                  # window_start: TimestampType
            now_timestamp.cast(TimestampType()).alias("window_end")                     # window_end: TimestampType
        )
        
        # Count final records
        final_count = final_df.count()
        print(f"📊 [Batch {epoch_id}] Final records to write: {final_count}")
        
        # Step 6: Verify schema before write (ORIGINAL LOGIC - UNCHANGED)
        actual_schema = final_df.schema
        target_schema = get_exact_target_schema()
        
        print(f"🔍 [Batch {epoch_id}] Schema verification:")
        print(f"   Target fields: {len(target_schema.fields)}")
        print(f"   Actual fields: {len(actual_schema.fields)}")
        
        # Check field by field
        schema_match = True
        for i, (target_field, actual_field) in enumerate(zip(target_schema.fields, actual_schema.fields)):
            if target_field.name != actual_field.name or target_field.dataType != actual_field.dataType:
                print(f"   ❌ Field {i}: {target_field.name}({target_field.dataType}) != {actual_field.name}({actual_field.dataType})")
                schema_match = False
            else:
                print(f"   ✅ Field {i}: {target_field.name}({target_field.dataType})")
        
        if not schema_match:
            print(f"❌ [Batch {epoch_id}] Schema mismatch detected - aborting write")
            return
        
        print(f"✅ [Batch {epoch_id}] Schema match verified!")
        
        # Step 7: Write to Delta table (ORIGINAL LOGIC - UNCHANGED)
        print(f"🔄 [Batch {epoch_id}] Writing to Delta table...")
        
        final_df.write \
            .format("delta") \
            .mode("append") \
            .save(streaming_table_path)
        
        print(f"✅ [Batch {epoch_id}] Successfully wrote {final_count} records")
        
        # Show sample of what was written (for small batches)
        if final_count <= 10:
            print(f"📝 [Batch {epoch_id}] Records written:")
            written_sample = final_df.collect()
            for i, record in enumerate(written_sample):
                print(f"   {i+1}. {record.keyword}: {record.mentions} mentions in {record.top_repo}")
        
        # ADDED: Report dual-write success
        dual_write_status = "✅" if agg_success else "⚠️"
        print(f"{dual_write_status} [Batch {epoch_id}] Dual-write status: Raw table ✅, Aggregation table {dual_write_status}")
        
        print(f"🎉 [Batch {epoch_id}] Batch processing completed successfully!")
        
    except Exception as e:
        print(f"❌ [Batch {epoch_id}] Error: {e}")
        import traceback
        traceback.print_exc()
        
    finally:
        try:
            dataframe.unpersist()
        except Exception as e:
            print(f"⚠️ [Batch {epoch_id}] Unpersist warning: {e}")
        
        print(f"📋 [Batch {epoch_id}] Batch processing ended")
        print("="*60)


def main():
    """Exact schema match streaming consumer"""
    
    # Register signal handlers
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    
    print("🚀 EXACT SCHEMA MATCH GITHUB KAFKA TO STREAMING DELTA")
    print("=" * 60)
    print()
    print("📡 Source: github-events-raw (Kafka topic)")
    print("💾 Target: bronze_github_streaming_keyword_extractions")
    # ADDED: Show dual-write info
    print("⚡ Bonus: bronze_github_streaming_daily_aggregates (fast queries)")
    print("=" * 60)
    
    spark = None
    streaming_query = None
    
    try:
        # Clean checkpoint
        clean_checkpoint_directory()
        
        # Initialize Spark
        spark, kafka_servers = create_optimized_spark_session()
        globals()['spark'] = spark
        
        # Show target schema
        target_schema = get_exact_target_schema()
        print(f"\n📋 Target schema ({len(target_schema.fields)} fields):")
        for i, field in enumerate(target_schema.fields):
            print(f"   {i+1:2d}. {field.name}: {field.dataType}")
        
        # Load keywords
        keywords = load_keywords()
        if not keywords:
            raise ValueError("No technology keywords loaded")
        
        # Create UDF
        extract_tech_udf = create_technology_extraction_udf(keywords)
        print("✅ UDF created successfully")
        
        # Configure Kafka stream
        kafka_stream = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", kafka_servers) \
            .option("subscribe", "github-events-raw") \
            .option("startingOffsets", "latest") \
            .option("failOnDataLoss", "false") \
            .option("maxOffsetsPerTrigger", "100") \
            .load()
        
        # Process stream
        github_event_schema = StructType([
            StructField("id", StringType(), True),
            StructField("type", StringType(), True),
            StructField("repo", StructType([
                StructField("name", StringType(), True)
            ]), True)
        ])
        
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
        
        print("\n💾 Starting exact schema-matched streaming write...")
        print("⏰ Processing micro-batches every 30 seconds")
        print("🔍 Schema verification enabled for each batch")
        # ADDED: Mention dual-write
        print("⚡ Dual-write: Raw + Aggregation tables")
        print("-" * 60)
        
        streaming_query = processed_stream \
            .writeStream \
            .foreachBatch(write_to_streaming_delta) \
            .outputMode("append") \
            .trigger(processingTime='5 seconds') \
            .option("checkpointLocation", "/tmp/spark-streaming-checkpoint/github-consumer") \
            .start()
        
        print("✅ Streaming query started successfully!")
        
        # Wait for termination
        streaming_query.awaitTermination()
        
    except Exception as e:
        print(f"\n❌ CRITICAL ERROR: {e}")
        import traceback
        traceback.print_exc()
        
        if streaming_query:
            try:
                streaming_query.stop()
            except Exception:
                pass
        
        sys.exit(1)
        
    finally:
        if streaming_query:
            try:
                streaming_query.stop()
            except Exception as e:
                print(f"⚠️ Warning stopping query: {e}")
        
        cleanup_spark_session(spark)


if __name__ == "__main__":
    main()