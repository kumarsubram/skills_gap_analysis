"""
Streaming Utils - Essential Reusable Functions
==============================================

Essential functions extracted from consumer DAG for reusability.
Place at: include/streaming/streaming_utils.py
"""

import os
import sys
import subprocess
import time
from datetime import datetime
from pathlib import Path

# Add project root for imports
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))


def check_prerequisites() -> bool:
    """
    Check streaming job prerequisites - extracted from DAG
    Returns: True if ready to proceed
    """
    print("🔍 STREAMING PREREQUISITES CHECK")
    print("=" * 50)
    
    checks_passed = 0
    total_checks = 4
    
    # Check 1: Streaming table
    try:
        from include.streaming.streaming_table_utils import check_streaming_table_exists
        if check_streaming_table_exists():
            print("✅ Streaming Delta table exists")
            checks_passed += 1
        else:
            print("❌ Streaming Delta table missing")
            print("💡 Run: python include/streaming/streaming_table_utils.py --ensure")
    except Exception as e:
        print(f"❌ Error checking streaming table: {e}")
    
    # Check 2: MinIO
    try:
        from include.storage.minio_connect import get_minio_client
        client = get_minio_client()
        client.list_buckets()
        print("✅ MinIO connection working")
        checks_passed += 1
    except Exception as e:
        print(f"❌ MinIO connection failed: {e}")
    
    # Check 3: Kafka topic
    try:
        from confluent_kafka import Consumer
        
        # Always localhost for container communication
        kafka_servers = 'kafka:29092'
        print(f"🔍 Testing Kafka: {kafka_servers}")
        
        consumer = Consumer({
            'bootstrap.servers': kafka_servers,
            'group.id': 'test-connection', 
            'auto.offset.reset': 'latest'
        })
        metadata = consumer.list_topics(timeout=10)
        consumer.close()
        
        if 'github-events-raw' in metadata.topics:
            print("✅ Kafka topic 'github-events-raw' exists")
            checks_passed += 1
        else:
            print("❌ Kafka topic missing - start producer first")
    except Exception as e:
        print(f"❌ Kafka connection error: {e}")
    
    # Check 4: Spark cluster
    try:
        import requests
        response = requests.get("http://spark-master:8080", timeout=5)
        if response.status_code == 200:
            print("✅ Spark cluster accessible")
            checks_passed += 1
        else:
            print("❌ Spark cluster not responding")
    except Exception as e:
        print(f"❌ Spark cluster check failed: {e}")
    
    print(f"\n📊 Prerequisites: {checks_passed}/{total_checks} passed")
    success = checks_passed >= 3  # Allow some flexibility
    
    if success:
        print("🎉 Ready to proceed")
    else:
        print("❌ Too many issues - fix before proceeding")
    
    return success


def monitor_spark_process(process, timeout_minutes=15) -> dict:
    """
    Monitor Spark process with real-time output - extracted from DAG
    Returns: dict with status and runtime info
    """
    start_time = time.time()
    timeout_seconds = timeout_minutes * 60
    last_output_time = start_time
    total_lines = 0
    
    print(f"🔄 Monitoring process (PID: {process.pid}) for {timeout_minutes} minutes...")
    print("📊 Real-time output:")
    print("-" * 50)
    
    while True:
        try:
            # Check if process ended
            if process.poll() is not None:
                elapsed = time.time() - start_time
                print(f"\n🛑 Process ended after {elapsed/60:.1f} minutes")
                return {
                    'status': 'completed',
                    'return_code': process.returncode,
                    'runtime_minutes': elapsed / 60,
                    'total_lines': total_lines
                }
            
            # Check timeout
            elapsed = time.time() - start_time
            if elapsed > timeout_seconds:
                print(f"\n⏰ Timeout reached ({timeout_minutes} minutes)")
                process.terminate()
                try:
                    process.wait(timeout=30)
                except subprocess.TimeoutExpired:
                    process.kill()
                return {
                    'status': 'timeout',
                    'return_code': 'timeout',
                    'runtime_minutes': elapsed / 60,
                    'total_lines': total_lines
                }
            
            # Read output
            if process.stdout and process.stdout.readable():
                line = process.stdout.readline()
                if line:
                    line = line.strip()
                    if line:
                        total_lines += 1
                        last_output_time = time.time()
                        timestamp = datetime.now().strftime("%H:%M:%S")
                        
                        # Filter and highlight important messages only
                        skip_patterns = [
                            'INFO SparkContext:', 'INFO TaskSetManager:', 'INFO Executor:',
                            'INFO BlockManagerInfo:', 'INFO DAGScheduler:', 'connections.max.idle.ms',
                            'bootstrap.servers', 'client.dns.lookup', 'metadata.max.age.ms',
                            'auto.include.jmx.reporter', 'default.api.timeout.ms'
                        ]
                        
                        # Skip verbose Spark logs
                        # if any(pattern in line for pattern in skip_patterns):
                        #     continue
                        
                        # Highlight important messages
                        if any(word in line.lower() for word in ['error', 'exception', 'failed']):
                            print(f"[{timestamp}] ❌ {line}")
                        elif any(word in line.lower() for word in ['warning', 'warn']):
                            print(f"[{timestamp}] ⚠️  {line}")
                        elif any(word in line.lower() for word in ['batch', 'records', 'wrote', 'processed']):
                            print(f"[{timestamp}] 📊 {line}")
                        elif any(word in line.lower() for word in ['started', 'completed', 'success']):
                            print(f"[{timestamp}] ✅ {line}")
                        # elif 'INFO' not in line:  # Only print non-INFO lines
                        #     print(f"[{timestamp}] {line}")
            
            # Check for silence (5 minutes)
            if time.time() - last_output_time > 300:
                print(f"\n⚠️  No output for 5 minutes - process status: {'Running' if process.poll() is None else 'Stopped'}")
                last_output_time = time.time()
            
            time.sleep(1)
            
        except KeyboardInterrupt:
            print("\n🛑 Interrupted - terminating process")
            process.terminate()
            elapsed = time.time() - start_time
            return {
                'status': 'interrupted',
                'return_code': 'interrupted',
                'runtime_minutes': elapsed / 60,
                'total_lines': total_lines
            }
        except Exception as e:
            print(f"\n❌ Monitoring error: {e}")
            elapsed = time.time() - start_time
            return {
                'status': 'error',
                'error': str(e),
                'runtime_minutes': elapsed / 60,
                'total_lines': total_lines
            }


def build_spark_submit_command(job_script_path, timeout_minutes=15) -> list:
    """
    Build spark-submit command with ALL required packages
    FIXED: Includes Delta Lake + S3A + Kafka packages
    """
    print(f"🔧 Building spark-submit command for {timeout_minutes} minute job")

    return [
        "/home/airflow/.local/bin/spark-submit",
        "--master", "spark://spark-master:7077",
        #"--master", "local[2]",

        # 🚀 COMPLETE PACKAGES - Delta Lake + S3A + Kafka
       "--jars", "/opt/spark/jars/delta-spark_2.13-4.0.0.jar,"
           "/opt/spark/jars/delta-storage-4.0.0.jar,"
           "/opt/spark/jars/spark-sql-kafka-0-10_2.13-4.0.0.jar,"
           "/opt/spark/jars/kafka-clients-3.9.0.jar,"
           "/opt/spark/jars/spark-token-provider-kafka-0-10_2.13-4.0.0.jar,"
           "/opt/spark/jars/hadoop-aws-3.3.6.jar,"
           "/opt/spark/jars/aws-java-sdk-bundle-1.12.367.jar",

        #"--packages", "io.delta:delta-spark_2.13:4.0.0,org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0,org.apache.hadoop:hadoop-aws:3.3.6",


        # Resource constraints
        "--conf", "spark.dynamicAllocation.enabled=false",
        "--conf", "spark.cores.max=1",
        "--conf", "spark.executor.cores=1",
        "--conf", "spark.executor.memory=2g",
        "--conf", "spark.driver.memory=1g",
        "--conf", "spark.driver.maxResultSize=128m",
        "--conf", "spark.serializer=org.apache.spark.serializer.KryoSerializer",
        "--conf", "spark.driver.host=airflow-worker",
        "--deploy-mode", "client",

        # Streaming configs
        "--conf", "spark.sql.streaming.checkpointLocation=/tmp/spark-streaming-checkpoint",
        "--conf", "spark.sql.streaming.forceDeleteTempCheckpointLocation=true",
        "--conf", "spark.sql.streaming.metricsEnabled=true",
        "--conf", "spark.sql.streaming.stopGracefullyOnShutdown=true",

        # Delta Lake
        "--conf", "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension",
        "--conf", "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog",

        # S3A configs (from environment) - CRITICAL FOR MINIO
        "--conf", f"spark.hadoop.fs.s3a.endpoint=http://{os.getenv('MINIO_ENDPOINT')}",
        "--conf", f"spark.hadoop.fs.s3a.access.key={os.getenv('MINIO_ACCESS_KEY')}",
        "--conf", f"spark.hadoop.fs.s3a.secret.key={os.getenv('MINIO_SECRET_KEY')}",
        "--conf", "spark.hadoop.fs.s3a.path.style.access=true",
        "--conf", "spark.hadoop.fs.s3a.connection.ssl.enabled=false",
        "--conf", "spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        "--conf", "spark.hadoop.fs.s3a.threads.keepalivetime=60",
        "--conf", "spark.hadoop.fs.s3a.connection.establish.timeout=30000",
        "--conf", "spark.hadoop.fs.s3a.connection.timeout=200000",
        "--conf", "spark.hadoop.fs.s3a.connection.ttl=300000",
        "--conf", "spark.hadoop.fs.s3a.retry.interval=500",
        "--conf", "spark.hadoop.fs.s3a.retry.throttle.interval=100",
        "--conf", "spark.hadoop.fs.s3a.assumed.role.session.duration=1800000",
        "--conf", "spark.hadoop.fs.s3a.multipart.purge.age=86400000",
        "--conf", "spark.hadoop.fs.s3a.connection.maximum=100",
        "--conf", "spark.hadoop.fs.s3a.fast.upload=true",

        # Job script
        job_script_path
    ]


def run_streaming_job(job_script_path, timeout_minutes=15) -> dict:
    """
    Run a streaming job with monitoring - main reusable function
    Args:
        job_script_path: Path to the Spark streaming job script
        timeout_minutes: How long to run before timeout
    Returns: dict with job results
    """
    print(f"🚀 STARTING STREAMING JOB: {job_script_path}")
    print(f"⏰ Timeout: {timeout_minutes} minutes")
    print("🔧 Resources: 1 core, 2GB memory")
    
    # Build command
    spark_cmd = build_spark_submit_command(job_script_path, timeout_minutes)
    
    try:
        # Start process
        print("🔄 Starting Spark process...")
        process = subprocess.Popen(
            spark_cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            universal_newlines=True
        )
        
        print(f"✅ Process started (PID: {process.pid})")
        
        # Monitor with timeout
        result = monitor_spark_process(process, timeout_minutes)
        
        # Determine success
        if result['return_code'] == 0:
            print("\n🎉 SUCCESS: Job completed without errors")
            result['success'] = True
            result['message'] = f"Completed successfully in {result['runtime_minutes']:.1f} minutes"
        elif result['return_code'] == 'timeout':
            print("\n✅ SUCCESS: Job timed out gracefully (expected)")
            result['success'] = True
            result['message'] = f"Timed out gracefully after {result['runtime_minutes']:.1f} minutes"
        else:
            print(f"\n❌ FAILED: Job failed with return code {result['return_code']}")
            result['success'] = False
            result['message'] = f"Failed with return code {result['return_code']}"
        
        return result
        
    except Exception as e:
        print(f"\n❌ EXCEPTION: {e}")
        return {
            'status': 'error',
            'success': False,
            'error': str(e),
            'message': f"Exception: {str(e)}"
        }


if __name__ == "__main__":
    # Test the utilities
    print("🧪 Testing streaming utilities")
    
    # Test prerequisites
    print("\n1. Testing prerequisites:")
    prereq_result = check_prerequisites()
    print(f"Prerequisites result: {prereq_result}")
    
    # Test command building
    print("\n2. Testing command building:")
    test_script = "/opt/airflow/include/spark_jobs/github_kafka_to_streaming_delta.py"
    cmd = build_spark_submit_command(test_script, 10)
    print(f"Command built with {len(cmd)} arguments")
    print(f"First few args: {cmd[:5]}")
    
    print("\n✅ Utilities test complete")