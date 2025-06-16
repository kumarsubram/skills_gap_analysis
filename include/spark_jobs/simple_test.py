from pyspark.sql import SparkSession

def main():
    # Create Spark session
    spark = SparkSession.builder \
        .appName("SimpleWorkingTest") \
        .getOrCreate()
        
    print("🚀 Spark cluster job started!")
    print(f"📊 Spark version: {spark.version}")
    print(f"🔧 Master URL: {spark.sparkContext.master}")
    print(f"🏭 App ID: {spark.sparkContext.applicationId}")
    print(f"🐍 Python version being used: {spark.sparkContext.pythonVer}")
    
    # Simple RDD test (lighter than DataFrame operations)
    print("\n🔢 Testing simple RDD operations...")
    numbers = spark.sparkContext.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
    
    # Basic operations
    total = numbers.sum()
    count = numbers.count()
    max_val = numbers.max()
    
    print(f"✅ Sum: {total}")
    print(f"✅ Count: {count}")
    print(f"✅ Max: {max_val}")
    
    # Test distributed processing
    print("\n🔄 Testing distributed map operation...")
    squared = numbers.map(lambda x: x * x).collect()
    print(f"✅ Squared numbers: {squared}")
    
    print("\n🎉 All tests completed successfully!")
    print("✅ Distributed Spark processing is working!")
    
    # Stop Spark session
    spark.stop()

if __name__ == "__main__":
    main()