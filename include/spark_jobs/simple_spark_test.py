from pyspark.sql import SparkSession

def main():
    # Create Spark session
    spark = SparkSession.builder \
        .appName("CleanSparkTest") \
        .getOrCreate()
    
    print("🚀 Spark job started!")
    print(f"📊 Spark version: {spark.version}")
    print(f"🔧 Master: {spark.sparkContext.master}")
    
    # Simple RDD operations (lightweight)
    print("\n🔢 Creating data...")
    numbers = spark.sparkContext.parallelize([1, 2, 3, 4, 5])
    
    print("✅ Computing sum...")
    total = numbers.sum()
    
    print("✅ Computing count...")
    count = numbers.count()
    
    print(f"✅ Results: Sum={total}, Count={count}")
    print("🎉 Job completed successfully!")
    
    spark.stop()

if __name__ == "__main__":
    main()