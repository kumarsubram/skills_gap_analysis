from pyspark.sql import SparkSession

def main():
    # Create Spark session
    spark = SparkSession.builder \
        .appName("SimpleDataProcessing") \
        .getOrCreate()
    
    print("🚀 Spark cluster job started!")
    print(f"📊 Spark version: {spark.version}")
    print(f"🔧 Master URL: {spark.sparkContext.master}")
    print(f"🏭 App ID: {spark.sparkContext.applicationId}")
    
    # Create sample data
    data = [
        (1, "Alice", 25, "Engineer"),
        (2, "Bob", 30, "Data Scientist"), 
        (3, "Charlie", 35, "Manager"),
        (4, "Diana", 28, "Analyst"),
        (5, "Eve", 32, "Developer")
    ]
    
    columns = ["id", "name", "age", "role"]
    df = spark.createDataFrame(data, columns)
    
    print("\n📋 Original Data:")
    df.show()
    
    # Simple data processing
    print("\n📊 Processing: Filter age > 30")
    senior_employees = df.filter(df.age > 30)
    senior_employees.show()
    
    print("\n📈 Processing: Average age by role")
    avg_age_by_role = df.groupBy("role").avg("age")
    avg_age_by_role.show()
    
    print(f"\n✅ Total records processed: {df.count()}")
    print(f"✅ Senior employees (>30): {senior_employees.count()}")
    
    print("\n🎉 Spark job completed successfully!")
    
    # Stop Spark session
    spark.stop()

if __name__ == "__main__":
    main()