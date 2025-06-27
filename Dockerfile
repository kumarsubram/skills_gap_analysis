FROM apache/airflow:3.0.0

USER root

# Install Java 17 (required for Spark)
RUN apt-get update && \
    apt-get install -y openjdk-17-jdk wget && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Auto-detect Java path
RUN JAVA_PATH=$(find /usr/lib/jvm -name "java-17-openjdk*" -type d | head -1) && \
    echo "JAVA_HOME=$JAVA_PATH" >> /etc/environment && \
    ln -sf $JAVA_PATH /usr/lib/jvm/default-java

ENV JAVA_HOME=/usr/lib/jvm/default-java

# Create Spark directory structure
RUN mkdir -p /opt/spark/jars

# 🚀 ADD DELTA LAKE JARS (same as Spark containers)
RUN echo "📦 Installing Delta Lake JARs for Airflow..." && \
    wget -q https://repo1.maven.org/maven2/io/delta/delta-spark_2.13/4.0.0/delta-spark_2.13-4.0.0.jar \
         -O /opt/spark/jars/delta-spark_2.13-4.0.0.jar && \
    wget -q https://repo1.maven.org/maven2/io/delta/delta-storage/4.0.0/delta-storage-4.0.0.jar \
         -O /opt/spark/jars/delta-storage-4.0.0.jar && \
    wget -q https://repo1.maven.org/maven2/org/antlr/antlr4-runtime/4.9.3/antlr4-runtime-4.9.3.jar \
         -O /opt/spark/jars/antlr4-runtime-4.9.3.jar && \
    echo "✅ Delta Lake JARs installed in Airflow"

# 🚀 ADD SPARK KAFKA CONNECTOR JARS (CRITICAL FOR STREAMING - THE MISSING PIECE!)
RUN echo "📦 Installing Kafka connector JARs (Spark 4.0 compatible)..." && \
    # Core Spark-Kafka connector (verified from official docs)
    wget -q https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.13/4.0.0/spark-sql-kafka-0-10_2.13-4.0.0.jar \
         -O /opt/spark/jars/spark-sql-kafka-0-10_2.13-4.0.0.jar && \
    wget -q https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.13/4.0.0/spark-token-provider-kafka-0-10_2.13-4.0.0.jar \
         -O /opt/spark/jars/spark-token-provider-kafka-0-10_2.13-4.0.0.jar && \
    # Updated Kafka clients (latest stable compatible with Spark 4.0)
    wget -q https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.9.0/kafka-clients-3.9.0.jar \
         -O /opt/spark/jars/kafka-clients-3.9.0.jar && \
    # Compression libraries (updated versions)
    wget -q https://repo1.maven.org/maven2/org/lz4/lz4-java/1.8.0/lz4-java-1.8.0.jar \
         -O /opt/spark/jars/lz4-java-1.8.0.jar && \
    wget -q https://repo1.maven.org/maven2/org/xerial/snappy/snappy-java/1.1.10.7/snappy-java-1.1.10.7.jar \
         -O /opt/spark/jars/snappy-java-1.1.10.7.jar && \
    wget -q https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.12.0/commons-pool2-2.12.0.jar \
         -O /opt/spark/jars/commons-pool2-2.12.0.jar && \
    echo "✅ Kafka + compression JARs installed (Spark 4.0 compatible)"

# Add S3A support JARs (SPARK 4.0 COMPATIBLE - BACK TO WORKING VERSIONS)
RUN echo "📦 Installing S3A JARs (Spark 4.0 compatible - proven working)..." && \
    # Remove any existing S3A JARs
    rm -f /opt/spark/jars/hadoop-aws-*.jar /opt/spark/jars/aws-*sdk*.jar && \
    # Use hadoop-aws 3.3.6 (AWS SDK V1) - most stable for Spark 4.0
    wget -q https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.6/hadoop-aws-3.3.6.jar \
         -O /opt/spark/jars/hadoop-aws-3.3.6.jar && \
    wget -q https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.367/aws-java-sdk-bundle-1.12.367.jar \
         -O /opt/spark/jars/aws-java-sdk-bundle-1.12.367.jar && \
    echo "✅ S3A JARs installed (hadoop-aws 3.3.6 + AWS SDK V1 1.12.367 - proven stable)"

# Set ownership for airflow user
RUN chown -R airflow:root /opt/spark

# 🔄 SWITCH BACK TO AIRFLOW USER FOR PYTHON PACKAGES
USER airflow

# ✅ ADD PYTHON PACKAGE INSTALLATION (BUILD TIME, NOT RUNTIME!)
RUN echo "📦 Installing critical Python packages at build time..." && \
    pip install --no-cache-dir \
    apache-airflow-providers-apache-spark \
    deltalake \
    pyarrow \
    apache-airflow-providers-fab==2.0.2 \
    confluent-kafka \
    requests && \
    echo "✅ Python packages installed successfully"