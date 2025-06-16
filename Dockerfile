FROM apache/airflow:3.0.0

USER root

# Install Java 17
RUN apt-get update && \
    apt-get install -y openjdk-17-jdk && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Auto-detect Java path
RUN JAVA_PATH=$(find /usr/lib/jvm -name "java-17-openjdk*" -type d | head -1) && \
    echo "JAVA_HOME=$JAVA_PATH" >> /etc/environment && \
    ln -sf $JAVA_PATH /usr/lib/jvm/default-java

ENV JAVA_HOME=/usr/lib/jvm/default-java

USER airflow
