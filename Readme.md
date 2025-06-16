# 🚀 Modern Data Platform: Airflow + Spark + Kafka + Delta Lake

A complete data streaming and orchestration platform with **Apache Airflow 3.0**, **Apache Spark 4.0**, **Apache Kafka**, and **Delta Lake 4.0** - all containerized for easy deployment.

## 📋 What's Included

| Component | Version | Purpose | Web UI |
|-----------|---------|---------|---------|
| **Apache Airflow** | 3.0 | Workflow orchestration | `:8085` |
| **Apache Spark** | 4.0 | Distributed processing | `:8060` |
| **Apache Kafka** | 3.9 (Confluent 7.9.1) | Event streaming | `:9090` |
| **Delta Lake** | 4.0 | Lakehouse storage | - |
| **MinIO** | Latest | S3-compatible storage | `:9001` |
| **PostgreSQL** | 15 | Airflow metadata | - |
| **Redis** | 7.2 | Celery backend | - |

## ⚡ Quick Start

### 1. Clone & Configure
```bash
git clone <your-repo>
cd airflow-spark
cp .env.example .env
# Edit .env with your settings
```

### 2. Start the Platform
```bash
# Clean build (recommended first time)
docker compose down -v --rmi all
docker compose build --no-cache
docker compose up -d

# Check all services
docker compose ps
```

### 3. Access Web Interfaces
- **Airflow**: `http://localhost:8085` (admin/adminks123)
- **Spark Master**: `http://localhost:8060`
- **Kafka UI**: `http://localhost:9090` (admin/adminks123)
- **MinIO Console**: `http://localhost:9001` (admin/adminks123)

## 🏗️ Architecture

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│   Kafka     │───▶│   Spark     │───▶│ Delta Lake  │───▶│  Analytics  │
│ (Streaming) │    │ (Processing)│    │ (Storage)   │    │   & ML      │
└─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘
       ▲                   ▲                   ▲
       │                   │                   │
┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│   Airflow   │    │   Workers   │    │    MinIO    │
│(Orchestrate)│    │ (2 nodes)   │    │ (S3 Store)  │
└─────────────┘    └─────────────┘    └─────────────┘
```

## 🔧 Configuration

### Environment Variables (.env)
```bash
# System
AIRFLOW_UID=50000
VPS_IP=your_server_ip_here

# Credentials
_AIRFLOW_WWW_USER_USERNAME=admin
_AIRFLOW_WWW_USER_PASSWORD=your_password
MINIO_ACCESS_KEY=admin
MINIO_SECRET_KEY=your_secret

# Resources
SPARK_WORKER_MEMORY=3G
SPARK_WORKER_CORES=2
```

### Memory Requirements
- **Minimum**: 8GB RAM
- **Recommended**: 16GB+ RAM
- **Production**: 32GB+ RAM

## 📊 Pre-configured Buckets

MinIO automatically creates these buckets:
- `delta-lake` - Delta Lake tables
- `spark-warehouse` - Spark SQL warehouse
- `kafka-data` - Streaming data archive

## 🎯 Common Use Cases

### 1. Batch Processing DAGs
```python
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# Submit Spark jobs via Airflow
spark_task = SparkSubmitOperator(
    task_id='process_data',
    application='/opt/airflow/dags/spark_jobs/etl.py',
    conn_id='spark_default'
)
```

### 2. Streaming Pipeline
```python
# Kafka → Spark Streaming → Delta Lake
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .load()

df.writeStream \
    .format("delta") \
    .option("checkpointLocation", "/opt/spark/checkpoints") \
    .start("s3a://delta-lake/streaming_data")
```

### 3. Delta Lake Queries
```python
# Read Delta table
df = spark.read.format("delta").load("s3a://delta-lake/my_table")

# Time travel
df_yesterday = spark.read.format("delta") \
    .option("timestampAsOf", "2024-01-01") \
    .load("s3a://delta-lake/my_table")
```

## 🛠️ Development

### Build Custom Images
```bash
# Rebuild Spark with custom dependencies
docker compose build --no-cache spark-master spark-worker-1 spark-worker-2
```

### View Logs
```bash
# Follow all logs
docker compose logs -f

# Specific service
docker compose logs -f spark-master
docker compose logs -f kafka
```

### Scaling Workers
```bash
# Scale Spark workers
docker compose up -d --scale spark-worker=4
```

## 🔍 Troubleshooting

### Common Issues

**Services not starting:**
```bash
# Check resources
docker system df
docker compose ps

# Clean restart
docker compose restart
```

**Spark packages not downloading:**
```bash
# Check Spark logs for package download
docker compose logs spark-master | grep -i delta
```

**Kafka connectivity:**
```bash
# Test Kafka from inside network
docker compose exec kafka kafka-topics --list --bootstrap-server localhost:9092
```

### Health Checks
All services include health checks. Look for `(healthy)` status:
```bash
docker compose ps
```

## 📁 Project Structure

```
airflow-spark/
├── dags/              # Airflow DAGs
├── data/              # Sample data
├── config/            # Configuration files
├── plugins/           # Airflow plugins
├── include/           # Shared utilities
├── docker-compose.yml # Main orchestration
├── Dockerfile.spark   # Custom Spark image
├── .env.example       # Environment template
└── README.md          # This file
```

## 🚀 Next Steps

1. **Create your first DAG** in `dags/` folder
2. **Test Kafka streaming** via Kafka UI
3. **Submit Spark jobs** through Airflow
4. **Query Delta tables** with time travel
5. **Build ML pipelines** with the integrated stack

## 📚 Documentation

- [Apache Airflow](https://airflow.apache.org/docs/)
- [Apache Spark](https://spark.apache.org/docs/latest/)
- [Delta Lake](https://docs.delta.io/latest/)
- [Apache Kafka](https://kafka.apache.org/documentation/)

## 🤝 Contributing

1. Fork the repository
2. Create feature branch (`git checkout -b feature/amazing-feature`)
3. Commit changes (`git commit -m 'Add amazing feature'`)
4. Push to branch (`git push origin feature/amazing-feature`)
5. Open Pull Request

---

**Built with ❤️ for modern data engineering**