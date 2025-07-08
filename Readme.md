# 🚀 Modern Data Platform: Airflow + Spark + Kafka + Delta Lake

A production-ready data platform implementing medallion architecture (Bronze → Silver → Gold → Analytics) for processing GitHub events and job postings. Built with **Apache Airflow 3.0**, **Apache Spark 4.0**, **Apache Kafka**, and **Delta Lake 4.0**.

## 🎯 Key Features

- **Real-time streaming** with Kafka (3-hour retention) and Spark Structured Streaming
- **Latest-only consumption** for real-time dashboards (30-second rolling windows)
- **Automatic hourly restarts** to ensure fresh data and prevent memory issues
- **Batch processing** with historical data backfill capabilities
- **Medallion architecture** for data quality and governance
- **Automated orchestration** with dependency-aware DAG scheduling
- **Resource optimization** with batch/streaming coordination
- **Delta Lake storage** for ACID transactions and time travel
- **Containerized deployment** for consistency across environments

## 📋 Technology Stack

| Component | Version | Purpose | Web UI |
|-----------|---------|---------|---------|
| **Apache Airflow** | 3.0 | Workflow orchestration | `:8085` |
| **Apache Spark** | 4.0 | Distributed processing | `:8060` |
| **Apache Kafka** | 3.9 (Confluent 7.9.1) | Event streaming | `:9090` |
| **Delta Lake** | 4.0 | Lakehouse storage | - |
| **MinIO** | Latest | S3-compatible storage | `:9001` |
| **PostgreSQL** | 15 | Airflow metadata | - |
| **Redis** | 7.2 | Celery backend | - |
| **JupyterLab** | Latest | Interactive notebooks | `:8888` |

## ⚡ Quick Start

### 1. Clone & Configure
```bash
git clone <your-repo>
cd airflow-spark
cp .env.example .env
# Edit .env with your secure credentials (see Configuration section below)
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
- **Airflow**: `http://localhost:8085` (use credentials from .env)
- **Spark**: `http://localhost:8060`
- **Kafka UI**: `http://localhost:9090` (use credentials from .env)
- **MinIO**: `http://localhost:9001` (use credentials from .env)
- **JupyterLab**: `http://localhost:8888` (use token from .env)
- **JupyterLab**: `http://localhost:8888` (use token from .env)

## 🏗️ Data Architecture

### Medallion Architecture Flow
```
GitHub Events API ──┐
                    ├─► Bronze Layer ──► Silver Layer ──► Gold Layer ──► Analytics
Job Postings APIs ──┘    (Raw Data)     (Cleaned)      (Business)     (Insights)
```

### Infrastructure Components
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

### Data Processing Layers

1. **Bronze Layer**: Raw data ingestion
   - GitHub events (streaming & batch)
   - Job postings from multiple APIs
   - Preserved in original format with metadata

2. **Silver Layer**: Cleaned and normalized data
   - Deduplicated records
   - Standardized schema
   - Technology keyword extraction

3. **Gold Layer**: Business-ready data
   - Technology classification
   - Daily activity aggregations
   - Performance optimized tables

4. **Analytics Layer**: Pre-computed insights
   - Technology trend analysis (7d, 30d, 90d, all-time)
   - Market intelligence dashboards (Future)
   - ML-ready feature stores (Future)

## 🔧 Configuration

### Environment Variables (.env)

**Important**: Copy `.env.example` to `.env` and update with secure credentials.

```bash
# System Configuration
AIRFLOW_UID=50000
VPS_IP=your_server_ip_here
AIRFLOW_ENV=local

# Database & Authentication (UPDATE THESE!)
POSTGRES_PASSWORD=your_secure_password
_AIRFLOW_WWW_USER_USERNAME=admin
_AIRFLOW_WWW_USER_PASSWORD=your_secure_password

# Object Storage (UPDATE THESE!)
MINIO_ACCESS_KEY=your_access_key
MINIO_SECRET_KEY=your_secret_key

# Application Services (UPDATE THESE!)
JUPYTER_TOKEN=your_secure_token
SPRING_SECURITY_USER_PASSWORD=your_secure_password

# Resource Allocation
SPARK_WORKER_MEMORY=6G
SPARK_WORKER_CORES=2

# Optional: GitHub API (for higher rate limits)
GITHUB_TOKEN=your_github_token
```

**Security Notes:**
- Never commit `.env` files to version control
- Use strong, unique passwords for all services
- Consider using environment-specific secrets management
- GitHub token is optional but recommended for higher API limits

### Memory Requirements
- **Minimum**: 8GB RAM
- **Recommended**: 16GB+ RAM
- **Production**: 32GB+ RAM

## 📊 Pre-configured Buckets

MinIO automatically creates these buckets:
- `delta-lake` - Delta Lake tables
- `spark-warehouse` - Spark SQL warehouse
- `kafka-data` - Streaming data archive

## 🚀 Data Pipelines

### Available DAGs

#### Streaming Pipeline (Real-time Dashboard)
- **`producer_github_events`**: Filters GitHub events by tech keywords → Kafka (3-second intervals)
- **`consumer_github_events`**: Kafka → Bronze Delta tables (5-second micro-batches, hourly restarts)
  - **Purpose**: Powers 30-second rolling dashboard with latest GitHub trends
  - **Strategy**: Always consumes from latest offset (skips old messages)
  - **Restart**: Automatic hourly restart for fresh data and memory management

#### Batch Pipeline (Scheduled)
- **`daily_github_bronze`**: Historical GitHub data → Bronze (1 PM daily)
- **`daily_github_processing_analytics`**: Silver → Gold → Analytics (2 PM daily)
- **`daily_jobs_bronze`**: Job postings APIs → Bronze (3 PM daily)
- **`daily_delta_vacuum`**: Table maintenance (5:30 PM daily)

#### Test & Monitoring DAGs
- **`hello_world_check`**: Basic Airflow connectivity test
- **`simple_spark_check`**: Spark cluster connectivity test
- **`simple_kafka_producer_check`**: Kafka connectivity test

### Pipeline Coordination

The platform uses a batch-aware streaming manager (`scripts/batch_streaming_manager`) to coordinate:

#### Real-Time Streaming Configuration
- **Kafka Retention**: 3 hours (messages older than 3 hours are deleted)
- **Consumer Strategy**: Always starts from latest messages (no lag concerns)
- **Restart Cycle**: Every hour for fresh data and consistent performance
- **Processing Interval**: 5-second micro-batches for near real-time updates

#### Daily Schedule
- **Streaming Phase**: Real-time processing (5:30 PM - 12:30 PM next day)
- **Prep Phase**: Stops streaming 30 minutes before batch (12:30 PM - 1:00 PM)
- **Batch Phase**: Historical data processing (1:00 PM - 5:00 PM)
- **Recovery Phase**: Container restarts and streaming resume (5:00 PM - 5:30 PM)

#### Starting the Manager
```bash
# Run in screen session (recommended)
screen -S streaming -dm ./scripts/batch_streaming_manager monitor

# Check status
./scripts/batch_streaming_manager status

# Manual control
./scripts/batch_streaming_manager stop    # Stop streaming
./scripts/batch_streaming_manager start   # Start streaming
```

### Data Processing Examples

#### 1. Manual DAG Execution
```bash
# Trigger specific DAG
docker compose exec airflow-webserver airflow dags trigger daily_github_bronze

# Trigger with custom date
docker compose exec airflow-webserver airflow dags trigger daily_github_bronze \
  --conf '{"date": "2025-01-15"}'
```

#### 2. Delta Lake Time Travel
```python
# Read latest data
df = spark.read.format("delta").load("s3a://delta-lake/gold/gold_github_technology_daily_activity")

# Read data from specific date
df_historical = spark.read.format("delta") \
    .option("timestampAsOf", "2025-01-01") \
    .load("s3a://delta-lake/gold/gold_github_technology_daily_activity")
```

#### 3. Streaming Data Query
```python
# Query real-time streaming data
streaming_df = spark.read.format("delta") \
    .load("s3a://delta-lake/bronze/bronze_github_streaming_keyword_extractions")
```

## 🛠️ Development & Operations

### Common Development Tasks

```bash
# Build and restart services
docker compose build --no-cache
docker compose up -d

# View logs for debugging
docker compose logs -f airflow-scheduler
docker compose logs -f spark-master

# Access container shells
docker compose exec airflow-webserver bash
docker compose exec spark-master bash

# Restart specific services
docker compose restart airflow-scheduler
docker compose restart spark-master

# Scale Spark workers
docker compose up -d --scale spark-worker=4
```

### Management Scripts

```bash
# Clean old logs (runs inside containers)
./cleanup_logs.sh

# Clean DAG run history
./cleanup_dag_history.sh

# List all Delta tables
./list_delta_tables.sh

# Deploy to production VPS
./deploy_to_vps.sh

# Start streaming coordination (runs in background)
./scripts/batch_streaming_manager monitor
```

### Adding New Components

1. **New DAG**: Create Python file in `dags/` directory
2. **New Spark Job**: Add to `include/spark_jobs/`
3. **Utility Functions**: Add to `include/utils/`
4. **Data Sources**: Update collectors in `include/jobs/`
5. **Keywords/Config**: Edit JSON files in `include/jsons/`

## 🔍 Troubleshooting

### Common Issues

**Services not starting:**
```bash
# Check system resources and container status
docker system df
docker compose ps

# View specific service logs
docker compose logs -f <service-name>

# Clean restart all services
docker compose down && docker compose up -d
```

**Memory/Resource Issues:**
```bash
# Reduce Spark worker memory in .env
SPARK_WORKER_MEMORY=3G  # Instead of 6G

# Scale down workers temporarily
docker compose up -d --scale spark-worker=1
```

**Data Pipeline Issues:**
```bash
# Check table status
./list_delta_tables.sh

# Verify DAG runs in Airflow UI
open http://localhost:8085

# Check streaming coordination
tail -f /tmp/batch_streaming.log
```

**Connection Problems:**
```bash
# Test Kafka connectivity
docker compose exec kafka kafka-topics --list --bootstrap-server kafka:29092

# Test MinIO access
docker compose exec airflow-webserver mc ls minio/delta-lake

# Test Spark connectivity
docker compose exec airflow-webserver /home/airflow/.local/bin/spark-submit --version
```

### Health Monitoring
```bash
# Check all service health
docker compose ps

# Monitor resource usage
docker stats

# Check disk space (important for streaming)
df -h
```

### Getting Help

1. **Check logs first**: `docker compose logs -f <service>`
2. **Verify configuration**: Ensure `.env` has correct credentials
3. **Resource allocation**: Confirm sufficient RAM/disk space
4. **Network connectivity**: Test service-to-service communication
5. **Data consistency**: Verify Delta table integrity

## 📁 Project Structure

```
airflow-spark/
├── dags/                  # Airflow DAG definitions
│   ├── daily_github_bronze.py
│   ├── daily_github_processing_analytics.py
│   ├── daily_jobs_bronze.py
│   ├── consumer_github_events.py
│   ├── producer_github_events.py
│   ├── daily_delta_vacuum.py
│   └── *_check.py        # Test DAGs
├── include/              # Shared utilities and jobs
│   ├── spark_jobs/       # Spark processing scripts
│   ├── utils/            # Helper functions
│   ├── jobs/             # Job posting collectors
│   ├── github/           # GitHub data processing
│   ├── storage/          # MinIO/S3 utilities
│   ├── streaming/        # Kafka streaming utilities
│   ├── schemas/          # Data schemas
│   └── jsons/            # Configuration files
├── notebooks/            # Jupyter exploration notebooks
├── scripts/              # Management scripts
│   └── batch_streaming_manager  # Coordination script
├── config/               # Airflow configuration
├── logs/                 # Airflow logs
├── data/                 # Local data storage
├── docker-compose.yaml   # Service orchestration
├── Dockerfile*           # Custom container images
├── .env.example          # Environment template
├── .env                  # Environment variables (create from .example)
└── README.md             # This documentation
```

## 🚀 Getting Started

### Initial Setup Checklist

1. **✅ Environment Setup**
   ```bash
   cp .env.example .env
   # Edit .env with secure credentials
   ```

2. **✅ Start Platform**
   ```bash
   docker compose up -d
   ```

3. **✅ Verify Services**
   - Open Airflow UI: http://localhost:8085
   - Check all services: `docker compose ps`

4. **✅ Test Connectivity**
   ```bash
   # Run test DAGs
   docker compose exec airflow-webserver airflow dags trigger hello_world_check
   docker compose exec airflow-webserver airflow dags trigger simple_spark_check
   ```

5. **✅ Start Data Processing**
   ```bash
   # Begin historical data processing
   docker compose exec airflow-webserver airflow dags trigger daily_github_bronze
   
   # Monitor progress in Airflow UI
   ```

### Next Steps

1. **Explore Data**: Use JupyterLab notebooks at http://localhost:8888
2. **Monitor Pipelines**: Check DAG execution in Airflow UI
3. **Query Results**: Access Delta tables via Spark or notebooks
4. **Scale Operations**: Adjust resource allocation in `.env`
5. **Add Data Sources**: Extend collectors in `include/jobs/`

## 📚 Additional Resources

- **Platform Documentation**: See `CLAUDE.md` for detailed guidance
- **Apache Airflow**: [Official Documentation](https://airflow.apache.org/docs/)
- **Apache Spark 4.0**: [Latest Features](https://spark.apache.org/docs/latest/)
- **Delta Lake 4.0**: [Advanced Features](https://docs.delta.io/latest/)
- **Medallion Architecture**: [Best Practices](https://databricks.com/glossary/medallion-architecture)

## 🛡️ Security & Production Notes

- **Never commit `.env` files** - they contain sensitive credentials
- **Use strong passwords** for all services in production
- **Enable SSL/TLS** for production deployments
- **Monitor resource usage** - streaming can consume significant disk space
- **Regular backups** of Delta tables and configurations
- **Network security** - restrict access to web interfaces in production

---

**🎯 Production-ready data platform for modern analytics and ML workflows**