# 🚀 Modern Data Platform: Airflow + Spark + Kafka + Delta Lake

A production-ready data platform implementing medallion architecture (Bronze → Silver → Gold → Analytics) for processing GitHub events and job postings. Built with **Apache Airflow 3.0**, **Apache Spark 4.0**, **Apache Kafka**, and **Delta Lake 4.0**.

## 🎯 Key Features

- **Real-time streaming** with Kafka and Spark Structured Streaming
- **Batch processing** with historical data backfill capabilities
- **Medallion architecture** for data quality and governance
- **Automated orchestration** with dependency-aware DAG scheduling
- **Delta Lake storage** for ACID transactions and time travel
- **Containerized deployment** for consistency across environments
- **Technology trend analysis** from GitHub events and job postings
- **Pre-computed analytics** for 7d, 30d, 90d, and all-time windows

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

### Prerequisites
- Docker and Docker Compose installed
- **Minimum**: 16GB RAM
- **Recommended**: 32GB+ RAM for production workloads

### 1. Clone the Repository
```bash
git clone <your-repo>
cd airflow-spark
```

### 2. Set Environment Variables
```bash
cp .env.example .env
# Edit .env with your credentials (see Configuration section below)
```

### 3. Start the Platform
```bash
docker compose up -d
```

### 4. Access Web Interfaces
- **Airflow**: `http://localhost:8085` (credentials from .env)
- **Spark**: `http://localhost:8060`
- **Kafka UI**: `http://localhost:9090` (credentials from .env)
- **MinIO**: `http://localhost:9001` (credentials from .env)
- **JupyterLab**: `http://localhost:8888` (token from .env)

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

### System Requirements
- **Minimum**: 16GB RAM
- **Recommended**: 32GB+ RAM
- **Production**: 64GB+ RAM
- **Storage**: 50GB+ available disk space

## 📊 Pre-configured Buckets

MinIO automatically creates these buckets:
- `delta-lake` - Delta Lake tables
- `spark-warehouse` - Spark SQL warehouse
- `kafka-data` - Streaming data archive

## 🚀 Data Pipelines

### Available DAGs

#### Streaming Pipeline
Streaming DAGs require manual triggering and management:

- **`producer_github_events`**: 
  - Filters GitHub events by technology keywords
  - Sends to Kafka topic `github-events-raw`
  - Processes events every 3 seconds

- **`consumer_github_events`**: 
  - Consumes from Kafka → Bronze Delta tables
  - 5-second micro-batch processing
  - Stores in `bronze_github_streaming_keyword_extractions`

#### Batch Pipeline
Batch DAGs run automatically on schedule:

- **`daily_github_bronze`**:
  - Downloads historical GitHub Archive data
  - Processes date-by-date into Bronze tables
  - Idempotent - checks if data already exists

- **`daily_github_processing_analytics`**:
  - Orchestrates Silver → Gold → Analytics pipeline
  - Creates technology trend analytics
  - Generates time-windowed insights (7d, 30d, 90d, all-time)

- **`daily_jobs_bronze`**:
  - Collects job postings from multiple APIs
  - Stores in Bronze layer for further processing

- **`daily_delta_vacuum`**:
  - Cleans up old Delta table versions
  - Optimizes storage and query performance

### DAG Scheduling

#### Streaming DAGs (Manual Trigger)
- **`producer_github_events`**: Filters GitHub events by tech keywords → Kafka
- **`consumer_github_events`**: Kafka → Bronze Delta tables
  - Processes data in 5-second micro-batches
  - Designed for real-time dashboards
  - Manual trigger and management required

#### Batch DAGs (Scheduled Daily)
- `daily_github_bronze` - Historical GitHub data → Bronze
- `daily_github_processing_analytics` - Silver → Gold → Analytics
- `daily_jobs_bronze` - Job postings APIs → Bronze
- `daily_delta_vacuum` - Table maintenance

#### Test DAGs (On-Demand)
- **`hello_world_check`**: Basic Airflow connectivity test
- **`simple_spark_check`**: Spark cluster connectivity test
- **`simple_kafka_producer_check`**: Kafka connectivity test

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

### Useful Scripts

```bash
# Clean old logs
./cleanup_logs.sh

# Clean DAG run history
./cleanup_dag_history.sh

# List all Delta tables with row counts
./list_delta_tables.sh

# Deploy to production VPS
./deploy_to_vps.sh
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

# Check Airflow scheduler logs
docker compose logs -f airflow-scheduler
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

1. **✅ Clone and Configure**
   ```bash
   git clone <your-repo>
   cd airflow-spark
   cp .env.example .env
   # Edit .env with secure credentials
   ```

2. **✅ Start Platform**
   ```bash
   docker compose up -d
   # Wait ~2-3 minutes for services to initialize
   ```

3. **✅ Verify Services**
   ```bash
   docker compose ps  # All services should be "Up"
   ```

4. **✅ Access Airflow UI**
   - Open: http://localhost:8085
   - Login with credentials from .env
   - All DAGs should be visible

5. **✅ Run Test DAGs**
   - In Airflow UI, trigger `hello_world_check`
   - Verify it completes successfully

6. **✅ Start Processing Data**
   - Batch processing starts automatically at scheduled times
   - For immediate processing: trigger `daily_github_bronze` manually
   - For streaming: manually trigger `producer_github_events` and `consumer_github_events`

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