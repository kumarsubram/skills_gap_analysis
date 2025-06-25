# GitHub Processing Analytics DAG

> **Single-Run Sequential Processing: Silver → Gold → Analytics**

A comprehensive Airflow DAG that processes GitHub Archive data through the complete medallion architecture in a single execution, delivering technology trend intelligence and market insights.

## 🎯 **Purpose**

Transforms raw GitHub event data into business-ready analytics by processing through three sequential layers:
- **Silver Layer**: Clean, validated technology mentions with daily aggregations
- **Gold Layer**: Business-classified technologies with categorization and volume tiers  
- **Analytics Layer**: Multi-timeframe trend analysis (7d, 30d, 90d, all-time)

## 📊 **Data Flow Architecture**

```
Bronze (Raw GitHub Data) 
    ↓
Silver (Clean Daily Aggregations) 
    ↓  
Gold (Business Classifications)
    ↓
Analytics (Trend Intelligence)
```

## 🚀 **Key Features**

### ✅ **Single-Run Execution**
- Processes Silver → Gold → Analytics sequentially in one DAG run
- No multi-run complexity or coordination issues
- Dynamic gap detection after each layer completion

### ✅ **Intelligent Assessment**
- Automatically detects missing data across all layers
- Processes only what's needed (true idempotency)
- Handles both automatic and manual date processing

### ✅ **Production-Ready**
- Comprehensive error handling with rollback capability
- Schema validation at each layer
- Real-time progress tracking and status reporting

### ✅ **Business Intelligence**
- 500+ technology classification with 2025 AI/ML focus
- Multi-timeframe trend analysis (7d, 30d, 90d, all-time)
- Technology ranking and hotness signals
- Category-based analytics and insights

## 📋 **Prerequisites**

### **Required Tables**
- `bronze_github_keyword_extractions` (input data)
- Auto-creates: Silver, Gold, and Analytics tables

### **Required Files**
- `include/jsons/tech_keywords.json` (500+ technology definitions)
- `include/business_logic/technology_classification.py`
- All supporting utilities in `include/utils/`

### **Infrastructure**
- MinIO S3 storage with Delta Lake 4.0
- Apache Spark 4.0 cluster
- Environment variables: `MINIO_*` credentials

## 🎮 **Usage**

### **Automatic Processing (Recommended)**
```bash
# Trigger from Airflow UI - no configuration needed
# Automatically finds and processes ALL missing data
```

The DAG will:
1. Assess what's missing across Silver, Gold, Analytics layers
2. Process everything needed in sequential order
3. Provide comprehensive progress reporting

### **Manual Date Processing**
```json
{
  "date": "2025-01-03"
}
```

Processes the specified date through the complete pipeline (Silver → Gold → Analytics).

## 📈 **Processing Strategies**

The DAG automatically selects the optimal strategy:

| Strategy | Description | Action |
|----------|-------------|--------|
| `single_run_complete_pipeline` | Missing data in multiple layers | Process Silver gaps → Gold gaps → Analytics refresh |
| `single_run_analytics_only` | Only Analytics needs refresh | Skip to Analytics processing |
| `manual_full_pipeline` | Manual date specified | Process specified date through all layers |

## 🔧 **Task Breakdown**

### **1. Create Tables** 
Creates Silver, Gold, and Analytics tables with proper schemas and partitioning.

### **2. Assessment**
```python
# Finds missing data across layers
- Silver gaps: Bronze dates not in Silver
- Gold gaps: Silver dates not in Gold  
- Analytics needs: Refresh requirements
```

### **3. Silver Processing**
```python
# Bronze → Silver transformation
- Daily technology mention aggregation
- Data quality scoring
- Deduplication and validation
- Partitioned by date
```

### **4. Gold Processing** 
```python
# Silver → Gold enrichment
- Technology categorization (ai_model, language, framework, etc.)
- Mention volume classification (high/medium/low)
- Data completeness assessment
- Business intelligence preparation
```

### **5. Analytics Processing**
```python
# Gold → Analytics insights
- 7-day trend analysis with hotness signals
- 30-day and 90-day trend directions
- All-time rankings and category analysis
- Multi-timeframe comparative metrics
```

### **6. Summary & Next Steps**
Comprehensive reporting with intelligent next steps based on processing results.

## 📊 **Output Tables**

### **Silver Layer**
`silver_github_keyword_trends`
- Daily technology mentions with aggregations
- Data quality scores and source tracking
- Partitioned by date for efficient queries

### **Gold Layer** 
`gold_github_technology_daily_activity`
- Business-classified technologies
- Mention volume tiers and completeness indicators
- Ready for business intelligence consumption

### **Analytics Layer**
- `analytics_github_technology_trends_7d` - Weekly trends with hotness signals
- `analytics_github_technology_trends_30d` - Monthly trend directions  
- `analytics_github_technology_trends_90d` - Quarterly analysis
- `analytics_github_technology_trends_alltime` - Historical rankings

## 🎯 **Business Intelligence Output**

### **Technology Rankings**
- Mention-based rankings across multiple timeframes
- Category-specific rankings (AI, languages, frameworks)
- Hotness signals (hot/warm/cool) for emerging technologies

### **Trend Analysis**
- Growth direction classification (trending up/stable/down)
- Peak activity identification and timing
- Data completeness scoring for reliability assessment

### **Category Insights**
- AI/ML technology adoption tracking (2025 focus)
- Programming language popularity trends
- Framework and tool ecosystem analysis

## ⚡ **Performance & Reliability**

### **Resource Management**
- Conservative memory settings prevent OOM issues
- Efficient Delta Lake partitioning by date
- Optimized Spark configurations for medallion processing

### **Error Handling**
- Graceful degradation with detailed error reporting
- Rollback capability for failed Analytics processing
- Comprehensive validation at each layer transition

### **Monitoring**
- Real-time progress tracking with timestamps
- Success/failure status with actionable next steps
- Performance metrics and processing times

## 🧪 **Testing & Validation**

### **Schema Validation**
Automatic validation ensures data quality:
```python
- Silver: 10 columns including aggregations and quality scores
- Gold: 10 columns with business classifications  
- Analytics: 13-14 columns per timeframe with trend metrics
```

### **Data Quality Checks**
- Minimum record thresholds for processing
- Technology classification accuracy validation
- Cross-layer consistency verification

## 🚨 **Troubleshooting**

### **Common Issues**

| Issue | Cause | Solution |
|-------|-------|----------|
| No Bronze data | Bronze pipeline not run | Run `daily_github_bronze` DAG first |
| Schema validation failed | Classification mismatch | Check `technology_classification.py` output |
| Assessment shows no processing | All layers current | Normal - system is up to date |
| Partial Analytics success | Individual timeframe issues | Check Analytics rollback in logs |

### **Status Indicators**

| Status | Meaning | Action |
|--------|---------|--------|
| ✅ COMPLETE | All layers processed successfully | Ready for dashboard consumption |
| 🔄 PARTIAL SUCCESS | Some layers completed | Check logs and re-run for failed layers |  
| ⏩ NO ACTION NEEDED | System up to date | Run Bronze pipeline for new data |
| ❌ BLOCKED | Bronze data issues | Fix Bronze data quality first |

### **Debug Commands**
```python
# Check table status
python include/utils/delta_table_utils.py keyword_extractions --layer bronze
python include/utils/delta_table_utils.py keyword_trends --layer silver  
python include/utils/delta_table_utils.py technology_daily_activity --layer gold

# Test classification system
python include/business_logic/technology_classification.py

# Validate pipeline status  
python include/utils/github_date_processing.py
```

## 🎪 **Integration Points**

### **Upstream Dependencies**
- **Required**: `daily_github_bronze` DAG (provides Bronze data)
- **Optional**: Streaming Bronze data from Kafka consumer

### **Downstream Consumers**
- Analytics dashboards and reporting tools
- API endpoints for technology trend queries
- Business intelligence and market research

### **Cross-Platform Compatibility**
- **Development**: Mac with localhost MinIO
- **Production**: VPS with remote MinIO  
- Automatic environment detection and endpoint configuration

## 📚 **Related Documentation**

- [Bronze Pipeline](./daily_github_bronze.py) - Raw GitHub data collection
- [Streaming Pipeline](./consumer_github_events.py) - Real-time GitHub events
- [Technology Classification](../include/business_logic/technology_classification.py) - Business logic
- [Schema Definitions](../include/schemas/github_schemas.py) - Data contracts

## 🏆 **Success Metrics**

### **Technical KPIs**
- Pipeline success rate: >95%
- Processing time: <45 minutes for complete pipeline
- Data quality: >90% valid technology classifications

### **Business Intelligence KPIs**  
- Technology trend rankings for 500+ technologies
- Multi-timeframe analysis (7d, 30d, 90d, all-time)
- Category insights across AI, languages, frameworks, and tools
- Actionable hotness signals for emerging technologies

---

**Architecture**: Single-run sequential processing ensures reliable, comprehensive technology intelligence delivery in one execution cycle.