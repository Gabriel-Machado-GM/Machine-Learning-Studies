# Databricks notebook source
# MAGIC %md
# MAGIC # Customer 360 CQRS Architecture with Delta Live Tables
# MAGIC 
# MAGIC This notebook implements a high-performance Customer 360 platform using the CQRS (Command Query Responsibility Segregation) pattern on Databricks Lakehouse.
# MAGIC 
# MAGIC ## Architecture Overview
# MAGIC - **Command Model (Write Path)**: Bronze → Silver layers using Delta Live Tables with CDC processing
# MAGIC - **Query Model (Read Path)**: Gold layer with Materialized Views for optimized analytics
# MAGIC - **UUID v7**: Time-ordered identifiers for optimal indexing and data locality
# MAGIC - **Source**: `prd.s_stbr_dri_ifr.tb_output_modellica_ifrs9_cred`
# MAGIC - **Target**: `prd.sand_crc_estudos_ifrs9`

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Import Required Libraries and Setup

# COMMAND ----------

import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *
import time
import uuid
from datetime import datetime

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. UUID v7 Generation UDF
# MAGIC 
# MAGIC Implementing UUID v7 for time-ordered, globally unique identifiers as recommended in the architecture document.
# MAGIC This provides better indexing performance and data locality compared to random UUIDs.

# COMMAND ----------

@udf(returnType=StringType())
def generate_uuid_v7():
    """
    Generate UUID v7 - a time-ordered UUID for optimal database performance.
    
    UUID v7 structure (128 bits):
    - 48 bits: Unix timestamp in milliseconds
    - 12 bits: Version and variant bits
    - 62 bits: Random data for uniqueness
    
    Returns:
        str: UUID v7 string representation
    """
    # Get current timestamp in milliseconds
    timestamp_ms = int(time.time() * 1000)
    
    # Generate random bytes for the remaining parts
    random_a = uuid.uuid4().int >> 76  # 12 bits
    random_b = uuid.uuid4().int & 0x3FFFFFFFFFFFFFFF  # 62 bits
    
    # Construct UUID v7
    # timestamp (48 bits) + version (4 bits) + random_a (12 bits) + variant (2 bits) + random_b (62 bits)
    time_high = (timestamp_ms >> 16) & 0xFFFFFFFF
    time_mid = (timestamp_ms >> 4) & 0xFFFF
    time_low = ((timestamp_ms & 0xF) << 12) | (0x7 << 12) | random_a  # Version 7
    
    clock_seq = 0x8000 | (random_b >> 48)  # Variant bits (10) + 14 random bits
    node = random_b & 0xFFFFFFFFFFFF
    
    # Format as standard UUID string
    uuid_str = f"{time_high:08x}-{time_mid:04x}-{time_low:04x}-{clock_seq:04x}-{node:012x}"
    return uuid_str

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Data Quality Expectations
# MAGIC 
# MAGIC Define data quality rules as per Delta Live Tables best practices

# COMMAND ----------

# Data quality expectations
def get_data_quality_expectations():
    """
    Define data quality expectations for the pipeline
    """
    return {
        "valid_timestamp": "timestamp IS NOT NULL",
        "valid_contract_data": "contract_id IS NOT NULL OR account_id IS NOT NULL",
        "valid_amount": "amount IS NOT NULL AND amount >= 0",
        "future_timestamp": "timestamp <= current_timestamp()"
    }

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. CQRS Command Model - Bronze Layer (Data Ingestion)
# MAGIC 
# MAGIC The Bronze layer implements the ingestion part of the Command model, reading streaming data from the source Delta table.

# COMMAND ----------

@dlt.table(
    name="bronze_ifrs9_raw",
    comment="Bronze layer - Raw ingestion from source Delta table with data quality checks",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)
@dlt.expect_all(get_data_quality_expectations())
def bronze_ifrs9_raw():
    """
    Bronze layer: Raw data ingestion from source Delta table
    
    This function creates a streaming table that reads from the source Delta table
    and applies initial data quality checks.
    """
    return (
        spark.readStream
        .format("delta")
        .option("readChangeData", "true")  # Enable Change Data Feed for CDC
        .option("startingTimestamp", "2024-01-01")  # Adjust as needed
        .table("prd.s_stbr_dri_ifr.tb_output_modellica_ifrs9_cred")
        .select(
            "*",
            current_timestamp().alias("ingestion_timestamp"),
            lit("bronze").alias("processing_layer")
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. CQRS Command Model - Silver Layer (State Mutation & Business Logic)
# MAGIC 
# MAGIC The Silver layer implements the core of the Command model, handling state mutations and business logic transformations.

# COMMAND ----------

@dlt.table(
    name="silver_ifrs9_enriched",
    comment="Silver layer - Enriched and transformed data with UUID v7 identifiers",
    table_properties={
        "quality": "silver", 
        "pipelines.autoOptimize.managed": "true",
        "delta.enableChangeDataFeed": "true"  # Enable CDC for downstream consumption
    }
)
@dlt.expect_all({
    "valid_contract_uuid": "contract_uuid IS NOT NULL",
    "valid_processed_timestamp": "processed_timestamp IS NOT NULL"
})
def silver_ifrs9_enriched():
    """
    Silver layer: Business logic transformations and UUID v7 generation
    
    This layer applies business transformations and generates UUID v7 identifiers
    for optimal indexing and data locality.
    """
    return (
        dlt.read_stream("bronze_ifrs9_raw")
        .filter(col("_change_type").isin(["insert", "update_postimage"]))  # Handle CDC operations
        .withColumn("contract_uuid", generate_uuid_v7())  # Generate UUID v7
        .withColumn("processed_timestamp", current_timestamp())
        .withColumn("processing_layer", lit("silver"))
        .withColumn(
            "customer_risk_category",
            when(col("pd_12m") <= 0.01, "Low Risk")
            .when(col("pd_12m") <= 0.05, "Medium Risk") 
            .when(col("pd_12m") <= 0.15, "High Risk")
            .otherwise("Very High Risk")
        )
        .withColumn(
            "provision_amount_calculated",
            col("exposure_amount") * col("pd_12m") * col("lgd")
        )
        .withColumn(
            "business_date",
            to_date(col("reference_date"))
        )
        # Add data lineage information
        .withColumn("source_table", lit("prd.s_stbr_dri_ifr.tb_output_modellica_ifrs9_cred"))
        .withColumn("pipeline_id", lit("customer_360_cqrs"))
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. CQRS Command Model - CDC Processing with APPLY CHANGES INTO
# MAGIC 
# MAGIC Implementation of Change Data Capture processing using DLT's APPLY CHANGES INTO functionality

# COMMAND ----------

# Apply changes to maintain current state (SCD Type 1)
dlt.apply_changes(
    target="silver_contracts_current_state",
    source="silver_ifrs9_enriched", 
    keys=["contract_uuid"],
    sequence_by=col("processed_timestamp"),
    apply_as_deletes=expr("_change_type = 'delete'"),
    except_column_list=["_change_type", "_commit_version", "_commit_timestamp"],
    stored_as_scd_type=1,
    comment="Current state of contracts using SCD Type 1 - overwrites existing records"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. CQRS Query Model - Gold Layer (Materialized Views)
# MAGIC 
# MAGIC The Gold layer implements the Query model with denormalized, pre-aggregated views optimized for analytics and BI.

# COMMAND ----------

@dlt.table(
    name="gold_customer_360_summary",
    comment="Gold layer - Customer 360 aggregated view optimized for analytics and BI",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true"
    }
)
def gold_customer_360_summary():
    """
    Gold layer: Customer 360 aggregated view for analytics
    
    This creates a denormalized view optimized for BI dashboards and ML feature stores.
    Implements the Query side of CQRS pattern.
    """
    return (
        dlt.read("silver_contracts_current_state")
        .groupBy("customer_id", "business_date", "customer_risk_category")
        .agg(
            count("contract_uuid").alias("total_contracts"),
            sum("exposure_amount").alias("total_exposure"),
            sum("provision_amount_calculated").alias("total_provisions"),
            avg("pd_12m").alias("avg_probability_default"),
            avg("lgd").alias("avg_loss_given_default"),
            max("processed_timestamp").alias("last_updated"),
            collect_list("contract_uuid").alias("contract_list")
        )
        .withColumn(
            "exposure_risk_ratio", 
            col("total_provisions") / col("total_exposure")
        )
        .withColumn(
            "customer_360_id",
            generate_uuid_v7()
        )
        .withColumn("gold_layer_timestamp", current_timestamp())
        .withColumn("data_freshness_hours", 
            (unix_timestamp(current_timestamp()) - unix_timestamp(col("last_updated"))) / 3600
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Advanced Analytics - ML Feature Store Ready View

# COMMAND ----------

@dlt.table(
    name="gold_ml_features_customer_risk",
    comment="ML-ready feature store for customer risk modeling",
    table_properties={
        "quality": "gold",
        "ml.feature_store": "true"
    }
)
def gold_ml_features_customer_risk():
    """
    ML Feature Store: Pre-computed features for customer risk modeling
    
    This view creates features optimized for machine learning models,
    particularly for churn prediction and risk assessment.
    """
    return (
        dlt.read("gold_customer_360_summary")
        .select(
            col("customer_id"),
            col("business_date"),
            col("total_exposure").alias("feature_total_exposure"),
            col("exposure_risk_ratio").alias("feature_risk_ratio"),
            col("avg_probability_default").alias("feature_avg_pd"),
            col("total_contracts").alias("feature_contract_count"),
            # Risk scoring features
            when(col("customer_risk_category") == "Low Risk", 1)
            .when(col("customer_risk_category") == "Medium Risk", 2)
            .when(col("customer_risk_category") == "High Risk", 3)
            .otherwise(4).alias("feature_risk_score"),
            # Temporal features
            dayofweek(col("business_date")).alias("feature_day_of_week"),
            month(col("business_date")).alias("feature_month"),
            year(col("business_date")).alias("feature_year"),
            # Data quality indicators
            col("data_freshness_hours").alias("feature_data_freshness")
        )
        .withColumn("ml_feature_timestamp", current_timestamp())
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Data Quality Monitoring and Alerting

# COMMAND ----------

@dlt.table(
    name="data_quality_metrics",
    comment="Data quality monitoring and metrics for the pipeline"
)
def data_quality_metrics():
    """
    Data Quality Monitoring: Track pipeline health and data quality metrics
    """
    bronze_count = dlt.read("bronze_ifrs9_raw").count()
    silver_count = dlt.read("silver_contracts_current_state").count()
    gold_count = dlt.read("gold_customer_360_summary").count()
    
    return spark.createDataFrame([
        (current_timestamp(), bronze_count, silver_count, gold_count, 
         silver_count/bronze_count if bronze_count > 0 else 0,
         gold_count/silver_count if silver_count > 0 else 0)
    ], ["check_timestamp", "bronze_records", "silver_records", "gold_records", 
        "bronze_to_silver_ratio", "silver_to_gold_ratio"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Pipeline Configuration and Metadata

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create the target schema if it doesn't exist
# MAGIC CREATE SCHEMA IF NOT EXISTS prd.sand_crc_estudos_ifrs9
# MAGIC COMMENT 'Customer 360 CQRS study schema for IFRS9 data processing'

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Usage Instructions and Next Steps
# MAGIC 
# MAGIC ### To deploy this pipeline:
# MAGIC 
# MAGIC 1. **Create a Delta Live Tables Pipeline** in Databricks:
# MAGIC    ```
# MAGIC    Pipeline Name: customer-360-cqrs-ifrs9
# MAGIC    Source Code: This notebook
# MAGIC    Target Schema: prd.sand_crc_estudos_ifrs9
# MAGIC    Pipeline Mode: Triggered or Continuous
# MAGIC    ```
# MAGIC 
# MAGIC 2. **Configure Pipeline Settings**:
# MAGIC    - Enable Auto Scaling
# MAGIC    - Set appropriate cluster size based on data volume
# MAGIC    - Configure alerts for pipeline failures
# MAGIC 
# MAGIC 3. **Create Materialized Views** for the Query Model (run separately in Databricks SQL):
# MAGIC    ```sql
# MAGIC    CREATE MATERIALIZED VIEW prd.sand_crc_estudos_ifrs9.mv_customer_360_dashboard
# MAGIC    SCHEDULE CRON '0 * * * *'  -- Refresh hourly
# MAGIC    AS SELECT * FROM prd.sand_crc_estudos_ifrs9.gold_customer_360_summary;
# MAGIC    ```
# MAGIC 
# MAGIC 4. **Set up Unity Catalog Governance**:
# MAGIC    - Enable data lineage tracking
# MAGIC    - Configure access controls
# MAGIC    - Set up data classification tags
# MAGIC 
# MAGIC ### Benefits of this CQRS Implementation:
# MAGIC 
# MAGIC - **Scalable Architecture**: Independent scaling of read and write workloads
# MAGIC - **High Performance**: UUID v7 ensures optimal indexing and data locality
# MAGIC - **Data Quality**: Built-in expectations and monitoring
# MAGIC - **ML Ready**: Feature store integration for advanced analytics
# MAGIC - **Governable**: Full lineage tracking with Unity Catalog
# MAGIC - **Maintainable**: Declarative DLT approach reduces operational overhead

# COMMAND ----------

# MAGIC %md
# MAGIC ## Architecture Summary
# MAGIC 
# MAGIC This implementation provides:
# MAGIC 
# MAGIC 1. **Command Model (Write Path)**:
# MAGIC    - Bronze: Raw data ingestion with CDC
# MAGIC    - Silver: Business logic and UUID v7 generation
# MAGIC    - APPLY CHANGES INTO: SCD Type 1 for current state
# MAGIC 
# MAGIC 2. **Query Model (Read Path)**:
# MAGIC    - Gold: Denormalized views for analytics
# MAGIC    - ML Features: Pre-computed features for modeling
# MAGIC    - Materialized Views: Low-latency dashboard serving
# MAGIC 
# MAGIC 3. **Advanced Features**:
# MAGIC    - Time-ordered UUID v7 identifiers
# MAGIC    - Data quality monitoring
# MAGIC    - CDC processing
# MAGIC    - ML feature store integration
# MAGIC    - Unity Catalog governance