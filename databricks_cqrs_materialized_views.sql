-- Databricks SQL: CQRS Query Model - Materialized Views
-- File: databricks_cqrs_materialized_views.sql
-- Purpose: Implement the Query side of CQRS pattern using Materialized Views for optimal dashboard performance

-- ========================================
-- CQRS Query Model Implementation
-- ========================================

-- Create the target schema for our Customer 360 platform
CREATE SCHEMA IF NOT EXISTS prd.sand_crc_estudos_ifrs9
COMMENT 'Customer 360 CQRS platform for IFRS9 credit risk analytics';

-- ========================================
-- 1. Real-time Dashboard Materialized View
-- ========================================

CREATE OR REPLACE MATERIALIZED VIEW prd.sand_crc_estudos_ifrs9.mv_customer_360_dashboard
SCHEDULE CRON '0 * * * *'  -- Refresh every hour for near real-time dashboards
COMMENT 'Real-time Customer 360 dashboard view optimized for BI tools'
TBLPROPERTIES (
  'quality' = 'gold',
  'refresh_policy' = 'auto',
  'dashboard_optimized' = 'true'
)
AS 
SELECT 
  customer_id,
  business_date,
  customer_risk_category,
  total_contracts,
  total_exposure,
  total_provisions,
  avg_probability_default,
  avg_loss_given_default,
  exposure_risk_ratio,
  last_updated,
  data_freshness_hours,
  -- Dashboard-specific calculations
  CASE 
    WHEN exposure_risk_ratio < 0.05 THEN 'Healthy'
    WHEN exposure_risk_ratio < 0.15 THEN 'Monitor'
    ELSE 'Alert'
  END as portfolio_health_status,
  
  -- Time-based aggregations for trending
  LAG(total_exposure) OVER (PARTITION BY customer_id ORDER BY business_date) as prev_exposure,
  LAG(exposure_risk_ratio) OVER (PARTITION BY customer_id ORDER BY business_date) as prev_risk_ratio,
  
  -- Calculate exposure growth
  CASE 
    WHEN LAG(total_exposure) OVER (PARTITION BY customer_id ORDER BY business_date) IS NOT NULL 
    THEN ((total_exposure - LAG(total_exposure) OVER (PARTITION BY customer_id ORDER BY business_date)) / 
          LAG(total_exposure) OVER (PARTITION BY customer_id ORDER BY business_date)) * 100
    ELSE NULL
  END as exposure_growth_pct,
  
  current_timestamp() as mv_refresh_timestamp

FROM prd.sand_crc_estudos_ifrs9.gold_customer_360_summary
WHERE business_date >= date_sub(current_date(), 90)  -- Last 90 days for performance
ORDER BY customer_id, business_date DESC;

-- ========================================
-- 2. Executive Summary Materialized View
-- ========================================

CREATE OR REPLACE MATERIALIZED VIEW prd.sand_crc_estudos_ifrs9.mv_executive_summary
SCHEDULE CRON '0 8 * * *'  -- Refresh daily at 8 AM for executive reports
COMMENT 'Executive summary view for high-level portfolio insights'
TBLPROPERTIES (
  'quality' = 'gold',
  'refresh_policy' = 'auto',
  'executive_reporting' = 'true'
)
AS
SELECT 
  business_date,
  COUNT(DISTINCT customer_id) as total_customers,
  SUM(total_exposure) as portfolio_exposure,
  SUM(total_provisions) as total_provisions,
  AVG(exposure_risk_ratio) as avg_portfolio_risk,
  
  -- Risk distribution
  SUM(CASE WHEN customer_risk_category = 'Low Risk' THEN total_exposure ELSE 0 END) as low_risk_exposure,
  SUM(CASE WHEN customer_risk_category = 'Medium Risk' THEN total_exposure ELSE 0 END) as medium_risk_exposure,
  SUM(CASE WHEN customer_risk_category = 'High Risk' THEN total_exposure ELSE 0 END) as high_risk_exposure,
  SUM(CASE WHEN customer_risk_category = 'Very High Risk' THEN total_exposure ELSE 0 END) as very_high_risk_exposure,
  
  -- Customer count by risk category
  COUNT(CASE WHEN customer_risk_category = 'Low Risk' THEN customer_id END) as low_risk_customers,
  COUNT(CASE WHEN customer_risk_category = 'Medium Risk' THEN customer_id END) as medium_risk_customers,
  COUNT(CASE WHEN customer_risk_category = 'High Risk' THEN customer_id END) as high_risk_customers,
  COUNT(CASE WHEN customer_risk_category = 'Very High Risk' THEN customer_id END) as very_high_risk_customers,
  
  -- Portfolio health indicators
  (SUM(total_provisions) / SUM(total_exposure)) * 100 as portfolio_provision_rate_pct,
  MAX(data_freshness_hours) as max_data_age_hours,
  current_timestamp() as executive_report_timestamp

FROM prd.sand_crc_estudos_ifrs9.gold_customer_360_summary
WHERE business_date >= date_sub(current_date(), 365)  -- Last year for trending
GROUP BY business_date
ORDER BY business_date DESC;

-- ========================================
-- 3. Risk Monitoring Materialized View
-- ========================================

CREATE OR REPLACE MATERIALIZED VIEW prd.sand_crc_estudos_ifrs9.mv_risk_monitoring_alerts
SCHEDULE CRON '0 */4 * * *'  -- Refresh every 4 hours for risk monitoring
COMMENT 'Risk monitoring and alerting view for proactive portfolio management'
TBLPROPERTIES (
  'quality' = 'gold',
  'refresh_policy' = 'auto',
  'risk_monitoring' = 'true'
)
AS
WITH risk_thresholds AS (
  SELECT 
    customer_id,
    business_date,
    customer_risk_category,
    total_exposure,
    exposure_risk_ratio,
    avg_probability_default,
    
    -- Define risk alert conditions
    CASE 
      WHEN exposure_risk_ratio > 0.20 THEN 'HIGH_PROVISION_RATE'
      WHEN avg_probability_default > 0.15 THEN 'HIGH_DEFAULT_PROBABILITY' 
      WHEN total_exposure > 10000000 THEN 'LARGE_EXPOSURE'  -- Adjust threshold as needed
      WHEN data_freshness_hours > 24 THEN 'STALE_DATA'
      ELSE NULL
    END as alert_type,
    
    CASE 
      WHEN exposure_risk_ratio > 0.25 OR avg_probability_default > 0.20 THEN 'CRITICAL'
      WHEN exposure_risk_ratio > 0.15 OR avg_probability_default > 0.10 THEN 'HIGH'
      WHEN exposure_risk_ratio > 0.10 OR avg_probability_default > 0.05 THEN 'MEDIUM'
      ELSE 'LOW'
    END as alert_severity
    
  FROM prd.sand_crc_estudos_ifrs9.gold_customer_360_summary
  WHERE business_date = (SELECT MAX(business_date) FROM prd.sand_crc_estudos_ifrs9.gold_customer_360_summary)
)
SELECT 
  customer_id,
  business_date,
  customer_risk_category,
  total_exposure,
  exposure_risk_ratio,
  avg_probability_default,
  alert_type,
  alert_severity,
  current_timestamp() as alert_generated_timestamp,
  
  -- Additional context for alerts
  CASE alert_type
    WHEN 'HIGH_PROVISION_RATE' THEN CONCAT('Provision rate: ', ROUND(exposure_risk_ratio * 100, 2), '%')
    WHEN 'HIGH_DEFAULT_PROBABILITY' THEN CONCAT('Default probability: ', ROUND(avg_probability_default * 100, 2), '%')
    WHEN 'LARGE_EXPOSURE' THEN CONCAT('Large exposure: $', FORMAT_NUMBER(total_exposure, 2))
    WHEN 'STALE_DATA' THEN 'Data freshness issue detected'
    ELSE 'Normal'
  END as alert_description

FROM risk_thresholds
WHERE alert_type IS NOT NULL
ORDER BY alert_severity DESC, total_exposure DESC;

-- ========================================
-- 4. ML Feature Store Materialized View
-- ========================================

CREATE OR REPLACE MATERIALIZED VIEW prd.sand_crc_estudos_ifrs9.mv_ml_feature_store
SCHEDULE CRON '0 2 * * *'  -- Refresh daily at 2 AM for ML model training
COMMENT 'ML feature store with time-series features for predictive modeling'
TBLPROPERTIES (
  'quality' = 'gold',
  'refresh_policy' = 'auto',
  'ml_feature_store' = 'true',
  'feature_engineering' = 'time_series'
)
AS
WITH customer_time_series AS (
  SELECT 
    customer_id,
    business_date,
    total_exposure,
    exposure_risk_ratio,
    avg_probability_default,
    total_contracts,
    
    -- Time-based features (rolling windows)
    AVG(total_exposure) OVER (
      PARTITION BY customer_id 
      ORDER BY business_date 
      ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) as exposure_7day_avg,
    
    AVG(exposure_risk_ratio) OVER (
      PARTITION BY customer_id 
      ORDER BY business_date 
      ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) as risk_ratio_30day_avg,
    
    STDDEV(total_exposure) OVER (
      PARTITION BY customer_id 
      ORDER BY business_date 
      ROWS BETWEEN 89 PRECEDING AND CURRENT ROW
    ) as exposure_90day_volatility,
    
    -- Trend features
    FIRST_VALUE(total_exposure) OVER (
      PARTITION BY customer_id 
      ORDER BY business_date 
      ROWS BETWEEN 29 PRECEDING AND 30 PRECEDING
    ) as exposure_30days_ago,
    
    LAG(exposure_risk_ratio, 7) OVER (
      PARTITION BY customer_id 
      ORDER BY business_date
    ) as risk_ratio_7days_ago,
    
    ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY business_date) as customer_age_days
    
  FROM prd.sand_crc_estudos_ifrs9.gold_customer_360_summary
  WHERE business_date >= date_sub(current_date(), 365)
)
SELECT 
  customer_id,
  business_date,
  
  -- Current state features
  total_exposure as feature_current_exposure,
  exposure_risk_ratio as feature_current_risk_ratio,
  avg_probability_default as feature_current_pd,
  total_contracts as feature_contract_count,
  
  -- Time-series features
  exposure_7day_avg as feature_exposure_7d_avg,
  risk_ratio_30day_avg as feature_risk_30d_avg,
  COALESCE(exposure_90day_volatility, 0) as feature_exposure_volatility,
  
  -- Change features
  CASE 
    WHEN exposure_30days_ago IS NOT NULL AND exposure_30days_ago > 0 
    THEN ((total_exposure - exposure_30days_ago) / exposure_30days_ago) * 100
    ELSE 0
  END as feature_exposure_30d_change_pct,
  
  CASE 
    WHEN risk_ratio_7days_ago IS NOT NULL 
    THEN exposure_risk_ratio - risk_ratio_7days_ago
    ELSE 0
  END as feature_risk_7d_change,
  
  -- Customer lifecycle features
  customer_age_days as feature_customer_tenure_days,
  CASE 
    WHEN customer_age_days <= 30 THEN 'New'
    WHEN customer_age_days <= 180 THEN 'Growing'
    WHEN customer_age_days <= 365 THEN 'Mature'
    ELSE 'Established'
  END as feature_customer_lifecycle_stage,
  
  -- Seasonal features
  EXTRACT(month FROM business_date) as feature_month,
  EXTRACT(quarter FROM business_date) as feature_quarter,
  EXTRACT(dayofweek FROM business_date) as feature_day_of_week,
  
  current_timestamp() as feature_extraction_timestamp

FROM customer_time_series
WHERE business_date >= date_sub(current_date(), 90)  -- Last 90 days for active ML features
ORDER BY customer_id, business_date DESC;

-- ========================================
-- 5. Data Lineage and Governance Views
-- ========================================

CREATE OR REPLACE VIEW prd.sand_crc_estudos_ifrs9.v_data_lineage_summary
COMMENT 'Data lineage summary for governance and compliance'
AS
SELECT 
  'prd.s_stbr_dri_ifr.tb_output_modellica_ifrs9_cred' as source_table,
  'prd.sand_crc_estudos_ifrs9' as target_schema,
  'customer_360_cqrs_pipeline' as pipeline_name,
  'Bronze -> Silver -> Gold' as processing_layers,
  'CQRS Pattern with UUID v7' as architecture_pattern,
  current_timestamp() as lineage_documented_at;

-- ========================================
-- Usage Examples and Dashboard Queries
-- ========================================

-- Example 1: Real-time portfolio health dashboard
-- SELECT * FROM prd.sand_crc_estudos_ifrs9.mv_customer_360_dashboard 
-- WHERE business_date >= date_sub(current_date(), 7)
-- ORDER BY exposure_risk_ratio DESC;

-- Example 2: Executive KPI summary
-- SELECT 
--   business_date,
--   portfolio_exposure,
--   portfolio_provision_rate_pct,
--   total_customers,
--   avg_portfolio_risk
-- FROM prd.sand_crc_estudos_ifrs9.mv_executive_summary
-- WHERE business_date >= date_sub(current_date(), 30)
-- ORDER BY business_date DESC;

-- Example 3: Risk alerts for immediate attention  
-- SELECT * FROM prd.sand_crc_estudos_ifrs9.mv_risk_monitoring_alerts
-- WHERE alert_severity IN ('CRITICAL', 'HIGH')
-- ORDER BY alert_severity DESC, total_exposure DESC;

-- ========================================
-- Performance Optimization Commands
-- ========================================

-- Optimize tables for better query performance
-- OPTIMIZE prd.sand_crc_estudos_ifrs9.mv_customer_360_dashboard ZORDER BY (customer_id, business_date);
-- OPTIMIZE prd.sand_crc_estudos_ifrs9.mv_executive_summary ZORDER BY (business_date);
-- OPTIMIZE prd.sand_crc_estudos_ifrs9.mv_risk_monitoring_alerts ZORDER BY (alert_severity, customer_id);

-- ========================================
-- Monitoring and Maintenance
-- ========================================

-- Check materialized view refresh status
-- SELECT 
--   table_name,
--   last_refresh_time,
--   refresh_status
-- FROM system.information_schema.materialized_views
-- WHERE table_schema = 'prd.sand_crc_estudos_ifrs9';