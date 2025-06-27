CREATE OR REFRESH STREAMING LIVE TABLE silver_core_metrics (
  CONSTRAINT data_completeness EXPECT (total_readings >= 10),
  CONSTRAINT availability_range EXPECT (availability_pct BETWEEN 0 AND 100),
  CONSTRAINT health_score_range EXPECT (system_health_score BETWEEN 0 AND 100)
)
COMMENT "Core operational metrics aggregated every 15 minutes over 1-hour windows"
TBLPROPERTIES (
  "quality" = "silver",
  "pipelines.autoOptimize.managed" = "true",
  "pipelines.trigger.interval" = "15 minutes"
)
AS
WITH windowed_metrics AS (
  SELECT 
    site_id,
    window(event_timestamp, '1 hour', '15 minutes') AS time_window,
    
    -- METRIC 1: SYSTEM AVAILABILITY (%)
    CAST(SUM(CASE WHEN fault_code = 'OK' THEN 1 ELSE 0 END) AS DOUBLE) / 
    CAST(COUNT(*) AS DOUBLE) * 100 AS availability_pct,
    
    -- METRIC 2: TRACKING ACCURACY (degrees)
    AVG(ABS(angle_actual - angle_target)) AS avg_angle_deviation_deg,
    CAST(SUM(CASE WHEN ABS(angle_actual - angle_target) <= 2.0 THEN 1 ELSE 0 END) AS DOUBLE) / 
    CAST(COUNT(*) AS DOUBLE) * 100 AS tracking_accuracy_pct,
    
    -- METRIC 3: FAULT RATE (non-OK fault codes per hour)
    CAST(SUM(CASE WHEN fault_code != 'OK' THEN 1 ELSE 0 END) AS DOUBLE) / 
    (CAST(COUNT(*) AS DOUBLE) / 4) AS fault_rate_per_hour,
    SUM(CASE WHEN fault_code != 'OK' THEN 1 ELSE 0 END) AS fault_events,
    
    -- METRIC 4: MOTOR PERFORMANCE
    AVG(rpm) AS avg_rpm,
    AVG(torque) AS avg_torque,
    AVG(motor_temp) AS avg_motor_temp,
    CAST(SUM(
      CASE 
        WHEN motor_temp <= 70 THEN 100
        WHEN motor_temp <= 80 THEN 75
        WHEN motor_temp <= 90 THEN 50
        ELSE 25
      END
    ) AS DOUBLE) / CAST(COUNT(*) AS DOUBLE) AS motor_health_score,
    
    -- ENVIRONMENTAL CONTEXT
    AVG(irradiance) AS avg_irradiance,
    AVG(ambient_temp) AS avg_ambient_temp,
    AVG(wind_speed) AS avg_wind_speed,
    
    -- OPERATIONAL STATS
    APPROX_COUNT_DISTINCT(tracker_row) AS active_trackers,
    COUNT(*) AS total_readings,
    
    -- IRRADIANCE-WEIGHTED AVAILABILITY
    SUM(CASE WHEN fault_code = 'OK' THEN irradiance ELSE 0 END) / 
    SUM(irradiance) * 100 AS energy_weighted_availability_pct,
    
    -- TIME INFO
    MIN(event_timestamp) AS window_start_ts,
    MAX(event_timestamp) AS window_end_ts,
    CURRENT_TIMESTAMP() AS calculated_at
    
  FROM STREAM(bronze_raw_stream)
  WATERMARK event_timestamp DELAY OF INTERVAL 10 MINUTES AS stream
  GROUP BY 
    site_id,
    window(event_timestamp, '1 hour', '15 minutes')
),

metrics_with_health AS (
  SELECT 
    *,
    time_window.start AS window_start,
    time_window.end AS window_end,
    
    -- COMPOSITE HEALTH SCORE (0-100)
    GREATEST(
      (availability_pct * 0.40 +
       (100 - LEAST(avg_angle_deviation_deg * 10, 100)) * 0.30 +
       (100 - LEAST(fault_rate_per_hour * 20, 100)) * 0.20 +
       motor_health_score * 0.10),
      0
    ) AS system_health_score
  FROM windowed_metrics
)

SELECT 
  site_id,
  window_start,
  window_end,
  
  -- Core KPIs
  ROUND(availability_pct, 2) AS availability_pct,
  ROUND(avg_angle_deviation_deg, 3) AS avg_angle_deviation_deg,
  ROUND(tracking_accuracy_pct, 2) AS tracking_accuracy_pct,
  ROUND(fault_rate_per_hour, 2) AS fault_rate_per_hour,
  ROUND(energy_weighted_availability_pct, 2) AS energy_weighted_availability_pct,
  
  -- System health
  ROUND(system_health_score, 1) AS system_health_score,
  CASE 
    WHEN system_health_score >= 95 THEN 'EXCELLENT'
    WHEN system_health_score >= 85 THEN 'GOOD'
    WHEN system_health_score >= 70 THEN 'WARNING'
    WHEN system_health_score >= 50 THEN 'CRITICAL'
    ELSE 'FAILURE'
  END AS alert_level,
  
  CASE 
    WHEN fault_events > 0 THEN 'HIGH'
    WHEN fault_rate_per_hour > 1 THEN 'MEDIUM'
    WHEN avg_angle_deviation_deg > 3 THEN 'MEDIUM'
    WHEN avg_motor_temp > 80 THEN 'MEDIUM'
    WHEN availability_pct < 95 THEN 'LOW'
    ELSE 'ROUTINE'
  END AS maintenance_priority,
  
  -- Fault and motor details
  fault_events,
  ROUND(avg_rpm, 1) AS avg_rpm,
  ROUND(avg_torque, 1) AS avg_torque,
  ROUND(motor_health_score, 1) AS motor_health_score,
  
  -- Environmental context
  ROUND(avg_irradiance, 2) AS avg_irradiance,
  ROUND(avg_ambient_temp, 1) AS avg_ambient_temp,
  ROUND(avg_wind_speed, 1) AS avg_wind_speed,
  ROUND(avg_motor_temp, 1) AS avg_motor_temp,
  
  -- Operational stats
  active_trackers,
  total_readings,
  calculated_at

FROM metrics_with_health;