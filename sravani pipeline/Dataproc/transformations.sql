-- =====================================================================
-- Signal final transformation (Spark SQL dialect)
--
-- This is the Spark-SQL equivalent of the BigQuery transformation script.
-- It runs INSIDE the PySpark job against a temp view over ONE file's long
-- rows, so no source table is ever landed in BigQuery.
--
-- {view} is replaced at runtime with the temp view name (signal_long).
-- To use this file instead of the copy embedded in the .py, upload it to GCS
-- and pass --transform-sql-gcs=gs://<bucket>/<path>/transformations.sql
--
-- service_area / department are read as COALESCE(service_area, login_service_area)
-- so either source header spelling populates them.
-- NULL / blank rule: numeric -> 0, string -> 'N/A', dates stay NULL.
-- =====================================================================
WITH base AS (
  SELECT
    COALESCE(emp_cid, 0) AS emp_cid,
    COALESCE(ser_cid, 0) AS ser_cid,
    clinician_type,
    COALESCE(service_area, login_service_area) AS service_area,
    COALESCE(department,   login_department)   AS department,
    specialty, user_type, clinician_name,
    reporting_period_start_date, reporting_period_end_date,
    metric,
    COALESCE(CAST(numerator AS DOUBLE), 0.0) AS numerator
  FROM {view}
),
pivoted AS (
  SELECT
    emp_cid, ser_cid, clinician_type, service_area, department,
    specialty, user_type, clinician_name,
    reporting_period_start_date, reporting_period_end_date,
    SUM(CASE WHEN metric LIKE '%99201%' THEN numerator END) AS np1,
    SUM(CASE WHEN metric LIKE '%99202%' THEN numerator END) AS np2,
    SUM(CASE WHEN metric LIKE '%99203%' THEN numerator END) AS np3,
    SUM(CASE WHEN metric LIKE '%99204%' THEN numerator END) AS np4,
    SUM(CASE WHEN metric LIKE '%99205%' THEN numerator END) AS np5,
    SUM(CASE WHEN metric LIKE '%99211%' THEN numerator END) AS ep1,
    SUM(CASE WHEN metric LIKE '%99212%' THEN numerator END) AS ep2,
    SUM(CASE WHEN metric LIKE '%99213%' THEN numerator END) AS ep3,
    SUM(CASE WHEN metric LIKE '%99214%' THEN numerator END) AS ep4,
    SUM(CASE WHEN metric LIKE '%99215%' THEN numerator END) AS ep5,
    SUM(CASE WHEN metric = 'Appointments per Day'              THEN numerator END) AS appt_per_day,
    SUM(CASE WHEN metric = 'Appointments Closed - Same Day'    THEN numerator END) AS ac_same,
    SUM(CASE WHEN metric = 'Appointments Closed - 1 to 2 Days' THEN numerator END) AS ac_12,
    SUM(CASE WHEN metric = 'Appointments Closed - 3 to 4 Days' THEN numerator END) AS ac_34,
    SUM(CASE WHEN metric = 'Appointments Closed - 5 to 6 Days' THEN numerator END) AS ac_56,
    SUM(CASE WHEN metric = 'Appointments Closed - 7+ Days'     THEN numerator END) AS ac_7p,
    SUM(CASE WHEN metric = 'Days with Appointments'            THEN numerator END) AS days_appt,
    SUM(CASE WHEN metric = 'Time in Notes per Appointment'     THEN numerator END) AS notes_per_appt,
    SUM(CASE WHEN metric = 'Time in Notes per Day'             THEN numerator END) AS notes_per_day,
    SUM(CASE WHEN metric = 'Pajama Time'                       THEN numerator END) AS pajama,
    SUM(CASE WHEN metric = 'Time On Unscheduled Days'          THEN numerator END) AS unsched,
    SUM(CASE WHEN metric = 'Time Outside Scheduled Hours'      THEN numerator END) AS outside_sched
  FROM base
  GROUP BY emp_cid, ser_cid, clinician_type, service_area, department,
           specialty, user_type, clinician_name,
           reporting_period_start_date, reporting_period_end_date
),
parsed AS (
  SELECT
    *,
    COALESCE(CAST(FLOOR((DATEDIFF(reporting_period_end_date,
                                  reporting_period_start_date) + 1) / 7) AS BIGINT), 0)
      AS calc_weeks,
    TRUNC(reporting_period_end_date, 'MONTH') AS calc_month_year,
    (COALESCE(ep1,0.0)+COALESCE(ep2,0.0)+COALESCE(ep3,0.0)+COALESCE(ep4,0.0)+COALESCE(ep5,0.0))
      AS calc_sum_est,
    (COALESCE(np1,0.0)+COALESCE(np2,0.0)+COALESCE(np3,0.0)+COALESCE(np4,0.0)+COALESCE(np5,0.0))
      AS calc_sum_new
  FROM pivoted
)
SELECT
  emp_cid,
  ser_cid,
  COALESCE(NULLIF(TRIM(CAST(clinician_type AS STRING)), ''), 'N/A') AS clinician_type,
  COALESCE(NULLIF(TRIM(CAST(service_area   AS STRING)), ''), 'N/A') AS service_area,
  COALESCE(NULLIF(TRIM(CAST(department     AS STRING)), ''), 'N/A') AS department,
  COALESCE(NULLIF(TRIM(CAST(specialty      AS STRING)), ''), 'N/A') AS specialty,
  COALESCE(NULLIF(TRIM(CAST(user_type          AS STRING)), ''), 'N/A') AS user_type,
  COALESCE(NULLIF(TRIM(CAST(clinician_name     AS STRING)), ''), 'N/A') AS clinician_name,
  reporting_period_start_date,
  reporting_period_end_date,
  calc_weeks       AS number_of_weeks_in_reporting_period,
  calc_month_year  AS month_year,
  COALESCE(ROUND((appt_per_day / NULLIF(calc_weeks, 0)) * 4, 5), 0)  AS no_of_appointment,
  COALESCE(ROUND(ac_same / NULLIF(appt_per_day, 0), 5), 0)          AS Same_Day_Rate,
  COALESCE(ROUND((COALESCE(ac_same,0)+COALESCE(ac_12,0))
                 / NULLIF(appt_per_day,0), 5), 0)                   AS Rate_1_2_Days,
  COALESCE(ROUND((COALESCE(ac_same,0)+COALESCE(ac_12,0)+COALESCE(ac_34,0))
                 / NULLIF(appt_per_day,0), 5), 0)                   AS Rate_3_4_Days,
  COALESCE(ROUND((COALESCE(ac_same,0)+COALESCE(ac_12,0)+COALESCE(ac_34,0)+COALESCE(ac_56,0))
                 / NULLIF(appt_per_day,0), 5), 0)                   AS Rate_5_6_Days,
  COALESCE(ROUND(ac_7p / NULLIF(appt_per_day, 0), 5), 0)           AS Rate_7_Plus_Days,
  COALESCE(CAST(ROUND(days_appt) AS BIGINT), 0)                    AS Days_with_Appointments,
  COALESCE(ROUND(notes_per_appt / NULLIF(appt_per_day, 0), 5), 0)  AS Calculated_Time_in_Notes_per_Appointment,
  COALESCE(ROUND(notes_per_day  / NULLIF(days_appt, 0), 5), 0)     AS Calculated_Time_in_Notes_per_Day,
  COALESCE(ROUND((pajama        / NULLIF(calc_weeks,0)) * 4 / 60, 5), 0) AS New_Numerator_Pajama_Time_hrs,
  COALESCE(ROUND((unsched       / NULLIF(calc_weeks,0)) * 4 / 60, 5), 0) AS New_Numerator_Time_On_Unscheduled_Days_hrs,
  COALESCE(ROUND((outside_sched / NULLIF(calc_weeks,0)) * 4 / 60, 5), 0) AS New_Numerator_Time_Outside_Scheduled_Hours_hrs,
  COALESCE(calc_sum_est, 0.0)                                      AS Sum_of_Established_Patients,
  COALESCE(ROUND(ep1 / NULLIF(calc_sum_est,0), 5), 0.0)           AS Established_Level_1,
  COALESCE(ROUND(ep2 / NULLIF(calc_sum_est,0), 5), 0.0)           AS Established_Level_2,
  COALESCE(ROUND(ep3 / NULLIF(calc_sum_est,0), 5), 0.0)           AS Established_Level_3,
  COALESCE(ROUND(ep4 / NULLIF(calc_sum_est,0), 5), 0.0)           AS Established_Level_4,
  COALESCE(ROUND(ep5 / NULLIF(calc_sum_est,0), 5), 0.0)           AS Established_Level_5,
  COALESCE(calc_sum_new, 0.0)                                     AS Sum_of_New_Patients,
  COALESCE(ROUND(np1 / NULLIF(calc_sum_new,0), 5), 0.0)          AS New_Patient_Level_1,
  COALESCE(ROUND(np2 / NULLIF(calc_sum_new,0), 5), 0.0)          AS New_Patient_Level_2,
  COALESCE(ROUND(np3 / NULLIF(calc_sum_new,0), 5), 0.0)          AS New_Patient_Level_3,
  COALESCE(ROUND(np4 / NULLIF(calc_sum_new,0), 5), 0.0)          AS New_Patient_Level_4,
  COALESCE(ROUND(np5 / NULLIF(calc_sum_new,0), 5), 0.0)          AS New_Patient_Level_5
FROM parsed