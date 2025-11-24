-- Deduplication Table
CREATE OR REPLACE TABLE JOB_PORTAL_DB.CLEAN.JOB_DATA_DEDUPE AS
SELECT * FROM (
  SELECT *,
         ROW_NUMBER() OVER (
           PARTITION BY company_name, job_title, job_location
           ORDER BY load_time DESC
         ) AS rn
  FROM JOB_PORTAL_DB.CLEAN.JOB_DATA_CLEANED
) WHERE rn = 1;

-- BI Summary Table
CREATE OR REPLACE TABLE JOB_PORTAL_DB.BI.JOBS_SUMMARY AS
SELECT
  job_location,
  COUNT(*) AS total_jobs,
  AVG(salary_min) AS avg_salary_min,
  AVG(salary_max) AS avg_salary_max
FROM JOB_PORTAL_DB.CLEAN.JOB_DATA_DEDUPE
GROUP BY job_location;