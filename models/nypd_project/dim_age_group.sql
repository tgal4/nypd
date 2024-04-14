{{ config(materialized='table') }}

WITH age_group_source AS (
	SELECT DISTINCT  
		susp_age_group AS age_group -- ALWAYS filled
	FROM 
		{{ ref('nypd_trg') }} 
	UNION
	SELECT DISTINCT  
		vic_age_group AS age_group -- ALWAYS filled
	FROM 
		{{ ref('nypd_trg') }}
	),
age_group_filtered AS (
	SELECT 
		age_group 
	FROM 
		age_group_source
	WHERE
		-- hardcoding, dbt test can be created on raw data for this
		age_group IN ('<18', '18-24', '25-44', '45-64', '65+', 'UNKNOWN')
		)
SELECT 
	upper(md5(age_group)) AS age_group_hkey,
	age_group
FROM 
	age_group_filtered