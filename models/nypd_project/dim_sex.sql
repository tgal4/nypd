{{ config(materialized='table') }}

{{ config(tags=["DM"]) }}

WITH sex_source AS (
	SELECT DISTINCT  
		susp_sex AS sex
	FROM 
		{{ ref('nypd_trg') }}
	UNION
	SELECT DISTINCT  
		vic_sex AS sex
	FROM 
		{{ ref('nypd_trg') }}
	)
SELECT 
	upper(md5(COALESCE(sex, 'UNKNOWN'))) AS sex_hkey,
	COALESCE(sex,'UNKNOWN') AS sex
FROM 
	sex_source