{{ config(materialized='table') }}

WITH jurisdiction_source AS (
	SELECT DISTINCT  
		jurisdiction_code AS jurisdiction_code, -- always filled, dummy value NOT needed so far
		juris_desc AS jurisdiction_description
	FROM 
		{{ ref('nypd_trg') }} 
	)
-- 
SELECT 
	upper(md5(jurisdiction_code::varchar)) AS jurisdiction_hkey, -- jurisdiction_code is unique 
	jurisdiction_description
FROM 
	jurisdiction_source