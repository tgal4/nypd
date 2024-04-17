{{ config(materialized='table') }}

{{ config(tags=["DM"]) }}

WITH borough_source AS (
	SELECT DISTINCT  
		COALESCE(boro_nm, 'UNKNOWN') AS borough
	FROM 
		{{ ref('nypd_trg') }} 
	)
-- 
SELECT 
	upper(md5(borough)) AS borough_hkey,
	borough
FROM 
	borough_source