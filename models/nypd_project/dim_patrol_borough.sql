{{ config(materialized='table') }}

{{ config(tags=["DM"]) }}

WITH patrol_borough_source AS (
	SELECT DISTINCT  
		patrol_boro AS patrol_borough -- ALWAYS filled
	FROM 
		{{ ref('nypd_trg') }} 
	)
-- 
SELECT 
	upper(md5(patrol_borough)) AS patrol_borough_hkey, -- jurisdiction_code is unique 
	patrol_borough
FROM 
	patrol_borough_source