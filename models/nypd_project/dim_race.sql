{{ config(materialized='table') }}

{{ config(tags=["DM"]) }}

WITH race_source AS (
	SELECT DISTINCT  
		susp_race AS race -- ALWAYS filled
	FROM 
		{{ ref('nypd_trg') }} 
	UNION
	SELECT DISTINCT  
		vic_race AS race -- ALWAYS filled
	FROM 
		{{ ref('nypd_trg') }}
	)
SELECT 
	upper(md5(COALESCE(race,'UNKNOWN'))) AS race_hkey,
	race
FROM 
	race_source
WHERE
	-- we can filter out nulls, there is an unknown value already
	race IS NOT null