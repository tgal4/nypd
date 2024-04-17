{{ config(materialized='table') }}

{{ config(tags=["DM"]) }}

WITH parks_source AS (
	SELECT DISTINCT  
		COALESCE(parks_nm, 'UNKNOWN') AS parks
	FROM 
		{{ ref('nypd_trg') }} 
	)
SELECT 
	upper(md5(parks)) AS parks_hkey,
	parks
FROM 
	parks_source