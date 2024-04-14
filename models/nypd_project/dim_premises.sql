{{ config(materialized='table') }}

WITH premises_source AS (
	SELECT DISTINCT  
		COALESCE(prem_typ_desc, 'UNKNOWN') AS premises
	FROM 
		{{ ref('nypd_trg') }}
	)
SELECT 
	upper(md5(premises)) AS premises_hkey,
	premises
FROM 
	premises_source