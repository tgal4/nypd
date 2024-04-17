{{ config(materialized='table') }}

{{ config(tags=["DM"]) }}

WITH classification_source AS (
	SELECT DISTINCT 
		COALESCE(pd_cd,-1) as classicifaction_code, 
		COALESCE(pd_desc, 'UNKNOWN') as classification_description 
	FROM 
		{{ ref('nypd_trg') }} 
	)

SELECT 
	upper(md5(classicifaction_code::varchar || classification_description )) AS classification_hkey, -- using md5 to ID generation. On large scale it will have duplicates.
	classicifaction_code AS classicifaction_code,
	classification_description AS classification_description
FROM 
	classification_source