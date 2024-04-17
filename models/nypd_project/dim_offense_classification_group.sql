{{ config(materialized='table') }}

{{ config(tags=["DM"]) }}

WITH offense_classification_group_source AS (
	SELECT DISTINCT  
		COALESCE(ky_cd, -1) AS offense_classicifaction_group_code, -- always filled, dummy value NOT needed so far
		COALESCE(ofns_desc, 'UNKNOWN') AS offense_classicifaction_group_description
	FROM 
		{{ ref('nypd_trg') }} 
	)
-- 
SELECT 
	upper(md5(offense_classicifaction_group_code::varchar||offense_classicifaction_group_description)) AS offense_classification_group_hkey, -- jurisdiction_code is unique 
	offense_classicifaction_group_code,
	offense_classicifaction_group_description
FROM 
	offense_classification_group_source