{{ config(materialized='table') }}

{{ config(tags=["DM"]) }}

WITH fct_source AS (
SELECT 
	cmplnt_num,
	coalesce(boro_nm,'UNKNOWN') AS boro_nm,
	jurisdiction_code,
	CASE 
		WHEN susp_age_group IN ('<18', '18-24', '25-44', '45-64', '65+', 'UNKNOWN') 
		THEN susp_age_group 
		ELSE 'UNKNOWN' 
	END AS susp_age_group , 
	CASE 
		WHEN vic_age_group IN ('<18', '18-24', '25-44', '45-64', '65+', 'UNKNOWN') 
		THEN vic_age_group 
		ELSE 'UNKNOWN' 
	END AS vic_age_group ,
	COALESCE(pd_cd,-1) AS pd_cd,
	COALESCE(pd_desc, 'UNKNOWN') AS pd_desc ,
	COALESCE(ky_cd,-1) AS ky_cd,
	COALESCE(ofns_desc, 'UNKNOWN') AS ofns_desc ,	
	patrol_boro,
	COALESCE(prem_typ_desc, 'UNKNOWN') AS prem_typ_desc,
	COALESCE(susp_race, 'UNKNOWN') AS susp_race,
	COALESCE(vic_race, 'UNKNOWN') AS vic_race,
	coalesce(susp_sex,'UNKNOWN') AS susp_sex,
	coalesce(vic_sex,'UNKNOWN') AS vic_sex,
	coalesce(parks_nm,'UNKNOWN') AS parks_nm,
	CASE WHEN crm_atpt_cptd_cd = 'COMPLETED' THEN TRUE ELSE FALSE END AS is_completed,
	CASE WHEN crm_atpt_cptd_cd = 'ATTEMPTED' THEN TRUE ELSE FALSE END AS is_attempted,
	CASE WHEN law_cat_cd = 'FELONY' THEN TRUE ELSE FALSE END AS is_felony,
	CASE WHEN law_cat_cd = 'MISDEMEANOR' THEN TRUE ELSE FALSE END AS is_misdemeanor,
	CASE WHEN law_cat_cd = 'VIOLATION' THEN TRUE ELSE FALSE END AS is_violation,
	juris_desc
FROM nypd_trg
)

SELECT 
	dim_complaint.complaint_hkey,
	dim_patrol_borough.patrol_borough_hkey ,
	suspect_age_group.age_group_hkey AS suspect_age_group_hkey,
	victim_age_group.age_group_hkey AS victim_age_group_hkey,
	dim_borough.borough_hkey,
	dim_jurisdiction.jurisdiction_hkey,
	dim_offense_classification.classification_hkey ,
	dim_offense_classification_group.offense_classification_group_hkey ,
	dim_parks.parks_hkey ,
	dim_premises.premises_hkey ,
	suspect_race.race_hkey AS suspect_race_hkey,
	victim_race.race_hkey AS victim_race_hkey,
	suspect_sex.sex_hkey AS suspect_sex_hkey,
	victim_sex.sex_hkey AS victim_sex_hkey,
	is_completed,
	is_attempted,
	is_felony,
	is_misdemeanor,
	is_violation
FROM 
	fct_source
	INNER JOIN {{ ref('dim_complaint') }} ON 
		dim_complaint.complaint_number = fct_source.cmplnt_num  
	INNER JOIN {{ ref('dim_patrol_borough') }} ON 
		dim_patrol_borough.patrol_borough = fct_source.patrol_boro 
	INNER JOIN {{ ref('dim_age_group') }} suspect_age_group ON 
		suspect_age_group.age_group = fct_source.susp_age_group 
	INNER JOIN {{ ref('dim_age_group') }} victim_age_group ON 
		victim_age_group.age_group = fct_source.vic_age_group
	INNER JOIN {{ ref('dim_borough') }} ON 
		dim_borough.borough = fct_source.boro_nm 
	INNER JOIN {{ ref('dim_jurisdiction') }} ON 
		dim_jurisdiction.jurisdiction_description = fct_source.juris_desc 
	INNER JOIN {{ ref('dim_offense_classification') }} ON 
		dim_offense_classification.classicifaction_code = fct_source.pd_cd
		AND dim_offense_classification.classification_description = fct_source.pd_desc
	INNER JOIN {{ ref('dim_offense_classification_group') }} ON 
		dim_offense_classification_group.offense_classicifaction_group_code = fct_source.ky_cd
		AND dim_offense_classification_group.offense_classicifaction_group_description = fct_source.ofns_desc
	INNER JOIN {{ ref('dim_parks') }} ON 
		dim_parks.parks = fct_source.parks_nm 		
	INNER JOIN {{ ref('dim_premises') }} ON 
		dim_premises.premises = fct_source.prem_typ_desc 			
	INNER JOIN {{ ref('dim_race') }} AS suspect_race ON 
		suspect_race.race = fct_source.susp_race 
	INNER JOIN {{ ref('dim_race') }} AS victim_race ON 
		victim_race.race = fct_source.vic_race 		
	INNER JOIN {{ ref('dim_sex') }} AS suspect_sex ON 
		suspect_sex.sex = fct_source.susp_sex 
	INNER JOIN {{ ref('dim_sex') }} AS victim_sex ON 
		victim_sex.sex = fct_source.vic_sex	