SELECT 
	dc.complaint_number,
	extract('year' FROM dc.complaint_from_dt::date) as complaint_year,
	extract('month' FROM dc.complaint_from_dt::date) as complaint_month,
	dc.complaint_from_dt::date as complaint_date,
    dc.complaint_from_dt::time as complaint_time,
    case 
        when extract('hour' FROM dc.complaint_from_dt) >= 18 or extract('hour' FROM dc.complaint_from_dt) < 6 then 'NIGHT'
        when extract('hour' FROM dc.complaint_from_dt) between 6 and 11 then 'MORNING'
        when extract('hour' FROM dc.complaint_from_dt) between 12 and 17 then 'AFTERNOON' 
    end as complaint_part_of_the_day,
	dc.complaint_reporting_dt as complaint_reported_date,
	sdr.race as suspect_race,
	sds.sex as suspect_sex,
    sdag.age_group AS suspect_age_group,
	vdr.race as victim_race,
	vds.sex as victim_sex,
	vdag.age_group AS victim_age_group,
	dj.jurisdiction_description,
	db.borough,
	dpb.patrol_borough,
	docg.offense_classicifaction_group_description,
	doc.classification_description,
	1 AS crime,
	fc.is_completed::int as crime_success,
	fc.is_attempted::int as crime_attempted,
	fc.is_felony::int as crime_felony,
	fc.is_misdemeanor::int as crime_misdemeanor,
	fc.is_violation::int as crime_violation,
    CASE 
        WHEN (case when sdr.race != 'UNKNOWN' THEN 1 ELSE 0 END + CASE WHEN sds.sex != 'UNKNOWN' THEN 1 ELSE 0 END + CASE WHEN sdag.age_group != 'UNKNOWN' THEN 1 ELSE 0 END) = 0 then 'UNIDENTIFIED'
        WHEN (case when sdr.race != 'UNKNOWN' THEN 1 ELSE 0 END + CASE WHEN sds.sex != 'UNKNOWN' THEN 1 ELSE 0 END + CASE WHEN sdag.age_group != 'UNKNOWN' THEN 1 ELSE 0 END) between 1 and 2 then 'PARTIALLY IDENTIFIED'
        WHEN (case when sdr.race != 'UNKNOWN' THEN 1 ELSE 0 END + CASE WHEN sds.sex != 'UNKNOWN' THEN 1 ELSE 0 END + CASE WHEN sdag.age_group != 'UNKNOWN' THEN 1 ELSE 0 END) = 3 then 'IDENTIFIED'
    END AS suspect_identified    
FROM 
	{{ ref('fct_complaint') }} fc
	-- to gather basic information from a complaint
	INNER JOIN {{ ref('dim_complaint') }} dc ON
		fc.complaint_hkey = dc.complaint_hkey 
	-- to gather suspect age
	INNER JOIN {{ ref('dim_age_group') }} sdag ON
		fc.suspect_age_group_hkey = sdag.age_group_hkey  
	-- to gather victim age
	INNER JOIN {{ ref('dim_age_group') }} vdag ON
		fc.victim_age_group_hkey = vdag.age_group_hkey  
	-- to gather suspect race
	INNER JOIN {{ ref('dim_race') }} sdr ON
		fc.suspect_race_hkey = sdr.race_hkey  
	-- to gather victim race
	INNER JOIN {{ ref('dim_race') }} vdr ON
		fc.victim_race_hkey = vdr.race_hkey  
	-- to gather suspect sex
	INNER JOIN {{ ref('dim_sex') }} sds ON
		fc.suspect_sex_hkey = sds.sex_hkey  
	-- to gather victim sex
	INNER JOIN {{ ref('dim_sex') }} vds ON
		fc.victim_sex_hkey = vds.sex_hkey  
	-- to gather the responsible jurisdiction
	INNER JOIN {{ ref('dim_jurisdiction') }} dj ON
		fc.jurisdiction_hkey = dj.jurisdiction_hkey
	-- to gather the borough
	INNER JOIN {{ ref('dim_borough') }} db  ON
		fc.borough_hkey = db.borough_hkey
	-- to gather the patrol borough
	INNER JOIN {{ ref('dim_patrol_borough') }} dpb  ON
		fc.patrol_borough_hkey = dpb.patrol_borough_hkey
	-- to gather classification group
	INNER JOIN {{ ref('dim_offense_classification_group') }} docg ON
		fc.offense_classification_group_hkey = docg.offense_classification_group_hkey
	-- to gather classification 
	INNER JOIN {{ ref('dim_offense_classification') }} doc ON
		fc.classification_hkey = doc.classification_hkey