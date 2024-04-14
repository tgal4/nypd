{{ config(materialized='table') }}

with nypd_source as (
    SELECT 
        CASE WHEN CMPLNT_NUM = '(null)' THEN NULL ELSE CMPLNT_NUM END AS CMPLNT_NUM, -- Randomly generated persistent ID for each complaint
        CASE WHEN addr_pct_cd = '(null)' then null else addr_pct_cd::bigint END AS addr_pct_cd, -- The precinct in which the incident occurred
        CASE WHEN boro_nm  = '(null)' then null else boro_nm END AS boro_nm, -- The name of the borough in which the incident occurred
        CASE WHEN cmplnt_fr_dt = '(null)' THEN NULL ELSE cmplnt_fr_dt::date END AS cmplnt_fr_dt, -- Exact date of occurrence for the reported event (or starting date of occurrence, if CMPLNT_TO_DT exists)
        CASE WHEN cmplnt_fr_tm = '(null)' THEN NULL ELSE cmplnt_fr_tm END AS cmplnt_fr_tm, -- Exact time of occurrence for the reported event (or starting time of occurrence, if CMPLNT_TO_TM exists)
        CAST(cmplnt_fr_dt || ' ' || cmplnt_fr_tm AS timestamp) AS cmplnt_fr, -- calc
        CASE WHEN cmplnt_to_dt = '(null)' THEN NULL ELSE cmplnt_to_dt::date END AS cmplnt_to_dt, -- Ending date of occurrence for the reported event, if exact time of occurrence is unknown
        CASE WHEN cmplnt_to_tm = '(null)' THEN NULL ELSE cmplnt_to_tm END AS cmplnt_to_tm, -- Ending time of occurrence for the reported event, if exact time of occurrence is unknown
        CAST(CASE WHEN cmplnt_to_dt = '(null)' THEN NULL ELSE cmplnt_to_dt::date END || ' ' || CASE WHEN cmplnt_to_tm = '(null)' THEN NULL ELSE cmplnt_to_tm END AS timestamp) AS cmplnt_to, -- calc
        CASE WHEN crm_atpt_cptd_cd = '(null)'THEN NULL ELSE crm_atpt_cptd_cd END AS crm_atpt_cptd_cd, -- Indicator of whether crime was successfully completed or attempted, but failed or was interrupted prematurely
        CASE WHEN hadevelopt = '(null)' THEN NULL ELSE hadevelopt END AS hadevelopt, -- Name of NYCHA housing development of occurrence, if applicable
        CASE WHEN housing_psa = '(null)' THEN NULL ELSE housing_psa::bigint END AS housing_psa, -- Development Level Code
        CASE WHEN jurisdiction_code = '(null)' THEN NULL ELSE jurisdiction_code::bigint END AS jurisdiction_code, -- Jurisdiction responsible for incident. Either internal, like Police(0), Transit(1), and Housing(2); or external(3), like Correction, Port Authority, etc.
        CASE WHEN juris_desc = '(null)' THEN NULL ELSE juris_desc END AS juris_desc, -- Description of the jurisdiction code
        CASE WHEN ky_cd = '(null)' THEN NULL ELSE ky_cd::bigint END AS ky_cd, -- Three digit offense classification code
        CASE WHEN law_cat_cd = '(null)' THEN NULL ELSE law_cat_cd END AS law_cat_cd, -- Level of offense: felony, misdemeanor, violation
        CASE WHEN loc_of_occur_desc = '(null)' THEN NULL ELSE loc_of_occur_desc END AS loc_of_occur_desc, -- Specific location of occurrence in or around the premises; inside, opposite of, front of, rear of
        CASE WHEN ofns_desc = '(null)' THEN NULL ELSE ofns_desc END AS ofns_desc, -- Description of offense corresponding with key code
        CASE WHEN parks_nm = '(null)' THEN NULL ELSE parks_nm END AS parks_nm, -- Name of NYC park, playground or greenspace of occurrence, if applicable (state parks are not included)
        CASE WHEN patrol_boro = '(null)' THEN NULL ELSE patrol_boro END AS patrol_boro, -- The name of the patrol borough in which the incident occurred
        CASE WHEN pd_cd = '(null)' THEN NULL ELSE pd_cd::bigint END AS pd_cd, -- Three digit internal classification code (more granular than Key Code)
        CASE WHEN pd_desc = '(null)' THEN NULL ELSE pd_desc END AS pd_desc, -- Description of internal classification corresponding with PD code (more granular than Offense Description)
        CASE WHEN prem_typ_desc = '(null)' THEN NULL ELSE prem_typ_desc END AS prem_typ_desc, -- Specific description of premises; grocery store, residence, street, etc.
        CASE WHEN rpt_dt = '(null)' THEN NULL ELSE rpt_dt::date END AS rpt_dt, -- Date event was reported to police
        CASE WHEN station_name = '(null)' THEN NULL ELSE station_name END AS station_name, -- Transit station name
        CASE WHEN susp_age_group = '(null)' THEN NULL ELSE susp_age_group END AS susp_age_group, -- Suspect’s Age Group
        CASE WHEN susp_race = '(null)' THEN NULL ELSE susp_race END AS susp_race, -- Suspect’s Race Description
        CASE WHEN susp_sex = '(null)' THEN NULL ELSE susp_sex END AS susp_sex, -- Suspect’s Sex Description
        CASE WHEN transit_district = '(null)' THEN NULL ELSE transit_district END AS transit_district, -- Transit district in which the offense occurred.
        CASE WHEN vic_age_group = '(null)' THEN NULL ELSE vic_age_group END AS vic_age_group, -- Victim’s Age Group
        CASE WHEN vic_race = '(null)' THEN NULL ELSE vic_race END AS vic_race, -- Victim’s Race Description
        CASE WHEN vic_sex = '(null)' THEN NULL ELSE vic_sex END AS vic_sex, -- Victim’s Sex Description
        CASE WHEN x_coord_cd = '(null)' THEN NULL ELSE x_coord_cd::bigint END AS x_coord_cd,
        CASE WHEN y_coord_cd = '(null)' THEN NULL ELSE y_coord_cd::bigint END AS y_coord_cd,
        CASE WHEN latitude = '(null)' THEN NULL ELSE latitude::float END AS latitude,
        CASE WHEN longitude = '(null)' THEN NULL ELSE longitude::float END AS longitude,
        CASE WHEN lat_lon = '(null)' THEN NULL ELSE lat_lon END AS lat_lon,
        CASE WHEN new_georeferenced_column = '(null)' THEN NULL ELSE new_georeferenced_column END AS new_georeferenced_column
    FROM nypd
)

SELECT * FROM nypd_source