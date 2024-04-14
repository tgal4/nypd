{{ config(materialized='table') }}

WITH complaint_source AS (
	SELECT 
		cmplnt_num AS complaint_number,
		cmplnt_fr AS complaint_from_dt,
		cmplnt_to AS complaint_to_dt,
		rpt_dt AS complaint_reporting_dt,
		x_coord_cd AS complaint_ny_system_x_coord,
		y_coord_cd AS complaint_ny_system_y_coord,
		latitude,
		longitude
	FROM 
		{{ ref('nypd_trg') }})
 
SELECT 
	upper(md5(complaint_number)) AS complaint_hkey, -- complaint number IS UNIQUE IN the SOURCE DATA 
	complaint_number,
	complaint_from_dt,
	complaint_to_dt,
	complaint_reporting_dt,
	complaint_ny_system_x_coord,
	complaint_ny_system_y_coord,
	latitude,
	longitude
FROM 
	complaint_source