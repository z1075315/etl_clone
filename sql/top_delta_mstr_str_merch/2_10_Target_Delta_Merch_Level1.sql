/*
Picking only the newly Updated records based on Delta run date and build the Delta_Merch_Level1 Table for loading into the bucket. 
*/
DELETE FROM `{{ params.project_ID }}.{{ params.Optumera_Target_DS }}.Delta_Merch_Level1` WHERE TRUE;
INSERT
	`{{ params.project_ID }}.{{ params.Optumera_Target_DS }}.Delta_Merch_Level1` ( MERCH_LEVEL1_ID,
	MERCH_LEVEL1_NAME,
	MERCH_LEVEL1_DESC,
	ACTIVE_F,
	RETAILER_MERCH_LEVEL1_ID,
	ATTR_2,
	DELETE_F,
	BANNER_ID,
	CREATED_USER,
	CREATED_TS,
	UPDATED_USER,
	UPDATED_TS )
SELECT
	MERCH_LEVEL1_ID,
	MERCH_LEVEL1_NAME,
	MERCH_LEVEL1_DESC,
	ACTIVE_F,
	RETAILER_MERCH_LEVEL1_ID,
	ATTR_2,
	DELETE_F,
	BANNER_ID,
	CREATED_USER,
	CREATED_TS,
	UPDATED_USER,
	UPDATED_TS
FROM
	`{{ params.project_ID }}.{{ params.Optumera_Target_DS }}.Merch_Level1`

WHERE
	UPDATED_TS > TIMESTAMP(CURRENT_DATE)