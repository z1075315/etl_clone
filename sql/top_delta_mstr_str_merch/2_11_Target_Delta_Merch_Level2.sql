/*
Picking only the newly Updated records based on Delta run date and build the Delta_Merch_Level2 Table for loading into the bucket. 
*/

DELETE FROM `{{ params.project_ID }}.{{ params.Optumera_Target_DS }}.Delta_Merch_Level2` WHERE TRUE;
INSERT
	`{{ params.project_ID }}.{{ params.Optumera_Target_DS }}.Delta_Merch_Level2` ( MERCH_LEVEL1_ID,
	MERCH_LEVEL2_ID,
	MERCH_LEVEL2_NAME,
	MERCH_LEVEL2_DESC,
	ACTIVE_F,
	RETAILER_MERCH_LEVEL2_ID,
	ATTR_2,
	ATTR_3,
	DELETE_F,
	BANNER_ID,
	CREATED_USER,
	CREATED_TS,
	UPDATED_USER,
	UPDATED_TS )
SELECT
	MERCH_LEVEL1_ID,
	MERCH_LEVEL2_ID,
	MERCH_LEVEL2_NAME,
	MERCH_LEVEL2_DESC,
	ACTIVE_F,
	RETAILER_MERCH_LEVEL2_ID,
	ATTR_2,
	ATTR_3,
	DELETE_F,
	BANNER_ID,
	CREATED_USER,
	CREATED_TS,
	UPDATED_USER,
	UPDATED_TS 
FROM
	`{{ params.project_ID }}.{{ params.Optumera_Target_DS }}.Merch_Level2`
WHERE
	UPDATED_TS > TIMESTAMP(CURRENT_DATE)