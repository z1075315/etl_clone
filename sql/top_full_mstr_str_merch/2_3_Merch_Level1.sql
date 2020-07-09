/*
1. Select distinct department and generate row number over MERCH_LEVEL1_ID and build MERCH_LEVEL1 table.
*/

DELETE FROM `{{ params.project_ID }}.{{ params.Optumera_Target_DS }}.Merch_Level1` WHERE TRUE;
INSERT
	`{{ params.project_ID }}.{{ params.Optumera_Target_DS }}.Merch_Level1` ( MERCH_LEVEL1_ID,
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
WITH
Hier AS (
SELECT DISTINCT 
	Sales_org,
	Department_Name,
	Department_Desc
FROM
	`{{ params.project_ID }}.{{ params.Optumera_Stage_DS }}.Stage_Merch_Hierarchy` 
),Dept as 
(
SELECT
	Department_Name as MERCH_LEVEL1_NAME,
	Department_Desc as MERCH_LEVEL1_DESC,
	'Y' as ACTIVE_F,
	Department_Name as RETAILER_MERCH_LEVEL1_ID,
	CAST (Sales_org AS STRING) as ATTR_2,
	'N' as DELETE_F,
	b.banner_id as BANNER_ID,
	'TOPMSTRMERHFL' as CREATED_USER,
	CURRENT_TIMESTAMP() as CREATED_TS,
	'TOPMSTRMERHFL' as UPDATED_USER,
	CURRENT_TIMESTAMP() as UPDATED_TS
FROM
	Hier a
INNER JOIN
	`{{ params.project_ID }}.{{ params.Optumera_Target_DS }}.Banner` b 
ON
	b.RETAILER_BANNER_ID  = a.Sales_Org
ORDER BY
	ATTR_2,MERCH_LEVEL1_NAME
)
SELECT
	ROW_NUMBER() OVER () MERCH_LEVEL1_ID,
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
	Dept
