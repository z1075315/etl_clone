/*
1. Select distinct department, sub-department category and join with Merch_level_2 table to get the MERCH_LEVEL1_ID, MERCH_LEVEL2_ID 
2. generate row number over MERCH_LEVEL3_ID and build MERCH_LEVEL3 table.
*/

DELETE FROM `{{ params.project_ID }}.{{ params.Optumera_Target_DS }}.Merch_Level3` WHERE TRUE;
INSERT
	`{{ params.project_ID }}.{{ params.Optumera_Target_DS }}.Merch_Level3` ( MERCH_LEVEL1_ID,
	MERCH_LEVEL2_ID,
	MERCH_LEVEL3_ID,
	MERCH_LEVEL3_NAME,
	MERCH_LEVEL3_DESC,
	ACTIVE_F,
	RETAILER_MERCH_LEVEL3_ID,
	ATTR_2,
	ATTR_3,
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
	Department_Desc,
	SubDepartment_Name,
	SubDepartment_Desc,
	Category_Name,
	Category_Desc
FROM
	`{{ params.project_ID }}.{{ params.Optumera_Stage_DS }}.Stage_Merch_Hierarchy` 
ORDER BY
	Sales_org,Department_Name,SubDepartment_Name,Category_Name	
),Cat as
(
SELECT
	Merch2.MERCH_LEVEL1_ID,
	Merch2.MERCH_LEVEL2_ID,
	Category_Name as MERCH_LEVEL3_NAME,
	Category_Desc as MERCH_LEVEL3_DESC,
	'Y' as ACTIVE_F,
	Category_Name as RETAILER_MERCH_LEVEL3_ID,
	CAST (Sales_org AS STRING) as ATTR_2,
	CONCAT(Department_Name,'|||',SubDepartment_Name,'|||',Category_Name) AS ATTR_3,
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
	b.RETAILER_BANNER_ID = a.Sales_Org
INNER JOIN
	`{{ params.project_ID }}.{{ params.Optumera_Target_DS }}.Merch_Level2` Merch2 
ON
	Merch2.ATTR_3 = CONCAT(a.Department_Name,'|||',a.SubDepartment_Name)
AND Merch2.ATTR_2  = a.Sales_Org

ORDER BY 
	ATTR_2,MERCH_LEVEL1_ID,MERCH_LEVEL2_ID,MERCH_LEVEL3_NAME
)
SELECT
	MERCH_LEVEL1_ID,
	MERCH_LEVEL2_ID,
	ROW_NUMBER() OVER () MERCH_LEVEL3_ID,
	MERCH_LEVEL3_NAME,
	MERCH_LEVEL3_DESC,
	ACTIVE_F,
	RETAILER_MERCH_LEVEL3_ID,
	ATTR_2,
	ATTR_3,
	DELETE_F,
	BANNER_ID,
	CREATED_USER,
	CREATED_TS,
	UPDATED_USER,
	UPDATED_TS 
FROM
	Cat a
