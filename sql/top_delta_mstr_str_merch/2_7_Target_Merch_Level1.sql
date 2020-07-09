/* 
Insert,Soft Delete and Update is done based on the Flags(I,D,U and FU) in Merch_Level1 table
*/
UPDATE 
	`{{ params.project_ID }}.{{ params.Optumera_Target_DS }}.Merch_Level1` T
SET
	T.ACTIVE_F = 'N'
	,T.DELETE_F = 'Y'
	,T.UPDATED_USER = 'TOPMSTRMERHDL'
	,T.UPDATED_TS = CURRENT_TIMESTAMP()
FROM 
	`{{ params.project_ID }}.{{ params.Optumera_Stage_DS }}.Delta_Merch_Hierarchy_Department` S
WHERE 
S.Flag = 'D'
AND S.Prev_Sales_org = T.ATTR_2
AND S.Prev_Department_Name = T.MERCH_LEVEL1_NAME;

/*Merch level 1 - Update DELETE FROM Flag(FU) */

UPDATE 
	`{{ params.project_ID }}.{{ params.Optumera_Target_DS }}.Merch_Level1` T
SET
	T.ACTIVE_F = 'Y'
	,T.DELETE_F = 'N'
	,T.UPDATED_USER = 'TOPMSTRMERHDL'
	,T.UPDATED_TS = CURRENT_TIMESTAMP()
FROM 
	`{{ params.project_ID }}.{{ params.Optumera_Stage_DS }}.Delta_Merch_Hierarchy_Department` S
WHERE 
	S.Flag = 'FU'
AND S.Curr_Sales_org = T.ATTR_2
AND S.Curr_Department_Name = T.MERCH_LEVEL1_NAME;


/*Merch level 1 - Insert */

INSERT
	`{{ params.project_ID }}.{{ params.Optumera_Target_DS }}.Merch_Level1` (
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
	UPDATED_TS )
SELECT
(SELECT
	MAX(MERCH_LEVEL1_ID)
FROM
	`{{ params.project_ID }}.{{ params.Optumera_Target_DS }}.Merch_Level1`)+ROW_NUMBER() OVER() AS MERCH_LEVEL1_ID,
	a.Curr_Department_Name,
	a.Curr_Department_Name,
	'Y',
	a.Curr_Department_Name,
	CAST (a.Curr_Sales_org AS STRING),
	'N',
	b.banner_id,
	'TOPMSTRMERHDL',
	CURRENT_TIMESTAMP(),
	'TOPMSTRMERHDL',
	CURRENT_TIMESTAMP()
FROM
	`{{ params.project_ID }}.{{ params.Optumera_Stage_DS }}.Delta_Merch_Hierarchy_Department` a
INNER JOIN
	`{{ params.project_ID }}.{{ params.Optumera_Target_DS }}.Banner` b
ON
	b.RETAILER_BANNER_ID  = a.Curr_Sales_Org
AND a.flag = 'I'