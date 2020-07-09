/* 
Insert,Soft Delete and Update is done based on the Flags(I,D,U and FU) in Merch_Level2 table
*/
UPDATE 
	`{{ params.project_ID }}.{{ params.Optumera_Target_DS }}.Merch_Level2` T
SET
	T.ACTIVE_F = 'N'
	,T.DELETE_F = 'Y'
	,T.UPDATED_USER = 'TOPMSTRMERHDL'
	,T.UPDATED_TS = CURRENT_TIMESTAMP()
FROM 
	`{{ params.project_ID }}.{{ params.Optumera_Stage_DS }}.Delta_Merch_Hierarchy_SubDepartment` S
WHERE 
	S.Flag = 'D'
AND S.Prev_Sales_org = T.ATTR_2
AND CONCAT(S.Prev_Department_Name,'|||',S.Prev_SubDepartment_Name)= T.ATTR_3;

/*Merch level 2 - Update DELETE FROM Flag(FU) */

UPDATE 
`{{ params.project_ID }}.{{ params.Optumera_Target_DS }}.Merch_Level2` T
SET
	T.ACTIVE_F = 'Y'
	,T.DELETE_F = 'N'
	,T.UPDATED_USER = 'TOPMSTRMERHDL'
	,T.UPDATED_TS = CURRENT_TIMESTAMP()
FROM 
	`{{ params.project_ID }}.{{ params.Optumera_Stage_DS }}.Delta_Merch_Hierarchy_SubDepartment` S
WHERE 
	S.Flag = 'FU'
AND S.Curr_Sales_org = T.ATTR_2
AND CONCAT(S.Curr_Department_Name,'|||',S.Curr_SubDepartment_Name)= T.ATTR_3;

/*Merch level 2 - Insert */
INSERT
	`{{ params.project_ID }}.{{ params.Optumera_Target_DS }}.Merch_Level2` (
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
	UPDATED_TS )
SELECT
	Merch1.MERCH_LEVEL1_ID,
(SELECT
	MAX(MERCH_LEVEL2_ID)
FROM
	`{{ params.project_ID }}.{{ params.Optumera_Target_DS }}.Merch_Level2`)+ROW_NUMBER() OVER() AS MERCH_LEVEL2_ID,
	a.Curr_SubDepartment_Name,
	a.Curr_SubDepartment_Name,
	'Y',
	a.Curr_SubDepartment_Name,
	CAST (a.Curr_Sales_org AS STRING),
	CONCAT(Curr_Department_Name,'|||',Curr_SubDepartment_Name),
	'N',
	b.banner_id,
	'TOPMSTRMERHDL',
	CURRENT_TIMESTAMP(),
	'TOPMSTRMERHDL',
	CURRENT_TIMESTAMP()
FROM
	`{{ params.project_ID }}.{{ params.Optumera_Stage_DS }}.Delta_Merch_Hierarchy_SubDepartment` a
INNER JOIN
	`{{ params.project_ID }}.{{ params.Optumera_Target_DS }}.Banner` b
ON
	b.RETAILER_BANNER_ID  = a.Curr_Sales_Org
AND a.flag = 'I'
INNER JOIN
	`{{ params.project_ID }}.{{ params.Optumera_Target_DS }}.Merch_Level1` Merch1
ON
	Merch1.MERCH_LEVEL1_NAME = a.Curr_Department_Name
AND Merch1.ATTR_2 = a.Curr_Sales_org