/*
Join Merch_Level3 with Stage_Adjacency and populate MERCH_LEVEL3_ID's for respective hierarchy and its adjacents.
*/
--Insert Record--
CREATE OR REPLACE TABLE  
`{{params.project_ID}}.{{params.Optumera_Target_DS}}.Str_Merch_Lvl3_Adjacency`(
  STORE_ID INT64 NOT NULL
  , MERCH_LEVEL3_ID INT64 NOT NULL
  , FOOTAGE FLOAT64
  , AISLE STRING
  , AISLE_SIDE_VALLEY STRING
  , VALLEY STRING
  , SECTION_ID INT64
  , LEFT_MERCH_LEVEL3_ID INT64
  , RIGHT_MERCH_LEVEL3_ID INT64
  , CREATED_USER STRING NOT NULL
  , CREATED_TS TIMESTAMP NOT NULL
  , UPDATED_USER STRING
  , UPDATED_TS TIMESTAMP
 ) AS
 SELECT 
 DISTINCT 
 STORE_ID
  , MERCH_LEVEL3_ID
  , FOOTAGE
  , AISLE
  , AISLESIDE
  , VALLEY
  , SECTION_ID
  , LEFT_MERCH_LEVEL3_ID
  , RIGHT_MERCH_LEVEL3_ID
  , CREATED_USER
  , CREATED_TS
  , UPDATED_USER
  , UPDATED_TS
  
  FROM(
SELECT   
	store.STORE_ID
	, merch.MERCH_LEVEL3_ID AS MERCH_LEVEL3_ID
	, FOOTAGE
	, AISLE 
	, AISLESIDE
	, VALLEY
	, NULL as SECTION_ID
	, merch_l.MERCH_LEVEL3_ID AS LEFT_MERCH_LEVEL3_ID
	, merch_r.MERCH_LEVEL3_ID AS RIGHT_MERCH_LEVEL3_ID
	, 'TOPDTLADJFL' AS CREATED_USER
	, CURRENT_TIMESTAMP() AS CREATED_TS
	, 'TOPDTLADJFL' AS UPDATED_USER
	, CURRENT_TIMESTAMP() AS UPDATED_TS
FROM 
	`{{params.project_ID}}.{{params.Optumera_Stage_DS}}.Stage_Adjacency` sa
INNER JOIN
	`{{params.project_ID}}.{{params.Optumera_Target_DS}}.Store` store ON 
	store.STORE_NUMBER = sa.STORE_NO
	AND store.TEXT_1 = sa.SALES_ORG 
	AND store.ACTIVE_F  = 'Y'
INNER JOIN
	`{{params.project_ID}}.{{params.Optumera_Target_DS}}.Merch_Level3` merch ON 
	merch.ATTR_3 = CONCAT(sa.DEPARTMENT,'|||',sa.SUBDEPARTMENT,'|||',sa.CATEGORY)
	AND merch.ATTR_2 = sa.SALES_ORG
	AND merch.ACTIVE_F = 'Y'
LEFT OUTER JOIN 
	`{{params.project_ID}}.{{params.Optumera_Target_DS}}.Merch_Level3` merch_l ON 
	merch_l.ATTR_3 = sa.LEFT_SPACE_LEVEL_ID
	AND merch_l.ATTR_2 = sa.SALES_ORG
	AND merch_l.ACTIVE_F = 'Y'
LEFT OUTER JOIN 
	`{{params.project_ID}}.{{params.Optumera_Target_DS}}.Merch_Level3` merch_r ON 
	merch_r.ATTR_3 = sa.RIGHT_SPACE_LEVEL_ID
	AND merch_r.ATTR_2 = sa.SALES_ORG
	AND merch_r.ACTIVE_F = 'Y'
WHERE 
	(merch_l.MERCH_LEVEL3_ID IS NOT NULL AND merch_r.MERCH_LEVEL3_ID IS NOT NULL) OR 
	(merch_l.MERCH_LEVEL3_ID IS NULL AND merch_r.MERCH_LEVEL3_ID IS NOT NULL) OR 
	(merch_l.MERCH_LEVEL3_ID IS NOT NULL AND merch_r.MERCH_LEVEL3_ID IS NULL) 
	)
	
WHERE LEFT_MERCH_LEVEL3_ID IS NOT NULL OR RIGHT_MERCH_LEVEL3_ID IS NOT NULL