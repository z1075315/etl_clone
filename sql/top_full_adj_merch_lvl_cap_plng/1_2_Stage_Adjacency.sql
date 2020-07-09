/*
build the left and right hierarchy for all the hierarchy by concatenating department, subdepartment and category.
*/

--Insert Record--
CREATE OR REPLACE TABLE   
	`{{params.project_ID}}.{{params.Optumera_Stage_DS}}.Stage_Adjacency` (	
	SALES_ORG STRING
	,STORE_NO STRING
	,VALLEY STRING
	,AISLE STRING
	,AISLESIDE STRING
	,DEPARTMENT STRING
	,SUBDEPARTMENT STRING
	,CATEGORY STRING
	,FOOTAGE FLOAT64
	,LEFT_SPACE_LEVEL_ID STRING
	,RIGHT_SPACE_LEVEL_ID STRING
	,VALIDFROM DATE
	,VALIDTO DATE
) AS
WITH POG_Store_Bays AS (
SELECT 
	
	Department
	, SubDepartment
	, Category
	, StoreNo
	, SalesOrg
	, Footage
	, Aisle, AisleSide  
	, LocationName
	, StartBay  
	, ValidFrom, ValidTo
	, FLR_Key, POG_Key 
	, lag(StartBay, 1) OVER (PARTITION BY FLR_Key, LocationName, Aisle, AisleSide ORDER BY StartBay) PreviousStartBay
	, lead(StartBay, 1) OVER (PARTITION BY FLR_Key, LocationName, Aisle, AisleSide ORDER BY StartBay) NextStartBay    
FROM
	`{{params.project_ID}}.{{params.Optumera_Stage_DS}}.Adjacency_Footage_Live`
)
SELECT 
DISTINCT
	src.SalesOrg
	, src.StoreNo, src.LocationName, CAST(src.Aisle AS STRING) as Aisle, src.AisleSide
	, src.Department, src.SubDepartment, src.Category, src.Footage
	, CONCAT(lft.Department, '|||', lft.SubDepartment, '|||', lft.Category) AS Left_Space_Level_Id
	, CONCAT(rgt.Department, '|||', rgt.SubDepartment, '|||', rgt.Category ) AS Right_Space_Level_Id
	, src.ValidFrom, src.ValidTo  
FROM 
	POG_Store_Bays src 
LEFT OUTER JOIN
	POG_Store_Bays lft ON src.FLR_Key = lft.FLR_Key
	AND src.LocationName = lft.LocationName
	AND src.Aisle = lft.Aisle
	AND src.AisleSide = lft.AisleSide
	AND src.PreviousStartBay = lft.StartBay
	AND src.POG_Key <> lft.POG_Key -- Avoid referring to the same category/pog
LEFT OUTER JOIN
	POG_Store_Bays rgt ON src.FLR_Key = rgt.FLR_Key 
	AND src.LocationName = rgt.LocationName
	AND src.Aisle = rgt.Aisle
	AND src.AisleSide = rgt.AisleSide
	AND src.NextStartBay = rgt.StartBay
	AND src.POG_Key <> rgt.POG_Key
WHERE 
	(lft.POG_Key IS NOT NULL OR rgt.POG_Key IS NOT NULL) -- At leadt one adjacent record should exist

