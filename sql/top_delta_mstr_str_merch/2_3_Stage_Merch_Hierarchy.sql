/*
1. pick the distinct hierarchy from Merch_Hierarchy for each sales org
2. Exclude the hierarchies present in the Hierarchy_exclusion_override with types as "3 - Exclude" and build tge Stage_Merch_Hierarchy table. 
*/

CREATE OR REPLACE TABLE
	`{{ params.project_ID }}.{{ params.Optumera_Stage_DS }}.Stage_Merch_Hierarchy` (Sales_Org STRING NOT NULL,
	Department_Name STRING,
    Department_Desc STRING,
    SubDepartment_Name STRING,
    SubDepartment_Desc STRING,
    Category_Name STRING,
    Category_Desc STRING
	) AS
WITH
Hier AS (
SELECT DISTINCT 
	Department,
	Sub_Department,
	Category,
	Sales_Org
FROM
	`{{ params.project_ID }}.{{ params.Optumera_Stage_DS }}.Merch_Hierarchy` ),
Exc AS (
SELECT 
	Department,Sub_Department,Category,Sales_org 
FROM 
	`{{ params.project_ID }}.{{ params.Optumera_Stage_DS }}.Hierarchy_exclusion_override`
WHERE Types = '3 - Exclude'),
Final AS (
SELECT 
	Hier.Department,Hier.Sub_Department,Hier.Category,Hier.Sales_org 
FROM 
	Hier
LEFT JOIN  Exc 
ON  
	CONCAT( Exc.Sales_org,'|||', Exc.Department,'|||', Exc.Sub_Department,'|||',Exc.Category) = 
	CONCAT( Hier.Sales_org,'|||', Hier.Department,'|||', Hier.Sub_Department,'|||',Hier.Category) 
WHERE 
	CONCAT( Exc.Sales_org,'|||', Exc.Department,'|||', Exc.Sub_Department,'|||',Exc.Category) IS NULL )
SELECT
	Sales_Org,
	Department,
	Department,
	Sub_Department,
	Sub_Department,
	Category,
	Category
FROM
	Final