/*
Compare the Stage_Merch_Hierarchy_Prev and Stage_Merch_Hierarchy table and loading into Delta_Merch_Hierarchy_Department Table.
Picking the Insert(I),Delete(D) and Update(U) Department records from the Delta_Merch_Hierarchy_Department Table.
Picking the Flag Update(FU) Department records from the Delta_Merch_Hierarchy_Department Table by joining with the target Merch_Level1 Table on Department Name and Sales_org 
*/

CREATE OR REPLACE TABLE `{{ params.project_ID }}.{{ params.Optumera_Stage_DS }}.Delta_Merch_Hierarchy_Department`( 
Curr_Department_Name  STRING,  
Curr_Sales_org  STRING,  
Prev_Department_Name  STRING,  
Prev_Sales_org  STRING,  
Flag  STRING
) AS


SELECT
	Curr.Department_Name AS Curr_Department_Name,
	Curr.Sales_org AS Curr_Sales_org,
	Prev.Department_Name AS Prev_Department_Name,
	Prev.Sales_org AS Prev_Sales_org,
	CASE WHEN curr.Department_Name is null then 'D'
	WHEN prev.Department_Name is null then 'I'
	ELSE 'E' End AS Flag
FROM 
(SELECT DISTINCT 
	Sales_org,Department_Name 
FROM 
	`{{ params.project_ID }}.{{ params.Optumera_Stage_DS }}.Stage_Merch_Hierarchy`) Curr
FULL OUTER JOIN 
(SELECT DISTINCT 
	Sales_org,Department_Name 
FROM 
	`{{ params.project_ID }}.{{ params.Optumera_Stage_DS }}.Stage_Merch_Hierarchy_Prev`) Prev
ON
	curr.Department_Name = Prev.Department_Name AND 
	curr.sales_org = prev.sales_org;
	
UPDATE 
	`{{ params.project_ID }}.{{ params.Optumera_Stage_DS }}.Delta_Merch_Hierarchy_Department` T
SET
	T.Flag = 'FU'
WHERE 
	CONCAT (T.Curr_Department_Name,'|||',T.curr_sales_org) IN
(SELECT 
	CONCAT(merch1.MERCH_LEVEL1_NAME,'|||',merch1.ATTR_2) AS Sal_org_hier  
FROM 
(SELECT * 
FROM 
	`{{ params.project_ID }}.{{ params.Optumera_Stage_DS }}.Delta_Merch_Hierarchy_Department` 
WHERE 
	Flag = 'I') art 
INNER JOIN `{{ params.project_ID }}.{{ params.Optumera_Target_DS }}.Merch_Level1` merch1 
ON
	art.Curr_Department_Name = merch1.MERCH_LEVEL1_NAME AND 
	art.curr_sales_org = merch1.ATTR_2);
