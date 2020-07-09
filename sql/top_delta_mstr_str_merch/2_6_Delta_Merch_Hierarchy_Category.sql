/*
Compare the Stage_Merch_Hierarchy_Prev and Stage_Merch_Hierarchy table and loading into Delta_Merch_Hierarchy_Category Table.
Picking the Insert(I),Delete(D) and Update(U) Department,SubDepartment,Category records from the Delta_Merch_Hierarchy_Category Table.
Picking the Flag Update(FU) Department,SubDepartment,Category records from the Delta_Merch_Hierarchy_Category Table by joining with the target Merch_Level3 Table on Department Name,SubDepartment Name,Category Name and Sales_org 
*/

CREATE OR REPLACE TABLE
`{{ params.project_ID }}.{{ params.Optumera_Stage_DS }}.Delta_Merch_Hierarchy_Category`
( 
Curr_Department_Name  STRING,  
Curr_SubDepartment_Name  STRING,  
Curr_Category_Name  STRING,  
Curr_Sales_org  STRING,  
Prev_Department_Name  STRING,  
Prev_SubDepartment_Name  STRING,  
Prev_Category_Name  STRING,  
Prev_Sales_org  STRING,  
Flag  STRING       
) AS

SELECT
	Curr.Department_Name AS Curr_Department_Name,
	Curr.SubDepartment_Name AS Curr_SubDepartment_Name,
	Curr.Category_Name AS Curr_Category_Name,
	Curr.Sales_org AS Curr_Sales_org,
	Prev.Department_Name AS Prev_Department_Name,
	Prev.SubDepartment_Name AS Prev_SubDepartment_Name,
	Prev.Category_Name AS Prev_Category_Name,
	Prev.Sales_org AS Prev_Sales_org,
	CASE WHEN curr.Category_Name is null then 'D'
	WHEN prev.Category_Name is null then 'I'
	ELSE 'E' End AS Flag
FROM 
(SELECT DISTINCT 
	Sales_org, Department_Name, SubDepartment_Name, Category_Name  
FROM 
	`{{ params.project_ID }}.{{ params.Optumera_Stage_DS }}.Stage_Merch_Hierarchy`) Curr
FULL OUTER JOIN 
(SELECT DISTINCT 
	Sales_org, Department_Name, SubDepartment_Name, Category_Name 
FROM 
	`{{ params.project_ID }}.{{ params.Optumera_Stage_DS }}.Stage_Merch_Hierarchy_Prev`) Prev
ON 
	curr.Department_Name = Prev.Department_Name AND 
	curr.SubDepartment_Name = Prev.SubDepartment_Name AND 
	curr.Category_Name = Prev.Category_Name AND 
	curr.sales_org = prev.sales_org;

UPDATE 
	`{{ params.project_ID }}.{{ params.Optumera_Stage_DS }}.Delta_Merch_Hierarchy_Category` T
SET
	T.Flag = 'FU'
WHERE 
	CONCAT (T.Curr_Department_Name,'|||',T.Curr_SubDepartment_Name,'|||',T.Curr_Category_Name,'|||',T.curr_sales_org) IN 
(SELECT 
	CONCAT(merch3.ATTR_3,'|||',merch3.ATTR_2) AS Sal_org_hier  
FROM 
(SELECT * 
FROM 
	`{{ params.project_ID }}.{{ params.Optumera_Stage_DS }}.Delta_Merch_Hierarchy_Category` 
WHERE Flag = 'I') art 

INNER JOIN 
`{{ params.project_ID }}.{{ params.Optumera_Target_DS }}.Merch_Level3` merch3 
ON
	CONCAT (art.Curr_Department_Name,'|||',art.Curr_SubDepartment_Name,'|||',art.Curr_Category_Name) = merch3.ATTR_3 AND 
	art.curr_sales_org = merch3.ATTR_2);
