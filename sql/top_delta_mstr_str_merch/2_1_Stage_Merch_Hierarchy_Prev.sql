/*
Truncate the Stage_Merch_Hierarchy_Prev table and Copy the Stage_Merch_Hierarchy into Stage_Merch_Hierarchy_Prev Table
*/

CREATE OR REPLACE TABLE
  `{{ params.project_ID }}.{{ params.Optumera_Stage_DS }}.Stage_Merch_Hierarchy_Prev` (Sales_Org STRING NOT NULL,
	Department_Name STRING,
    Department_Desc STRING,
    SubDepartment_Name STRING,
    SubDepartment_Desc STRING,
    Category_Name STRING,
    Category_Desc STRING) AS

SELECT
	Sales_Org,
	Department_Name,
    Department_Desc,
    SubDepartment_Name,
    SubDepartment_Desc,
    Category_Name,
    Category_Desc
FROM
  `{{ params.project_ID }}.{{ params.Optumera_Stage_DS }}.Stage_Merch_Hierarchy` 