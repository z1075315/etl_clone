/*
1.Inner join the tables Weekwise_Merch_Hierarchy_Article_UOMs and Weekwise_Merch_Hierarchy_Article_UOMs_Store_Capacity on Week_year,Sales_Org,Store_no,Article and UOM
2.Derive the Capacity Share by dividing Category_Capacity and Store_Capacity.
*/

CREATE OR REPLACE TABLE 
  `{{params.project_ID}}.{{params.Optumera_Stage_DS}}.Weekwise_Merch_Hierarchy_Article_UOMs_Capacity_Share` (
    Week_Begin_Date DATE,
    Week_End_Date DATE,
    Week_Year STRING,
    Week_id INT64,
    Sales_Org STRING,
    Store_No STRING,
    Department STRING,
    Sub_Department STRING,
    Category STRING,
    Article STRING,
    UOM STRING,
    Store_Capacity INT64,
    Category_Capacity INT64,
    Capacity_Share FLOAT64 ) AS
SELECT 
    sto.Week_Begin_Date
  , sto.Week_End_Date
  , sto.Week_Year
  , sto.Week_id
  , sto.Sales_Org
  , sto.Store_No
  , pog.Department
  , pog.Sub_Department
  , pog.Category
  , sto.Article
  , sto.UOM
  , sto.Capacity Store_Capacity
  , pog.Capacity Category_Capacity
  , (pog.Capacity / sto.Capacity) Capacity_Share
FROM 
  `{{params.project_ID}}.{{params.Optumera_Stage_DS}}.Weekwise_Merch_Hierarchy_Article_UOMs` pog inner join
  `{{params.project_ID}}.{{params.Optumera_Stage_DS}}.Weekwise_Merch_Hierarchy_Article_UOMs_Store_Capacity` sto  on 
            sto.Sales_Org = pog.Sales_Org
        and sto.Store_No  = pog.Store_No
        and sto.Article   = pog.Article
        and sto.UOM       = pog.UOM
        and sto.Week_Year = pog.Week_Year