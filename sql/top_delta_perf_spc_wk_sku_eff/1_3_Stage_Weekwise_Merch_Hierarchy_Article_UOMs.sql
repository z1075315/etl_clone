/*
1.List down all the article and UOM of the respective sales_org,Store_no and week Hierarchy from the Position,Product and fixture Table for 
  types = '2 - SKU Capacity Share' from Weekwise_Merch_Hierarchy Table.
2. Aggregate the Capacity in the above query at Sales_org,Store_no,Week Hierarchy,Spc_Product_ID level
*/

CREATE OR REPLACE TABLE 
`{{params.project_ID}}.{{params.Optumera_Stage_DS}}.Weekwise_Merch_Hierarchy_Article_UOMs`(
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
Capacity INT64
) AS
select
    wsm.Week_Begin_Date
  , wsm.Week_End_Date
  , wsm.Week_Year
  , wsm.Week_id
  , wsm.Sales_Org
  , wsm.Store_No
  , wsm.Department
  , wsm.Sub_Department
  , wsm.Category
  , prd.Desc1 Article
  , replace(replace(prd.Spc_Product_ID, prd.Desc1, ''), concat('-', wsm.Sales_Org), '') UOM
  , sum(pos.Capacity) Capacity
from
 (select Week_Begin_Date, Week_End_Date, Week_Year, Week_id,Sales_Org,Store_No
  ,Department,Sub_Department,Category,POG_Key
from `{{params.project_ID}}.{{params.Optumera_Stage_DS}}.Weekwise_Merch_Hierarchy` where types = '2 - SKU Capacity Share') wsm
inner join
	`{{params.GCP_WOW_ENV}}.adp_planogram_view.pog_ix_spc_position_curr_v` pos on wsm.POG_Key = pos.DbParentPlanogramKey inner join
	`{{params.GCP_WOW_ENV}}.adp_planogram_view.pog_ix_spc_product_curr_v` prd on prd.DbKey = pos.DbParentProductKey inner join
	`{{params.GCP_WOW_ENV}}.adp_planogram_view.pog_ix_spc_fixture_curr_v` fix on fix.DBPARENTPLANOGRAMKEY = wsm.POG_Key  and fix.DBKEY = pos.DBPARENTFIXTUREKEY 
where pos.Rec_Del_Flag = 'N' and prd.Rec_Del_Flag = 'N' and fix.Rec_Del_Flag = 'N' and (fix.Flag1 = 0 or fix.Flag1 is NULL)
group by 
    wsm.Week_Begin_Date
  , wsm.Week_End_Date
  , wsm.Week_Year
  , wsm.Week_id
  , wsm.Sales_Org
  , wsm.Store_No
  , wsm.Department, wsm.Sub_Department, wsm.Category
  , prd.Desc1
  , prd.Spc_Product_ID