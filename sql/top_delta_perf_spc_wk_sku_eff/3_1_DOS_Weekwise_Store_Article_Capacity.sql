/*
Capacity calculation:
1. list down all the Week that has fixture flag1 as 1 or null
2. aggregate the capacity from position table at sales org, store number,Week,article level for the pog_keys from the Weekwise_Merch_Hierarchy table
*/
CREATE OR REPLACE TABLE 
  `{{params.project_ID}}.{{params.Optumera_Stage_DS}}.DOS_Weekwise_Store_Article_Capacity` (
Week_Begin_Date DATE,
Week_End_Date DATE,
Week_Year STRING,
Sales_Org STRING,
Store_No STRING,
Article_Id STRING,
Capacity   INT64
) AS
SELECT
  wsm.Week_Begin_Date,
  wsm.Week_End_Date,
  wsm.Week_Year,
  wsm.Sales_Org,
  wsm.Store_No,
  prd.Spc_Product_ID Article_Id,
  SUM(pos.Capacity) Capacity
FROM
  `{{params.project_ID}}.{{params.Optumera_Stage_DS}}.Weekwise_Merch_Hierarchy` wsm
INNER JOIN
  `{{params.GCP_WOW_ENV}}.adp_planogram_view.pog_ix_spc_position_curr_v` pos
ON
	wsm.POG_Key = pos.DbParentPlanogramKey
INNER JOIN
  `{{params.GCP_WOW_ENV}}.adp_planogram_view.pog_ix_spc_product_curr_v` prd
ON
	prd.DbKey = pos.DbParentProductKey 
INNER JOIN
	`{{params.GCP_WOW_ENV}}.adp_planogram_view.pog_ix_spc_fixture_curr_v` fix 
ON 
	fix.DBPARENTPLANOGRAMKEY = wsm.POG_Key  and fix.DBKEY = pos.DBPARENTFIXTUREKEY 
WHERE pos.Rec_Del_Flag = 'N' and prd.Rec_Del_Flag = 'N' and fix.Rec_Del_Flag = 'N' and (fix.Flag1 = 0 or fix.Flag1 is NULL)
GROUP BY
  wsm.Week_Begin_Date,
  wsm.Week_End_Date,
  wsm.Week_Year,
  wsm.Sales_Org,
  wsm.Store_No,
  prd.Spc_Product_ID