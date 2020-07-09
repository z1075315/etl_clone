/*
1.Calculate the DOS_Threshold by joining the Weekwise_Merch_Hierarchy with the Position,Product,Fixture,Floor Performance tables 
 at Week,Sales_org,Store_No,Hierarchy and Article Level. 
2. Left join the DOS_Weekwise_Store_Article_Capacity and DOS_Weekwise_Store_Article_Avg_Sales with above results.
3. Calculate the DOS and DOS_Error from the above result at Week,Sales_org,Store_No,Hierarchy and Article Level. 
*/
CREATE OR REPLACE TABLE 
 `{{params.project_ID}}.{{params.Optumera_Stage_DS}}.Weekwise_Merch_Hierarchy_DOS_Error` (
Week_Begin_Date DATE, 
Week_End_Date DATE,
Week_Year STRING, 
Sales_Org STRING,
Store_No STRING,
Department STRING,
Sub_Department STRING,
Category STRING,
Article_Id STRING,
DOS_Threshold FLOAT64, 
DOS FLOAT64,
DOS_Error INT64 
) AS
SELECT
 wsm.Week_Begin_Date,
 wsm.Week_End_Date,
 wsm.Week_Year,
 wsm.Sales_Org,
 wsm.Store_No,
 wsm.Department,
 wsm.Sub_Department,
 wsm.Category,
 prd.Spc_Product_ID Article_Id,
 IFNULL(NULLIF(prf.Value25,0),3) DOS_Threshold,
 CASE
 WHEN IFNULL(avg.sum_avg_sales_qty, 0) > 0 
 THEN cap.Capacity / avg.sum_avg_sales_qty
 ELSE cap.Capacity
 END DOS,
 CASE
 WHEN ( CASE
 WHEN IFNULL(avg.sum_avg_sales_qty,0) > 0 
 THEN cap.Capacity / avg.sum_avg_sales_qty
 ELSE cap.Capacity
 END
 ) < IFNULL(NULLIF(prf.Value25,0),3) THEN 1 ELSE 0
 END DOS_Error
FROM
 `{{params.project_ID}}.{{params.Optumera_Stage_DS}}.Weekwise_Merch_Hierarchy` wsm
INNER JOIN
 `{{params.GCP_WOW_ENV}}.adp_planogram_view.pog_ix_flr_performance_curr_v` prf
ON
 wsm.FLR_Key = prf.DbParentFloorplanKey
 AND wsm.POG_Key = prf.DbParentPlanogramKey
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
 fix.DBPARENTPLANOGRAMKEY = wsm.POG_Key and fix.DBKEY = pos.DBPARENTFIXTUREKEY 
LEFT OUTER JOIN
 `{{params.project_ID}}.{{params.Optumera_Stage_DS}}.DOS_Weekwise_Store_Article_Capacity` cap
ON
 cap.week_year=wsm.week_year
 AND cap.sales_org = wsm.sales_org
 AND cap.store_no = wsm.store_no
 AND cap.Article_Id = prd.spc_product_id
LEFT OUTER JOIN
 `{{params.project_ID}}.{{params.Optumera_Stage_DS}}.DOS_Weekwise_Store_Article_Avg_Sales` avg
ON
 avg.Week_Begin_Date=wsm.Week_Begin_Date
 AND avg.sales_org = wsm.sales_org
 AND avg.store_no = wsm.store_no
 AND avg.Article_Id = prd.spc_product_id
WHERE prf.Rec_Del_Flag = 'N' and pos.Rec_Del_Flag = 'N' and prd.Rec_Del_Flag = 'N' and fix.Rec_Del_Flag = 'N' and (fix.Flag1 = 0 or fix.Flag1 is NULL)
