/*
1.List down the Article_Id by joining Weekwise_Merch_Hierarchy with the Position,Product and Fixture tables and group them at 
  sales_org,Store_no,Week and Article_Id.
2.Aggregate the sales value at Sales_Org,Store_no and Article_Id by joing the above result with the respective finance tables.
*/
CREATE OR REPLACE TABLE 
  `{{params.project_ID}}.{{params.Optumera_Stage_DS}}.DOS_Weekwise_Store_Article_Avg_Sales`(
 Week_Begin_Date DATE ,
Sales_Org STRING,
Store_No STRING,
Article_Id STRING ,
sales_count INT64 ,
sum_sales_qty FLOAT64 , 
sum_avg_sales_qty FLOAT64,
Avg_Sales_Qty FLOAT64 
) AS
with art as  (
select 
    wsm.Week_Begin_Date
  , wsm.Week_End_Date
  , Week_Year
  , wsm.Sales_Org
  , wsm.Store_No  
  , prd.Spc_Product_ID Article_Id
from
	`{{params.project_ID}}.{{params.Optumera_Stage_DS}}.Weekwise_Merch_Hierarchy` wsm inner join
	`{{params.GCP_WOW_ENV}}.adp_planogram_view.pog_ix_spc_position_curr_v` pos on wsm.POG_Key = pos.DbParentPlanogramKey inner join
	`{{params.GCP_WOW_ENV}}.adp_planogram_view.pog_ix_spc_product_curr_v` prd on prd.DbKey = pos.DbParentProductKey inner join
	`{{params.GCP_WOW_ENV}}.adp_planogram_view.pog_ix_spc_fixture_curr_v` fix on fix.DBPARENTPLANOGRAMKEY = wsm.POG_Key   
	and fix.DBKEY = pos.DBPARENTFIXTUREKEY 
where 
	wsm.Sales_Org in ('{{ params.sales_org_BWS }}','{{ params.sales_org_DAN }}') and pos.Rec_Del_Flag = 'N' and prd.Rec_Del_Flag = 'N'
	and fix.Rec_Del_Flag = 'N' and (fix.Flag1 = 0 or fix.Flag1 is NULL)
group by 
	wsm.Week_Begin_Date
	, wsm.Week_End_Date
	, wsm.Week_Year
	, wsm.Sales_Org
	, wsm.Store_No
	, prd.Spc_Product_ID 
)
select 
    art.Week_Begin_Date Week_Begin_Date
  , art.Sales_Org, art.Store_No
  , art.Article_Id
  , count(fin.Sales_Qty_SUoM) sales_count # testing purpose
  , sum(fin.Sales_Qty_SUoM) sum_sales_qty # testing purpose
  , sum(fin.Sales_Qty_SUoM)/52 sum_avg_sales_qty  
  , avg(fin.Sales_Qty_SUoM)  Avg_Sales_Qty # testing purpose
from
  art inner join
  `{{params.GCP_WOW_ENV}}.gs_edg_fin_data.fin_edg_profit_v` fin on fin.Site = art.Store_No
     and concat(fin.Article, fin.Sales_Unit, "-", fin.SalesOrg) = art.Article_Id
     and fin.Calendar_Day between Date_Sub(art.Week_Begin_Date, INTERVAL 52 WEEK)  and art.Week_End_Date
	 and fin.salesorg = art.sales_org     
group by 
   art.Sales_Org
 , art.Store_No
 , art.Article_Id
 , art.Week_Begin_Date;
 
INSERT
  `{{params.project_ID}}.{{params.Optumera_Stage_DS}}.DOS_Weekwise_Store_Article_Avg_Sales`(
  Week_Begin_Date ,
  Sales_Org ,
  Store_No ,
  Article_Id ,
  sales_count ,
  sum_sales_qty ,
  sum_avg_sales_qty ,  
  Avg_Sales_Qty
)
with art as  (
select 
  wsm.Week_Begin_Date, wsm.Week_End_Date, Week_Year
  , wsm.Sales_Org, wsm.Store_No  
  , prd.Spc_Product_ID Article_Id
from
	`{{params.project_ID}}.{{params.Optumera_Stage_DS}}.Weekwise_Merch_Hierarchy` wsm inner join
	`{{params.GCP_WOW_ENV}}.adp_planogram_view.pog_ix_spc_position_curr_v` pos on wsm.POG_Key = pos.DbParentPlanogramKey inner join
	`{{params.GCP_WOW_ENV}}.adp_planogram_view.pog_ix_spc_product_curr_v` prd on prd.DbKey = pos.DbParentProductKey inner join
	`{{params.GCP_WOW_ENV}}.adp_planogram_view.pog_ix_spc_fixture_curr_v` fix on fix.DBPARENTPLANOGRAMKEY = wsm.POG_Key   
	and fix.DBKEY = pos.DBPARENTFIXTUREKEY 
where 
	wsm.Sales_Org in ('{{ params.sales_org_SUPER}}','{{ params.sales_org_METRO}}') and pos.Rec_Del_Flag = 'N' and prd.Rec_Del_Flag = 'N' 
	and fix.Rec_Del_Flag = 'N' and (fix.Flag1 = 0 or fix.Flag1 is NULL)
group by 
	wsm.Week_Begin_Date
	, wsm.Week_End_Date
	, wsm.Week_Year
	, wsm.Sales_Org
	, wsm.Store_No
	, prd.Spc_Product_ID

)
select 
    art.Week_Begin_Date Week_Begin_Date
  , art.Sales_Org, art.Store_No
  , art.Article_Id
  , count(fin.Sales_Qty_SUoM) sales_count # testing purpose
  , sum(fin.Sales_Qty_SUoM) sum_sales_qty # testing purpose
  , sum(fin.Sales_Qty_SUoM)/52 sum_avg_sales_qty  
  , avg(fin.Sales_Qty_SUoM)  Avg_Sales_Qty # testing purpose
from
  art inner join
  `{{params.GCP_WOW_ENV}}.gs_smkt_fin_data.fin_smkt_profit_v` fin on fin.Site = art.Store_No
     and concat(fin.Article, fin.Sales_Unit, "-", fin.SalesOrg) = art.Article_Id
     and fin.Calendar_Day between Date_Sub(art.Week_Begin_Date, INTERVAL 52 WEEK)  and art.Week_End_Date
	 and fin.salesorg = art.sales_org     
group by 
   art.Sales_Org
 , art.Store_No
 , art.Article_Id
 , art.Week_Begin_Date;
 
INSERT
  `{{params.project_ID}}.{{params.Optumera_Stage_DS}}.DOS_Weekwise_Store_Article_Avg_Sales`(
  Week_Begin_Date ,
  Sales_Org ,
  Store_No ,
  Article_Id ,
  sales_count ,
  sum_sales_qty ,
  sum_avg_sales_qty ,  
  Avg_Sales_Qty
)
with art as  (
select 
    wsm.Week_Begin_Date
  , wsm.Week_End_Date
  , Week_Year
  , wsm.Sales_Org
  , wsm.Store_No  
  , prd.Spc_Product_ID Article_Id
from
	`{{params.project_ID}}.{{params.Optumera_Stage_DS}}.Weekwise_Merch_Hierarchy` wsm inner join
	`{{params.GCP_WOW_ENV}}.adp_planogram_view.pog_ix_spc_position_curr_v` pos on wsm.POG_Key = pos.DbParentPlanogramKey inner join
	`{{params.GCP_WOW_ENV}}.adp_planogram_view.pog_ix_spc_product_curr_v` prd on prd.DbKey = pos.DbParentProductKey inner join
	`{{params.GCP_WOW_ENV}}.adp_planogram_view.pog_ix_spc_fixture_curr_v` fix on fix.DBPARENTPLANOGRAMKEY = wsm.POG_Key   
	and fix.DBKEY = pos.DBPARENTFIXTUREKEY 
where 
	wsm.Sales_Org in ('{{ params.sales_org_WNZ}}') and pos.Rec_Del_Flag = 'N' and prd.Rec_Del_Flag = 'N' 
	and fix.Rec_Del_Flag = 'N' and (fix.Flag1 = 0 or fix.Flag1 is NULL)
group by 
	wsm.Week_Begin_Date
	, wsm.Week_End_Date
	, wsm.Week_Year
	, wsm.Sales_Org
	, wsm.Store_No
	, prd.Spc_Product_ID
)
select 
    art.Week_Begin_Date Week_Begin_Date
  , art.Sales_Org
  , art.Store_No
  , art.Article_Id
  , count(fin.Sales_Qty_SUoM) sales_count # testing purpose
  , sum(fin.Sales_Qty_SUoM) sum_sales_qty # testing purpose
  , sum(fin.Sales_Qty_SUoM)/52 sum_avg_sales_qty  
  , avg(fin.Sales_Qty_SUoM)  Avg_Sales_Qty # testing purpose
from
  art inner join
  `{{params.GCP_WOW_ENV}}.gs_nz_fin_data.fin_smktnz_profit_v` fin on fin.Site = art.Store_No
     and concat(fin.Article, fin.Sales_Unit, "-", fin.SalesOrg) = art.Article_Id
     and fin.Calendar_Day between Date_Sub(art.Week_Begin_Date, INTERVAL 52 WEEK)  and art.Week_End_Date
	 and fin.salesorg = art.sales_org     
group by 
   art.Sales_Org
 , art.Store_No
 , art.Article_Id
 , art.Week_Begin_Date;