/*
1.List down all the article Id of the respective sales_org,Store_no and week Hierarchy from the Position,Product and fixture Table for 
  types = 'INCLUDE' from Weekwise_Merch_Hierarchy Table.
2. Aggregate the Sales value based on the Article Id of the respective Sales_org,Store_no and Week Hierarchy from the respective finance table
   of each sales_org
*/

CREATE OR REPLACE TABLE `{{params.project_ID}}.{{params.Optumera_Stage_DS}}.Weekwise_Merch_Hierarchy_Performance_Promo_Reg_Sale_INCLUDE`(
Week_Begin_Date  DATE,
Week_End_Date DATE,
Week_Year STRING,
Week_id INT64,
Sales_Org STRING,
Store_No STRING,
Department STRING,
Sub_Department STRING,
Category STRING,
Sales_ExclTax FLOAT64,
Sales_InclTax FLOAT64,
Sales_Qty FLOAT64,
Interim_GP FLOAT64,
Net_Sales FLOAT64,
Promo_Sales FLOAT64,
Promo_Sales_Qty FLOAT64,
Total_Sales FLOAT64,
Total_Sales_Qty FLOAT64) AS
with art as (
  select distinct
      wsm.Week_Begin_Date
    , wsm.Week_End_Date
    , wsm.Week_Year
    , wsm.Week_id
    , wsm.Sales_Org
    , cast(wsm.Store_No as string) Store_No
    , wsm.Department
    , wsm.Sub_Department
    , wsm.Category
    , prd.Spc_Product_ID Article_Id
  from
    `{{params.project_ID}}.{{params.Optumera_Stage_DS}}.Weekwise_Merch_Hierarchy` wsm inner join
    `{{params.GCP_WOW_ENV}}.adp_planogram_view.pog_ix_spc_position_curr_v` pos on wsm.POG_Key = pos.DbParentPlanogramKey inner join
    `{{params.GCP_WOW_ENV}}.adp_planogram_view.pog_ix_spc_product_curr_v` prd on prd.DbKey = pos.DbParentProductKey inner join
	`{{params.GCP_WOW_ENV}}.adp_planogram_view.pog_ix_spc_fixture_curr_v` fix on fix.DBPARENTPLANOGRAMKEY = wsm.POG_Key and fix.DBKEY = pos.DBPARENTFIXTUREKEY 
     where wsm.types = 'INCLUDE' and wsm.Sales_Org in ('{{params.sales_org_BWS}}', '{{params.sales_org_DAN}}') and pos.Rec_Del_Flag = 'N' and prd.Rec_Del_Flag = 'N' 
	 and fix.Rec_Del_Flag = 'N' and (fix.Flag1 = 0 or fix.Flag1 is NULL)
)
select 
    art.Week_Begin_Date
  , art.Week_End_Date
  , art.Week_Year
  , art.Week_id
  , art.Sales_Org
  , art.Store_No
  , art.Department
  , art.Sub_Department
  , art.Category  
  , sum(fin.Sales_ExclTax) Sales_ExclTax
  , sum(fin.Sales_InclTax) Sales_InclTax
  , sum(fin.Sales_Qty_SUoM) Sales_Qty
  , sum(fin.Interim_GP) Interim_GP
  , sum(fin.Net_Sales) Net_Sales  
  , IFNULL(sum(fin.Promo_Sales),0) Promo_Sales
  , IFNULL(sum(fin.Promo_Sales_Qty_SUoM),0) Promo_Sales_Qty 
  , IFNULL(sum(fin.Sales_ExclTax), 0) + IFNULL(sum(fin.Promo_Sales), 0) Total_Sales
  , IFNULL(sum(fin.Sales_Qty_SUoM), 0) + IFNULL(sum(fin.Promo_Sales_Qty_SUoM), 0) Total_Sales_Qty

  
from
  art left outer join
  `{{params.GCP_WOW_ENV}}.gs_edg_fin_data.fin_edg_profit_v` fin on fin.Site = art.Store_No
     and concat(fin.Article, fin.Sales_Unit, "-", fin.SalesOrg) = art.Article_Id
     and fin.Calendar_Day between art.Week_Begin_Date and art.Week_End_Date
	 and fin.SalesOrg = art.Sales_Org
group by 
    art.Week_Begin_Date, art.Week_End_Date, art.Week_Year,art.Week_id
  , art.Sales_Org, art.Store_No
  , art.Department, art.Sub_Department, art.Category;

INSERT
`{{params.project_ID}}.{{params.Optumera_Stage_DS}}.Weekwise_Merch_Hierarchy_Performance_Promo_Reg_Sale_INCLUDE`(
Week_Begin_Date,
Week_End_Date,
Week_Year,
Week_id,
Sales_Org,
Store_No,
Department,
Sub_Department,
Category,
Sales_ExclTax,
Sales_InclTax,
Sales_Qty,
Interim_GP,
Net_Sales,
Promo_Sales,
Promo_Sales_Qty,
Total_Sales,
Total_Sales_Qty)
with art as (
  select distinct
      wsm.Week_Begin_Date
    , wsm.Week_End_Date
    , wsm.Week_Year
    , wsm.Week_id
    , wsm.Sales_Org
    , cast(wsm.Store_No as string) Store_No
    , wsm.Department
    , wsm.Sub_Department
    , wsm.Category
    , prd.Spc_Product_ID Article_Id
  from
    `{{params.project_ID}}.{{params.Optumera_Stage_DS}}.Weekwise_Merch_Hierarchy` wsm inner join
    `{{params.GCP_WOW_ENV}}.adp_planogram_view.pog_ix_spc_position_curr_v` pos on wsm.POG_Key = pos.DbParentPlanogramKey inner join
    `{{params.GCP_WOW_ENV}}.adp_planogram_view.pog_ix_spc_product_curr_v` prd on prd.DbKey = pos.DbParentProductKey inner join
	`{{params.GCP_WOW_ENV}}.adp_planogram_view.pog_ix_spc_fixture_curr_v` fix on fix.DBPARENTPLANOGRAMKEY = wsm.POG_Key  and fix.DBKEY = pos.DBPARENTFIXTUREKEY 
     where wsm.types = 'INCLUDE' and wsm.Sales_Org in ('{{params.sales_org_SUPER}}', '{{params.sales_org_METRO}}') and pos.Rec_Del_Flag = 'N' and prd.Rec_Del_Flag = 'N' 
	 and fix.Rec_Del_Flag = 'N' and (fix.Flag1 = 0 or fix.Flag1 is NULL)
)
select 
    art.Week_Begin_Date
  , art.Week_End_Date
  , art.Week_Year
  , art.Week_id
  , art.Sales_Org, art.Store_No
  , art.Department, art.Sub_Department, art.Category  
  , sum(fin.Sales_ExclTax) Sales_ExclTax
  , sum(fin.Sales_InclTax) Sales_InclTax
  , sum(fin.Sales_Qty_SUoM) Sales_Qty
  , sum(fin.Interim_GP) Interim_GP
  , sum(fin.Net_Sales) Net_Sales  
  , IFNULL(sum(fin.Promo_Sales),0) Promo_Sales
  , IFNULL(sum(fin.Promo_Sales_Qty_SUoM),0) Promo_Sales_Qty 
  , IFNULL(sum(fin.Sales_ExclTax), 0) + IFNULL(sum(fin.Promo_Sales), 0) Total_Sales
  , IFNULL(sum(fin.Sales_Qty_SUoM), 0) + IFNULL(sum(fin.Promo_Sales_Qty_SUoM), 0) Total_Sales_Qty

  
from
  art left outer join
  `{{params.GCP_WOW_ENV}}.gs_smkt_fin_data.fin_smkt_profit_v` fin on fin.Site = art.Store_No
     and concat(fin.Article, fin.Sales_Unit, "-", fin.SalesOrg) = art.Article_Id
     and fin.Calendar_Day between art.Week_Begin_Date and art.Week_End_Date
	 and fin.SalesOrg = art.Sales_Org
group by 
    art.Week_Begin_Date
  , art.Week_End_Date
  , art.Week_Year
  , art.Week_id
  , art.Sales_Org
  , art.Store_No
  , art.Department
  , art.Sub_Department
  , art.Category;

INSERT
`{{params.project_ID}}.{{params.Optumera_Stage_DS}}.Weekwise_Merch_Hierarchy_Performance_Promo_Reg_Sale_INCLUDE`(
Week_Begin_Date,
Week_End_Date,
Week_Year,
Week_id,
Sales_Org,
Store_No,
Department,
Sub_Department,
Category,
Sales_ExclTax,
Sales_InclTax,
Sales_Qty,
Interim_GP,
Net_Sales,
Promo_Sales,
Promo_Sales_Qty,
Total_Sales,
Total_Sales_Qty)
with art as (
  select distinct
      wsm.Week_Begin_Date
    , wsm.Week_End_Date
    , wsm.Week_Year
    , wsm.Week_id
    , wsm.Sales_Org
    , cast(wsm.Store_No as string) Store_No
    , wsm.Department
    , wsm.Sub_Department
    , wsm.Category
    , prd.Spc_Product_ID Article_Id
  from
    `{{params.project_ID}}.{{params.Optumera_Stage_DS}}.Weekwise_Merch_Hierarchy` wsm inner join
    `{{params.GCP_WOW_ENV}}.adp_planogram_view.pog_ix_spc_position_curr_v` pos on wsm.POG_Key = pos.DbParentPlanogramKey inner join
    `{{params.GCP_WOW_ENV}}.adp_planogram_view.pog_ix_spc_product_curr_v` prd on prd.DbKey = pos.DbParentProductKey inner join
	`{{params.GCP_WOW_ENV}}.adp_planogram_view.pog_ix_spc_fixture_curr_v` fix on fix.DBPARENTPLANOGRAMKEY = wsm.POG_Key  and fix.DBKEY = pos.DBPARENTFIXTUREKEY 
     where wsm.types = 'INCLUDE' and wsm.Sales_Org in ('{{params.sales_org_WNZ}}') and pos.Rec_Del_Flag = 'N' and prd.Rec_Del_Flag = 'N' 
	 and fix.Rec_Del_Flag = 'N' and (fix.Flag1 = 0 or fix.Flag1 is NULL)
)
select 
    art.Week_Begin_Date
  , art.Week_End_Date
  , art.Week_Year
  , art.Week_id
  , art.Sales_Org
  , art.Store_No
  , art.Department, art.Sub_Department, art.Category  
  , sum(fin.Sales_ExclTax) Sales_ExclTax
  , sum(fin.Sales_InclTax) Sales_InclTax
  , sum(fin.Sales_Qty_SUoM) Sales_Qty
  , sum(fin.Interim_GP) Interim_GP
  , sum(fin.Net_Sales) Net_Sales  
  , IFNULL(sum(fin.Promo_Sales),0) Promo_Sales
  , IFNULL(sum(fin.Promo_Sales_Qty_SUoM),0) Promo_Sales_Qty 
  , IFNULL(sum(fin.Sales_ExclTax), 0) + IFNULL(sum(fin.Promo_Sales), 0) Total_Sales
  , IFNULL(sum(fin.Sales_Qty_SUoM), 0) + IFNULL(sum(fin.Promo_Sales_Qty_SUoM), 0) Total_Sales_Qty

  
from
  art left outer join
  `{{params.GCP_WOW_ENV}}.gs_nz_fin_data.fin_smktnz_profit_v` fin on fin.Site = art.Store_No
     and concat(fin.Article, fin.Sales_Unit, "-", fin.SalesOrg) = art.Article_Id
     and fin.Calendar_Day between art.Week_Begin_Date and art.Week_End_Date
	 and fin.SalesOrg = art.Sales_Org
group by 
    art.Week_Begin_Date
  , art.Week_End_Date
  , art.Week_Year
  , art.Week_id
  , art.Sales_Org
  , art.Store_No
  , art.Department
  , art.Sub_Department
  , art.Category;