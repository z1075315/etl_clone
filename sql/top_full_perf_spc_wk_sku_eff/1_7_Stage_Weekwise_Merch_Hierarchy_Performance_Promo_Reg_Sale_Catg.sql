/*
1.List down all the article Id of the respective sales_org,Store_no and week Hierarchy from the Position,Product and fixture Table for 
  types = '1 - Defined% Share' from Weekwise_Merch_Hierarchy Table.
2. Aggregate the Sales value based on the Article Id of the respective Sales_org,Store_no and Week Hierarchy from the respective finance table
   of each sales_org
3. Multiply the above aggregated sales value with the percent column for the respective Hierarchy_Group and then aggregate the value by
   Week,Store_no,Sales_Org and Hierarchy
*/

CREATE OR REPLACE TABLE
  `{{params.project_ID}}.{{params.Optumera_Stage_DS}}.Weekwise_Merch_Hierarchy_Performance_Promo_Reg_Sale_Catg` ( Week_Begin_Date  DATE, 
Week_End_Date DATE, 
Week_Year STRING, 
week_id INT64, 
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
    , wsm.Week_Year,week_id
    , wsm.Sales_Org
    , cast(wsm.Store_No as string) Store_No
    , wsm.Department
    , wsm.Sub_Department
    , wsm.Category
    , prd.Spc_Product_ID Article_Id
    , wsm.Hierarchy_Group
    , wsm.Percent
  from
    (select Week_Begin_Date, Week_End_Date, Week_Year, Week_id,Sales_Org,Store_No,Department,Sub_Department,Category,POG_Key,Hierarchy_Group,Percent 
  from `{{params.project_ID}}.{{params.Optumera_Stage_DS}}.Weekwise_Merch_Hierarchy` where types = '1 - Defined% Share' and Sales_Org in ('{{params.sales_org_BWS}}', '{{params.sales_org_DAN}}'))wsm inner join
    `{{params.GCP_WOW_ENV}}.adp_planogram_view.pog_ix_spc_position_curr_v` pos on wsm.POG_Key = pos.DbParentPlanogramKey inner join
    `{{params.GCP_WOW_ENV}}.adp_planogram_view.pog_ix_spc_product_curr_v` prd on prd.DbKey = pos.DbParentProductKey inner join
	`{{params.GCP_WOW_ENV}}.adp_planogram_view.pog_ix_spc_fixture_curr_v` fix on fix.DBPARENTPLANOGRAMKEY = wsm.POG_Key  and fix.DBKEY = pos.DBPARENTFIXTUREKEY 
	where pos.Rec_Del_Flag = 'N' and prd.Rec_Del_Flag = 'N' and fix.Rec_Del_Flag = 'N' and (fix.Flag1 = 0 or fix.Flag1 is NULL)
)
, perf as (
select 
    art.Week_Begin_Date
  , art.Week_End_Date
  , art.Week_Year
  , art.week_id
  , art.Sales_Org
  , art.Store_No
  , art.Department
  , art.Sub_Department
  , art.Category 
  , art.Hierarchy_Group
  , art.percent
  , sum(ifnull(fin.Sales_ExclTax,0)) Sales_ExclTax
  , sum(ifnull(fin.Sales_InclTax,0)) Sales_InclTax
  , sum(ifnull(fin.Sales_Qty_SUoM,0)) Sales_Qty
  , sum(ifnull(fin.Promo_Sales,0)) Promo_Sales
  , sum(ifnull(fin.Promo_Sales_Qty_SUoM,0)) Promo_Sales_Qty  
  , sum(ifnull(fin.Interim_GP,0)) Interim_GP
  , sum(ifnull(fin.Net_Sales,0)) Net_Sales  
  
from
  art left outer join
  `{{params.GCP_WOW_ENV}}.gs_edg_fin_data.fin_edg_profit_v` fin on fin.Site = art.Store_No
     and concat(fin.Article, fin.Sales_Unit, "-", fin.SalesOrg) = art.Article_Id
     and fin.Calendar_Day between art.Week_Begin_Date and art.Week_End_Date 
	 and fin.SalesOrg = art.Sales_Org
group by 
    art.Week_Begin_Date
  , art.Week_End_Date
  , art.Week_Year
  , art.week_id
  , art.Sales_Org
  , art.Store_No
  , art.Department
  , art.Sub_Department
  , art.Category
  , art.Hierarchy_Group
  , art.percent
  )
, final as  (
 select 
    perf.Week_Begin_Date
  , perf.Week_End_Date
  , perf.Week_Year
  , perf.week_id
  , perf.Sales_Org
  , perf.Store_No
  , perf.Department
  , perf.Sub_Department
  , perf.Category 
  , perf.Hierarchy_Group
  , perf.percent
  ,round(SUM(perf.Sales_ExclTax) OVER (PARTITION BY perf.Week_Year, perf.Sales_Org, perf.Store_No, perf.Hierarchy_Group )* perf.percent,2) Sales_ExclTax
  ,round(SUM(perf.Sales_InclTax) OVER (PARTITION BY perf.Week_Year, perf.Sales_Org, perf.Store_No, perf.Hierarchy_Group )* perf.percent,2)  Sales_InclTax
  ,round(SUM(perf.Sales_Qty) OVER (PARTITION BY perf.Week_Year, perf.Sales_Org, perf.Store_No, perf.Hierarchy_Group )* perf.percent,2)  Sales_Qty
  ,round(SUM(perf.Promo_Sales) OVER (PARTITION BY perf.Week_Year, perf.Sales_Org, perf.Store_No, perf.Hierarchy_Group )* perf.percent,2)  Promo_Sales
   ,round(SUM(perf.Promo_Sales_Qty) OVER (PARTITION BY perf.Week_Year, perf.Sales_Org, perf.Store_No, perf.Hierarchy_Group )* perf.percent,2)  Promo_Sales_Qty
  ,round(SUM(perf.Interim_GP) OVER (PARTITION BY perf.Week_Year, perf.Sales_Org, perf.Store_No, perf.Hierarchy_Group )* perf.percent,2)  Interim_GP
  ,round(SUM(perf.Net_Sales) OVER (PARTITION BY perf.Week_Year, perf.Sales_Org, perf.Store_No, perf.Hierarchy_Group )* perf.percent,2)  Net_Sales
  from perf

)
 select 
    final.Week_Begin_Date
  , final.Week_End_Date
  , final.Week_Year
  , final.week_id
  , final.Sales_Org
  , final.Store_No
  , final.Department
  , final.Sub_Department
  , final.Category 
  , final.Sales_ExclTax 
  , final.Sales_InclTax 
  , final.Sales_Qty
  , final.Interim_GP
  , final.Net_Sales
  , final.Promo_Sales
  , final.Promo_Sales_Qty
  , IFNULL(final.Sales_ExclTax, 0) + IFNULL(final.Promo_Sales, 0) Total_Sales
  , IFNULL(final.Sales_Qty, 0) + IFNULL(final.Promo_Sales_Qty, 0) Total_Sales_Qty
  from final
 where final.percent > 0;
 
 INSERT
  `{{params.project_ID}}.{{params.Optumera_Stage_DS}}.Weekwise_Merch_Hierarchy_Performance_Promo_Reg_Sale_Catg` ( Week_Begin_Date,
    Week_End_Date,
    Week_Year,
    week_id,
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
    Total_Sales_Qty )
with art as (
  select distinct
      wsm.Week_Begin_Date
    , wsm.Week_End_Date
    , wsm.Week_Year
    , week_id
    , wsm.Sales_Org
    , cast(wsm.Store_No as string) Store_No
    , wsm.Department
    , wsm.Sub_Department
    , wsm.Category
    , prd.Spc_Product_ID Article_Id
    , wsm.Hierarchy_Group
    , wsm.Percent
  from
    (select * from `{{params.project_ID}}.{{params.Optumera_Stage_DS}}.Weekwise_Merch_Hierarchy` where types = '1 - Defined% Share' and Sales_Org in ('{{params.sales_org_SUPER}}', '{{params.sales_org_METRO}}')) wsm inner join
    `{{params.GCP_WOW_ENV}}.adp_planogram_view.pog_ix_spc_position_curr_v` pos on wsm.POG_Key = pos.DbParentPlanogramKey inner join
    `{{params.GCP_WOW_ENV}}.adp_planogram_view.pog_ix_spc_product_curr_v` prd on prd.DbKey = pos.DbParentProductKey inner join
	`{{params.GCP_WOW_ENV}}.adp_planogram_view.pog_ix_spc_fixture_curr_v` fix on fix.DBPARENTPLANOGRAMKEY = wsm.POG_Key and fix.DBKEY = pos.DBPARENTFIXTUREKEY 
	where pos.Rec_Del_Flag = 'N' and prd.Rec_Del_Flag = 'N' and fix.Rec_Del_Flag = 'N' and (fix.Flag1 = 0 or fix.Flag1 is NULL)
)
, perf as (
select 
    art.Week_Begin_Date
  , art.Week_End_Date
  , art.Week_Year
  , art.week_id
  , art.Sales_Org
  , art.Store_No
  , art.Department
  , art.Sub_Department
  , art.Category 
  , art.Hierarchy_Group
  , art.percent
  , sum(ifnull(fin.Sales_ExclTax,0)) Sales_ExclTax
  , sum(ifnull(fin.Sales_InclTax,0)) Sales_InclTax
  , sum(ifnull(fin.Sales_Qty_SUoM,0)) Sales_Qty
  , sum(ifnull(fin.Promo_Sales,0)) Promo_Sales
  , sum(ifnull(fin.Promo_Sales_Qty_SUoM,0)) Promo_Sales_Qty  
  , sum(ifnull(fin.Interim_GP,0)) Interim_GP
  , sum(ifnull(fin.Net_Sales,0)) Net_Sales  
  
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
  , art.week_id
  , art.Sales_Org
  , art.Store_No
  , art.Department
  , art.Sub_Department
  , art.Category
  , art.Hierarchy_Group
  , art.percent
  )
, final as  (
 select 
    perf.Week_Begin_Date
  , perf.Week_End_Date
  , perf.Week_Year
  , perf.week_id
  , perf.Sales_Org
  , perf.Store_No
  , perf.Department
  , perf.Sub_Department
  , perf.Category 
  , perf.Hierarchy_Group
  , perf.percent
  ,round(SUM(perf.Sales_ExclTax) OVER (PARTITION BY perf.Week_Year, perf.Sales_Org, perf.Store_No, perf.Hierarchy_Group )* perf.percent,2) Sales_ExclTax
  ,round(SUM(perf.Sales_InclTax) OVER (PARTITION BY perf.Week_Year, perf.Sales_Org, perf.Store_No, perf.Hierarchy_Group )* perf.percent,2)  Sales_InclTax
  ,round(SUM(perf.Sales_Qty) OVER (PARTITION BY perf.Week_Year, perf.Sales_Org, perf.Store_No, perf.Hierarchy_Group )* perf.percent,2)  Sales_Qty
  ,round(SUM(perf.Promo_Sales) OVER (PARTITION BY perf.Week_Year, perf.Sales_Org, perf.Store_No, perf.Hierarchy_Group )* perf.percent,2)  Promo_Sales
   ,round(SUM(perf.Promo_Sales_Qty) OVER (PARTITION BY perf.Week_Year, perf.Sales_Org, perf.Store_No, perf.Hierarchy_Group )* perf.percent,2)  Promo_Sales_Qty
  ,round(SUM(perf.Interim_GP) OVER (PARTITION BY perf.Week_Year, perf.Sales_Org, perf.Store_No, perf.Hierarchy_Group )* perf.percent,2)  Interim_GP
  ,round(SUM(perf.Net_Sales) OVER (PARTITION BY perf.Week_Year, perf.Sales_Org, perf.Store_No, perf.Hierarchy_Group )* perf.percent,2)  Net_Sales
  from perf

)
 select 
    final.Week_Begin_Date
  , final.Week_End_Date
  , final.Week_Year
  , final.week_id
  , final.Sales_Org
  , final.Store_No
  , final.Department
  , final.Sub_Department
  , final.Category 
  , final.Sales_ExclTax 
  , final.Sales_InclTax 
  , final.Sales_Qty
  , final.Interim_GP
  , final.Net_Sales
  , final.Promo_Sales
  , final.Promo_Sales_Qty
  , IFNULL(final.Sales_ExclTax, 0) + IFNULL(final.Promo_Sales, 0) Total_Sales
  , IFNULL(final.Sales_Qty, 0) + IFNULL(final.Promo_Sales_Qty, 0) Total_Sales_Qty
  from final
 where final.percent > 0;
 
 INSERT
  `{{params.project_ID}}.{{params.Optumera_Stage_DS}}.Weekwise_Merch_Hierarchy_Performance_Promo_Reg_Sale_Catg` ( Week_Begin_Date,
    Week_End_Date,
    Week_Year,
    week_id,
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
    Total_Sales_Qty )
with art as (
  select distinct
      wsm.Week_Begin_Date
    , wsm.Week_End_Date
    , wsm.Week_Year
    , week_id
    , wsm.Sales_Org
    , cast(wsm.Store_No as string) Store_No
    , wsm.Department
    , wsm.Sub_Department
    , wsm.Category
    , prd.Spc_Product_ID Article_Id
    , wsm.Hierarchy_Group
    , wsm.Percent
  from
    (select * from `{{params.project_ID}}.{{params.Optumera_Stage_DS}}.Weekwise_Merch_Hierarchy` where types = '1 - Defined% Share' and Sales_Org in ('{{params.sales_org_WNZ}}'))wsm inner join
    `{{params.GCP_WOW_ENV}}.adp_planogram_view.pog_ix_spc_position_curr_v` pos on wsm.POG_Key = pos.DbParentPlanogramKey inner join
    `{{params.GCP_WOW_ENV}}.adp_planogram_view.pog_ix_spc_product_curr_v` prd on prd.DbKey = pos.DbParentProductKey inner join
	`{{params.GCP_WOW_ENV}}.adp_planogram_view.pog_ix_spc_fixture_curr_v` fix on fix.DBPARENTPLANOGRAMKEY = wsm.POG_Key and fix.DBKEY = pos.DBPARENTFIXTUREKEY 
	where pos.Rec_Del_Flag = 'N' and prd.Rec_Del_Flag = 'N' and fix.Rec_Del_Flag = 'N' and (fix.Flag1 = 0 or fix.Flag1 is NULL)
)
, perf as (
select 
    art.Week_Begin_Date
  , art.Week_End_Date
  , art.Week_Year
  , art.week_id
  , art.Sales_Org
  , art.Store_No
  , art.Department
  , art.Sub_Department
  , art.Category 
  , art.Hierarchy_Group
  , art.percent
  , sum(ifnull(fin.Sales_ExclTax,0)) Sales_ExclTax
  , sum(ifnull(fin.Sales_InclTax,0)) Sales_InclTax
  , sum(ifnull(fin.Sales_Qty_SUoM,0)) Sales_Qty
  , sum(ifnull(fin.Promo_Sales,0)) Promo_Sales
  , sum(ifnull(fin.Promo_Sales_Qty_SUoM,0)) Promo_Sales_Qty  
  , sum(ifnull(fin.Interim_GP,0)) Interim_GP
  , sum(ifnull(fin.Net_Sales,0)) Net_Sales  
  
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
  , art.week_id
  , art.Sales_Org
  , art.Store_No
  , art.Department
  , art.Sub_Department
  , art.Category
  , art.Hierarchy_Group
  , art.percent
  )
, final as  (
 select 
    perf.Week_Begin_Date
  , perf.Week_End_Date
  , perf.Week_Year
  , perf.week_id
  , perf.Sales_Org
  , perf.Store_No
  , perf.Department
  , perf.Sub_Department
  , perf.Category 
  , perf.Hierarchy_Group
  , perf.percent
  , round(SUM(perf.Sales_ExclTax) OVER (PARTITION BY perf.Week_Year, perf.Sales_Org, perf.Store_No, perf.Hierarchy_Group )* perf.percent,2) Sales_ExclTax
  , round(SUM(perf.Sales_InclTax) OVER (PARTITION BY perf.Week_Year, perf.Sales_Org, perf.Store_No, perf.Hierarchy_Group )* perf.percent,2)  Sales_InclTax
  , round(SUM(perf.Sales_Qty) OVER (PARTITION BY perf.Week_Year, perf.Sales_Org, perf.Store_No, perf.Hierarchy_Group )* perf.percent,2)  Sales_Qty
  , round(SUM(perf.Promo_Sales) OVER (PARTITION BY perf.Week_Year, perf.Sales_Org, perf.Store_No, perf.Hierarchy_Group )* perf.percent,2)  Promo_Sales
   , round(SUM(perf.Promo_Sales_Qty) OVER (PARTITION BY perf.Week_Year, perf.Sales_Org, perf.Store_No, perf.Hierarchy_Group )* perf.percent,2)  Promo_Sales_Qty
  , round(SUM(perf.Interim_GP) OVER (PARTITION BY perf.Week_Year, perf.Sales_Org, perf.Store_No, perf.Hierarchy_Group )* perf.percent,2)  Interim_GP
  , round(SUM(perf.Net_Sales) OVER (PARTITION BY perf.Week_Year, perf.Sales_Org, perf.Store_No, perf.Hierarchy_Group )* perf.percent,2)  Net_Sales
  from perf
 
)
 select 
    final.Week_Begin_Date
  , final.Week_End_Date
  , final.Week_Year
  , final.week_id
  , final.Sales_Org
  , final.Store_No
  , final.Department
  , final.Sub_Department
  , final.Category 
  , final.Sales_ExclTax 
  , final.Sales_InclTax 
  , final.Sales_Qty
  , final.Interim_GP
  , final.Net_Sales
  , final.Promo_Sales
  , final.Promo_Sales_Qty
  , IFNULL(final.Sales_ExclTax, 0) + IFNULL(final.Promo_Sales, 0) Total_Sales
  , IFNULL(final.Sales_Qty, 0) + IFNULL(final.Promo_Sales_Qty, 0) Total_Sales_Qty
  from final
 where final.percent > 0;