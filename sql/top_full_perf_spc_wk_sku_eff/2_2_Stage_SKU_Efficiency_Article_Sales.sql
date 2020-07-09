/*
1.List down all the article Id of the respective sales_org,Store_no and week Hierarchy from the Position,Product and fixture Table for pog_keys
  from SKU_Efficiency_Space Table.
2. Aggregate the Sales value based on the Article Id of the respective Sales_org,Store_no and Week Hierarchy from the respective finance table
   of each sales_org
*/
CREATE OR REPLACE TABLE `{{params.project_ID}}.{{params.Optumera_Stage_DS}}.SKU_Efficiency_Article_Sales` (
Sales_Org STRING,
Store_No STRING,
Department STRING,
Sub_Department STRING,
Category STRING,
Article_No STRING,
Sales FLOAT64
) AS
with art as (
  select distinct
    spc.Week_Begin_Date, spc.Week_End_Date
    , spc.Sales_Org, spc.Store_No
    , spc.Department, spc.Sub_Department, spc.Category
    , prd.Desc1 Article_No
  from
    `{{params.project_ID}}.{{params.Optumera_Stage_DS}}.SKU_Efficiency_Space` spc inner join
    `{{params.GCP_WOW_ENV}}.adp_planogram_view.pog_ix_spc_position_curr_v` pos on spc.POG_Key = pos.DbParentPlanogramKey inner join
    `{{params.GCP_WOW_ENV}}.adp_planogram_view.pog_ix_spc_product_curr_v` prd on prd.DbKey = pos.DbParentProductKey inner join
	`{{params.GCP_WOW_ENV}}.adp_planogram_view.pog_ix_spc_fixture_curr_v` fix on fix.DBPARENTPLANOGRAMKEY = spc.POG_Key   
	and fix.DBKEY = pos.DBPARENTFIXTUREKEY 
	where spc.Sales_Org in ('{{ params.sales_org_BWS}}','{{params.sales_org_DAN}}') and pos.Rec_Del_Flag = 'N' and prd.Rec_Del_Flag = 'N' 
	and fix.Rec_Del_Flag = 'N' and (fix.Flag1 = 0 or fix.Flag1 is NULL)
)
select 
  art.Sales_Org, art.Store_No
  , art.Department, art.Sub_Department, art.Category
  , art.Article_No
  , sum(IFNULL(fin.Sales_ExclTax, 0)) Sales
from
  art left outer join
  `{{params.GCP_WOW_ENV}}.gs_edg_fin_data.fin_edg_profit_v` fin on fin.SalesOrg = art.Sales_Org
     and fin.Site = art.Store_No
     and fin.Article = art.Article_No
     and fin.Calendar_Day between art.Week_Begin_Date and art.Week_End_Date
group by 
  art.Sales_Org, art.Store_No
  , art.Department, art.Sub_Department, art.Category
  , art.Article_No;

INSERT `{{params.project_ID}}.{{params.Optumera_Stage_DS}}.SKU_Efficiency_Article_Sales` (
 Sales_Org, 
 Store_No,
 Department, 
 Sub_Department, 
 Category,
  Article_No,
  Sales
)
with art as (
  select distinct
    spc.Week_Begin_Date, spc.Week_End_Date
    , spc.Sales_Org, spc.Store_No
    , spc.Department, spc.Sub_Department, spc.Category
    , prd.Desc1 Article_No
  from
    `{{params.project_ID}}.{{params.Optumera_Stage_DS}}.SKU_Efficiency_Space` spc inner join
    `{{params.GCP_WOW_ENV}}.adp_planogram_view.pog_ix_spc_position_curr_v` pos on spc.POG_Key = pos.DbParentPlanogramKey inner join
    `{{params.GCP_WOW_ENV}}.adp_planogram_view.pog_ix_spc_product_curr_v` prd on prd.DbKey = pos.DbParentProductKey inner join
	`{{params.GCP_WOW_ENV}}.adp_planogram_view.pog_ix_spc_fixture_curr_v` fix on fix.DBPARENTPLANOGRAMKEY = spc.POG_Key  
	and fix.DBKEY = pos.DBPARENTFIXTUREKEY 
	where spc.Sales_Org in ('{{ params.sales_org_SUPER}}','{{params.sales_org_METRO}}') and pos.Rec_Del_Flag = 'N' and prd.Rec_Del_Flag = 'N' 
	and fix.Rec_Del_Flag = 'N' and (fix.Flag1 = 0 or fix.Flag1 is NULL)
)
select 
  art.Sales_Org, art.Store_No
  , art.Department, art.Sub_Department, art.Category
  , art.Article_No
  , sum(IFNULL(fin.Sales_ExclTax, 0)) Sales
from
  art left outer join
  `{{params.GCP_WOW_ENV}}.gs_smkt_fin_data.fin_smkt_profit_v` fin on fin.SalesOrg = art.Sales_Org
     and fin.Site = art.Store_No
     and fin.Article = art.Article_No
     and fin.Calendar_Day between art.Week_Begin_Date and art.Week_End_Date
group by 
  art.Sales_Org, art.Store_No
  , art.Department, art.Sub_Department, art.Category
  , art.Article_No;
  
INSERT `{{params.project_ID}}.{{params.Optumera_Stage_DS}}.SKU_Efficiency_Article_Sales` (
 Sales_Org, 
 Store_No,
 Department, 
 Sub_Department, 
 Category,
  Article_No,
  Sales
)
with art as (
  select distinct
    spc.Week_Begin_Date, spc.Week_End_Date
    , spc.Sales_Org, spc.Store_No
    , spc.Department, spc.Sub_Department, spc.Category
    , prd.Desc1 Article_No
  from
    `{{params.project_ID}}.{{params.Optumera_Stage_DS}}.SKU_Efficiency_Space` spc inner join
    `{{params.GCP_WOW_ENV}}.adp_planogram_view.pog_ix_spc_position_curr_v` pos on spc.POG_Key = pos.DbParentPlanogramKey inner join
    `{{params.GCP_WOW_ENV}}.adp_planogram_view.pog_ix_spc_product_curr_v` prd on prd.DbKey = pos.DbParentProductKey inner join
	`{{params.GCP_WOW_ENV}}.adp_planogram_view.pog_ix_spc_fixture_curr_v` fix on fix.DBPARENTPLANOGRAMKEY = spc.POG_Key   
	and fix.DBKEY = pos.DBPARENTFIXTUREKEY 
	where spc.Sales_Org in ('{{ params.sales_org_WNZ}}') and pos.Rec_Del_Flag = 'N' and prd.Rec_Del_Flag = 'N' 
	and fix.Rec_Del_Flag = 'N' and (fix.Flag1 = 0 or fix.Flag1 is NULL)
)
select 
  art.Sales_Org, art.Store_No
  , art.Department, art.Sub_Department, art.Category
  , art.Article_No
  , sum(IFNULL(fin.Sales_ExclTax, 0)) Sales
from
  art left outer join
  `{{params.GCP_WOW_ENV}}.gs_nz_fin_data.fin_smktnz_profit_v` fin on fin.SalesOrg = art.Sales_Org
     and fin.Site = art.Store_No
     and fin.Article = art.Article_No
     and fin.Calendar_Day between art.Week_Begin_Date and art.Week_End_Date
group by 
  art.Sales_Org, art.Store_No
  , art.Department, art.Sub_Department, art.Category
  , art.Article_No;