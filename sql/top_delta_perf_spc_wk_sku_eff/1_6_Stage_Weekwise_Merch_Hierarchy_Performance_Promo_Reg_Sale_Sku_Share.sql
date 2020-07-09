/*
1.Inner join the tables Weekwise_Merch_Hierarchy_Article_UOMs_Capacity_Share and respective finance table of each sales_org on 
  Week,Sales_Org,Store_no,Article and UOM
2.Multiply the Sales value and capacity share and then aggregate the resultant value.
*/
CREATE OR REPLACE TABLE
  `{{params.project_ID}}.{{params.Optumera_Stage_DS}}.Weekwise_Merch_Hierarchy_Performance_Promo_Reg_Sale_Sku_Share`( Week_Begin_Date DATE,
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
With Sku as 
(select
  shr.Week_Begin_Date
  , shr.Week_End_Date
  , shr.Week_Year
  , shr.Week_id
  , shr.Sales_Org
  , shr.Store_No  
  , shr.Department Department
  , shr.Sub_Department Sub_Department
  , shr.Category Category
  
  , case when ifnull(shr.Capacity_Share, 0) > 0 
    then round(sum(ifnull(fin.Sales_ExclTax, 0) * shr.Capacity_Share), 2) 
    else round(sum(ifnull(fin.Sales_ExclTax, 0)), 2) end Sales_ExclTax
  
  , case when ifnull(shr.Capacity_Share, 0) > 0 
    then round(sum(ifnull(fin.Sales_InclTax, 0) * shr.Capacity_Share), 2) 
    else round(sum(ifnull(fin.Sales_InclTax, 0)), 2) end Sales_InclTax
    
  , case when ifnull(shr.Capacity_Share, 0) > 0 
    then round(sum(ifnull(fin.Sales_Qty_SUoM, 0) * shr.Capacity_Share), 2) 
    else round(sum(ifnull(fin.Sales_Qty_SUoM, 0)), 2) end Sales_Qty
  
  , case when ifnull(shr.Capacity_Share, 0) > 0 
    then round(sum(ifnull(fin.Interim_GP, 0) * shr.Capacity_Share), 2) 
    else round(sum(ifnull(fin.Interim_GP, 0)), 2) end Interim_GP
  
  , case when ifnull(shr.Capacity_Share, 0) > 0 
    then round(sum(ifnull(fin.Net_Sales, 0) * shr.Capacity_Share), 2) 
    else round(sum(ifnull(fin.Net_Sales, 0)), 2) end Net_Sales
    
 , case when ifnull(shr.Capacity_Share, 0) > 0 
    then round(sum(ifnull(fin.Promo_Sales, 0) * shr.Capacity_Share), 2) 
    else round(sum(ifnull(fin.Promo_Sales, 0)), 2) end Promo_Sales

 , case when ifnull(shr.Capacity_Share, 0) > 0 
    then round(sum(ifnull(fin.Promo_Sales_Qty_SUoM, 0) * shr.Capacity_Share), 2) 
    else round(sum(ifnull(fin.Promo_Sales_Qty_SUoM, 0)), 2) end Promo_Sales_Qty

from 
 
 (Select * from `{{params.project_ID}}.{{params.Optumera_Stage_DS}}.Weekwise_Merch_Hierarchy_Article_UOMs_Capacity_Share` where sales_org in ('{{params.sales_org_BWS}}', '{{params.sales_org_DAN}}')) shr left outer join   
 `{{params.GCP_WOW_ENV}}.gs_edg_fin_data.fin_edg_profit_v` fin on 
        fin.SalesOrg = shr.Sales_Org
    and fin.Site = shr.Store_No
    and fin.Article = shr.Article
    and fin.Sales_Unit = shr.UOM 
    and fin.Calendar_Day between shr.Week_Begin_Date and shr.Week_End_Date  

group by 
    shr.Week_Begin_Date
  , shr.Week_End_Date
  , shr.Week_Year
  , shr.Week_id
  , shr.Sales_Org
  , shr.Store_no  
  , shr.Capacity_Share
  , shr.Department
  , shr.Sub_Department
  , shr.Category
)
select  Week_Begin_Date
  , Week_End_Date
  , Week_Year
  , Week_id
  , Sales_Org
  , Store_No  
  , Department
  , Sub_Department
  , Category
  , sum(Sales_ExclTax) Sales_ExclTax
  , sum(Sales_InclTax) Sales_InclTax
  , sum(Sales_Qty) Sales_Qty
  , sum(Interim_GP) Interim_GP
  , sum(Net_Sales) Net_Sales
  , sum(Promo_Sales) Promo_Sales
  , sum(Promo_Sales_Qty) Promo_Sales_Qty
  , (sum(Sales_ExclTax) + sum(Promo_Sales)) Total_Sales
  , (sum(Sales_Qty) + sum(Promo_Sales_Qty)) Total_Sales_Qty
from Sku group by Week_Begin_Date
  , Week_End_Date
  , Week_Year
  , Week_id
  , Sales_Org
  , Store_No  
  , Department
  , Sub_Department
  , Category;
  
INSERT
  `{{params.project_ID}}.{{params.Optumera_Stage_DS}}.Weekwise_Merch_Hierarchy_Performance_Promo_Reg_Sale_Sku_Share`( Week_Begin_Date,
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
    Total_Sales_Qty )
With Sku as 
(select
    shr.Week_Begin_Date
  , shr.Week_End_Date
  , shr.Week_Year
  , shr.Week_id
  , shr.Sales_Org
  , shr.Store_No  
  , shr.Department Department
  , shr.Sub_Department Sub_Department
  , shr.Category Category
  
  , case when ifnull(shr.Capacity_Share, 0) > 0 
    then round(sum(ifnull(fin.Sales_ExclTax, 0) * shr.Capacity_Share), 2) 
    else round(sum(ifnull(fin.Sales_ExclTax, 0)), 2) end Sales_ExclTax
  
  , case when ifnull(shr.Capacity_Share, 0) > 0 
    then round(sum(ifnull(fin.Sales_InclTax, 0) * shr.Capacity_Share), 2) 
    else round(sum(ifnull(fin.Sales_InclTax, 0)), 2) end Sales_InclTax
    
  , case when ifnull(shr.Capacity_Share, 0) > 0 
    then round(sum(ifnull(fin.Sales_Qty_SUoM, 0) * shr.Capacity_Share), 2) 
    else round(sum(ifnull(fin.Sales_Qty_SUoM, 0)), 2) end Sales_Qty
  
  , case when ifnull(shr.Capacity_Share, 0) > 0 
    then round(sum(ifnull(fin.Interim_GP, 0) * shr.Capacity_Share), 2) 
    else round(sum(ifnull(fin.Interim_GP, 0)), 2) end Interim_GP
  
  , case when ifnull(shr.Capacity_Share, 0) > 0 
    then round(sum(ifnull(fin.Net_Sales, 0) * shr.Capacity_Share), 2) 
    else round(sum(ifnull(fin.Net_Sales, 0)), 2) end Net_Sales
    
 , case when ifnull(shr.Capacity_Share, 0) > 0 
    then round(sum(ifnull(fin.Promo_Sales, 0) * shr.Capacity_Share), 2) 
    else round(sum(ifnull(fin.Promo_Sales, 0)), 2) end Promo_Sales

 , case when ifnull(shr.Capacity_Share, 0) > 0 
    then round(sum(ifnull(fin.Promo_Sales_Qty_SUoM, 0) * shr.Capacity_Share), 2) 
    else round(sum(ifnull(fin.Promo_Sales_Qty_SUoM, 0)), 2) end Promo_Sales_Qty

from 
 
 (Select * from `{{params.project_ID}}.{{params.Optumera_Stage_DS}}.Weekwise_Merch_Hierarchy_Article_UOMs_Capacity_Share` where sales_org in ('{{params.sales_org_SUPER}}', '{{params.sales_org_METRO}}')) shr left outer join   
 `{{params.GCP_WOW_ENV}}.gs_smkt_fin_data.fin_smkt_profit_v` fin on 
        fin.SalesOrg = shr.Sales_Org
    and fin.Site = shr.Store_No
    and fin.Article = shr.Article
    and fin.Sales_Unit = shr.UOM 
    and fin.Calendar_Day between shr.Week_Begin_Date and shr.Week_End_Date  

group by 
    shr.Week_Begin_Date
  , shr.Week_End_Date
  , shr.Week_Year
  , shr.Week_id
  , shr.Sales_Org
  , shr.Store_no  
  , shr.Capacity_Share
  , shr.Department
  , shr.Sub_Department
  , shr.Category
)
select  Week_Begin_Date
  , Week_End_Date
  , Week_Year
  , Week_id
  , Sales_Org
  , Store_No  
  , Department
  , Sub_Department
  , Category
  , sum(Sales_ExclTax) Sales_ExclTax
  , sum(Sales_InclTax) Sales_InclTax
  , sum(Sales_Qty) Sales_Qty
  , sum(Interim_GP) Interim_GP
  , sum(Net_Sales) Net_Sales
  , sum(Promo_Sales) Promo_Sales
  , sum(Promo_Sales_Qty) Promo_Sales_Qty
  , (sum(Sales_ExclTax) + sum(Promo_Sales)) Total_Sales
  , (sum(Sales_Qty) + sum(Promo_Sales_Qty)) Total_Sales_Qty
from Sku group by Week_Begin_Date
  , Week_End_Date
  , Week_Year
  , Week_id
  , Sales_Org
  , Store_No  
  , Department
  , Sub_Department
  , Category;
  
INSERT
  `{{params.project_ID}}.{{params.Optumera_Stage_DS}}.Weekwise_Merch_Hierarchy_Performance_Promo_Reg_Sale_Sku_Share`( Week_Begin_Date,
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
    Total_Sales_Qty )
With Sku as 
(select
    shr.Week_Begin_Date
  , shr.Week_End_Date
  , shr.Week_Year
  , shr.Week_id
  , shr.Sales_Org
  , shr.Store_No  
  , shr.Department Department
  , shr.Sub_Department Sub_Department
  , shr.Category Category
  
  , case when ifnull(shr.Capacity_Share, 0) > 0 
    then round(sum(ifnull(fin.Sales_ExclTax, 0) * shr.Capacity_Share), 2) 
    else round(sum(ifnull(fin.Sales_ExclTax, 0)), 2) end Sales_ExclTax
  
  , case when ifnull(shr.Capacity_Share, 0) > 0 
    then round(sum(ifnull(fin.Sales_InclTax, 0) * shr.Capacity_Share), 2) 
    else round(sum(ifnull(fin.Sales_InclTax, 0)), 2) end Sales_InclTax
    
  , case when ifnull(shr.Capacity_Share, 0) > 0 
    then round(sum(ifnull(fin.Sales_Qty_SUoM, 0) * shr.Capacity_Share), 2) 
    else round(sum(ifnull(fin.Sales_Qty_SUoM, 0)), 2) end Sales_Qty
  
  , case when ifnull(shr.Capacity_Share, 0) > 0 
    then round(sum(ifnull(fin.Interim_GP, 0) * shr.Capacity_Share), 2) 
    else round(sum(ifnull(fin.Interim_GP, 0)), 2) end Interim_GP
  
  , case when ifnull(shr.Capacity_Share, 0) > 0 
    then round(sum(ifnull(fin.Net_Sales, 0) * shr.Capacity_Share), 2) 
    else round(sum(ifnull(fin.Net_Sales, 0)), 2) end Net_Sales
    
 , case when ifnull(shr.Capacity_Share, 0) > 0 
    then round(sum(ifnull(fin.Promo_Sales, 0) * shr.Capacity_Share), 2) 
    else round(sum(ifnull(fin.Promo_Sales, 0)), 2) end Promo_Sales

 , case when ifnull(shr.Capacity_Share, 0) > 0 
    then round(sum(ifnull(fin.Promo_Sales_Qty_SUoM, 0) * shr.Capacity_Share), 2) 
    else round(sum(ifnull(fin.Promo_Sales_Qty_SUoM, 0)), 2) end Promo_Sales_Qty

from 
 
 (Select * from `{{params.project_ID}}.{{params.Optumera_Stage_DS}}.Weekwise_Merch_Hierarchy_Article_UOMs_Capacity_Share` where sales_org in ('{{params.sales_org_WNZ}}')) shr left outer join   
 `{{params.GCP_WOW_ENV}}.gs_nz_fin_data.fin_smktnz_profit_v` fin on 
        fin.SalesOrg = shr.Sales_Org
    and fin.Site = shr.Store_No
    and fin.Article = shr.Article
    and fin.Sales_Unit = shr.UOM 
    and fin.Calendar_Day between shr.Week_Begin_Date and shr.Week_End_Date  

group by 
    shr.Week_Begin_Date
  , shr.Week_End_Date
  , shr.Week_Year
  , shr.Week_id
  , shr.Sales_Org
  , shr.Store_no  
  , shr.Capacity_Share
  , shr.Department
  , shr.Sub_Department
  , shr.Category
)
select  Week_Begin_Date
  , Week_End_Date
  , Week_Year
  , Week_id
  , Sales_Org
  , Store_No  
  , Department
  , Sub_Department
  , Category
  , sum(Sales_ExclTax) Sales_ExclTax
  , sum(Sales_InclTax) Sales_InclTax
  , sum(Sales_Qty) Sales_Qty
  , sum(Interim_GP) Interim_GP
  , sum(Net_Sales) Net_Sales
  , sum(Promo_Sales) Promo_Sales
  , sum(Promo_Sales_Qty) Promo_Sales_Qty
  , (sum(Sales_ExclTax) + sum(Promo_Sales)) Total_Sales
  , (sum(Sales_Qty) + sum(Promo_Sales_Qty)) Total_Sales_Qty
from Sku group by Week_Begin_Date
  , Week_End_Date
  , Week_Year
  , Week_id
  , Sales_Org
  , Store_No  
  , Department
  , Sub_Department
  , Category;