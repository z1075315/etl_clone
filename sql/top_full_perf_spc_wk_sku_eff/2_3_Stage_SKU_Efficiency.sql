/*
1.Aggregation of Sales value and count of articles(Category articles) is done at Sales_Org,Store_no and Hierarchy Level from the SKU_Efficiency_Article_Sales Table.
2.Find the number of articles contributing(Contributing articles) to the 80% of the Sales value from the SKU_Efficiency_Article_Sales table by inner join with the above result.
3.Sku Efficiency is calculated by dividing Contrubuting articles by Category articles.

*/

CREATE OR REPLACE TABLE 
  `{{params.project_ID}}.{{params.Optumera_Stage_DS}}.Stage_SKU_Efficiency` (
    Sales_Org STRING,
    Store_No STRING,
    Department STRING,
    Sub_Department STRING,
    Category STRING,
    Category_Articles INT64,
    Contributing_Articles INT64,
    SKU_Efficiency_Prcnt FLOAT64
)AS
with Category_Perf as (
  select
    Sales_Org, Store_No
    , Department, Sub_Department, Category
    , sum(Sales) Sales
    , count(Article_No) Articles
  from
    `{{params.project_ID}}.{{params.Optumera_Stage_DS}}.SKU_Efficiency_Article_Sales`
  group by Sales_Org, Store_No, Department, Sub_Department, Category
), Article_Perf as (
  select
    sls.Sales_Org, sls.Store_No
    , sls.Department, sls.Sub_Department, sls.Category
    , round(prf.Sales * 0.80, 2) Category_Sales # Target (80%) category sales for SKU Efficiency
    , prf.Articles Category_Articles
    , sum(sls.Sales) over (partition by sls.Sales_Org, sls.Store_No, sls.Category order by sls.Sales desc) Article_Sales
    , count(sls.Article_No) over (partition by sls.Sales_Org, sls.Store_No, sls.Category order by sls.Sales desc) Contributing_Articles
  from
    `{{params.project_ID}}.{{params.Optumera_Stage_DS}}.SKU_Efficiency_Article_Sales` sls inner join
    Category_Perf prf on prf.Sales_Org = sls.Sales_Org
      and prf.Store_No = sls.Store_No
      and prf.Department = sls.Department
      and prf.Sub_Department = sls.Sub_Department
      and prf.Category = sls.Category
  where prf.Sales > 0
)
select 
  Sales_Org, Store_No
  , Department, Sub_Department, Category
  , Category_Articles
  , min(Contributing_Articles) Contributing_Articles  
  , round((min(Contributing_Articles) / Category_Articles) * 100.0, 2) SKU_Efficiency_Prcnt
from Article_Perf
where Category_Sales < Article_Sales # Articles contributing to sales exceeding 80%
group by Sales_Org, Store_No, Department, Sub_Department, Category, Category_Articles