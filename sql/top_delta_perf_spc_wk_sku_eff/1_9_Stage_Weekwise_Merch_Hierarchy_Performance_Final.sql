/*
1.Left join the Stage_Weekwise_Merch_Hierarchy_DOS_Error_PCT Table with the Weekwise_Merch_Hierarchy_Space_Performance table.
2.Left join the Stage_Weekwise_Assort_Covrg Table with the above result(1).
3.Union all the Weekwise_Merch_Hierarchy_Performance_Promo_Reg_Sale_Catg, Weekwise_Merch_Hierarchy_Performance_Promo_Reg_Sale_INCLUDE 
  and Weekwise_Merch_Hierarchy_Performance_Promo_Reg_Sale_Sku_Share
4. Left join the results(3) with results(2).
5. Left join the Weekwise_Merch_Hierarchy with results(4) and exclude the Hierarchy which is of types = '1 - Defined% Share' and Percent = '0.0'
*/

CREATE OR REPLACE TABLE
  `{{params.project_ID}}.{{params.Optumera_Stage_DS}}.Stage_Weekwise_Merch_Hierarchy_Performance_Final` ( Week_Begin_Date  DATE,
    Week_End_Date  DATE,
    Week_Year STRING,
    week_id INT64,
    Sales_Org STRING,
    Sales_Org_Desc STRING,
    Store_No STRING,
    Department STRING,
    Sub_Department STRING,
    Category STRING,
    Used_Linear FLOAT64,        
	Avbl_Linear FLOAT64,        
	Total_Linear FLOAT64,        
	Square FLOAT64,
	Capacity  INT64,
    DOS_Error_Pct FLOAT64,
    Assort_Covrg FLOAT64,
    Net_Sales FLOAT64,
    Sales_InclTax FLOAT64,
    Regular_Sales_Units FLOAT64,
    Total_Gross_Margin FLOAT64,
    Regular_Sales FLOAT64,
    Promo_Sales FLOAT64,
    Promo_Sales_Units FLOAT64,
    Total_Sales FLOAT64,
    Total_Sales_Units FLOAT64) AS
SELECT
  spc.Week_Begin_Date,
  spc.Week_End_Date,
  spc.Week_Year,
  spc.week_id,
  spc.Sales_Org,
  spc.Sales_Org_Desc,
  spc.Store_No,
  spc.Department,
  spc.Sub_Department,
  spc.Category,
  spc.Used_Linear,
  spc.Avbl_Linear,
  spc.Total_Linear,
  spc.Square,
  spc.Capacity,
  IFNULL(dos.DOS_Error_Pct,
    0) DOS_Error_Pct,
  IFNULL(asscov.Assort_Covrg,
    0) Assort_Covrg,
  perf.Net_Sales,
  perf.Sales_InclTax,
  perf.Sales_Qty Regular_Sales_Units,
  perf.Interim_GP Total_Gross_Margin,
  perf.Sales_ExclTax Regular_Sales,
  perf.Promo_Sales,
  perf.Promo_Sales_Qty Promo_Sales_Units,
  perf.Total_Sales,
  perf.Total_Sales_Qty Total_Sales_Units
FROM
  `{{params.project_ID}}.{{params.Optumera_Stage_DS}}.Weekwise_Merch_Hierarchy_Space_Performance` spc 
LEFT OUTER JOIN
  `{{params.project_ID}}.{{params.Optumera_Stage_DS}}.Stage_Weekwise_Merch_Hierarchy_DOS_Error_PCT` dos
ON
      spc.Week_Year = dos.Week_Year
  AND spc.Sales_Org = dos.Sales_Org
  AND spc.Store_No = dos.Store_No
  AND spc.Department = dos.Department
  AND spc.Sub_Department = dos.Sub_Department
  AND spc.Category = dos.Category
LEFT OUTER JOIN 
  `{{params.project_ID}}.{{params.Optumera_Stage_DS}}.Stage_Weekwise_Assort_Covrg` asscov
ON
      spc.Week_Year = asscov.Week_Year
  AND spc.Sales_Org = asscov.Sales_Org
  AND spc.Store_No = asscov.Store_No
  AND spc.Department = asscov.Department
  AND spc.Sub_Department = asscov.Sub_Department
  AND spc.Category = asscov.Category
  
LEFT OUTER JOIN
  (
  SELECT
    Week_Begin_Date,
    Week_End_Date,
    Week_Year,
    week_id,
    Sales_Org,
    Store_No,
    Department,
    Sub_Department,
    Category,
    Net_Sales,
    Sales_InclTax,
    Sales_Qty,
    Interim_GP,
    Sales_ExclTax,
    Promo_Sales,
    Promo_Sales_Qty,
    Total_Sales,
    Total_Sales_Qty
  FROM
    `{{params.project_ID}}.{{params.Optumera_Stage_DS}}.Weekwise_Merch_Hierarchy_Performance_Promo_Reg_Sale_Catg`
  UNION ALL
  SELECT
    Week_Begin_Date,
    Week_End_Date,
    Week_Year,
    week_id,
    Sales_Org,
    Store_No,
    Department,
    Sub_Department,
    Category,
    Net_Sales,
    Sales_InclTax,
    Sales_Qty,
    Interim_GP,
    Sales_ExclTax,
    Promo_Sales,
    Promo_Sales_Qty,
    Total_Sales,
    Total_Sales_Qty
  FROM
    `{{params.project_ID}}.{{params.Optumera_Stage_DS}}.Weekwise_Merch_Hierarchy_Performance_Promo_Reg_Sale_INCLUDE`
  UNION ALL
  SELECT
    Week_Begin_Date,
    Week_End_Date,
    Week_Year,
    week_id,
    Sales_Org,
    Store_No,
    Department,
    Sub_Department,
    Category,
    Net_Sales,
    Sales_InclTax,
    Sales_Qty,
    Interim_GP,
    Sales_ExclTax,
    Promo_Sales,
    Promo_Sales_Qty,
    Total_Sales,
    Total_Sales_Qty
  FROM
    `{{params.project_ID}}.{{params.Optumera_Stage_DS}}.Weekwise_Merch_Hierarchy_Performance_Promo_Reg_Sale_Sku_Share` ) perf
ON
      spc.Week_Year = perf.Week_Year
  AND spc.Sales_Org = perf.Sales_Org
  AND spc.Store_No = perf.Store_No
  AND spc.Department = perf.Department
  AND spc.Sub_Department = perf.Sub_Department
  AND spc.Category = perf.Category

LEFT OUTER JOIN (
  SELECT
    DISTINCT Sales_org,
    Department,
    Sub_Department,
    Category
  FROM
    `{{params.project_ID}}.{{params.Optumera_Stage_DS}}.Weekwise_Merch_Hierarchy`
  WHERE
    types = '1 - Defined% Share'
    AND Percent = 0.0) Sku
ON
      spc.Sales_Org = Sku.Sales_Org
  AND spc.Department = Sku.Department
  AND spc.Sub_Department = Sku.Sub_Department
  AND spc.Category = sku.Category

WHERE
      Sku.Sales_Org IS NULL
  AND Sku.Department IS NULL
  AND Sku.Sub_Department IS NULL
  AND sku.Category IS NULL
