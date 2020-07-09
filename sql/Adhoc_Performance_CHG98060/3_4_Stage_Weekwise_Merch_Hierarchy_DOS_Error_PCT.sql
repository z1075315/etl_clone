/*
1.DOS_Error_Pct is calculated by dividing the sum of DOS_Error and Count of DOS_Error
*/
CREATE OR REPLACE TABLE
  `{{params.project_ID}}.{{params.Optumera_Stage_DS}}.Stage_Weekwise_Merch_Hierarchy_DOS_Error_PCT` (Week_Year STRING,
Sales_Org STRING,
Store_No STRING,
Department STRING , 
Sub_Department STRING , 
Category STRING,
DOS_Error_PCT FLOAT64) AS
    SELECT
      Week_Year,
      Sales_Org,
      Store_No,
      Department,
      Sub_Department,
      Category,
      Round((SUM(DOS_Error)/ COUNT(DOS_Error))*100,2) AS DOS_Error_Pct
    FROM `{{params.project_ID}}.{{params.Optumera_Stage_DS}}.Weekwise_Merch_Hierarchy_DOS_Error`
    GROUP BY
      Week_Year,
      Sales_Org,
      Store_No,
      Department,
      Sub_Department,
      Category