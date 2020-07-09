/*
1.Build the target table by joining the Merch_Level3 table and Store table 
*/

CREATE OR REPLACE TABLE 
  `{{params.project_ID}}.{{params.Optumera_Target_DS}}.Str_Merch_Lvl3_SKU_Efficiency` (STORE_ID INT64,
    MERCH_LEVEL3_ID INT64,
    SKU_EFFICIENCY FLOAT64,
    CREATED_USER STRING,
    CREATED_TS TIMESTAMP,
    UPDATED_USER STRING,
    UPDATED_TS TIMESTAMP) AS
WITH
  sku AS (
  SELECT
    Sales_org,
    store_no,
    Department,
    Sub_Department,
    Category,
    SKU_Efficiency_Prcnt
  FROM
    `{{params.project_ID}}.{{params.Optumera_Stage_DS}}.Stage_SKU_Efficiency`
    )
SELECT
  store.STORE_ID,
  merch.MERCH_LEVEL3_ID,
  sku.SKU_Efficiency_Prcnt,
  'TOPDTLSKEFFFL',
  CURRENT_TIMESTAMP(),
  'TOPDTLSKEFFFL',
  CURRENT_TIMESTAMP()
FROM
  sku
INNER JOIN
  `{{params.project_ID}}.{{params.Optumera_Target_DS}}.Merch_Level3` merch
ON
  merch.ATTR_3 = CONCAT(sku.Department,'|||',sku.Sub_Department,'|||',sku.category)
  AND merch.ATTR_2 = sku.Sales_Org
  AND merch.ACTIVE_F = 'Y'
INNER JOIN
  `{{params.project_ID}}.{{params.Optumera_Target_DS}}.Store` store
ON
  store.STORE_NUMBER = sku.store_no
 and store.TEXT_1 = sku.Sales_Org
 and store.ACTIVE_F = 'Y'