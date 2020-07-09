/*
1.List down the Latest 52 weeks data from Stage_Fiscal_Week Table.

List down all the Week hierarchy, planogram falls into the below condition:
1. floor and planogram status should be live(1)and historic(4)
2. planogram desc10 should be "C%"
3. floor DbDateEffectiveTo should be null or greater than or equal to respective calendar Week_Begin_Date date.
4. Pog DbDateEffectiveTo should be null or greater than or equal to respective calendar Week_Begin_Date date.
5. Floor DbDateEffectiveFrom less than or equal to respective calendar Week_End_Date date.
6. Pog DbDateEffectiveFrom less than or equal to respective calendar Week_End_Date date.
7. dense rank should be applied to pick the unique rows

*/

CREATE OR REPLACE TABLE
  `{{params.project_ID}}.{{params.Optumera_Stage_DS}}.SKU_Efficiency_Space` (
Week_Begin_Date DATE,
Week_End_Date DATE,
Sales_Org STRING,
Store_No STRING,
Department STRING,
Sub_Department STRING,
Category STRING,
POG_Key INT64 
) AS
WITH
  cal AS (
  SELECT
    DATE_SUB(MAX(Week_Begin_Date), INTERVAL 52 WEEK) Week_Begin_Date,
    MAX(Week_Begin_Date) Week_End_Date
  FROM
    `{{params.project_ID}}.{{params.Optumera_Stage_DS}}.Stage_Fiscal_Week`
  WHERE
    Week_End_Date <= CURRENT_DATE() ), sku as
    
(
  SELECT
    DISTINCT cal.Week_Begin_Date,
    cal.Week_End_Date,
    SUBSTR(flr.Desc11, 1, 4) Sales_Org,
    CAST(flr.Value1 AS string) Store_No,
    UPPER(pog.Department) Department,
    UPPER(pog.Desc4) Sub_Department,
    pog.Category,
    pog.DbKey POG_Key,
    DENSE_RANK() OVER (PARTITION BY cal.Week_Begin_Date, flr.Desc11, CAST(flr.Value1 AS INT64),
      UPPER(ver.Department),
      UPPER(ver.Desc4),
      UPPER(ver.Category)
    ORDER BY
      flr.DbDateEffectiveFrom DESC,
      flr.DbKey DESC,
      pog.Dbkey DESC,
      ver.DBKey DESC) FP_Seq
  FROM
    cal
  INNER JOIN
    `{{params.GCP_WOW_ENV}}.adp_planogram_view.pog_ix_flr_floorplan_curr_v` flr
  ON
    EXTRACT(date
    FROM
      flr.DbDateEffectiveFrom) <= cal.Week_End_Date
    AND (EXTRACT(date
      FROM
        flr.DbDateEffectiveTo) >= cal.Week_Begin_Date
      OR flr.DbDateEffectiveTo IS NULL)
  INNER JOIN
    `{{params.GCP_WOW_ENV}}.adp_planogram_view.pog_ix_flr_section_curr_v` sec ON flr.DbKey = sec.DbParentFloorplanKey
  INNER JOIN
  `{{params.GCP_WOW_ENV}}.adp_planogram_view.pog_ix_spc_planogram_curr_v` pog ON pog.DbKey = sec.DbParentPlanogramKey
  INNER JOIN
    `{{params.GCP_WOW_ENV}}.adp_planogram_view.pog_ix_spc_planogram_curr_v` ver ON pog.DbFamilyKey = ver.DbFamilyKey
    AND EXTRACT(date FROM ver.DbDateEffectiveFrom) <= cal.Week_End_Date
    AND (EXTRACT(date FROM ver.DbDateEffectiveTo) >= cal.Week_Begin_Date OR ver.DbDateEffectiveTo IS NULL)
  WHERE
    flr.DbStatus IN (1,4)
    AND pog.DbStatus IN (1,4)
    AND ver.DbStatus IN (1,4)
    AND pog.Desc10 LIKE 'C%' 
    AND flr.Rec_Del_Flag = 'N'
    AND sec.Rec_Del_Flag = 'N'
    AND pog.Rec_Del_Flag = 'N'
    AND ver.Rec_Del_Flag = 'N'
    ) 
SELECT
  sku.Week_Begin_Date,
  sku.Week_End_Date,
  sku.Sales_Org,
  sku.Store_No,
  sku.Department,
  sku.Sub_Department,
  sku.Category,
  sku.POG_Key
FROM
  sku
WHERE
  FP_SEQ = 1