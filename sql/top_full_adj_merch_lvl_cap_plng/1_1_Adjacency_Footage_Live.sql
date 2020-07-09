/*
list down all the planogram falls into the below condition:
1. floor and planogram status should be live(1)and historic(4)
2. planogram desc10 should be "C%"
3. floor DbDateEffectiveTo should be null or greater than or equal to parameterized date.
4. dense rank should be applied to pick the unique rows

Used-linear calculation:
1. list down all the planogram that has fixture type as 1,2,7,8 and fixture flag1 as 1 or null
2. apply ceiling function to the linear value from position table for the above planogram.
3. aggregate the used linear value by planogram key level.

Available-linear calculation:
1. list down all the planogram that has fixture type as 0,3,4,5,6,9,12 and fixture flag1 as 1 or null
2. aggregate the available linear value by planogram key level.

Total-linear calculation:
1. sum the available and used linear values using full outer join.
2. left join the results with Hierarchy_Space_Override and multiply the total-linear with mul-factor on sales org, store number and hierarchy.
*/

--Roundoff Function 
Create Temp Function roundOff(number FLOAT64,multiple FLOAT64) AS
(
(ceiling(number / multiple)) * multiple
);

--Insert Record--
CREATE OR REPLACE TABLE  
	`{{params.project_ID}}.{{params.Optumera_Stage_DS}}.Adjacency_Footage_Live`(
	Department STRING
  , SubDepartment STRING
  , Category STRING
  , StoreNo STRING
  , SalesOrg STRING
  , Footage FLOAT64
  , Aisle INT64
  , AisleSide STRING
  , LocationName STRING
  , StartBay INT64
  , ValidFrom DATE
  , ValidTo DATE
  , FLR_Key INT64
  , POG_Key INT64
) AS
WITH pog AS (
SELECT DISTINCT 
	UPPER(pog.Department) Department
	, UPPER(pog.Desc4) SubDepartment
	, UPPER(pog.Category) Category
	, CAST(flr.Value1 AS STRING) StoreNo
	, SUBSTR(flr.Desc11, 1, 4) Sales_Org
	, CAST(ffx.Value1 AS INT64) Aisle
	, CONCAT(ffx.Desc1,CAST(ffx.Value1 AS STRING)) AisleSide
	, ffx.Desc2 LocationName #Valley
	, CAST(ffx.Value2 AS INT64) StartBay
	, EXTRACT(DATE FROM flr.DbDateEffectiveFrom) ValidFrom
	, IFNULL(EXTRACT(DATE FROM flr.DbDateEffectiveTo), cast('9999-12-31' AS DATE)) ValidTo
	, flr.DbKey FLR_Key
	, pog.DbKey POG_Key
	, dense_rank() over (partition by flr.Desc11, cast(flr.Value1 AS INT64), upper(pog.Department), upper(pog.Desc4), upper(pog.Category)
	  order by flr.DbDateEffectiveFrom desc, flr.DbKey desc, pog.Dbkey desc,ver.DBKey desc) FP_Seq
FROM
	`{{params.GCP_WOW_ENV}}.adp_planogram_view.pog_ix_spc_planogram_curr_v` pog
INNER JOIN
	`{{params.GCP_WOW_ENV}}.adp_planogram_view.pog_ix_flr_section_curr_v` sec ON pog.DbKey = sec.DbParentPlanogramKey
INNER JOIN
	`{{params.GCP_WOW_ENV}}.adp_planogram_view.pog_ix_flr_floorplan_curr_v` flr ON flr.DbKey = sec.DbParentFloorplanKey
INNER JOIN
	`{{params.GCP_WOW_ENV}}.adp_planogram_view.pog_ix_flr_fixture_curr_v` ffx ON flr.DbKey = ffx.DbParentFloorplanKey AND ffx.DbKey = sec.DbParentFixtureKey
INNER JOIN 
	`{{params.GCP_WOW_ENV}}.adp_planogram_view.pog_ix_spc_planogram_curr_v` ver ON pog.DbFamilyKey = ver.DbFamilyKey
WHERE
	pog.DbStatus IN (1) 
	AND flr.DbStatus IN (1)
	AND (flr.DbDateEffectiveTo IS NULL OR EXTRACT(DATE FROM flr.DbDateEffectiveTo) >= '{{params.flr_effective_date}}') 
	AND (pog.DbDateEffectiveTo IS NULL OR EXTRACT(DATE FROM pog.DbDateEffectiveTo) >= '{{params.pog_effective_date}}') 
	AND pog.Rec_Del_Flag = 'N' 
	AND sec.Rec_Del_Flag = 'N'
	AND flr.Rec_Del_Flag = 'N'
	AND ffx.Rec_Del_Flag = 'N' 
	AND ver.Rec_Del_Flag = 'N'
	AND ver.DbStatus in (1)
	AND pog.Desc10 like 'C%'
	AND SUBSTR(flr.Desc11, 1, 4) in ('{{params.sales_org_SUPER}}','{{params.sales_org_METRO}}','{{params.sales_org_BWS}}','{{params.sales_org_DAN}}') 
), 
pog_Final AS (
SELECT
	pog.Department
	, pog.SubDepartment
	, pog.Category
	, pog.StoreNo
	, pog.Sales_Org AS SalesOrg
	, pog.Aisle
	, pog.AisleSide
	, pog.LocationName
	, pog.StartBay
	, pog.ValidFrom
	, pog.ValidTo
	, pog.FLR_Key
	, pog.POG_Key
FROM
	pog
WHERE 
	FP_Seq = 1
),
usr AS (
SELECT
	pog_Final.Department
	, pog_Final.SubDepartment
	, pog_Final.Category
	, pog_Final.StoreNo
	, pog_Final.SalesOrg
	, pog_Final.Aisle
	, pog_Final.AisleSide
	, pog_Final.LocationName
	, pog_Final.StartBay
	, pog_Final.ValidFrom
	, pog_Final.ValidTo
	, pog_Final.FLR_Key
	, pog_Final.POG_Key
	, fix.width width
	, fix.DBKEY
	, fix.SPC_FIXTURE_TYPE
	, sum(pos.Linear) Linear
FROM
	pog_Final 
INNER JOIN
	`{{params.GCP_WOW_ENV}}.adp_planogram_view.pog_ix_spc_position_curr_v` pos ON pog_Final.POG_Key = pos.DbParentPlanogramKey 
INNER JOIN
	`{{params.GCP_WOW_ENV}}.adp_planogram_view.pog_ix_spc_product_curr_v` prd ON prd.DbKey = pos.DbParentProductKey 
INNER JOIN
	`{{params.GCP_WOW_ENV}}.adp_planogram_view.pog_ix_spc_fixture_curr_v` fix ON fix.DBPARENTPLANOGRAMKEY = pog_Final.POG_Key
	AND fix.DBKEY = pos.DBPARENTFIXTUREKEY 
WHERE 
	fix.SPC_FIXTURE_TYPE in (1,2,7,8) AND pos.Rec_Del_Flag = 'N' AND prd.Rec_Del_Flag = 'N' AND fix.Rec_Del_Flag = 'N' and (fix.Flag1 = 0 or fix.Flag1 is NULL)
GROUP BY
	pog_Final.Department
	, pog_Final.SubDepartment
	, pog_Final.Category
	, pog_Final.StoreNo
	, pog_Final.SalesOrg
	, pog_Final.Aisle
	, pog_Final.AisleSide
	, pog_Final.LocationName
	, pog_Final.StartBay
	, pog_Final.ValidFrom
	, pog_Final.ValidTo
	, pog_Final.FLR_Key
	, pog_Final.POG_Key
	, fix.width
	, fix.DBKEY
	, fix.SPC_FIXTURE_TYPE
),use as (
SELECT
	usr.Department
	, usr.SubDepartment
	, usr.Category
	, usr.StoreNo
	, usr.SalesOrg
	, usr.Aisle
	, usr.AisleSide
	, usr.LocationName
	, usr.StartBay
	, usr.ValidFrom
	, usr.ValidTo
	, usr.FLR_Key
	, usr.POG_Key
	, usr.Linear 
	, usr.width 
	, usr.DBKEY
	, usr.SPC_FIXTURE_TYPE
	, roundOff(usr.Linear, case when usr.width = 0 then 1 else usr.width end) AS UsedLinear
FROM
	usr
),
used AS (
SELECT
	Department, SubDepartment, Category, StoreNo, SalesOrg, Aisle
	, AisleSide, LocationName, StartBay, ValidFrom, ValidTo, FLR_Key, POG_Key
	, sum(UsedLinear) UsedLinear
FROM 
	use
GROUP BY
	Department, SubDepartment, Category, StoreNo, SalesOrg, Aisle,
	AisleSide, LocationName, StartBay, ValidFrom, ValidTo, FLR_Key, POG_Key
),
avlr AS (
SELECT
	pog_Final.Department
	, pog_Final.SubDepartment
	, pog_Final.Category
	, pog_Final.StoreNo
	, pog_Final.SalesOrg
	, pog_Final.Aisle
	, pog_Final.AisleSide
	, pog_Final.LocationName
	, pog_Final.StartBay
	, pog_Final.ValidFrom
	, pog_Final.ValidTo
	, pog_Final.FLR_Key
	, pog_Final.POG_Key
	, sum(fix.Linear) AvblLinear  
FROM
	pog_Final 
INNER JOIN
	`{{params.GCP_WOW_ENV}}.adp_planogram_view.pog_ix_spc_fixture_curr_v` fix ON fix.DBPARENTPLANOGRAMKEY = pog_Final.POG_Key 
WHERE 
	fix.SPC_FIXTURE_TYPE in (0,3,4,5,6,9,12) and fix.Rec_Del_Flag = 'N' and (fix.Flag1 = 0 or fix.Flag1 is NULL)
GROUP BY
	pog_Final.Department, pog_Final.SubDepartment, pog_Final.Category, pog_Final.StoreNo, pog_Final.SalesOrg, pog_Final.Aisle, pog_Final.AisleSide,
	pog_Final.LocationName, pog_Final.StartBay, pog_Final.ValidFrom, pog_Final.ValidTo, pog_Final.FLR_Key, pog_Final.POG_Key
),
space AS (
SELECT
	IFNULL(avlr.Department,used.Department) Department
	, IFNULL(avlr.SubDepartment,used.SubDepartment) Sub_Department
	, IFNULL(avlr.Category,used.Category) Category
	, IFNULL(avlr.StoreNo,used.StoreNo) Store_No
	, IFNULL(avlr.SalesOrg,used.SalesOrg) Sales_Org
	, IFNULL(avlr.Aisle,used.Aisle) Aisle
	, IFNULL(avlr.AisleSide,used.AisleSide) AisleSide
	, IFNULL(avlr.LocationName,used.LocationName) LocationName
	, IFNULL(avlr.StartBay,used.StartBay) StartBay
	, IFNULL(avlr.ValidFrom,used.ValidFrom) ValidFrom
	, IFNULL(avlr.ValidTo,used.ValidTo) ValidTo
	, IFNULL(avlr.FLR_Key,used.FLR_Key) FLR_Key
	, IFNULL(avlr.POG_Key,used.POG_Key) POG_Key
	, used.UsedLinear UsedLinear
	, avlr.AvblLinear AvblLinear
	, IFNULL(used.UsedLinear,0) + IFNULL(avlr.AvblLinear,0)  TotalLinear

FROM 
	used 
FULL OUTER JOIN 
	avlr ON used.Department= avlr.Department 
	AND used.SubDepartment= avlr.SubDepartment 
	AND used.Category= avlr.Category 
	AND used.StoreNo = avlr.StoreNo 
	AND used.SalesOrg = avlr.SalesOrg 
	AND used.Aisle = avlr.Aisle 
	AND used.AisleSide = avlr.AisleSide 
	AND used.LocationName = avlr.LocationName 
	AND used.StartBay = avlr.StartBay 
	AND used.ValidFrom = avlr.ValidFrom 
	AND used.ValidTo = avlr.ValidTo 
	AND used.FLR_Key = avlr.FLR_Key 
	AND used.POG_Key = avlr.POG_Key  
)
SELECT
	space.Department, space.Sub_Department, space.Category, space.Store_No, space.Sales_Org,
	space.TotalLinear * IFNULL(ovr.Mul_Factor,1) AS TotalLinear, space.Aisle, space.AisleSide,
	space.LocationName, space.StartBay, space.ValidFrom, space.ValidTo, space.FLR_Key, space.POG_Key 
FROM 
	space 
LEFT OUTER JOIN 
	`{{params.project_ID}}.{{params.Optumera_Stage_DS}}.Hierarchy_Space_Override` ovr ON space.Sales_Org = ovr.Sales_org 
	AND space.Store_No = ovr.Store_No 
	AND space.Department= ovr.Department 
	AND space.Sub_Department= ovr.Sub_department 
	AND space.Category= ovr.Category

