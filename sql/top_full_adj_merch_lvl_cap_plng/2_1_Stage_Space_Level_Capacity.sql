/*
list down all the hierarchy, planogram falls into the below condition:
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

Capacity calculation:
1. list down all the planogram that has fixture flag1 as 1 or null
2. aggregate the capacity from position table at sales org, store number and hierarchy level.
*/

--Roundoff Function 
Create Temp Function roundOff(number FLOAT64,multiple FLOAT64) as
(
(ceiling(number / multiple)) * multiple
);

--Insert Record--
CREATE OR REPLACE TABLE  
`{{params.project_ID}}.{{params.Optumera_Stage_DS}}.Stage_Space_Level_Capacity` ( 
Department STRING,
SubDepartment STRING,
Category STRING,
Sales_Org STRING,
Capacity FLOAT64,
Used_Linear FLOAT64,
Avbl_Linear FLOAT64,
Total_Linear FLOAT64
) AS
WITH Hierarchy AS (
SELECT DISTINCT
	SUBSTR(pog.Desc2, 1, 4) SalesOrg
	, SUBSTR(pog.Desc2, 8) SalesOrgDesc
	, cast(flr.Value1 as string) as Store_no
	, upper(pog.Department) Department
	, upper(pog.Desc4) SubDepartment
	, upper(pog.Category) Category
	, pog.Status1 Status
	, pog.Dbkey POG_Key
	, DENSE_RANK() OVER (PARTITION BY  flr.Desc11, CAST(flr.Value1 as INT64), UPPER(ver.Department), UPPER(ver.Desc4),
	  UPPER(ver.Category) ORDER BY flr.DbDateEffectiveFrom DESC, flr.DbKey DESC, pog.Dbkey DESC, ver.DBKey DESC) Status_Seq
FROM
	`{{params.GCP_WOW_ENV}}.adp_planogram_view.pog_ix_spc_planogram_curr_v` pog 
INNER JOIN
	`{{params.GCP_WOW_ENV}}.adp_planogram_view.pog_ix_flr_section_curr_v` sec ON pog.DbKey = sec.DbParentPlanogramKey 
INNER JOIN
	`{{params.GCP_WOW_ENV}}.adp_planogram_view.pog_ix_flr_floorplan_curr_v` flr ON flr.DbKey = sec.DbParentFloorplanKey
INNER JOIN
	`{{params.GCP_WOW_ENV}}.adp_planogram_view.pog_ix_spc_planogram_curr_v` ver ON pog.DbFamilyKey = ver.DbFamilyKey
	AND pog.Dbkey = ver.Dbkey
	AND (EXTRACT(DATE FROM ver.DbDateEffectiveTo) >= '{{params.pog_effective_date}}' OR ver.DbDateEffectiveTo IS NULL)
WHERE 
	pog.DbStatus IN (1)
	AND SUBSTR(flr.Desc11, 1, 4) IN ('{{params.sales_org_SUPER}}','{{params.sales_org_BWS}}','{{params.sales_org_DAN}}','{{params.sales_org_METRO}}','{{params.sales_org_WNZ}}')
	AND flr.DbStatus in (1)
	AND (flr.DbDateEffectiveTo IS NULL OR EXTRACT(DATE FROM flr.DbDateEffectiveTo) >=  '{{params.flr_effective_date}}')
	AND pog.Rec_Del_Flag = 'N' 
	AND sec.Rec_Del_Flag = 'N'
	AND flr.Rec_Del_Flag = 'N'
	AND ver.Rec_Del_Flag = 'N'   
),
Hier as (
SELECT
	SalesOrg
	,SalesOrgDesc
	,Store_no
	,Department
	,SubDepartment
	,Category
	,Status 
	,POG_Key
FROM
	Hierarchy
WHERE 
	Status_Seq = 1
),
usr as (
SELECT
	Hier.POG_Key
	, Hier.SalesOrg, Hier.SalesOrgDesc, Hier.Store_no
	, Hier.Department, Hier.SubDepartment, Hier.Category
	, fix.width width
	, fix.DBKEY
	, fix.SPC_FIXTURE_TYPE
	, sum(pos.Linear) Linear	
FROM
	Hier 
INNER JOIN
	`{{params.GCP_WOW_ENV}}.adp_planogram_view.pog_ix_spc_position_curr_v` pos ON Hier.POG_Key = pos.DbParentPlanogramKey 
INNER JOIN
	`{{params.GCP_WOW_ENV}}.adp_planogram_view.pog_ix_spc_product_curr_v` prd ON prd.DbKey = pos.DbParentProductKey 
INNER JOIN
	`{{params.GCP_WOW_ENV}}.adp_planogram_view.pog_ix_spc_fixture_curr_v` fix ON fix.DBPARENTPLANOGRAMKEY = Hier.POG_Key
	AND fix.DBKEY = pos.DBPARENTFIXTUREKEY 
WHERE 
	fix.SPC_FIXTURE_TYPE in (1,2,7,8) AND pos.Rec_Del_Flag = 'N' AND prd.Rec_Del_Flag = 'N' AND fix.Rec_Del_Flag = 'N' and (fix.Flag1 = 0 or fix.Flag1 is NULL)
GROUP BY
	Hier.POG_Key
	, Hier.SalesOrg, Hier.SalesOrgDesc, Hier.Store_no
	, Hier.Department, Hier.SubDepartment, Hier.Category
	, fix.width
	, fix.DBKEY
	, fix.SPC_FIXTURE_TYPE

),use as (
SELECT
	usr.POG_Key
	, usr.SalesOrg, usr.SalesOrgDesc, usr.Store_no
	, usr.Department, usr.SubDepartment, usr.Category
	, usr.Linear 
	, usr.width 
	, usr.DBKEY
	, usr.SPC_FIXTURE_TYPE
	, roundOff(usr.Linear, case when usr.width = 0 then 1 else usr.width end) as UsedLinear
FROM
	usr
),
used as (
SELECT
	SalesOrg, SalesOrgDesc, Store_no  
	, Department, SubDepartment, Category
	, sum(UsedLinear) UsedLinear
FROM 
	use
GROUP BY
	SalesOrg, SalesOrgDesc , Store_no, Department, SubDepartment, Category
),
avlr as (
SELECT
	Hier.SalesOrg, Hier.SalesOrgDesc , Hier.Store_no 
	, Hier.Department, Hier.SubDepartment, Hier.Category
	, sum(fix.Linear) AvblLinear  
FROM
	Hier 
INNER JOIN
	`{{params.GCP_WOW_ENV}}.adp_planogram_view.pog_ix_spc_fixture_curr_v` fix ON fix.DBPARENTPLANOGRAMKEY = Hier.POG_Key 
WHERE 
	fix.SPC_FIXTURE_TYPE in (0,3,4,5,6,9,12) AND fix.Rec_Del_Flag = 'N' and (fix.Flag1 = 0 or fix.Flag1 is NULL)
GROUP BY
	Hier.SalesOrg, Hier.SalesOrgDesc , Hier.Store_no, Hier.Department, Hier.SubDepartment, Hier.Category
),
space as (
SELECT
	IFNULL(avlr.SalesOrg,used.SalesOrg) Sales_Org
	, IFNULL(avlr.SalesOrgDesc,used.SalesOrgDesc) Sales_Org_Desc
	, IFNULL(avlr.Store_no,used.Store_no) Store_no
	, IFNULL(avlr.Department,used.Department) Department
	, IFNULL(avlr.SubDepartment,used.SubDepartment) Sub_Department
	, IFNULL(avlr.Category,used.Category) Category
	, used.UsedLinear UsedLinear
	, avlr.AvblLinear AvblLinear
	, IFNULL(used.UsedLinear,0) + IFNULL(avlr.AvblLinear,0)  TotalLinear
FROM 
	used 
FULL OUTER JOIN 
	avlr ON used.SalesOrg = avlr.SalesOrg AND
	used.Store_no = avlr.Store_no AND
	used.Department= avlr.Department AND
	used.SubDepartment= avlr.SubDepartment AND
	used.Category= avlr.Category
),
fnlspace as (
SELECT
	space.Sales_Org, space.Sales_Org_Desc , space.Store_no 
	, space.Department, space.Sub_Department, space.Category
	, space.UsedLinear
	, space.AvblLinear
	, space.TotalLinear * IFNULL(ovr.Mul_Factor,1) as TotalLinear
FROM 
	space 
LEFT OUTER JOIN 
	`{{params.project_ID}}.{{params.Optumera_Stage_DS}}.Hierarchy_Space_Override` ovr ON 
	space.Sales_Org = ovr.Sales_org AND
	space.Store_No = ovr.Store_No AND
	space.Department= ovr.Department AND
	space.Sub_Department= ovr.Sub_department AND
	space.Category= ovr.Category
),
cap as (
SELECT
	Hier.SalesOrg as Sales_Org, Hier.SalesOrgDesc  as Sales_Org_Desc , Hier.Store_no
	, Hier.Department, Hier.SubDepartment as Sub_Department, Hier.Category
	, sum(pos.Capacity) capacity
FROM
	Hier 
INNER JOIN
	`{{params.GCP_WOW_ENV}}.adp_planogram_view.pog_ix_spc_position_curr_v` pos ON Hier.POG_Key = pos.DbParentPlanogramKey 
INNER JOIN
	`{{params.GCP_WOW_ENV}}.adp_planogram_view.pog_ix_spc_product_curr_v` prd ON prd.DbKey = pos.DbParentProductKey 
INNER JOIN
	`{{params.GCP_WOW_ENV}}.adp_planogram_view.pog_ix_spc_fixture_curr_v` fix ON fix.DBPARENTPLANOGRAMKEY = Hier.POG_Key AND fix.DBKEY = pos.DBPARENTFIXTUREKEY
WHERE  
	pos.Rec_Del_Flag = 'N' AND prd.Rec_Del_Flag = 'N' and fix.Rec_Del_Flag = 'N' and (fix.Flag1 = 0 or fix.Flag1 is NULL)
GROUP BY
	Hier.SalesOrg, Hier.SalesOrgDesc , Hier.Store_no, Hier.Department, Hier.SubDepartment, Hier.Category
),
final as (
SELECT
	fnlspace.Store_no,fnlspace.Department, fnlspace.Sub_Department, fnlspace.Category
	, fnlspace.Sales_Org
	, cap.capacity
	, fnlspace.UsedLinear
	, fnlspace.AvblLinear
	, fnlspace.TotalLinear
FROM 
	fnlspace 
LEFT OUTER JOIN 
	cap ON
	fnlspace.Sales_Org = cap.Sales_org AND
	fnlspace.Store_no = cap.Store_no AND
	fnlspace.Department= cap.Department AND
	fnlspace.Sub_Department= cap.Sub_department AND
	fnlspace.Category= cap.Category
)
SELECT
	final.Department, final.Sub_Department, final.Category
	, final.Sales_Org
	, sum(final.capacity) Capacity
	, sum(final.UsedLinear) UsedLinear
	, sum(final.AvblLinear) AvblLinear
	, sum(final.TotalLinear) TotalLinear
FROM 
	final
GROUP BY 
	final.Department, final.Sub_Department, final.Category, final.Sales_Org
