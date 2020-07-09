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
Create Temp Function roundOff(number FLOAT64,multiple FLOAT64) as
(
(ceiling(number / multiple)) * multiple
);

--Insert Record--
CREATE OR REPLACE TABLE 
	`{{params.project_ID}}.{{params.Optumera_Stage_DS}}.Stage_Planogram` ( 
	 Pog_Id STRING,
    Store_No STRING,
    Pog_Start_Date DATE,
    Pog_End_Date DATE,
    FLR_Start_Date DATE,
    Pog_Status INT64,
    Sales_org STRING,
    Department STRING,
    SubDepartment STRING,
    Category STRING,
    Used_Linear FLOAT64,
    Avbl_Linear FLOAT64,
    Total_Linear FLOAT64 
) AS

WITH Hier as(

SELECT DISTINCT
	pog.DbKey Pog_ID,
	cast(flr.Value1 as string) Store_No,
	CAST (pog.DbDateEffectiveFrom AS DATE) Pog_Start_Date,
	IFNULL(EXTRACT(date FROM pog.DbDateEffectiveTo), cast('9999-12-31' as date)) Pog_End_Date,
	CAST (flr.DbDateEffectiveFrom AS DATE) FLR_Start_Date,
	flr.dbstatus Pog_Status,
	SUBSTR(flr.Desc11, 1, 4) sales_org,
	UPPER(pog.DEPARTMENT) DEPARTMENT,
	UPPER(pog.DESC4) SUBDEPARTMENT,
	UPPER(pog.CATEGORY) CATEGORY,
	DENSE_RANK() OVER (PARTITION BY flr.Desc11, CAST(flr.Value1 as INT64), UPPER(ver.Department), UPPER(ver.Desc4), UPPER(ver.Category)
	ORDER BY flr.DbDateEffectiveFrom DESC, flr.DbKey DESC, pog.Dbkey DESC,ver.DBKey DESC) FP_Seq
FROM
	`{{params.GCP_WOW_ENV}}.adp_planogram_view.pog_ix_spc_planogram_curr_v` pog
INNER JOIN
	`{{params.GCP_WOW_ENV}}.adp_planogram_view.pog_ix_flr_section_curr_v` sec ON pog.DbKey = sec.DbParentPlanogramKey
INNER JOIN
	`{{params.GCP_WOW_ENV}}.adp_planogram_view.pog_ix_flr_floorplan_curr_v` flr ON flr.DbKey = sec.DbParentFloorplanKey
INNER JOIN
	`{{params.GCP_WOW_ENV}}.adp_planogram_view.pog_ix_spc_planogram_curr_v` ver ON pog.DbFamilyKey = ver.DbFamilyKey 
WHERE 
	SUBSTR(flr.Desc11, 1, 4) IN ('{{params.sales_org_SUPER}}','{{params.sales_org_BWS}}','{{params.sales_org_DAN}}','{{params.sales_org_METRO}}','{{params.sales_org_WNZ}}')
	AND pog.DbStatus in (1)
	AND flr.DbStatus in (1) 
	AND ver.DbStatus in (1)
	AND pog.Desc10 like 'C%'
	AND (flr.DbDateEffectiveTo is null or EXTRACT(date FROM flr.DbDateEffectiveTo) >= '{{params.flr_effective_date}}')
	AND (pog.DbDateEffectiveTo is null or EXTRACT(date FROM pog.DbDateEffectiveTo) >= '{{params.pog_effective_date}}')
	AND pog.Rec_Del_Flag = 'N' 
	AND sec.Rec_Del_Flag = 'N'
	AND flr.Rec_Del_Flag = 'N'
	AND ver.Rec_Del_Flag = 'N'   
),

Plano as
(
SELECT
	Hier.Pog_Id as POG_Key,
	Hier.Store_No,
	Hier.Pog_Start_Date,
	Hier.Pog_End_Date,
	Hier.FLR_Start_Date,
	Hier.Pog_Status,
	Hier.Sales_Org,
	Hier.Department,
	Hier.SubDepartment,
	Hier.Category
FROM
	Hier
WHERE
	FP_SEQ = 1
),
usr as (
SELECT
	Plano.POG_Key,Plano.Store_No, Plano.Pog_Start_Date,Plano.Pog_End_Date,Plano.FLR_Start_Date,Plano.Pog_Status,
	Plano.Sales_Org , Plano.Department, Plano.SubDepartment, Plano.Category
	, fix.width width
	, fix.DBKEY
	, fix.SPC_FIXTURE_TYPE
	, sum(pos.Linear) Linear
FROM
	Plano 
INNER JOIN
	`{{params.GCP_WOW_ENV}}.adp_planogram_view.pog_ix_spc_position_curr_v` pos ON Plano.POG_Key = pos.DbParentPlanogramKey 
INNER JOIN
	`{{params.GCP_WOW_ENV}}.adp_planogram_view.pog_ix_spc_product_curr_v` prd ON prd.DbKey = pos.DbParentProductKey 
INNER JOIN
	`{{params.GCP_WOW_ENV}}.adp_planogram_view.pog_ix_spc_fixture_curr_v` fix ON fix.DBPARENTPLANOGRAMKEY = Plano.POG_Key
	AND fix.DBKEY = pos.DBPARENTFIXTUREKEY 
WHERE 
	fix.SPC_FIXTURE_TYPE in (1,2,7,8) AND pos.Rec_Del_Flag = 'N' AND prd.Rec_Del_Flag = 'N' AND fix.Rec_Del_Flag = 'N' and (fix.Flag1 = 0 or fix.Flag1 is NULL)
GROUP BY
	Plano.POG_Key,Plano.Store_No, Plano.Pog_Start_Date,Plano.Pog_End_Date,Plano.FLR_Start_Date,Plano.Pog_Status,
	Plano.Sales_Org , Plano.Department, Plano.SubDepartment, Plano.Category
	, fix.width
	, fix.DBKEY
	, fix.SPC_FIXTURE_TYPE
),
use as(
SELECT
	usr.POG_Key,usr.Store_No, usr.Pog_Start_Date,usr.Pog_End_Date,usr.FLR_Start_Date,usr.Pog_Status,
	usr.Sales_Org , usr.Department, usr.SubDepartment, usr.Category
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
	POG_Key,Store_No,Pog_Start_Date,Pog_End_Date,FLR_Start_Date,Pog_Status,
	Sales_Org, Department, SubDepartment, Category,
	sum(UsedLinear) UsedLinear
FROM 
	use
GROUP BY
	POG_Key,Store_No, Pog_Start_Date, Pog_End_Date, FLR_Start_Date, Pog_Status, Sales_Org, Department, SubDepartment, Category
),
avlr as (
SELECT
	Plano.POG_Key, Plano.Store_No, Plano.Pog_Start_Date, Plano.Pog_End_Date, Plano.FLR_Start_Date,
	Plano.Pog_Status, Plano.Sales_Org , Plano.Department, Plano.SubDepartment, Plano.Category,
	sum(fix.Linear) AvblLinear  
FROM
  Plano 
INNER JOIN
	`{{params.GCP_WOW_ENV}}.adp_planogram_view.pog_ix_spc_fixture_curr_v` fix ON fix.DBPARENTPLANOGRAMKEY = Plano.POG_Key 
WHERE 
	fix.SPC_FIXTURE_TYPE in (0,3,4,5,6,9,12) AND fix.Rec_Del_Flag = 'N' and (fix.Flag1 = 0 or fix.Flag1 is NULL)
GROUP BY
	Plano.POG_Key, Plano.Store_No, Plano.Pog_Start_Date, Plano.Pog_End_Date, Plano.FLR_Start_Date,
	Plano.Pog_Status, Plano.Sales_Org , Plano.Department, Plano.SubDepartment, Plano.Category
),
space as (
SELECT
	IFNULL(avlr.POG_Key,used.POG_Key) POG_Key
	, IFNULL(avlr.Store_No,used.Store_No) Store_No
	, IFNULL(avlr.Pog_Start_Date,used.Pog_Start_Date) Pog_Start_Date
	, IFNULL(avlr.Pog_End_Date,used.Pog_End_Date) Pog_End_Date
	, IFNULL(avlr.FLR_Start_Date,used.FLR_Start_Date) FLR_Start_Date
	, IFNULL(avlr.Pog_Status,used.Pog_Status) Pog_Status
	, IFNULL(avlr.Sales_Org,used.Sales_Org) Sales_Org
	, IFNULL(avlr.Department,used.Department) Department
	, IFNULL(avlr.SubDepartment,used.SubDepartment) Sub_Department
	, IFNULL(avlr.Category,used.Category) Category
	, used.UsedLinear UsedLinear
	, avlr.AvblLinear AvblLinear
	, IFNULL(used.UsedLinear,0) + IFNULL(avlr.AvblLinear,0)  TotalLinear
FROM 
	used 
FULL OUTER JOIN 
	avlr ON
	used.POG_Key = avlr.POG_Key AND
	used.Store_No = avlr.Store_No AND
	used.Pog_Start_Date = avlr.Pog_Start_Date AND
	used.Pog_End_Date = avlr.Pog_End_Date AND
	used.FLR_Start_Date = avlr.FLR_Start_Date AND
	used.Pog_Status = avlr.Pog_Status AND
	used.Sales_Org = avlr.Sales_Org AND
	used.Department= avlr.Department AND
	used.SubDepartment= avlr.SubDepartment AND
	used.Category= avlr.Category
)
SELECT
	CAST (space.POG_Key as STRING) as Pog_Id,space.Store_No, space.Pog_Start_Date,space.Pog_End_Date,
	space.FLR_Start_Date,space.Pog_Status,space.Sales_Org, space.Department, space.Sub_Department, space.Category, 
	space.UsedLinear, space.AvblLinear, space.TotalLinear * IFNULL(ovr.Mul_Factor,1) as TotalLinear
FROM 
	space 
LEFT OUTER JOIN 
	`{{params.project_ID}}.{{params.Optumera_Stage_DS}}.Hierarchy_Space_Override` ovr ON 
	space.Sales_Org = ovr.Sales_org AND
	space.Store_No = ovr.Store_No AND
	space.Department= ovr.Department AND
	space.Sub_Department= ovr.Sub_department AND
	space.Category= ovr.Category