/*
list down all the hierarchy falls into the below condition:
1. floor and planogram status should be live(1)and historic(4)
2. planogram desc10 should be "C%"
3. floor and planogram DbDateEffectiveTo should be null or greater than or equal to parameterized date.
4. dense rank should be applied to pick the unique rows
*/

CREATE OR REPLACE TABLE
	`{{ params.project_ID }}.{{ params.Optumera_Stage_DS }}.Merch_Hierarchy` (Sales_Org STRING,
    Sales_Org_Desc STRING,
    Store_No STRING,
    FLR_Status STRING,
    FLR_Key INT64,
    Department STRING,
    Sub_Department STRING,
    Category STRING,
    DbDateEffectiveTo TIMESTAMP,
    DbDateEffectiveFrom TIMESTAMP,
    Equipment STRING,
    POG_Width FLOAT64,
    POG_Size FLOAT64,
    POG_Segments INT64,
    POG_Status STRING,
    POG_Key INT64
	) AS
with spc AS (
SELECT DISTINCT
	substr(flr.Desc11, 1, 4) Sales_Org
	, substr(flr.Desc11, 8) Sales_Org_Desc
	, cast(flr.Value1 AS string) Store_No
	, case when flr.DBStatus= 1 then 'Live' ELSE 'Historic' end FLR_Status
	, flr.DbKey FLR_Key
	, upper(pog.Department) Department
	, upper(pog.Desc4) Sub_Department
	, upper(pog.Category) Category
	, pog.Desc5 Equipment
	, pog.Width POG_Width
	, pog.Value3 POG_Size
	, pog.NumberOfSegments POG_Segments
	, flr.DbDateEffectiveFROM DbDateEffectiveFROM
	, flr.DbDateEffectiveTo DbDateEffectiveTo
	, pog.Status1 POG_Status
	, pog.DbKey POG_Key
	, pog.DbFamilyKey DbFamilyKey   
	, dense_rank() over (partition by flr.Desc11, cast(flr.Value1 AS INT64), upper(pog.Department), upper(pog.Desc4), upper(pog.Category)  
	order by flr.DbDateEffectiveFROM desc , flr.DbKey desc, pog.Dbkey desc
	) FP_Seq
FROM
	`{{ params.GCP_WOW_ENV }}.adp_planogram_view.pog_ix_flr_floorplan_curr_v` flr  
INNER JOIN
	`{{ params.GCP_WOW_ENV }}.adp_planogram_view.pog_ix_flr_section_curr_v` sec on flr.DbKey = sec.DbParentFloorplanKey INNER JOIN
	`{{ params.GCP_WOW_ENV }}.adp_planogram_view.pog_ix_spc_planogram_curr_v` pog on pog.DbKey = sec.DbParentPlanogramKey INNER JOIN
	`{{ params.GCP_WOW_ENV }}.adp_planogram_view.pog_ix_spc_planogram_curr_v` ver on pog.DbFamilyKey = ver.DbFamilyKey
INNER JOIN 
	`{{ params.project_ID }}.{{ params.Optumera_Stage_DS }}.Stage_Banner` ban 
ON 
	ban.SalesOrganisation = substr(flr.Desc11, 1, 4)

WHERE
	flr.DbStatus in (1,4)
	AND pog.DbStatus in (1,4)
	AND pog.Desc10 like 'C%'
	AND (flr.DbDateEffectiveTo is null or EXTRACT(date FROM flr.DbDateEffectiveTo) >= '{{ params.flr_effective_date }}')
	AND (pog.DbDateEffectiveTo is null or EXTRACT(date FROM pog.DbDateEffectiveTo) >= '{{ params.pog_effective_date }}')
	AND flr.Rec_Del_Flag = 'N' 
	AND sec.Rec_Del_Flag = 'N'
	AND pog.Rec_Del_Flag = 'N'
	AND ver.Rec_Del_Flag = 'N' 
	)
SELECT
	Sales_Org, Sales_Org_Desc
	, Store_No, FLR_Status
	, FLR_Key, Department
	, Sub_Department, Category
	, DbDateEffectiveTo, DbDateEffectiveFROM
	, Equipment, POG_Width
	, POG_Size, POG_Segments
	, POG_Status, POG_Key
FROM 
	spc
WHERE 
	FP_Seq = 1