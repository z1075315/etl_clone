/*
List down all the Week hierarchy, planogram falls into the below condition:
1. Week_Begin_Date should be greater than or equal to parameterized date.
2. Week_End_Date should be less than or equal to parameterized date.
3. floor and planogram status should be live(1)and historic(4)
4. planogram desc10 should be "C%"
5. floor DbDateEffectiveTo should be null or greater than or equal to respective calendar Week_Begin_Date date.
6. Pog DbDateEffectiveTo should be null or greater than or equal to respective calendar Week_Begin_Date date.
7. Floor DbDateEffectiveFrom less than or equal to respective calendar Week_End_Date date.
8. Pog DbDateEffectiveFrom less than or equal to respective calendar Week_End_Date date.
9. dense rank should be applied to pick the unique rows
10.The above resultant data is joined with Hierarchy_exclusion_override for defining the types of Hierarchy like types 
('1 - Defined% Share','2 - SKU Capacity Share','3 - Exclude' and 'INCLUDE')
11.Exclusion of Hierarchy is applied on the above resultant data by excluding the types of '3 - Exclude'
*/
CREATE OR REPLACE TABLE `{{params.project_ID}}.{{params.Optumera_Stage_DS}}.Weekwise_Merch_Hierarchy`(
Week_Begin_Date DATE,        
Week_End_Date DATE,        
Week_Year STRING,        
Week_id INT64,        
Sales_Org STRING,        
Sales_Org_Desc STRING,        
Store_No STRING,        
FLR_Status STRING,
FLR_Key INT64,        
Department STRING,        
Sub_Department STRING,        
Category STRING,        
Equipment STRING,        
POG_Width FLOAT64,  
POG_Depth FLOAT64,      
POG_Size FLOAT64,        
POG_Segments INT64,        
POG_Status STRING,        
POG_Key INT64,        
types STRING,        
Hierarchy_Group STRING,        
Percent FLOAT64
) AS
with spc as (
SELECT DISTINCT
  cal.Week_Begin_Date Week_Begin_Date
  , cal.Week_End_Date Week_End_Date
  , cal.Week_Year Week_Year
  , cal.Week_id Week_id
  , substr(flr.Desc11, 1, 4) Sales_Org
  , substr(flr.Desc11, 8) Sales_Org_Desc
  , cast(flr.Value1 as string) Store_No
  , case when flr.DBStatus= 1 then 'Live' else 'Historic' end FLR_Status
  , flr.DbKey FLR_Key
  , upper(ver.Department) Department
  , upper(ver.Desc4) Sub_Department
  , upper(ver.Category) Category
  , ver.Desc5 Equipment
  , ver.Width POG_Width
  , ver.Depth POG_Depth
  , ver.Value3 POG_Size
  , ver.NumberOfSegments POG_Segments
  , flr.DbDateEffectiveFrom DbDateEffectiveFrom
  , ver.Status1 POG_Status
  , ver.DbKey POG_Key
  , ver.DbFamilyKey DbFamilyKey
  , flr.DbKey FLR_DbKey
  , dense_rank() over (partition by cal.Week_Begin_Date, flr.Desc11, cast(flr.Value1 as INT64), upper(ver.Department), upper(ver.Desc4), upper(ver.Category)
      order by flr.DbDateEffectiveFrom desc, flr.DbKey desc, pog.Dbkey desc,ver.DBKey desc) FP_Seq
FROM
 ( SELECT Week_Begin_Date,Week_End_Date,Week_Year,Week_id 
   FROM  `{{params.project_ID}}.{{params.Optumera_Stage_DS}}.Stage_Fiscal_Week` 
   --WHERE Week_Begin_Date >= DATE_SUB(Date "2019-12-01", INTERVAL 6 Week) and Week_End_Date <= Date "2019-12-01") cal inner join
   WHERE Week_End_Date <= CURRENT_DATE order by week_id desc limit {{params.Weeks_to_Delete}}) cal inner join
  `{{params.GCP_WOW_ENV}}.adp_planogram_view.pog_ix_flr_floorplan_curr_v` flr on
    EXTRACT(date FROM flr.DbDateEffectiveFrom) <= cal.Week_End_Date
    and (EXTRACT(date FROM flr.DbDateEffectiveTo) >= cal.Week_Begin_Date or flr.DbDateEffectiveTo is null) inner join
  `{{params.GCP_WOW_ENV}}.adp_planogram_view.pog_ix_flr_section_curr_v` sec on flr.DbKey = sec.DbParentFloorplanKey inner join
  `{{params.GCP_WOW_ENV}}.adp_planogram_view.pog_ix_spc_planogram_curr_v` pog on pog.DbKey = sec.DbParentPlanogramKey inner join
  `{{params.GCP_WOW_ENV}}.adp_planogram_view.pog_ix_spc_planogram_curr_v` ver on pog.DbFamilyKey = ver.DbFamilyKey and
    EXTRACT(date FROM ver.DbDateEffectiveFrom) <= cal.Week_End_Date
    and (EXTRACT(date FROM ver.DbDateEffectiveTo) >= cal.Week_Begin_Date or ver.DbDateEffectiveTo is null) inner join
	`{{params.project_ID}}.{{params.Optumera_Stage_DS}}.Stage_Banner` ban on ban.SalesOrganisation = substr(flr.Desc11, 1, 4)
WHERE
    flr.DbStatus in (1, 4)
and pog.DbStatus in (1, 4)
and ver.DbStatus in (1, 4)
and pog.Desc10 like 'C%'
and flr.Rec_Del_Flag = 'N' 
and sec.Rec_Del_Flag = 'N' 
and pog.Rec_Del_Flag = 'N' 
and ver.Rec_Del_Flag = 'N'
)
, hier as (
select
spc.Week_Begin_Date, spc.Week_End_Date, spc.Week_Year,spc.Week_id
, spc.Sales_Org, spc.Sales_Org_Desc, spc.Store_No
, spc.FLR_Status, spc.FLR_Key
, spc.Department, spc.Sub_Department, spc.Category
, spc.Equipment, spc.POG_Width,spc.POG_Depth,spc.POG_Size, spc.POG_Segments
, spc.POG_Status, spc.POG_Key
, ifnull( ovrd.types,"INCLUDE") types, ovrd.Hierarchy_Group, ovrd.Percent
from spc
Left outer join
`{{params.project_ID}}.{{params.Optumera_Stage_DS}}.Hierarchy_exclusion_override` ovrd
on spc.sales_org = ovrd.sales_org
and spc.Department = ovrd.Department
and spc.Sub_Department = ovrd.Sub_Department
and spc.Category = ovrd.Category
where spc.FP_Seq = 1
)
select Week_Begin_Date, Week_End_Date, Week_Year,Week_id, Sales_Org, Sales_Org_Desc, Store_No, FLR_Status, FLR_Key, Department, Sub_Department, Category, Equipment, POG_Width,POG_Depth, POG_Size, POG_Segments, POG_Status, POG_Key, types , Hierarchy_Group, Percent
from hier
where types <> '3 - Exclude'