/*
Used-linear calculation:
1. list down all the Week Hierarchy that has fixture type as 1,2,7,8 and fixture flag1 as 1 or null
2. apply ceiling function to the linear value from position table for the above Hierarchy.
3. aggregate the used linear value by Week Hierarchy level.

Available-linear calculation:
1. list down all the Week Hierarchy that has fixture type as 0,3,4,5,6,9,12 and fixture flag1 as 1 or null
2. aggregate the available linear value by Week Hierarchy level.

Total-linear calculation:
1. sum the available and used linear values using full outer join.
2. left join the results with Hierarchy_Space_Override and multiply the total-linear with mul-factor on sales org, store number and hierarchy.

Capacity calculation:
1. list down all the Week Hierarchy that has fixture flag1 as 1 or null
2. aggregate the capacity from position table at sales org, store number and Week hierarchy level.

Square calculation:
1.Multiply the POG_Width and POG_Depth from Weekwise_Merch_Hierarchy Table
2.Aggregate the Multiplied value of POG_Width and POG_Depth by Week Hierarchy level

*/

-- Round off Function
Create Temp Function roundOff(number FLOAT64,multiple FLOAT64) as
(
(ceiling(number / multiple)) * multiple
);

CREATE OR REPLACE TABLE
  `{{params.project_ID}}.{{params.Optumera_Stage_DS}}.Weekwise_Merch_Hierarchy_Space_Performance` ( Week_Begin_Date  DATE, 
Week_End_Date  DATE, 
Week_Year STRING, 
Week_id INT64, 
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
Capacity INT64) AS

with usr as (
select
    wsm.POG_Key
  , wsm.Week_Begin_Date
  , wsm.Week_End_Date
  , wsm.Week_Year
  , wsm.Week_id
  , wsm.Sales_Org
  , wsm.Sales_Org_Desc
  , wsm.Store_No  
  , wsm.Department
  , wsm.Sub_Department
  , wsm.Category
  , fix.width width
  , fix.DBKEY
  , fix.SPC_FIXTURE_TYPE
  , sum(pos.Linear) Linear
from
  `{{params.project_ID}}.{{params.Optumera_Stage_DS}}.Weekwise_Merch_Hierarchy` wsm inner join
  `{{params.GCP_WOW_ENV}}.adp_planogram_view.pog_ix_spc_position_curr_v` pos on wsm.POG_Key = pos.DbParentPlanogramKey inner join
  `{{params.GCP_WOW_ENV}}.adp_planogram_view.pog_ix_spc_product_curr_v` prd on prd.DbKey = pos.DbParentProductKey inner join
  `{{params.GCP_WOW_ENV}}.adp_planogram_view.pog_ix_spc_fixture_curr_v` fix on fix.DBPARENTPLANOGRAMKEY = wsm.POG_Key
  and fix.DBKEY = pos.DBPARENTFIXTUREKEY 
where fix.SPC_FIXTURE_TYPE in (1,2,7,8) and pos.Rec_Del_Flag = 'N' and prd.Rec_Del_Flag = 'N' and fix.Rec_Del_Flag = 'N' and (fix.Flag1 = 0 or fix.Flag1 is NULL)
group by 
    wsm.POG_Key
  , wsm.Week_Begin_Date
  , wsm.Week_End_Date
  , wsm.Week_Year
  , wsm.Week_id
  , wsm.Sales_Org
  , wsm.Sales_Org_Desc
  , wsm.Store_No  
  , wsm.Department
  , wsm.Sub_Department
  , wsm.Category
  , fix.width
  , fix.DBKEY
  , fix.SPC_FIXTURE_TYPE
),use as (
select
    usr.POG_Key
  , usr.Week_Begin_Date
  , usr.Week_End_Date
  , usr.Week_Year
  , usr.Week_id
  , usr.Sales_Org
  , usr.Sales_Org_Desc
  , usr.Store_No  
  , usr.Department
  , usr.Sub_Department
  , usr.Category
  , usr.Linear 
  , usr.width 
  , usr.Dbkey
  , usr.SPC_FIXTURE_TYPE
  , roundOff(usr.Linear, case when usr.width = 0 then 1 else usr.width end) as UsedLinear
from
usr 
),
used as (
select
    Week_Begin_Date
  , Week_End_Date
  , Week_Year,Week_id
  , Sales_Org
  , Sales_Org_Desc
  , Store_No  
  , Department
  , Sub_Department
  , Category
  , sum(UsedLinear) UsedLinear
from use
group by
    Week_Begin_Date
  , Week_End_Date
  , Week_Year,Week_id
  , Sales_Org
  , Sales_Org_Desc
  , Store_No  
  , Department
  , Sub_Department
  , Category
),
avlr as (
  select
      wsm.Week_Begin_Date, wsm.Week_End_Date, wsm.Week_Year,wsm.Week_id
    , wsm.Sales_Org, wsm.Sales_Org_Desc, wsm.Store_No  
    , wsm.Department, wsm.Sub_Department, wsm.Category
    , sum(fix.Linear) AvblLinear
   
  from
     `{{params.project_ID}}.{{params.Optumera_Stage_DS}}.Weekwise_Merch_Hierarchy` wsm inner join
    `{{params.GCP_WOW_ENV}}.adp_planogram_view.pog_ix_spc_fixture_curr_v` fix on fix.DBPARENTPLANOGRAMKEY = wsm.POG_Key 
    where fix.SPC_FIXTURE_TYPE in (0,3,4,5,6,9,12) and fix.Rec_Del_Flag = 'N' and (fix.Flag1 = 0 or fix.Flag1 is NULL)
  group by
      wsm.Week_Begin_Date, wsm.Week_End_Date, wsm.Week_Year,wsm.Week_id
    , wsm.Sales_Org, wsm.Sales_Org_Desc, wsm.Store_No
    , wsm.Department, wsm.Sub_Department, wsm.Category
  ),
 space as (
  select
  IFNULL(avlr.Week_Begin_Date,used.Week_Begin_Date) Week_Begin_Date
, IFNULL(avlr.Week_End_Date, used.Week_End_Date) Week_End_Date
, IFNULL(avlr.Week_Year, used.Week_Year) Week_Year
, IFNULL(avlr.Week_id, used.week_id) week_id
, IFNULL(avlr.Sales_Org,used.Sales_Org) Sales_Org
, IFNULL(avlr.Sales_Org_Desc,used.Sales_Org_Desc) Sales_Org_Desc
, IFNULL(avlr.Store_No,used.Store_No) Store_No
, IFNULL(avlr.Department,used.Department) Department
, IFNULL(avlr.Sub_Department,used.Sub_Department) Sub_Department
, IFNULL(avlr.Category,used.Category) Category
, used.UsedLinear UsedLinear
, avlr.AvblLinear AvblLinear
, IFNULL(used.UsedLinear,0) + IFNULL(avlr.AvblLinear,0)  TotalLinear

  from used full outer join avlr on
  used.Week_Begin_Date= avlr.week_Begin_Date and
  used.Week_End_Date= avlr.Week_End_date and
  used.Week_Year = avlr.week_year and
  used.Week_id = avlr.week_id and
  used.Sales_Org = avlr.Sales_org and
  used.Store_No = avlr.Store_No and
  used.Department= avlr.Department and
  used.Sub_Department= avlr.Sub_department and
  used.Category= avlr.Category
),
fnlspace as (
select
    space.Week_Begin_Date, space.Week_End_Date, space.Week_Year, space.Week_id
  , space.Sales_Org, space.Sales_Org_Desc, space.Store_No  
  , space.Department, space.Sub_Department, space.Category
  , space.UsedLinear
  , space.AvblLinear
  , space.TotalLinear * IFNULL(ovr.Mul_Factor,1) as TotalLinear
from space left outer join `{{params.project_ID}}.{{params.Optumera_Stage_DS}}.Hierarchy_Space_Override` ovr on 
  space.Sales_Org = ovr.Sales_org and
  space.Store_No = ovr.Store_No and
  space.Department= ovr.Department and
  space.Sub_Department= ovr.Sub_department and
  space.Category= ovr.Category
  ),
cap as (
select
    wsm.Week_Begin_Date, wsm.Week_End_Date, wsm.Week_Year, wsm.Week_id
  , wsm.Sales_Org, wsm.Sales_Org_Desc, wsm.Store_No  
  , wsm.Department, wsm.Sub_Department, wsm.Category
  , sum(pos.Capacity) capacity
 from
  `{{params.project_ID}}.{{params.Optumera_Stage_DS}}.Weekwise_Merch_Hierarchy` wsm inner join
  `{{params.GCP_WOW_ENV}}.adp_planogram_view.pog_ix_spc_position_curr_v` pos on wsm.POG_Key = pos.DbParentPlanogramKey inner join
  `{{params.GCP_WOW_ENV}}.adp_planogram_view.pog_ix_spc_product_curr_v` prd on prd.DbKey = pos.DbParentProductKey inner join
  `{{params.GCP_WOW_ENV}}.adp_planogram_view.pog_ix_spc_fixture_curr_v` fix on fix.DBPARENTPLANOGRAMKEY = wsm.POG_Key
  and fix.DBKEY = pos.DBPARENTFIXTUREKEY 
where  pos.Rec_Del_Flag = 'N' and prd.Rec_Del_Flag = 'N' and fix.Rec_Del_Flag = 'N' and (fix.Flag1 = 0 or fix.Flag1 is NULL)
group by
    wsm.Week_Begin_Date, wsm.Week_End_Date, wsm.Week_Year, Week_id
  , wsm.Sales_Org, wsm.Sales_Org_Desc, wsm.Store_No  
  , wsm.Department, wsm.Sub_Department, wsm.Category
),
capspace as 
(
select
    fnlspace.Week_Begin_Date, fnlspace.Week_End_Date, fnlspace.Week_Year, fnlspace.Week_id
  , fnlspace.Sales_Org, fnlspace.Sales_Org_Desc, fnlspace.Store_No  
  , fnlspace.Department, fnlspace.Sub_Department, fnlspace.Category
  , fnlspace.UsedLinear
  , fnlspace.AvblLinear
  , fnlspace.TotalLinear
  , cap.capacity
 from fnlspace Left outer join cap
 on
  fnlspace.Week_Begin_Date= cap.week_Begin_Date and
  fnlspace.Week_End_Date= cap.Week_End_date and
  fnlspace.Week_Year = cap.week_year and
  fnlspace.Week_id = cap.week_id and
  fnlspace.Sales_Org = cap.Sales_org and
  fnlspace.Store_No = cap.Store_No and
  fnlspace.Department= cap.Department and
  fnlspace.Sub_Department= cap.Sub_department and
  fnlspace.Category= cap.Category
),
square as (select
    wsm.Week_Begin_Date, wsm.Week_End_Date, wsm.Week_Year, wsm.Week_id
  , wsm.Sales_Org, wsm.Sales_Org_Desc, wsm.Store_No  
  , wsm.Department, wsm.Sub_Department, wsm.Category
  , sum(wsm.POG_Width * wsm.POG_Depth) Square
 from `{{params.project_ID}}.{{params.Optumera_Stage_DS}}.Weekwise_Merch_Hierarchy` wsm
 group by 
  wsm.Week_Begin_Date, wsm.Week_End_Date, wsm.Week_Year, wsm.Week_id
  , wsm.Sales_Org, wsm.Sales_Org_Desc, wsm.Store_No  
  , wsm.Department, wsm.Sub_Department, wsm.Category

)
select
    capspace.Week_Begin_Date, capspace.Week_End_Date, capspace.Week_Year, capspace.Week_id
  , capspace.Sales_Org, capspace.Sales_Org_Desc, capspace.Store_No  
  , capspace.Department, capspace.Sub_Department, capspace.Category
  , capspace.UsedLinear
  , capspace.AvblLinear
  , capspace.TotalLinear
  , square.Square
  , capspace.capacity
 from capspace LEFT OUTER JOIN square
 on
  capspace.Week_Begin_Date= square.week_Begin_Date and
  capspace.Week_End_Date= square.Week_End_date and
  capspace.Week_Year = square.week_year and
  capspace.Week_id = square.week_id and
  capspace.Sales_Org = square.Sales_org and
  capspace.Store_No = square.Store_No and
  capspace.Department= square.Department and
  capspace.Sub_Department= square.Sub_department and
  capspace.Category= square.Category
