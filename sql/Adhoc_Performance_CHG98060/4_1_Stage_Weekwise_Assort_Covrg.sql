/*
1.List down the Count of distinct Article_Id by joining Weekwise_Merch_Hierarchy with the Position,Product and Fixture tables and group them at 
  sales_org,Store_no,Week and Hierarchy.
2.Assortment Coverage is calculated by dividing the Article_Id_count by the max of Article_Id_count of that Hierarchy 
  at Week,Store_no,Sales_org and Hierarchy level
*/
CREATE OR REPLACE TABLE
  `{{params.project_ID}}.{{params.Optumera_Stage_DS}}.Stage_Weekwise_Assort_Covrg` ( Week_Year STRING,
Sales_Org STRING,
Store_No STRING,
Department STRING,
Sub_Department STRING,
Category STRING,
Article_Count INT64,
Max_Article_Count INT64,
Assort_Covrg FLOAT64 ) AS
with assort_count as (
    SELECT
      wsm.Week_Begin_Date,
      wsm.Week_End_Date,
      wsm.Week_Year,
      wsm.Sales_Org,
      wsm.Store_No,
      wsm.Department,
      wsm.Sub_Department,
      wsm.Category,
      count(distinct prd.Spc_Product_ID) Article_Id_count
    FROM
     `{{params.project_ID}}.{{params.Optumera_Stage_DS}}.Weekwise_Merch_Hierarchy` wsm
    INNER JOIN
      `{{params.GCP_WOW_ENV}}.adp_planogram_view.pog_ix_spc_position_curr_v` pos
    ON
      wsm.POG_Key = pos.DbParentPlanogramKey
    INNER JOIN
      `{{params.GCP_WOW_ENV}}.adp_planogram_view.pog_ix_spc_product_curr_v` prd
    ON
      prd.DbKey = pos.DbParentProductKey
	INNER JOIN
	  `{{params.GCP_WOW_ENV}}.adp_planogram_view.pog_ix_spc_fixture_curr_v` fix
	ON  
	  fix.DBPARENTPLANOGRAMKEY = wsm.POG_Key  and fix.DBKEY = pos.DBPARENTFIXTUREKEY 
    Where pos.Rec_Del_Flag = 'N' and prd.Rec_Del_Flag = 'N' and fix.Rec_Del_Flag = 'N' and (fix.Flag1 = 0 or fix.Flag1 is NULL)
      group by
      wsm.Week_Begin_Date,
      wsm.Week_End_Date,
      wsm.Week_Year,
      wsm.Sales_Org,
      wsm.Store_No,
      wsm.Department,
      wsm.Sub_Department,
      wsm.Category
    order by department, sub_department, category
    )

    SELECT
      ass_cnt.Week_Year,
      ass_cnt.Sales_Org,
      ass_cnt.Store_No,
      ass_cnt.Department,
      ass_cnt.Sub_Department,
      ass_cnt.Category,
      ass_cnt.Article_Id_count,
      ass_cnt_max.max_article_id_count,
      Round(ass_cnt.Article_Id_count/ass_cnt_max.max_article_id_count,2) assort_covrg
      from assort_count ass_cnt
      inner join 
      (select 
  ass_cnt.Week_Begin_Date,
  ass_cnt.Week_End_Date,
  ass_cnt.Week_Year,
  ass_cnt.Sales_Org,
  ass_cnt.Department,
  ass_cnt.Sub_Department,
  ass_cnt.Category,
  max(ass_cnt.Article_Id_count) max_article_id_count from assort_count ass_cnt
  group by 
  ass_cnt.Week_Begin_Date,
  ass_cnt.Week_End_Date,
  ass_cnt.Week_Year,
  ass_cnt.Sales_Org,
  ass_cnt.Department,
  ass_cnt.Sub_Department,
  ass_cnt.Category
      ) ass_cnt_max
      on 
      ass_cnt_max.Week_Begin_Date = ass_cnt.Week_Begin_Date 
      and ass_cnt_max.Week_End_Date = ass_cnt.Week_End_Date
      and ass_cnt_max.Week_Year =ass_cnt.Week_year
      and ass_cnt_max.Sales_Org = ass_cnt.Sales_org
      and ass_cnt_max.Department = ass_cnt.Department
      and ass_cnt_max.Sub_Department = ass_cnt.Sub_department
      and ass_cnt_max.Category = ass_cnt.category