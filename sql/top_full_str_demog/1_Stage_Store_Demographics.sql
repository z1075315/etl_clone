/*
1. select the BWS, Dan murphy's demographics data from Demographic_Census_AUS
2. select metro demographics data from Demographic_Census_AUS and join them with quantium metro based on store number and sales org
3. select supers demographics data from Demographic_Census_AUS and join them with quantium supers based on store number and sales org
4. select the newzealand demographics data from Demographic_Census_WNZ
5. Union all the demographics data.
*/

CREATE OR REPLACE TABLE
  `{{ params.project_ID }}.{{ params.Optumera_Stage_DS }}.Stage_Demographics`(Store_Number STRING,
    Sales_org STRING,
    Tot_P_M INT64,
    Tot_P_F INT64,
    Tot_P_P INT64,
    Age_0_4_yr_M INT64,
    Age_0_4_yr_F INT64,
    Age_0_4_yr_P INT64,
    Age_5_14_yr_M INT64,
    Age_5_14_yr_F INT64,
    Age_5_14_yr_P INT64,
    Age_15_19_yr_M INT64,
    Age_15_19_yr_F INT64,
    Age_15_19_yr_P INT64,
    Age_20_24_yr_M INT64,
    Age_20_24_yr_F INT64,
    Age_20_24_yr_P INT64,
    Age_25_34_yr_M INT64,
    Age_25_34_yr_F INT64,
    Age_25_34_yr_P INT64,
    Age_35_44_yr_M INT64,
    Age_35_44_yr_F INT64,
    Age_35_44_yr_P INT64,
    Age_45_54_yr_M INT64,
    Age_45_54_yr_F INT64,
    Age_45_54_yr_P INT64,
    Age_55_64_yr_M INT64,
    Age_55_64_yr_F INT64,
    Age_55_64_yr_P INT64,
    Age_65_74_yr_M INT64,
    Age_65_74_yr_F INT64,
    Age_65_74_yr_P INT64,
    Age_75_84_yr_M INT64,
    Age_75_84_yr_F INT64,
    Age_75_84_yr_P INT64,
    Age_85ov_M INT64,
    Age_85ov_F INT64,
    Age_85ov_P INT64,
    Average_household_size FLOAT64,
    Aust_Tot_Resp INT64,
    Aust_Abor_Tot_Resp INT64,
    Chinese_Tot_Resp INT64,
    Croatian_Tot_Resp INT64,
    Dutch_Tot_Resp INT64,
    English_Tot_Resp INT64,
    Filipino_Tot_Resp INT64,
    French_Tot_Resp INT64,
    German_Tot_Resp INT64,
    Greek_Tot_Resp INT64,
    Hungarian_Tot_Resp INT64,
    Indian_Tot_Resp INT64,
    Irish_Tot_Resp INT64,
    Italian_Tot_Resp INT64,
    Korean_Tot_Resp INT64,
    Lebanese_Tot_Resp INT64,
    Macedonian_Tot_Resp INT64,
    Maltese_Tot_Resp INT64,
    Maori_Tot_Resp INT64,
    NZ_Tot_Resp INT64,
    Polish_Tot_Resp INT64,
    Russian_Tot_Resp INT64,
    Scottish_Tot_Resp INT64,
    Serbian_Tot_Resp INT64,
    Sth_African_Tot_Resp INT64,
    Spanish_Tot_Resp INT64,
    Sri_Lankan_Tot_Resp INT64,
    Turkish_Tot_Resp INT64,
    Vietnamese_Tot_Resp INT64,
    Welsh_Tot_Resp INT64,
    Other_Tot_Resp INT64,
    Tot_P_Tot_Resp INT64,
    P_Afghanistan_Tot INT64,
    P_Australia_Tot INT64,
    P_Bangladesh_Tot INT64,
    P_Bosnia_Herzegov_Tot INT64,
    P_Cambodia_Tot INT64,
    P_Canada_Tot INT64,
    P_Chile_Tot INT64,
    P_China_Tot INT64,
    P_Croatia_Tot INT64,
    P_Egypt_Tot INT64,
    P_England_Tot INT64,
    P_Fiji_Tot INT64,
    P_France_Tot INT64,
    P_Germany_Tot INT64,
    P_Greece_Tot INT64,
    P_Hong_Kong_Tot INT64,
    P_India_Tot INT64,
    P_Indonesia_Tot INT64,
    P_Iran_Tot INT64,
    P_Iraq_Tot INT64,
    P_Ireland_Tot INT64,
    P_Italy_Tot INT64,
    P_Japan_Tot INT64,
    P_Korea_South_Tot INT64,
    P_Lebanon_Tot INT64,
    P_Malaysia_Tot INT64,
    P_Malta_Tot INT64,
    P_Mauritius_Tot INT64,
    P_Myanmar_Tot INT64,
    P_Nepal_Tot INT64,
    P_Netherlands_Tot INT64,
    P_New_Zealand_Tot INT64,
    P_Nthern_Ireland_Tot INT64,
    P_Pakistan_Tot INT64,
    P_PNG_Tot INT64,
    P_Philippines_Tot INT64,
    P_Poland_Tot INT64,
    P_Scotland_Tot INT64,
    P_Singapore_Tot INT64,
    P_South_Africa_Tot INT64,
    P_SE_Europe_nfd_Tot INT64,
    P_Sri_Lanka_Tot INT64,
    P_Taiwan_Tot INT64,
    P_Thailand_Tot INT64,
    P_FYROM_Tot INT64,
    P_Turkey_Tot INT64,
    P_USA_Tot INT64,
    P_Vietnam_Tot INT64,
    P_Wales_Tot INT64,
    P_Zimbabwe_Tot INT64,
    P_Elsewhere_Tot INT64,
    Buddhism_P INT64,
    Christianity_Tot_P INT64,
    Hinduism_P INT64,
    Islam_P INT64,
    Judaism_P INT64,
    Othr_Rel_Sikhism_P INT64,
    Other_Religions_Tot_P INT64,
    SB_OSB_NRA_Tot_P INT64,
    Pre_school_M INT64,
    Pre_school_F INT64,
    Pre_school_P INT64,
    Infants_Primary_Tot_P INT64,
    Secondary_Tot_M INT64,
    Secondary_Tot_F INT64,
    Secondary_Tot_P INT64,
    Tec_Furt_Educ_inst_Tot_M INT64,
    Tec_Furt_Educ_inst_Tot_F INT64,
    Tec_Furt_Educ_inst_Tot_P INT64,
    Uni_other_Tert_Instit_Tot_M INT64,
    Uni_other_Tert_Instit_Tot_F INT64,
    Uni_other_Tert_Instit_Tot_P INT64,
    Other_type_educ_instit_Tot_M INT64,
    Other_type_educ_instit_Tot_F INT64,
    Other_type_educ_instit_Tot_P INT64,
    Negative_Nil_income_Tot INT64,
    HI_1_149_Tot INT64,
    HI_150_299_Tot INT64,
    HI_300_399_Tot INT64,
    HI_400_499_Tot INT64,
    HI_500_649_Tot INT64,
    HI_650_799_Tot INT64,
    HI_800_999_Tot INT64,
    HI_1000_1249_Tot INT64,
    HI_1250_1499_Tot INT64,
    HI_1500_1749_Tot INT64,
    HI_1750_1999_Tot INT64,
    HI_2000_2499_Tot INT64,
    HI_2500_2999_Tot INT64,
    HI_3000_3499_Tot INT64,
    HI_3500_3999_Tot INT64,
    HI_4000_more_Tot INT64,
    Num_MVs_per_dweling_0_MVs INT64,
    Num_MVs_per_dweling_1_MVs INT64,
    Num_MVs_per_dweling_2_MVs INT64,
    Num_MVs_per_dweling_3_MVs INT64,
    Num_MVs_per_dweling_4mo_MVs INT64,
    Total_FamHhold INT64,
    Total_NonFamHhold INT64,
    OPDs_Separate_house_Persons INT64,
    OPDs_SD_r_t_h_th_1_sty_Psns INT64,
    OPDs_SD_r_t_h_th_2_m_sty_Psns INT64,
    OPDs_SD_r_t_h_th_Tot_Psns INT64,
    OPDs_F_ap_I_1or2_sty_blk_Ps INT64,
    OPDs_F_ap_I_3_sty_blk_Psns INT64,
    OPDs_F_ap_I_4_m_sty_blk_Ps INT64,
    OPDs_Flt_apt_Att_house_Ps INT64,
    OPDs_Flt_apart_Tot_Psns INT64,
    OPDs_Oth_dw_Cvn_Ps INT64,
    OPDs_Oth_dwg_cab_hboat_Ps INT64,
    OPDs_Ot_dwg_Im_hm_tnt_SO_Ps INT64,
    OPDs_Ot_dwg_Hs_f_att_sh_of_Ps INT64,
    OPDs_Other_dwelling_Tot_Psns INT64,
    OPDs_Dwlling_structur_NS_Psns INT64,
    OPDs_Tot_OPDs_Persons INT64,
    Total_PDs_Persons INT64,
    P_Ag_For_Fshg_Tot INT64,
    P_Mining_Tot INT64,
    P_Manufact_Tot INT64,
    P_El_Gas_Wt_Waste_Tot INT64,
    P_Constru_Tot INT64,
    P_WhlesaleTde_Tot INT64,
    P_RetTde_Tot INT64,
    P_Accom_food_Tot INT64,
    P_Trans_post_wrehsg_Tot INT64,
    P_Info_media_teleco_Tot INT64,
    P_Fin_Insur_Tot INT64,
    P_RtnHir_REst_Tot INT64,
    P_Pro_scien_tec_Tot INT64,
    P_Admin_supp_Tot INT64,
    P_Public_admin_sfty_Tot INT64,
    P_Educ_trng_Tot INT64,
    P_HlthCare_SocAs_Tot INT64,
    P_Art_recn_Tot INT64,
    P_Oth_scs_Tot INT64,
    P_ID_NS_Tot INT64,
    P_Tot_Managers INT64,
    P_Tot_Professionals INT64,
    P_Tot_TechnicTrades_W INT64,
    P_Tot_CommunPersnlSvc_W INT64,
    P_Tot_ClericalAdminis_W INT64,
    P_Tot_Sales_W INT64,
    P_Tot_Mach_oper_drivers INT64,
    P_Tot_Labourers INT64,
    Ancestry_Aust_NZ_P INT64,
    Ancestry_UK_Ire_P INT64,
    Ancestry_Other_Europe_P INT64,
    Ancestry_Western_Asia_P INT64,
    Ancestry_Southern_Asia_P INT64,
    Ancestry_Eastern_Asia_P INT64,
    Ancestry_South_Eastern_Asia_P INT64,
    Ancestry_Elsewhere_P INT64,
    Born_Aust_NZ_P INT64,
    Born_UK_Ire_P INT64,
    Born_Other_Europe_P INT64,
    Born_Western_Asia_P INT64,
    Born_Southern_Asia_P INT64,
    Born_Eastern_Asia_P INT64,
    Born_South_Eastern_Asia_P INT64,
    Born_Elsewhere_P INT64,
    Ancestry_Aboriginal_Only_P INT64,
    Religion_Non_Drinking_P INT64,
    Ave_Basket_Size FLOAT64,
    Ave_basket_Size_Units FLOAT64,
    Morning_6to10 FLOAT64,
    Day_11to4 FLOAT64,
    Night_5onwards FLOAT64,
    Weekend FLOAT64,
    B_Prop FLOAT64,
    M_Prop FLOAT64,
    P_Prop FLOAT64,
    HasCafe FLOAT64,
    FOS_Prop FLOAT64,
    FFN_Prop FLOAT64,
    FFL_Prop FLOAT64,
    Emergency_Impulse FLOAT64,
    Non_Food_Shop FLOAT64,
    Food_Shop FLOAT64,
    non_promo_prop FLOAT64,
    Fresh_Foodies FLOAT64,
    Modern_Families FLOAT64,
    Healthy_Premium_Entertaining FLOAT64,
    Cigarette_Impulse FLOAT64,
    Grab_Go FLOAT64,
    Sales_Proportion_Budget FLOAT64,
    Sales_Proportion_Mainstream FLOAT64,
    Sales_Proportion_Premium FLOAT64,
    Sales_Proportion_New_Families FLOAT64,
    Sales_Proportion_Young_Families FLOAT64,
    Sales_Proportion_Older_Families FLOAT64,
    Sales_Proportion_Young_Singles_Couples FLOAT64,
    Sales_Proportion_Midage_Singles_Couples FLOAT64,
    Sales_Proportion_Older_Singles_Couples FLOAT64,
    Sales_Proportion_Retirees FLOAT64,
    Sales_Proportion_Ultra_Healthy FLOAT64,
    Sales_Proportion_Free_From FLOAT64,
    Sales_Proportion_High_Protein_Traditionalists FLOAT64,
    Sales_Proportion_Health_Newcomer FLOAT64,
    Sales_Proportion_Convenience_Driven FLOAT64,
    Sales_Proportion_Unhealthy_Snackers FLOAT64,
    European FLOAT64,
    Mpi FLOAT64,
    asianindian FLOAT64,
    Christian FLOAT64,
    Hindu FLOAT64,
    Muslim FLOAT64,
    Affluence FLOAT64,
    Holiday INT64,
    UniArea INT64,
    Prop_Student_Allowance FLOAT64) AS
    
SELECT 
	LOC_NO AS Store_Number
	,Sales_org
	,Tot_P_M,Tot_P_F,Tot_P_P,Age_0_4_yr_M,Age_0_4_yr_F,Age_0_4_yr_P,Age_5_14_yr_M,Age_5_14_yr_F,Age_5_14_yr_P,Age_15_19_yr_M,Age_15_19_yr_F,Age_15_19_yr_P
	,Age_20_24_yr_M,Age_20_24_yr_F,Age_20_24_yr_P,Age_25_34_yr_M,Age_25_34_yr_F,Age_25_34_yr_P,Age_35_44_yr_M,Age_35_44_yr_F,Age_35_44_yr_P,Age_45_54_yr_M
	,Age_45_54_yr_F,Age_45_54_yr_P,Age_55_64_yr_M,Age_55_64_yr_F,Age_55_64_yr_P,Age_65_74_yr_M,Age_65_74_yr_F,Age_65_74_yr_P,Age_75_84_yr_M,Age_75_84_yr_F
	,Age_75_84_yr_P,Age_85ov_M,Age_85ov_F,Age_85ov_P,Average_household_size,Aust_Tot_Resp,Aust_Abor_Tot_Resp,Chinese_Tot_Resp,Croatian_Tot_Resp,Dutch_Tot_Resp
	,English_Tot_Resp,Filipino_Tot_Resp,French_Tot_Resp,German_Tot_Resp,Greek_Tot_Resp,Hungarian_Tot_Resp,Indian_Tot_Resp,Irish_Tot_Resp,Italian_Tot_Resp
	,Korean_Tot_Resp,Lebanese_Tot_Resp,Macedonian_Tot_Resp,Maltese_Tot_Resp,Maori_Tot_Resp,NZ_Tot_Resp,Polish_Tot_Resp,Russian_Tot_Resp,Scottish_Tot_Resp
	,Serbian_Tot_Resp,Sth_African_Tot_Resp,Spanish_Tot_Resp,Sri_Lankan_Tot_Resp,Turkish_Tot_Resp,Vietnamese_Tot_Resp,Welsh_Tot_Resp,Other_Tot_Resp,Tot_P_Tot_Resp
	,P_Afghanistan_Tot,P_Australia_Tot,P_Bangladesh_Tot,P_Bosnia_Herzegov_Tot,P_Cambodia_Tot,P_Canada_Tot,P_Chile_Tot,P_China_Tot,P_Croatia_Tot,P_Egypt_Tot
	,P_England_Tot,P_Fiji_Tot,P_France_Tot,P_Germany_Tot,P_Greece_Tot,P_Hong_Kong_Tot,P_India_Tot,P_Indonesia_Tot,P_Iran_Tot,P_Iraq_Tot,P_Ireland_Tot,P_Italy_Tot
	,P_Japan_Tot,P_Korea_South_Tot,P_Lebanon_Tot,P_Malaysia_Tot,P_Malta_Tot,P_Mauritius_Tot,P_Myanmar_Tot,P_Nepal_Tot,P_Netherlands_Tot,P_New_Zealand_Tot
	,P_Nthern_Ireland_Tot,P_Pakistan_Tot,P_PNG_Tot,P_Philippines_Tot,P_Poland_Tot,P_Scotland_Tot,P_Singapore_Tot,P_South_Africa_Tot,P_SE_Europe_nfd_Tot
	,P_Sri_Lanka_Tot,P_Taiwan_Tot,P_Thailand_Tot,P_FYROM_Tot,P_Turkey_Tot,P_USA_Tot,P_Vietnam_Tot,P_Wales_Tot,P_Zimbabwe_Tot,P_Elsewhere_Tot,Buddhism_P
	,Christianity_Tot_P,Hinduism_P,Islam_P,Judaism_P,Othr_Rel_Sikhism_P,Other_Religions_Tot_P,SB_OSB_NRA_Tot_P,Pre_school_M,Pre_school_F,Pre_school_P
	,Infants_Primary_Tot_P,Secondary_Tot_M,Secondary_Tot_F,Secondary_Tot_P,Tec_Furt_Educ_inst_Tot_M,Tec_Furt_Educ_inst_Tot_F,Tec_Furt_Educ_inst_Tot_P
	,Uni_other_Tert_Instit_Tot_M,Uni_other_Tert_Instit_Tot_F,Uni_other_Tert_Instit_Tot_P,Other_type_educ_instit_Tot_M,Other_type_educ_instit_Tot_F
	,Other_type_educ_instit_Tot_P,Negative_Nil_income_Tot,HI_1_149_Tot,HI_150_299_Tot,HI_300_399_Tot,HI_400_499_Tot,HI_500_649_Tot,HI_650_799_Tot,HI_800_999_Tot
	,HI_1000_1249_Tot,HI_1250_1499_Tot,HI_1500_1749_Tot,HI_1750_1999_Tot,HI_2000_2499_Tot,HI_2500_2999_Tot,HI_3000_3499_Tot,HI_3500_3999_Tot,HI_4000_more_Tot
	,Num_MVs_per_dweling_0_MVs,Num_MVs_per_dweling_1_MVs,Num_MVs_per_dweling_2_MVs,Num_MVs_per_dweling_3_MVs,Num_MVs_per_dweling_4mo_MVs,Total_FamHhold
	,Total_NonFamHhold,OPDs_Separate_house_Persons,OPDs_SD_r_t_h_th_1_sty_Psns,OPDs_SD_r_t_h_th_2_m_sty_Psns,OPDs_SD_r_t_h_th_Tot_Psns,OPDs_F_ap_I_1or2_sty_blk_Ps
	,OPDs_F_ap_I_3_sty_blk_Psns,OPDs_F_ap_I_4_m_sty_blk_Ps,OPDs_Flt_apt_Att_house_Ps,OPDs_Flt_apart_Tot_Psns,OPDs_Oth_dw_Cvn_Ps,OPDs_Oth_dwg_cab_hboat_Ps
	,OPDs_Ot_dwg_Im_hm_tnt_SO_Ps,OPDs_Ot_dwg_Hs_f_att_sh_of_Ps,OPDs_Other_dwelling_Tot_Psns,OPDs_Dwlling_structur_NS_Psns,OPDs_Tot_OPDs_Persons,Total_PDs_Persons
	,P_Ag_For_Fshg_Tot,P_Mining_Tot,P_Manufact_Tot,P_El_Gas_Wt_Waste_Tot,P_Constru_Tot,P_WhlesaleTde_Tot,P_RetTde_Tot,P_Accom_food_Tot,P_Trans_post_wrehsg_Tot
	,P_Info_media_teleco_Tot,P_Fin_Insur_Tot,P_RtnHir_REst_Tot,P_Pro_scien_tec_Tot,P_Admin_supp_Tot,P_Public_admin_sfty_Tot,P_Educ_trng_Tot,P_HlthCare_SocAs_Tot
	,P_Art_recn_Tot,P_Oth_scs_Tot,P_ID_NS_Tot,P_Tot_Managers,P_Tot_Professionals,P_Tot_TechnicTrades_W,P_Tot_CommunPersnlSvc_W,P_Tot_ClericalAdminis_W,P_Tot_Sales_W
	,P_Tot_Mach_oper_drivers,P_Tot_Labourers,Ancestry_Aust_NZ_P,Ancestry_UK_Ire_P,Ancestry_Other_Europe_P,Ancestry_Western_Asia_P,Ancestry_Southern_Asia_P
	,Ancestry_Eastern_Asia_P,Ancestry_South_Eastern_Asia_P,Ancestry_Elsewhere_P,Born_Aust_NZ_P,Born_UK_Ire_P,Born_Other_Europe_P,Born_Western_Asia_P
	,Born_Southern_Asia_P,Born_Eastern_Asia_P,Born_South_Eastern_Asia_P,Born_Elsewhere_P,Ancestry_Aboriginal_Only_P,Religion_Non_Drinking_P,
	NULL AS Ave_Basket_Size,
	NULL AS Ave_basket_Size_Units,
	NULL AS Morning_6to10,
	NULL AS Day_11to4,
	NULL AS Night_5onwards,
	NULL AS Weekend,
	NULL AS B_Prop,
	NULL AS M_Prop,
	NULL AS P_Prop,
	NULL AS HasCafe,
	NULL AS FOS_Prop,
	NULL AS FFN_Prop,
	NULL AS FFL_Prop,
	NULL AS Emergency_Impulse,
	NULL AS Non_Food_Shop,
	NULL AS Food_Shop,
	NULL AS non_promo_prop,
	NULL AS Fresh_Foodies,
	NULL AS Modern_Families,
	NULL AS Healthy_Premium_Entertaining,
	NULL AS Cigarette_Impulse,
	NULL AS Grab_Go,
	NULL AS Sales_Proportion_Budget,
	NULL AS Sales_Proportion_Mainstream,
	NULL AS Sales_Proportion_Premium,
	NULL AS Sales_Proportion_New_Families,
	NULL AS Sales_Proportion_Young_Families,
	NULL AS Sales_Proportion_Older_Families,
	NULL AS Sales_Proportion_Young_Singles_Couples,
	NULL AS Sales_Proportion_Midage_Singles_Couples,
	NULL AS Sales_Proportion_Older_Singles_Couples,
	NULL AS Sales_Proportion_Retirees,
	NULL AS Sales_Proportion_Ultra_Healthy,
	NULL AS Sales_Proportion_Free_From,
	NULL AS Sales_Proportion_High_Protein_Traditionalists,
	NULL AS Sales_Proportion_Health_Newcomer,
	NULL AS Sales_Proportion_Convenience_Driven,
	NULL AS Sales_Proportion_Unhealthy_Snackers,
	NULL AS European,
	NULL AS Mpi,
	NULL AS asianindian,
	NULL AS Christian,
	NULL AS Hindu,
	NULL AS Muslim,
	NULL AS Affluence,
	NULL AS Holiday,
	NULL AS UniArea,
	NULL AS Prop_Student_Allowance

FROM
	`{{ params.project_ID }}.{{ params.Optumera_Stage_DS }}.Demographic_Census_AUS` demo 
WHERE 
	demo.Sales_org IN ('{{ params.sales_org_BWS}}' , '{{ params.sales_org_DAN}}')
UNION ALL
SELECT 
	IFNULL(demo.LOC_NO, dqm.Store_Number) AS Store_Number
	,IFNULL(Sales_org, '{{ params.sales_org_METRO}}') AS Sales_org 
	,Tot_P_M,Tot_P_F,Tot_P_P,Age_0_4_yr_M,Age_0_4_yr_F,Age_0_4_yr_P,Age_5_14_yr_M,Age_5_14_yr_F,Age_5_14_yr_P,Age_15_19_yr_M,Age_15_19_yr_F,Age_15_19_yr_P
	,Age_20_24_yr_M,Age_20_24_yr_F,Age_20_24_yr_P,Age_25_34_yr_M,Age_25_34_yr_F,Age_25_34_yr_P,Age_35_44_yr_M,Age_35_44_yr_F,Age_35_44_yr_P,Age_45_54_yr_M
	,Age_45_54_yr_F,Age_45_54_yr_P,Age_55_64_yr_M,Age_55_64_yr_F,Age_55_64_yr_P,Age_65_74_yr_M,Age_65_74_yr_F,Age_65_74_yr_P,Age_75_84_yr_M,Age_75_84_yr_F
	,Age_75_84_yr_P,Age_85ov_M,Age_85ov_F,Age_85ov_P,Average_household_size,Aust_Tot_Resp,Aust_Abor_Tot_Resp,Chinese_Tot_Resp,Croatian_Tot_Resp,Dutch_Tot_Resp
	,English_Tot_Resp,Filipino_Tot_Resp,French_Tot_Resp,German_Tot_Resp,Greek_Tot_Resp,Hungarian_Tot_Resp,Indian_Tot_Resp,Irish_Tot_Resp,Italian_Tot_Resp
	,Korean_Tot_Resp,Lebanese_Tot_Resp,Macedonian_Tot_Resp,Maltese_Tot_Resp,Maori_Tot_Resp,NZ_Tot_Resp,Polish_Tot_Resp,Russian_Tot_Resp,Scottish_Tot_Resp
	,Serbian_Tot_Resp,Sth_African_Tot_Resp,Spanish_Tot_Resp,Sri_Lankan_Tot_Resp,Turkish_Tot_Resp,Vietnamese_Tot_Resp,Welsh_Tot_Resp,Other_Tot_Resp,Tot_P_Tot_Resp
	,P_Afghanistan_Tot,P_Australia_Tot,P_Bangladesh_Tot,P_Bosnia_Herzegov_Tot,P_Cambodia_Tot,P_Canada_Tot,P_Chile_Tot,P_China_Tot,P_Croatia_Tot,P_Egypt_Tot
	,P_England_Tot,P_Fiji_Tot,P_France_Tot,P_Germany_Tot,P_Greece_Tot,P_Hong_Kong_Tot,P_India_Tot,P_Indonesia_Tot,P_Iran_Tot,P_Iraq_Tot,P_Ireland_Tot,P_Italy_Tot
	,P_Japan_Tot,P_Korea_South_Tot,P_Lebanon_Tot,P_Malaysia_Tot,P_Malta_Tot,P_Mauritius_Tot,P_Myanmar_Tot,P_Nepal_Tot,P_Netherlands_Tot,P_New_Zealand_Tot
	,P_Nthern_Ireland_Tot,P_Pakistan_Tot,P_PNG_Tot,P_Philippines_Tot,P_Poland_Tot,P_Scotland_Tot,P_Singapore_Tot,P_South_Africa_Tot,P_SE_Europe_nfd_Tot
	,P_Sri_Lanka_Tot,P_Taiwan_Tot,P_Thailand_Tot,P_FYROM_Tot,P_Turkey_Tot,P_USA_Tot,P_Vietnam_Tot,P_Wales_Tot,P_Zimbabwe_Tot,P_Elsewhere_Tot,Buddhism_P
	,Christianity_Tot_P,Hinduism_P,Islam_P,Judaism_P,Othr_Rel_Sikhism_P,Other_Religions_Tot_P,SB_OSB_NRA_Tot_P,Pre_school_M,Pre_school_F,Pre_school_P
	,Infants_Primary_Tot_P,Secondary_Tot_M,Secondary_Tot_F,Secondary_Tot_P,Tec_Furt_Educ_inst_Tot_M,Tec_Furt_Educ_inst_Tot_F,Tec_Furt_Educ_inst_Tot_P
	,Uni_other_Tert_Instit_Tot_M,Uni_other_Tert_Instit_Tot_F,Uni_other_Tert_Instit_Tot_P,Other_type_educ_instit_Tot_M,Other_type_educ_instit_Tot_F
	,Other_type_educ_instit_Tot_P,Negative_Nil_income_Tot,HI_1_149_Tot,HI_150_299_Tot,HI_300_399_Tot,HI_400_499_Tot,HI_500_649_Tot,HI_650_799_Tot,HI_800_999_Tot
	,HI_1000_1249_Tot,HI_1250_1499_Tot,HI_1500_1749_Tot,HI_1750_1999_Tot,HI_2000_2499_Tot,HI_2500_2999_Tot,HI_3000_3499_Tot,HI_3500_3999_Tot,HI_4000_more_Tot
	,Num_MVs_per_dweling_0_MVs,Num_MVs_per_dweling_1_MVs,Num_MVs_per_dweling_2_MVs,Num_MVs_per_dweling_3_MVs,Num_MVs_per_dweling_4mo_MVs,Total_FamHhold
	,Total_NonFamHhold,OPDs_Separate_house_Persons,OPDs_SD_r_t_h_th_1_sty_Psns,OPDs_SD_r_t_h_th_2_m_sty_Psns,OPDs_SD_r_t_h_th_Tot_Psns,OPDs_F_ap_I_1or2_sty_blk_Ps
	,OPDs_F_ap_I_3_sty_blk_Psns,OPDs_F_ap_I_4_m_sty_blk_Ps,OPDs_Flt_apt_Att_house_Ps,OPDs_Flt_apart_Tot_Psns,OPDs_Oth_dw_Cvn_Ps,OPDs_Oth_dwg_cab_hboat_Ps
	,OPDs_Ot_dwg_Im_hm_tnt_SO_Ps,OPDs_Ot_dwg_Hs_f_att_sh_of_Ps,OPDs_Other_dwelling_Tot_Psns,OPDs_Dwlling_structur_NS_Psns,OPDs_Tot_OPDs_Persons,Total_PDs_Persons
	,P_Ag_For_Fshg_Tot,P_Mining_Tot,P_Manufact_Tot,P_El_Gas_Wt_Waste_Tot,P_Constru_Tot,P_WhlesaleTde_Tot,P_RetTde_Tot,P_Accom_food_Tot,P_Trans_post_wrehsg_Tot
	,P_Info_media_teleco_Tot,P_Fin_Insur_Tot,P_RtnHir_REst_Tot,P_Pro_scien_tec_Tot,P_Admin_supp_Tot,P_Public_admin_sfty_Tot,P_Educ_trng_Tot,P_HlthCare_SocAs_Tot
	,P_Art_recn_Tot,P_Oth_scs_Tot,P_ID_NS_Tot,P_Tot_Managers,P_Tot_Professionals,P_Tot_TechnicTrades_W,P_Tot_CommunPersnlSvc_W,P_Tot_ClericalAdminis_W,P_Tot_Sales_W
	,P_Tot_Mach_oper_drivers,P_Tot_Labourers,Ancestry_Aust_NZ_P,Ancestry_UK_Ire_P,Ancestry_Other_Europe_P,Ancestry_Western_Asia_P,Ancestry_Southern_Asia_P
	,Ancestry_Eastern_Asia_P,Ancestry_South_Eastern_Asia_P,Ancestry_Elsewhere_P,Born_Aust_NZ_P,Born_UK_Ire_P,Born_Other_Europe_P,Born_Western_Asia_P
	,Born_Southern_Asia_P,Born_Eastern_Asia_P,Born_South_Eastern_Asia_P,Born_Elsewhere_P,Ancestry_Aboriginal_Only_P,Religion_Non_Drinking_P
	,dqm.Ave_Basket_Size
	,dqm.Ave_basket_Size_Units
	,dqm.Morning_6to10
	,dqm.Day_11to4
	,dqm.Night_5onwards
	,dqm.Weekend
	,dqm.B_Prop
	,dqm.M_Prop
	,dqm.P_Prop
	,dqm.HasCafe
	,dqm.FOS_Prop
	,dqm.FFN_Prop
	,dqm.FFL_Prop
	,dqm.Emergency_Impulse
	,dqm.Non_Food_Shop
	,dqm.Food_Shop
	,dqm.non_promo_prop
	,dqm.Fresh_Foodies
	,dqm.Modern_Families
	,dqm.Healthy_Premium_Entertaining
	,dqm.Cigarette_Impulse
	,dqm.Grab_Go,
	NULL AS Sales_Proportion_Budget,
	NULL AS Sales_Proportion_Mainstream,
	NULL AS Sales_Proportion_Premium,
	NULL AS Sales_Proportion_New_Families,
	NULL AS Sales_Proportion_Young_Families,
	NULL AS Sales_Proportion_Older_Families,
	NULL AS Sales_Proportion_Young_Singles_Couples,
	NULL AS Sales_Proportion_Midage_Singles_Couples,
	NULL AS Sales_Proportion_Older_Singles_Couples,
	NULL AS Sales_Proportion_Retirees,
	NULL AS Sales_Proportion_Ultra_Healthy,
	NULL AS Sales_Proportion_Free_From,
	NULL AS Sales_Proportion_High_Protein_Traditionalists,
	NULL AS Sales_Proportion_Health_Newcomer,
	NULL AS Sales_Proportion_Convenience_Driven,
	NULL AS Sales_Proportion_Unhealthy_Snackers,
	NULL AS European,
	NULL AS Mpi,
	NULL AS asianindian,
	NULL AS Christian,
	NULL AS Hindu,
	NULL AS Muslim,
	NULL AS Affluence,
	NULL AS Holiday,
	NULL AS UniArea,
	NULL AS Prop_Student_Allowance
FROM (
SELECT * 
FROM 
	`{{ params.project_ID }}.{{ params.Optumera_Stage_DS }}.Demographic_Census_AUS` 
WHERE 
	Sales_org = '{{ params.sales_org_METRO}}') demo 
FULL OUTER JOIN          
	`{{ params.project_ID }}.{{ params.Optumera_Stage_DS }}.Demographic_Quantium_Metro` dqm 
ON
	demo.Loc_No = dqm.Store_Number

UNION ALL
SELECT 
	IFNULL(demo.LOC_NO, dqs.Store_Number) AS Store_Number,IFNULL(Sales_org, '{{ params.sales_org_SUPER}}') AS Sales_org 
	,Tot_P_M,Tot_P_F,Tot_P_P,Age_0_4_yr_M,Age_0_4_yr_F,Age_0_4_yr_P,Age_5_14_yr_M,Age_5_14_yr_F,Age_5_14_yr_P,Age_15_19_yr_M,Age_15_19_yr_F,Age_15_19_yr_P
	,Age_20_24_yr_M,Age_20_24_yr_F,Age_20_24_yr_P,Age_25_34_yr_M,Age_25_34_yr_F,Age_25_34_yr_P,Age_35_44_yr_M,Age_35_44_yr_F,Age_35_44_yr_P,Age_45_54_yr_M
	,Age_45_54_yr_F,Age_45_54_yr_P,Age_55_64_yr_M,Age_55_64_yr_F,Age_55_64_yr_P,Age_65_74_yr_M,Age_65_74_yr_F,Age_65_74_yr_P,Age_75_84_yr_M,Age_75_84_yr_F
	,Age_75_84_yr_P,Age_85ov_M,Age_85ov_F,Age_85ov_P,Average_household_size,Aust_Tot_Resp,Aust_Abor_Tot_Resp,Chinese_Tot_Resp,Croatian_Tot_Resp,Dutch_Tot_Resp
	,English_Tot_Resp,Filipino_Tot_Resp,French_Tot_Resp,German_Tot_Resp,Greek_Tot_Resp,Hungarian_Tot_Resp,Indian_Tot_Resp,Irish_Tot_Resp,Italian_Tot_Resp
	,Korean_Tot_Resp,Lebanese_Tot_Resp,Macedonian_Tot_Resp,Maltese_Tot_Resp,Maori_Tot_Resp,NZ_Tot_Resp,Polish_Tot_Resp,Russian_Tot_Resp,Scottish_Tot_Resp
	,Serbian_Tot_Resp,Sth_African_Tot_Resp,Spanish_Tot_Resp,Sri_Lankan_Tot_Resp,Turkish_Tot_Resp,Vietnamese_Tot_Resp,Welsh_Tot_Resp,Other_Tot_Resp,Tot_P_Tot_Resp
	,P_Afghanistan_Tot,P_Australia_Tot,P_Bangladesh_Tot,P_Bosnia_Herzegov_Tot,P_Cambodia_Tot,P_Canada_Tot,P_Chile_Tot,P_China_Tot,P_Croatia_Tot,P_Egypt_Tot
	,P_England_Tot,P_Fiji_Tot,P_France_Tot,P_Germany_Tot,P_Greece_Tot,P_Hong_Kong_Tot,P_India_Tot,P_Indonesia_Tot,P_Iran_Tot,P_Iraq_Tot,P_Ireland_Tot,P_Italy_Tot
	,P_Japan_Tot,P_Korea_South_Tot,P_Lebanon_Tot,P_Malaysia_Tot,P_Malta_Tot,P_Mauritius_Tot,P_Myanmar_Tot,P_Nepal_Tot,P_Netherlands_Tot,P_New_Zealand_Tot
	,P_Nthern_Ireland_Tot,P_Pakistan_Tot,P_PNG_Tot,P_Philippines_Tot,P_Poland_Tot,P_Scotland_Tot,P_Singapore_Tot,P_South_Africa_Tot,P_SE_Europe_nfd_Tot
	,P_Sri_Lanka_Tot,P_Taiwan_Tot,P_Thailand_Tot,P_FYROM_Tot,P_Turkey_Tot,P_USA_Tot,P_Vietnam_Tot,P_Wales_Tot,P_Zimbabwe_Tot,P_Elsewhere_Tot,Buddhism_P
	,Christianity_Tot_P,Hinduism_P,Islam_P,Judaism_P,Othr_Rel_Sikhism_P,Other_Religions_Tot_P,SB_OSB_NRA_Tot_P,Pre_school_M,Pre_school_F,Pre_school_P
	,Infants_Primary_Tot_P,Secondary_Tot_M,Secondary_Tot_F,Secondary_Tot_P,Tec_Furt_Educ_inst_Tot_M,Tec_Furt_Educ_inst_Tot_F,Tec_Furt_Educ_inst_Tot_P
	,Uni_other_Tert_Instit_Tot_M,Uni_other_Tert_Instit_Tot_F,Uni_other_Tert_Instit_Tot_P,Other_type_educ_instit_Tot_M,Other_type_educ_instit_Tot_F
	,Other_type_educ_instit_Tot_P,Negative_Nil_income_Tot,HI_1_149_Tot,HI_150_299_Tot,HI_300_399_Tot,HI_400_499_Tot,HI_500_649_Tot,HI_650_799_Tot,HI_800_999_Tot
	,HI_1000_1249_Tot,HI_1250_1499_Tot,HI_1500_1749_Tot,HI_1750_1999_Tot,HI_2000_2499_Tot,HI_2500_2999_Tot,HI_3000_3499_Tot,HI_3500_3999_Tot,HI_4000_more_Tot
	,Num_MVs_per_dweling_0_MVs,Num_MVs_per_dweling_1_MVs,Num_MVs_per_dweling_2_MVs,Num_MVs_per_dweling_3_MVs,Num_MVs_per_dweling_4mo_MVs,Total_FamHhold
	,Total_NonFamHhold,OPDs_Separate_house_Persons,OPDs_SD_r_t_h_th_1_sty_Psns,OPDs_SD_r_t_h_th_2_m_sty_Psns,OPDs_SD_r_t_h_th_Tot_Psns,OPDs_F_ap_I_1or2_sty_blk_Ps
	,OPDs_F_ap_I_3_sty_blk_Psns,OPDs_F_ap_I_4_m_sty_blk_Ps,OPDs_Flt_apt_Att_house_Ps,OPDs_Flt_apart_Tot_Psns,OPDs_Oth_dw_Cvn_Ps,OPDs_Oth_dwg_cab_hboat_Ps
	,OPDs_Ot_dwg_Im_hm_tnt_SO_Ps,OPDs_Ot_dwg_Hs_f_att_sh_of_Ps,OPDs_Other_dwelling_Tot_Psns,OPDs_Dwlling_structur_NS_Psns,OPDs_Tot_OPDs_Persons,Total_PDs_Persons
	,P_Ag_For_Fshg_Tot,P_Mining_Tot,P_Manufact_Tot,P_El_Gas_Wt_Waste_Tot,P_Constru_Tot,P_WhlesaleTde_Tot,P_RetTde_Tot,P_Accom_food_Tot,P_Trans_post_wrehsg_Tot
	,P_Info_media_teleco_Tot,P_Fin_Insur_Tot,P_RtnHir_REst_Tot,P_Pro_scien_tec_Tot,P_Admin_supp_Tot,P_Public_admin_sfty_Tot,P_Educ_trng_Tot,P_HlthCare_SocAs_Tot
	,P_Art_recn_Tot,P_Oth_scs_Tot,P_ID_NS_Tot,P_Tot_Managers,P_Tot_Professionals,P_Tot_TechnicTrades_W,P_Tot_CommunPersnlSvc_W,P_Tot_ClericalAdminis_W,P_Tot_Sales_W
	,P_Tot_Mach_oper_drivers,P_Tot_Labourers,Ancestry_Aust_NZ_P,Ancestry_UK_Ire_P,Ancestry_Other_Europe_P,Ancestry_Western_Asia_P,Ancestry_Southern_Asia_P
	,Ancestry_Eastern_Asia_P,Ancestry_South_Eastern_Asia_P,Ancestry_Elsewhere_P,Born_Aust_NZ_P,Born_UK_Ire_P,Born_Other_Europe_P,Born_Western_Asia_P
	,Born_Southern_Asia_P,Born_Eastern_Asia_P,Born_South_Eastern_Asia_P,Born_Elsewhere_P,Ancestry_Aboriginal_Only_P,Religion_Non_Drinking_P,
	NULL AS Ave_Basket_Size,
	NULL AS Ave_basket_Size_Units,
	NULL AS Morning_6to10,
	NULL AS Day_11to4,
	NULL AS Night_5onwards,
	NULL AS Weekend,
	NULL AS B_Prop,
	NULL AS M_Prop,
	NULL AS P_Prop,
	NULL AS HasCafe,
	NULL AS FOS_Prop,
	NULL AS FFN_Prop,
	NULL AS FFL_Prop,
	NULL AS Emergency_Impulse,
	NULL AS Non_Food_Shop,
	NULL AS Food_Shop,
	NULL AS non_promo_prop,
	NULL AS Fresh_Foodies,
	NULL AS Modern_Families,
	NULL AS Healthy_Premium_Entertaining,
	NULL AS Cigarette_Impulse,
	NULL AS Grab_Go
	,dqs.Sales_Proportion_Budget
	,dqs.Sales_Proportion_Mainstream
	,dqs.Sales_Proportion_Premium
	,dqs.Sales_Proportion_New_Families
	,dqs.Sales_Proportion_Young_Families
	,dqs.Sales_Proportion_Older_Families
	,dqs.Sales_Proportion_Young_Singles_Couples
	,dqs.Sales_Proportion_Midage_Singles_Couples
	,dqs.Sales_Proportion_Older_Singles_Couples
	,dqs.Sales_Proportion_Retirees
	,dqs.Sales_Proportion_Ultra_Healthy
	,dqs.Sales_Proportion_Free_From
	,dqs.Sales_Proportion_High_Protein_Traditionalists
	,dqs.Sales_Proportion_Health_Newcomer
	,dqs.Sales_Proportion_Convenience_Driven
	,dqs.Sales_Proportion_Unhealthy_Snackers,
	NULL AS European,
	NULL AS Mpi,
	NULL AS asianindian,
	NULL AS Christian,
	NULL AS Hindu,
	NULL AS Muslim,
	NULL AS Affluence,
	NULL AS Holiday,
	NULL AS UniArea,
	NULL AS Prop_Student_Allowance

FROM (
SELECT * 
FROM 
	`{{ params.project_ID }}.{{ params.Optumera_Stage_DS }}.Demographic_Census_AUS` 
WHERE 
	Sales_org = '{{ params.sales_org_SUPER}}') demo 
FULL OUTER JOIN
	`{{ params.project_ID }}.{{ params.Optumera_Stage_DS }}.Demographic_Quantium_Super` dqs 
ON
	demo.Loc_No = dqs.Store_Number

UNION ALL
SELECT 
	Store_Number,
	'{{ params.sales_org_WNZ}}' AS Sales_org,
	NULL AS Tot_P_M,
	NULL AS Tot_P_F,
	NULL AS Tot_P_P,
	NULL AS Age_0_4_yr_M,
	NULL AS Age_0_4_yr_F,
	NULL AS Age_0_4_yr_P,
	NULL AS Age_5_14_yr_M,
	NULL AS Age_5_14_yr_F,
	NULL AS Age_5_14_yr_P,
	NULL AS Age_15_19_yr_M,
	NULL AS Age_15_19_yr_F,
	NULL AS Age_15_19_yr_P,
	NULL AS Age_20_24_yr_M,
	NULL AS Age_20_24_yr_F,
	NULL AS Age_20_24_yr_P,
	NULL AS Age_25_34_yr_M,
	NULL AS Age_25_34_yr_F,
	NULL AS Age_25_34_yr_P,
	NULL AS Age_35_44_yr_M,
	NULL AS Age_35_44_yr_F,
	NULL AS Age_35_44_yr_P,
	NULL AS Age_45_54_yr_M,
	NULL AS Age_45_54_yr_F,
	NULL AS Age_45_54_yr_P,
	NULL AS Age_55_64_yr_M,
	NULL AS Age_55_64_yr_F,
	NULL AS Age_55_64_yr_P,
	NULL AS Age_65_74_yr_M,
	NULL AS Age_65_74_yr_F,
	NULL AS Age_65_74_yr_P,
	NULL AS Age_75_84_yr_M,
	NULL AS Age_75_84_yr_F,
	NULL AS Age_75_84_yr_P,
	NULL AS Age_85ov_M,
	NULL AS Age_85ov_F,
	NULL AS Age_85ov_P,
	NULL AS Average_household_size,
	NULL AS Aust_Tot_Resp,
	NULL AS Aust_Abor_Tot_Resp,
	NULL AS Chinese_Tot_Resp,
	NULL AS Croatian_Tot_Resp,
	NULL AS Dutch_Tot_Resp,
	NULL AS English_Tot_Resp,
	NULL AS Filipino_Tot_Resp,
	NULL AS French_Tot_Resp,
	NULL AS German_Tot_Resp,
	NULL AS Greek_Tot_Resp,
	NULL AS Hungarian_Tot_Resp,
	NULL AS Indian_Tot_Resp,
	NULL AS Irish_Tot_Resp,
	NULL AS Italian_Tot_Resp,
	NULL AS Korean_Tot_Resp,
	NULL AS Lebanese_Tot_Resp,
	NULL AS Macedonian_Tot_Resp,
	NULL AS Maltese_Tot_Resp,
	NULL AS Maori_Tot_Resp,
	NULL AS NZ_Tot_Resp,
	NULL AS Polish_Tot_Resp,
	NULL AS Russian_Tot_Resp,
	NULL AS Scottish_Tot_Resp,
	NULL AS Serbian_Tot_Resp,
	NULL AS Sth_African_Tot_Resp,
	NULL AS Spanish_Tot_Resp,
	NULL AS Sri_Lankan_Tot_Resp,
	NULL AS Turkish_Tot_Resp,
	NULL AS Vietnamese_Tot_Resp,
	NULL AS Welsh_Tot_Resp,
	NULL AS Other_Tot_Resp,
	NULL AS Tot_P_Tot_Resp,
	NULL AS P_Afghanistan_Tot,
	NULL AS P_Australia_Tot,
	NULL AS P_Bangladesh_Tot,
	NULL AS P_Bosnia_Herzegov_Tot,
	NULL AS P_Cambodia_Tot,
	NULL AS P_Canada_Tot,
	NULL AS P_Chile_Tot,
	NULL AS P_China_Tot,
	NULL AS P_Croatia_Tot,
	NULL AS P_Egypt_Tot,
	NULL AS P_England_Tot,
	NULL AS P_Fiji_Tot,
	NULL AS P_France_Tot,
	NULL AS P_Germany_Tot,
	NULL AS P_Greece_Tot,
	NULL AS P_Hong_Kong_Tot,
	NULL AS P_India_Tot,
	NULL AS P_Indonesia_Tot,
	NULL AS P_Iran_Tot,
	NULL AS P_Iraq_Tot,
	NULL AS P_Ireland_Tot,
	NULL AS P_Italy_Tot,
	NULL AS P_Japan_Tot,
	NULL AS P_Korea_South_Tot,
	NULL AS P_Lebanon_Tot,
	NULL AS P_Malaysia_Tot,
	NULL AS P_Malta_Tot,
	NULL AS P_Mauritius_Tot,
	NULL AS P_Myanmar_Tot,
	NULL AS P_Nepal_Tot,
	NULL AS P_Netherlands_Tot,
	NULL AS P_New_Zealand_Tot,
	NULL AS P_Nthern_Ireland_Tot,
	NULL AS P_Pakistan_Tot,
	NULL AS P_PNG_Tot,
	NULL AS P_Philippines_Tot,
	NULL AS P_Poland_Tot,
	NULL AS P_Scotland_Tot,
	NULL AS P_Singapore_Tot,
	NULL AS P_South_Africa_Tot,
	NULL AS P_SE_Europe_nfd_Tot,
	NULL AS P_Sri_Lanka_Tot,
	NULL AS P_Taiwan_Tot,
	NULL AS P_Thailand_Tot,
	NULL AS P_FYROM_Tot,
	NULL AS P_Turkey_Tot,
	NULL AS P_USA_Tot,
	NULL AS P_Vietnam_Tot,
	NULL AS P_Wales_Tot,
	NULL AS P_Zimbabwe_Tot,
	NULL AS P_Elsewhere_Tot,
	NULL AS Buddhism_P,
	NULL AS Christianity_Tot_P,
	NULL AS Hinduism_P,
	NULL AS Islam_P,
	NULL AS Judaism_P,
	NULL AS Othr_Rel_Sikhism_P,
	NULL AS Other_Religions_Tot_P,
	NULL AS SB_OSB_NRA_Tot_P,
	NULL AS Pre_school_M,
	NULL AS Pre_school_F,
	NULL AS Pre_school_P,
	NULL AS Infants_Primary_Tot_P,
	NULL AS Secondary_Tot_M,
	NULL AS Secondary_Tot_F,
	NULL AS Secondary_Tot_P,
	NULL AS Tec_Furt_Educ_inst_Tot_M,
	NULL AS Tec_Furt_Educ_inst_Tot_F,
	NULL AS Tec_Furt_Educ_inst_Tot_P,
	NULL AS Uni_other_Tert_Instit_Tot_M,
	NULL AS Uni_other_Tert_Instit_Tot_F,
	NULL AS Uni_other_Tert_Instit_Tot_P,
	NULL AS Other_type_educ_instit_Tot_M,
	NULL AS Other_type_educ_instit_Tot_F,
	NULL AS Other_type_educ_instit_Tot_P,
	NULL AS Negative_Nil_income_Tot,
	NULL AS HI_1_149_Tot,
	NULL AS HI_150_299_Tot,
	NULL AS HI_300_399_Tot,
	NULL AS HI_400_499_Tot,
	NULL AS HI_500_649_Tot,
	NULL AS HI_650_799_Tot,
	NULL AS HI_800_999_Tot,
	NULL AS HI_1000_1249_Tot,
	NULL AS HI_1250_1499_Tot,
	NULL AS HI_1500_1749_Tot,
	NULL AS HI_1750_1999_Tot,
	NULL AS HI_2000_2499_Tot,
	NULL AS HI_2500_2999_Tot,
	NULL AS HI_3000_3499_Tot,
	NULL AS HI_3500_3999_Tot,
	NULL AS HI_4000_more_Tot,
	NULL AS Num_MVs_per_dweling_0_MVs,
	NULL AS Num_MVs_per_dweling_1_MVs,
	NULL AS Num_MVs_per_dweling_2_MVs,
	NULL AS Num_MVs_per_dweling_3_MVs,
	NULL AS Num_MVs_per_dweling_4mo_MVs,
	NULL AS Total_FamHhold,
	NULL AS Total_NonFamHhold,
	NULL AS OPDs_Separate_house_Persons,
	NULL AS OPDs_SD_r_t_h_th_1_sty_Psns,
	NULL AS OPDs_SD_r_t_h_th_2_m_sty_Psns,
	NULL AS OPDs_SD_r_t_h_th_Tot_Psns,
	NULL AS OPDs_F_ap_I_1or2_sty_blk_Ps,
	NULL AS OPDs_F_ap_I_3_sty_blk_Psns,
	NULL AS OPDs_F_ap_I_4_m_sty_blk_Ps,
	NULL AS OPDs_Flt_apt_Att_house_Ps,
	NULL AS OPDs_Flt_apart_Tot_Psns,
	NULL AS OPDs_Oth_dw_Cvn_Ps,
	NULL AS OPDs_Oth_dwg_cab_hboat_Ps,
	NULL AS OPDs_Ot_dwg_Im_hm_tnt_SO_Ps,
	NULL AS OPDs_Ot_dwg_Hs_f_att_sh_of_Ps,
	NULL AS OPDs_Other_dwelling_Tot_Psns,
	NULL AS OPDs_Dwlling_structur_NS_Psns,
	NULL AS OPDs_Tot_OPDs_Persons,
	NULL AS Total_PDs_Persons,
	NULL AS P_Ag_For_Fshg_Tot,
	NULL AS P_Mining_Tot,
	NULL AS P_Manufact_Tot,
	NULL AS P_El_Gas_Wt_Waste_Tot,
	NULL AS P_Constru_Tot,
	NULL AS P_WhlesaleTde_Tot,
	NULL AS P_RetTde_Tot,
	NULL AS P_Accom_food_Tot,
	NULL AS P_Trans_post_wrehsg_Tot,
	NULL AS P_Info_media_teleco_Tot,
	NULL AS P_Fin_Insur_Tot,
	NULL AS P_RtnHir_REst_Tot,
	NULL AS P_Pro_scien_tec_Tot,
	NULL AS P_Admin_supp_Tot,
	NULL AS P_Public_admin_sfty_Tot,
	NULL AS P_Educ_trng_Tot,
	NULL AS P_HlthCare_SocAs_Tot,
	NULL AS P_Art_recn_Tot,
	NULL AS P_Oth_scs_Tot,
	NULL AS P_ID_NS_Tot,
	NULL AS P_Tot_Managers,
	NULL AS P_Tot_Professionals,
	NULL AS P_Tot_TechnicTrades_W,
	NULL AS P_Tot_CommunPersnlSvc_W,
	NULL AS P_Tot_ClericalAdminis_W,
	NULL AS P_Tot_Sales_W,
	NULL AS P_Tot_Mach_oper_drivers,
	NULL AS P_Tot_Labourers,
	NULL AS Ancestry_Aust_NZ_P,
	NULL AS Ancestry_UK_Ire_P,
	NULL AS Ancestry_Other_Europe_P,
	NULL AS Ancestry_Western_Asia_P,
	NULL AS Ancestry_Southern_Asia_P,
	NULL AS Ancestry_Eastern_Asia_P,
	NULL AS Ancestry_South_Eastern_Asia_P,
	NULL AS Ancestry_Elsewhere_P,
	NULL AS Born_Aust_NZ_P,
	NULL AS Born_UK_Ire_P,
	NULL AS Born_Other_Europe_P,
	NULL AS Born_Western_Asia_P,
	NULL AS Born_Southern_Asia_P,
	NULL AS Born_Eastern_Asia_P,
	NULL AS Born_South_Eastern_Asia_P,
	NULL AS Born_Elsewhere_P,
	NULL AS Ancestry_Aboriginal_Only_P,
	NULL AS Religion_Non_Drinking_P,
	NULL AS Ave_Basket_Size,
	NULL AS Ave_basket_Size_Units,
	NULL AS Morning_6to10,
	NULL AS Day_11to4,
	NULL AS Night_5onwards,
	NULL AS Weekend,
	NULL AS B_Prop,
	NULL AS M_Prop,
	NULL AS P_Prop,
	NULL AS HasCafe,
	NULL AS FOS_Prop,
	NULL AS FFN_Prop,
	NULL AS FFL_Prop,
	NULL AS Emergency_Impulse,
	NULL AS Non_Food_Shop,
	NULL AS Food_Shop,
	NULL AS non_promo_prop,
	NULL AS Fresh_Foodies,
	NULL AS Modern_Families,
	NULL AS Healthy_Premium_Entertaining,
	NULL AS Cigarette_Impulse,
	NULL AS Grab_Go,
	NULL AS Sales_Proportion_Budget,
	NULL AS Sales_Proportion_Mainstream,
	NULL AS Sales_Proportion_Premium,
	NULL AS Sales_Proportion_New_Families,
	NULL AS Sales_Proportion_Young_Families,
	NULL AS Sales_Proportion_Older_Families,
	NULL AS Sales_Proportion_Young_Singles_Couples,
	NULL AS Sales_Proportion_Midage_Singles_Couples,
	NULL AS Sales_Proportion_Older_Singles_Couples,
	NULL AS Sales_Proportion_Retirees,
	NULL AS Sales_Proportion_Ultra_Healthy,
	NULL AS Sales_Proportion_Free_From,
	NULL AS Sales_Proportion_High_Protein_Traditionalists,
	NULL AS Sales_Proportion_Health_Newcomer,
	NULL AS Sales_Proportion_Convenience_Driven,
	NULL AS Sales_Proportion_Unhealthy_Snackers,
	European,
	Mpi,
	Asianindian,
	Christian,
	Hindu,
	Muslim,
	Affluence,
	Holiday,
	UniArea,
	Prop_Student_Allowance
FROM
	`{{ params.project_ID }}.{{ params.Optumera_Stage_DS }}.Demographic_Census_WNZ`
