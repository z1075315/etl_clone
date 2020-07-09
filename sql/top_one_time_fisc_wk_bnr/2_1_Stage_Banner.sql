/*
build Stage_Banner from dim_site_v with distinct sales org and description for the parameterized sales org.
*/
CREATE OR REPLACE TABLE
	`{{ params.project_ID }}.{{ params.Optumera_Stage_DS }}.Stage_Banner` (
	SalesOrganisation STRING, 
	SalesOrganisationDescription STRING
	) AS
(SELECT DISTINCT 
	SalesOrganisation, 
	SalesOrganisationDescription 
FROM 
	`{{ params.GCP_WOW_ENV}}.adp_dm_masterdata_view.dim_site_v`
WHERE 
salesorganisation IN 
	('{{ params.sales_org_SUPER}}','{{ params.sales_org_BWS }}','{{ params.sales_org_DAN}}','{{ params.sales_org_METRO}}','{{ params.sales_org_WNZ}}') 
ORDER BY 
	SalesOrganisation )