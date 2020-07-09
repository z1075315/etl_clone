/*
build Banner table from Stage_Banner
*/
DELETE FROM `{{ params.project_ID }}.{{ params.Optumera_Target_DS }}.Banner` WHERE TRUE;

INSERT 
	`{{ params.project_ID }}.{{ params.Optumera_Target_DS }}.Banner`(
	BANNER_ID, 
	BANNER_NAME, 
	RETAILER_BANNER_ID, 
	CREATED_USER, 
	CREATED_TS, 
	UPDATED_USER, 
	UPDATED_TS )
(SELECT 
	ROW_NUMBER() OVER()  BANNER_ID,
	SalesOrganisationDescription,
	SalesOrganisation,
	"TOPOTBNR",
	CURRENT_TIMESTAMP(),
	"TOPOTBNR",
	CURRENT_TIMESTAMP() 
FROM
(SELECT 
	ROW_NUMBER() OVER()  BANNER_ID,
	SalesOrganisationDescription,
	SalesOrganisation 
FROM 
	`{{ params.project_ID }}.{{ params.Optumera_Stage_DS }}.Stage_Banner`
ORDER BY 
	SalesOrganisation))