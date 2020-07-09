/*
Join the stage store table with banner table and generate the STORE_ID and build the target table.
*/

DELETE FROM `{{ params.project_ID }}.{{ params.Optumera_Target_DS }}.Store` WHERE TRUE;
INSERT
	`{{ params.project_ID }}.{{ params.Optumera_Target_DS }}.Store` ( STORE_ID,
	STORE_NUMBER,
	STORE_NAME,
	CITY,
	STATE,
	COUNTRY,
	ADDRESS1,
	DISTRICT,
	REGION,
	ZONE,
	STORE_TYPE,
	ZIP_CODE,
	LATITUDE,
	LONGITUDE,
	STORE_OPEN_DATE,
	STORE_CLOSE_DATE,
	STORE_SIZE_LINEAR,
	STORE_SIZE_SQUARE,
	PHONE_NUMBER,
	ACTIVE_F,
	DELETE_F,
	CREATED_USER,
	CREATED_TS,
	UPDATED_USER,
	UPDATED_TS,
	TEXT_1,
	BANNER_ID )
WITH sto as 
(
SELECT
	Store_Number,
	Store_Name,
	City,
	State,
	Country,
	Address1,
	District,
	Region,
	Zone,
	Store_Type,
	Zip_Code,
	Latitude,
	Longitude,
	Store_Open_Date,
	Store_Close_Date,
	Store_Size_Linear,
	Store_Size_Square,
	Phone_Number,
	'Y' AS ACTIVE_F,
	'N' AS DELETE_F,
	'TOPMSTRSTRFL' AS CREATED_USER,
	CURRENT_TIMESTAMP() AS CREATED_TS,
	'TOPMSTRSTRFL' AS UPDATED_USER,
	CURRENT_TIMESTAMP() AS UPDATED_TS,
	Sales_Org,
	b.banner_id
FROM
	`{{ params.project_ID }}.{{ params.Optumera_Stage_DS }}.Stage_Store` a
INNER JOIN
	`{{ params.project_ID }}.{{ params.Optumera_Target_DS }}.Banner` b
ON
	b.RETAILER_BANNER_ID = a.Sales_Org
ORDER BY 
	Sales_Org,Store_Number
)
SELECT
	ROW_NUMBER() OVER() STORE_ID,
	a.Store_Number,
	a.Store_Name,
	a.City,
	a.State,
	a.Country,
	a.Address1,
	a.District,
	a.Region,
	a.Zone,
	a.Store_Type,
	a.Zip_Code,
	a.Latitude,
	a.Longitude,
	a.Store_Open_Date,
	a.Store_Close_Date,
	a.Store_Size_Linear,
	a.Store_Size_Square,
	a.Phone_Number,
	a.ACTIVE_F,
	a.DELETE_F,
	a.CREATED_USER,
	a.CREATED_TS,
	a.UPDATED_USER,
	a.UPDATED_TS,
	a.Sales_Org,
	a.banner_id
FROM
	sto a