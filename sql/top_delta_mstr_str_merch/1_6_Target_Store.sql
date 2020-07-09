/* 
Insert,Soft Delete and Update is done based on the Flags(I,D,U and FU) in Store table
*/
UPDATE 
	`{{ params.project_ID }}.{{ params.Optumera_Target_DS }}.Store` T
SET 
	T.STORE_NAME = S.Curr_Store_Name
	,T.CITY = S.Curr_City
	,T.STATE = S.Curr_State
	,T.COUNTRY = S.Curr_Country
	,T.ADDRESS1 = S.Curr_Address1
	,T.DISTRICT = S.Curr_District
	,T.REGION = S.Curr_Region
	,T.ZONE = S.Curr_Zone
	,T.STORE_TYPE = S.Curr_Store_Type
	,T.ZIP_CODE = S.Curr_Zip_Code
	,T.LATITUDE = S.Curr_Latitude
	,T.LONGITUDE = S.Curr_Longitude
	,T.STORE_OPEN_DATE = S.Curr_Store_Open_Date
	,T.STORE_CLOSE_DATE = S.Curr_Store_Close_Date
	,T.STORE_SIZE_LINEAR = S.Curr_Store_Size_Linear
	,T.STORE_SIZE_SQUARE = S.Curr_Store_Size_Square
	,T.PHONE_NUMBER = S.Curr_Phone_Number
	,T.UPDATED_USER = 'TOPMSTRSTRDL'
	,T.UPDATED_TS = CURRENT_TIMESTAMP()
 
FROM 
	`{{ params.project_ID }}.{{ params.Optumera_Stage_DS }}.Delta_Store` S
WHERE 
	S.Flag = 'U'
AND S.Curr_Sales_org = T.Text_1
AND S.Curr_Store_number = T.Store_Number;

/* UPDATE - Update DELETE FROM Flag(FU)*/
UPDATE 
	`{{ params.project_ID }}.{{ params.Optumera_Target_DS }}.Store` T
SET 
	T.STORE_NAME = S.Curr_Store_Name
	,T.CITY = S.Curr_City
	,T.STATE = S.Curr_State
	,T.COUNTRY = S.Curr_Country
	,T.ADDRESS1 = S.Curr_Address1
	,T.DISTRICT = S.Curr_District
	,T.REGION = S.Curr_Region
	,T.ZONE = S.Curr_Zone
	,T.STORE_TYPE = S.Curr_Store_Type
	,T.ZIP_CODE = S.Curr_Zip_Code
	,T.LATITUDE = S.Curr_Latitude
	,T.LONGITUDE = S.Curr_Longitude
	,T.STORE_OPEN_DATE = S.Curr_Store_Open_Date
	,T.STORE_CLOSE_DATE = S.Curr_Store_Close_Date
	,T.STORE_SIZE_LINEAR = S.Curr_Store_Size_Linear
	,T.STORE_SIZE_SQUARE = S.Curr_Store_Size_Square
	,T.PHONE_NUMBER = S.Curr_Phone_Number
	,T.ACTIVE_F = 'Y'
	,T.DELETE_F = 'N'
	,T.UPDATED_USER = 'TOPMSTRSTRDL'
	,T.UPDATED_TS = CURRENT_TIMESTAMP()

FROM 
	`{{ params.project_ID }}.{{ params.Optumera_Stage_DS }}.Delta_Store` S
WHERE 
	S.Flag = 'FU'
AND S.Curr_Sales_org = T.Text_1
AND S.Curr_Store_number = T.Store_Number;


/* UPDATE (SOFT DELETE) */
UPDATE 
	`{{ params.project_ID }}.{{ params.Optumera_Target_DS }}.Store` T
SET 
	T.ACTIVE_F = 'N'
	,T.DELETE_F = 'Y'
	,T.UPDATED_USER = 'TOPMSTRSTRDL'
	,T.UPDATED_TS = CURRENT_TIMESTAMP()
FROM 
	`{{ params.project_ID }}.{{ params.Optumera_Stage_DS }}.Delta_Store` S
WHERE 
	S.Flag = 'D'
AND S.Prev_Sales_org = T.Text_1
AND S.Prev_Store_number = T.Store_Number;

/* INSERT */
INSERT
	`{{ params.project_ID }}.{{ params.Optumera_Target_DS }}.Store` (STORE_ID,
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
SELECT
(SELECT
	MAX(STORE_ID)
FROM
	`{{ params.project_ID }}.{{ params.Optumera_Target_DS }}.Store`)+ROW_NUMBER() OVER() AS STORE_ID,
	a.Curr_Store_Number,
	a.Curr_Store_Name,
	a.Curr_City,
	a.Curr_State,
	a.Curr_Country,
	a.Curr_Address1,
	a.Curr_District,
	a.Curr_Region,
	a.Curr_Zone,
	a.Curr_Store_Type,
	a.Curr_Zip_Code,
	a.Curr_Latitude,
	a.Curr_Longitude,
	a.Curr_Store_Open_Date,
	a.Curr_Store_Close_Date,
	a.Curr_Store_Size_Linear,
	a.Curr_Store_Size_Square,
	a.Curr_Phone_Number,
	'Y' AS ACTIVE_F,
	'N' AS DELETE_F,
	'TOPMSTRSTRDL' AS CREATED_USER,
	CURRENT_TIMESTAMP() AS CREATED_TS,
	'TOPMSTRSTRDL' AS UPDATED_USER,
	CURRENT_TIMESTAMP() AS UPDATED_TS,
	a.Curr_Sales_Org,
	b.banner_id
FROM
	`{{ params.project_ID }}.{{ params.Optumera_Stage_DS }}.Delta_Store` a
INNER JOIN
	`{{ params.project_ID }}.{{ params.Optumera_Target_DS }}.Banner` b
ON
	b.RETAILER_BANNER_ID = a.Curr_Sales_Org AND 
	a.flag = 'I'
