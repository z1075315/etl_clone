/*
Picking only the newly Updated records based on Delta run date and build the Delta_Store Table for loading into the bucket. 
*/
DELETE FROM `{{ params.project_ID }}.{{ params.Optumera_Target_DS }}.Delta_Store` WHERE TRUE;
INSERT
  `{{ params.project_ID }}.{{ params.Optumera_Target_DS }}.Delta_Store` ( STORE_ID,
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
	STORE_ID,
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
    BANNER_ID 
FROM
  `{{ params.project_ID }}.{{ params.Optumera_Target_DS }}.Store`

WHERE
	UPDATED_TS > TIMESTAMP(CURRENT_DATE)

