/*
Truncate the Stage_Store_Prev table and Copy the Stage_Store into Stage_Store_Prev Table
*/
CREATE OR REPLACE TABLE
	`{{ params.project_ID }}.{{ params.Optumera_Stage_DS }}.Stage_Store_Prev` ( Store_Number STRING,
Store_Name STRING,
City STRING,
State STRING,
Country STRING,
Address1 STRING,
District STRING,
Region STRING,
Zone STRING,
Store_Type STRING,
Zip_Code STRING,
Latitude STRING,
Longitude STRING,
Store_Open_Date TIMESTAMP,
Store_Close_Date TIMESTAMP,
Store_Size_Linear FLOAT64,
Store_Size_Square FLOAT64,
Phone_Number STRING,
Sales_Org STRING   ) AS
    
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
    Sales_Org 
FROM
  `{{ params.project_ID }}.{{ params.Optumera_Stage_DS }}.Stage_Store`
