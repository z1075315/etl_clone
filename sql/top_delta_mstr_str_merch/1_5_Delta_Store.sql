/*
Compare the Stage_Store_Prev and Stage_Store table and loading into Delta_Store Table.
Picking the Insert(I),Delete(D) and Update(U) records from the Delta_Store`Table.
Picking the Flag Update(FU) records from the Delta_Store`Table by joining with the target Store Table on Store number and Sales_org 
*/
CREATE OR REPLACE TABLE `{{ params.project_ID }}.{{ params.Optumera_Stage_DS }}.Delta_Store`
(
Curr_Store_Number   STRING,   
Curr_Sales_Org   STRING,   
Curr_Store_Name   STRING,   
Curr_City   STRING,   
Curr_State   STRING,   
Curr_Country   STRING,   
Curr_Address1   STRING,   
Curr_District   STRING,   
Curr_Region   STRING,   
Curr_Zone   STRING,   
Curr_Store_Type   STRING,   
Curr_Zip_Code   STRING,   
Curr_Latitude   STRING,   
Curr_Longitude   STRING,   
Curr_Store_Open_Date   TIMESTAMP,   
Curr_Store_Close_Date   TIMESTAMP,   
Curr_Store_Size_Linear   FLOAT64,   
Curr_Store_Size_Square   FLOAT64,   
Curr_Phone_Number   STRING,   
Prev_Store_Number   STRING,   
Prev_Sales_Org   STRING,   
Prev_Store_Name   STRING,   
Prev_City   STRING,   
Prev_State   STRING,   
Prev_Country   STRING,   
Prev_Address1   STRING,   
Prev_District   STRING,   
Prev_Region   STRING,   
Prev_Zone   STRING,   
Prev_Store_Type   STRING,   
Prev_Zip_Code   STRING,   
Prev_Latitude   STRING,   
Prev_Longitude   STRING,   
Prev_Store_Open_Date   TIMESTAMP,   
Prev_Store_Close_Date   TIMESTAMP,   
Prev_Store_Size_Linear   FLOAT64,   
Prev_Store_Size_Square   FLOAT64,   
Prev_Phone_Number   STRING,   
flag   STRING
) AS
SELECT 
	curr.Store_Number AS Curr_Store_Number,
	curr.Sales_org AS Curr_Sales_Org ,
	curr.Store_Name  AS Curr_Store_Name ,
	curr.City  AS Curr_City ,
	curr.State  AS Curr_State ,
	curr.Country  AS Curr_Country ,
	curr.Address1  AS Curr_Address1 ,
	curr.District  AS Curr_District ,
	curr.Region  AS Curr_Region ,
	curr.Zone  AS Curr_Zone ,
	curr.Store_Type  AS Curr_Store_Type ,
	curr.Zip_Code  AS Curr_Zip_Code ,
	curr.Latitude  AS Curr_Latitude ,
	curr.Longitude  AS Curr_Longitude ,
	curr.Store_Open_Date  AS Curr_Store_Open_Date ,
	curr.Store_Close_Date  AS Curr_Store_Close_Date ,
	curr.Store_Size_Linear  AS Curr_Store_Size_Linear ,
	curr.Store_Size_Square  AS Curr_Store_Size_Square ,
	curr.Phone_Number  AS Curr_Phone_Number ,
	Prev.Store_Number AS Prev_Store_Number,
	Prev.Sales_org AS Prev_Sales_Org ,
	prev.Store_Name  AS Prev_Store_Name ,
	prev.City  AS Prev_City ,
	prev.State  AS Prev_State ,
	prev.Country  AS Prev_Country ,
	prev.Address1  AS Prev_Address1 ,
	prev.District  AS Prev_District ,
	prev.Region  AS Prev_Region ,
	prev.Zone  AS Prev_Zone ,
	prev.Store_Type  AS Prev_Store_Type ,
	prev.Zip_Code  AS Prev_Zip_Code ,
	prev.Latitude  AS Prev_Latitude ,
	prev.Longitude  AS Prev_Longitude ,
	prev.Store_Open_Date  AS Prev_Store_Open_Date ,
	prev.Store_Close_Date  AS Prev_Store_Close_Date ,
	prev.Store_Size_Linear  AS Prev_Store_Size_Linear ,
	prev.Store_Size_Square  AS Prev_Store_Size_Square ,
	prev.Phone_Number  AS Prev_Phone_Number ,
	case when curr.store_number is null then 'D'
	when prev.store_number is null then 'I'
	when  curr.store_number = prev.store_number AND (
	curr.Store_Name <> prev.Store_Name OR 
	curr.City <> prev.City OR 
	curr.State <> prev.State OR 
	curr.Country <> prev.Country OR 
	curr.Address1 <> prev.Address1 OR 
	curr.District <> prev.District OR 
	curr.Region <> prev.Region OR 
	curr.Zone <> prev.Zone OR 
	curr.Store_Type <> prev.Store_Type OR 
	curr.Zip_Code <> prev.Zip_Code OR 
	curr.Latitude <> prev.Latitude OR 
	curr.Longitude <> prev.Longitude OR 
	curr.Store_Open_Date <> prev.Store_Open_Date OR 
	curr.Store_Close_Date <> prev.Store_Close_Date OR 
	curr.Store_Size_Linear <> prev.Store_Size_Linear OR 
	curr.Store_Size_Square <> prev.Store_Size_Square OR 
	curr.Phone_Number <> prev.Phone_Number ) THEN 'U'
	ELSE 'E'
	END AS flag
FROM 
	`{{ params.project_ID }}.{{ params.Optumera_Stage_DS }}.Stage_Store` Curr
FULL OUTER JOIN 
	`{{ params.project_ID }}.{{ params.Optumera_Stage_DS }}.Stage_Store_Prev` Prev
ON
	curr.store_number = Prev.Store_number
AND curr.sales_org = prev.sales_org;
UPDATE 
	`{{ params.project_ID }}.{{ params.Optumera_Stage_DS }}.Delta_Store` T
SET
	T.Flag = 'FU'
WHERE 
	CONCAT (T.Curr_Store_Number,'|||',T.curr_sales_org) IN
(SELECT 
	CONCAT(Sto.STORE_NUMBER,'|||',Sto.TEXT_1) AS Sal_org_Store  
FROM 
(SELECT * 
FROM 
	`{{ params.project_ID }}.{{ params.Optumera_Stage_DS }}.Delta_Store` 
WHERE 
	Flag = 'I') art 
INNER JOIN 
	`{{ params.project_ID }}.{{ params.Optumera_Target_DS }}.Store` Sto 
ON 
	art.Curr_Store_Number = Sto.STORE_NUMBER AND 
	art.curr_sales_org = Sto.TEXT_1);