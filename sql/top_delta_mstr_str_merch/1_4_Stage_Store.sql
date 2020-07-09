/*
Exclude the stores present in the STORE_EXCLUSION table from Combined_Stores and build the stage store table.
*/

CREATE OR REPLACE TABLE
`{{ params.project_ID }}.{{ params.Optumera_Stage_DS }}.Stage_Store` ( Store_Number STRING,
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
Sales_Org STRING  ) AS
With master as (
SELECT
Site,
SiteDescription,
City,
State,
SiteCountry,
StreetName,
SalesDistrict,
RegionShortDescription,
ZoneShortDescription,
SiteType,
PostalCode,
Latitude,
Longitude,
SAFE.PARSE_TIMESTAMP("%Y%m%d", DateSiteOpened) AS Store_Open_Date,
SAFE.PARSE_TIMESTAMP("%Y%m%d", DateSiteClosed) AS Store_Close_Date,
0 AS Store_Size_Linear,
0 AS Store_Size_Square,
SitePhoneNumber,
SalesOrganisation
FROM
`{{ params.GCP_WOW_ENV }}.adp_dm_masterdata_view.dim_site_v`

WHERE
SalesOrganisation IN ('{{params.sales_org_SUPER}}','{{params.sales_org_BWS}}','{{params.sales_org_DAN}}','{{params.sales_org_METRO}}','{{params.sales_org_WNZ}}')
AND DistributionChannel = '{{params.distributionChannel}}'
AND SiteType='{{params.SiteType}}'
AND SiteTradingStatus = '{{params.SiteTradingStatus}}'
)
SELECT
com.Site,
com.SiteDescription,
com.City,
com.State,
com.SiteCountry,
com.StreetName,
com.SalesDistrict,
com.RegionShortDescription,
com.ZoneShortDescription,
com.SiteType,
com.PostalCode,
com.Latitude,
com.Longitude,
com.Store_Open_Date,
com.Store_Close_Date,
com.Store_Size_Linear,
com.Store_Size_Square,
com.SitePhoneNumber,
com.SalesOrganisation
FROM
master com
LEFT OUTER JOIN
`{{ params.project_ID }}.{{ params.Optumera_Stage_DS }}.STORE_EXCLUSION` exc
ON
com.Site = exc.STORE_NO and
com.SalesOrganisation = CAST(exc.Sales_Org  AS STRING)
WHERE
exc.STORE_NO is NULL

