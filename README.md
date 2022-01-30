ALL the Queries

CREATE
	OR replace VIEW LOCATION_LEVEL AS (
	WITH location_wise_info AS (
		SELECT location
			,total_cases
			,total_Deaths
			,total_recovered
		FROM data
		) SELECT location
	,sum(total_cases) AS Cases
	,sum(total_Deaths) AS Deaths
	,sum(total_recovered) AS Recovered FROM location_wise_info GROUP BY location ORDER BY location ASC
	);


  CREATE
	OR replace VIEW monthly_wise AS (
	WITH monthly_cases AS (
		SELECT location
			,DATE (DATE) AS DATE
			,new_cases
			,new_deaths
			,new_recovered
		FROM data
		) SELECT location
	,year(date_trunc('MONTH', DATE)) AS Year
	,monthname(date_trunc('MONTH', DATE)) AS Month
	,sum(new_cases) AS Total_cases
	,sum(new_deaths) AS Total_deaths
	,sum(new_recovered) AS Total_recovered FROM monthly_cases GROUP BY date_trunc('YEAR', DATE)
	,location
	,date_trunc('MONTH', DATE)
	);

  
CREATE VIEW PROVINCE_WISE
AS
SELECT LATITUDE
	,LONGITUDE
	,PROVINCE
	,SUM(CASE 
			WHEN UPPER(LOCATION_LEVEL) = 'PROVINCE'
				THEN TOTAL_ACTIVE_CASES
			ELSE 0
			END) TOTAL_ACTIVE_CASES
	,SUM(CASE 
			WHEN UPPER(LOCATION_LEVEL) = 'PROVINCE'
				THEN TOTAL_RECOVERED
			ELSE 0
			END) TOTAL_RECOVERED
	,SUM(CASE 
			WHEN UPPER(LOCATION_LEVEL) = 'PROVINCE'
				THEN TOTAL_DEATHS
			ELSE 0
			END) TOTAL_DEATHS
	,SUM(CASE 
			WHEN UPPER(LOCATION_LEVEL) = 'PROVINCE'
				THEN TOTAL_CASES
			ELSE 0
			END) TOTAL_CASES
	,(
		SUM(CASE 
				WHEN UPPER(LOCATION_LEVEL) = 'PROVINCE'
					THEN TOTAL_DEATHS
				ELSE 0
				END)
		) / (
		SUM(CASE 
				WHEN UPPER(LOCATION_LEVEL) = 'PROVINCE'
					THEN TOTAL_CASES
				ELSE 0
				END)
		) AS MORTALITY_RATE
FROM DATA
WHERE PROVINCE IS NOT NULL
GROUP BY LATITUDE
	,LONGITUDE
	,PROVINCE;

  CREATE
	OR replace VIEW year_wise AS (
	SELECT *
	,round(overall_recovered / overall_cases, 2) * 100 AS rate_of_recovered FROM (
	SELECT right(DATE, 4) AS year
		,location
		,sum(DISTINCT total_districts)
		,sum(DISTINCT total_cities)
		,sum(new_active_cases) AS total_new_active_cases
		,sum(new_cases) AS total_new_cases
		,sum(total_cases) AS overall_cases
		,sum(total_deaths) AS total_deaths
		,sum(new_deaths) AS total_new_deaths
		,sum(new_recovered) AS total_new_cases_recovered
		,sum(total_recovered) AS overall_recovered
	FROM data
	GROUP BY 1
		,2
	) ORDER BY 1 DESC
	,10 DESC
	);

  create or replace view MostSpikedRatedperLocationperdate
as
select * from (SELECT LOCATION
		,DATE
		,NEW_DEATHS
		,RANK() OVER (
			PARTITION BY LOCATION_ISO_CODE ORDER BY NEW_DEATHS DESC
			) NEW_DEATHS_rank
	FROM data) where  NEW_DEATHS_rank=1
