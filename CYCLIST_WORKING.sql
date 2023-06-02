---------------------------------------------
-- Data Preparation
---------------------------------------------

-- Import data through table data import wizard

-- Checking all the tables name

SELECT TABLE_NAME
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_SCHEMA = 'cyclistic';

-- Checking if all the data imported proper or not
SELECT COUNT(*)
FROM cyclistic.dec;
SELECT COUNT(*)
FROM cyclistic.jan;
SELECT COUNT(*)
FROM cyclistic.feb;
SELECT COUNT(*)
FROM cyclistic.mar;
SELECT COUNT(*)
FROM cyclistic.apr;
SELECT COUNT(*)
FROM cyclistic.may;
SELECT COUNT(*)
FROM cyclistic.jun;
SELECT COUNT(*)
FROM cyclistic.jul;
SELECT COUNT(*)
FROM cyclistic.aug;
SELECT COUNT(*)
FROM cyclistic.sep;
SELECT COUNT(*)
FROM cyclistic.oct;
SELECT COUNT(*)
FROM cyclistic.nov;

-- Checking if all the tables have the same number of columns & data type
SELECT COLUMN_NAME, DATA_TYPE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'dec';
SELECT COLUMN_NAME, DATA_TYPE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'jan';
SELECT COLUMN_NAME, DATA_TYPE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'feb';
SELECT COLUMN_NAME, DATA_TYPE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'mar';
SELECT COLUMN_NAME, DATA_TYPE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'apr';
SELECT COLUMN_NAME, DATA_TYPE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'may';
SELECT COLUMN_NAME, DATA_TYPE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'jun';
SELECT COLUMN_NAME, DATA_TYPE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'jul';
SELECT COLUMN_NAME, DATA_TYPE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'aug';
SELECT COLUMN_NAME, DATA_TYPE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'sep';
SELECT COLUMN_NAME, DATA_TYPE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'oct';
SELECT COLUMN_NAME, DATA_TYPE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'nov';

-- Checking if all the tables have the same number of columns
SELECT COUNT(*)
FROM information_schema.columns
WHERE table_name = 'dec';
SELECT COUNT(*)
FROM information_schema.columns
WHERE table_name = 'jan';
SELECT COUNT(*)
FROM information_schema.columns
WHERE table_name = 'feb';
SELECT COUNT(*)
FROM information_schema.columns
WHERE table_name = 'mar';
SELECT COUNT(*)
FROM information_schema.columns
WHERE table_name = 'apr';
SELECT COUNT(*)
FROM information_schema.columns
WHERE table_name = 'may';
SELECT COUNT(*)
FROM information_schema.columns
WHERE table_name = 'jun';
SELECT COUNT(*)
FROM information_schema.columns
WHERE table_name = 'jul';
SELECT COUNT(*)
FROM information_schema.columns
WHERE table_name = 'aug';
SELECT COUNT(*)
FROM information_schema.columns
WHERE table_name = 'sep';
SELECT COUNT(*)
FROM information_schema.columns
WHERE table_name = 'oct';
SELECT COUNT(*)
FROM information_schema.columns
WHERE table_name = 'nov';

-- Union of all the 12 tables into a single data table
CREATE TABLE CYCLIST_WORKING AS 
(   SELECT *
    FROM cyclistic.dec
    UNION
    SELECT *
    FROM jan
    UNION
    SELECT *
    FROM feb
    UNION
    SELECT *
    FROM mar
    UNION
    SELECT *
    FROM apr
    UNION
    SELECT *
    FROM may
    UNION
    SELECT *
    FROM jun
    UNION
    SELECT *
    FROM jul
    UNION
    SELECT *
    FROM aug
    UNION
    SELECT *
    FROM sep
    UNION
    SELECT *
    FROM oct
    UNION
    SELECT *
    FROM nov
);
-- 5733451 row(s) affected Records: 5733451  Duplicates: 0  Warnings: 0

---------------------------------------------
-- Data Quality Check
---------------------------------------------
-- In order to get accurate analysis, validate and make sure the dataset does not include any bias, null data, incorrect data, and duplicates.

-- Checking if table exists
SELECT table_name
FROM information_schema.tables 
WHERE table_schema = 'cyclistic' 
  AND table_name = 'CYCLIST_WORKING';

SELECT COUNT(*)
FROM CYCLIST_TRIP_DATA;

SELECT COLUMN_NAME, DATA_TYPE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'CYCLIST_WORKING';

-- View table
SELECT *
FROM CYCLIST_WORKING;

-- Check if ride ids (which are supposed to be unique) having count >1
SELECT ride_id, COUNT(1)
FROM CYCLIST_WORKING
GROUP BY ride_id
HAVING COUNT(1) > 1;

-- checking column has a NULL value
SELECT *
FROM CYCLIST_WORKING
WHERE started_at IS NULL OR ended_at IS NULL;
-- 0 row(s) returned

-- Checking for rows where column value is absent
-- We want to investigate if the blank fields are either due to empty strings, or null , or whitespaces

-- statement will update the column and set any rows with empty string values in the column to NULL.
UPDATE CYCLIST_WORKING
SET
    ride_id = NULL,
    rideable_type = NULL,
    start_station_name = NULL,
    start_station_id = NULL,
    end_station_name = NULL,
    end_station_id = NULL,
    start_lat = NULL,
    start_lng = NULL,
    end_lat = NULL,
    end_lng = NULL,
    member_casual = NULL
WHERE
    ride_id = '' OR
    rideable_type = '' OR
    start_station_name = '' OR
    start_station_id = '' OR
    end_station_name = '' OR
    end_station_id = '' OR
    start_lat = '' OR
    start_lng = '' OR
    end_lat = '' OR
    end_lng = '' OR
    member_casual = '';
-- 1323123 row(s) affected Rows matched: 1323123  Changed: 1323123  Warnings: 0

---------------------------------------------
-- Data Cleaning
---------------------------------------------
-- Delete all rows where any field is null
DELETE
FROM CYCLIST_WORKING
WHERE RIDE_ID IS NULL
OR RIDEABLE_TYPE IS NULL
OR STARTED_AT IS NULL
OR ENDED_AT IS NULL
OR START_STATION_NAME IS NULL
OR START_STATION_ID IS NULL
OR END_STATION_NAME IS NULL
OR END_STATION_ID IS NULL
OR START_LAT IS NULL
OR START_LNG IS NULL
OR END_LAT IS NULL
OR END_LNG IS NULL
OR MEMBER_CASUAL IS NULL;
-- Below query deletes 1323123 row(s) affected rows

-- identify and exclude data with anomalies where the start time is later or the same as the end time
Select *
FROM CYCLIST_WORKING
WHERE STARTED_AT >= ENDED_AT;

DELETE
FROM CYCLIST_WORKING
WHERE STARTED_AT >= ENDED_AT;
-- 300 rows deleted

-- Checking if ride ids still have count >1
SELECT ride_id, COUNT(1)
FROM CYCLIST_WORKING
GROUP BY ride_id
HAVING COUNT(1) > 1;

-- Check again for any nulls
SELECT COUNT(*) 
FROM CYCLIST_WORKING
WHERE START_STATION_ID IS NULL OR START_STATION_NAME IS NULL;

SELECT COUNT(*)
FROM cyclistic.CYCLIST_WORKING;

-- Save the changes made within a transaction
COMMIT;

--------------------
-- Data Exploration
--------------------
-- Adding a new column to calculate the ride length from TIMESTAMPDIFF
ALTER TABLE CYCLIST_WORKING
ADD COLUMN ride_length INT;

UPDATE CYCLIST_WORKING
SET ride_length = TIMESTAMPDIFF(MINUTE, started_at, ended_at);

-- Add new columns for day_of_week month and year
ALTER TABLE CYCLIST_WORKING
ADD COLUMN day_of_week VARCHAR(50),
ADD COLUMN month_m VARCHAR(50),
ADD COLUMN year_y VARCHAR(50);

-- Update the new columns with extracted day_of_week month and year values
UPDATE CYCLIST_WORKING
SET day_of_week = DATE_FORMAT(started_at, '%W'),
    month_m = DATE_FORMAT(started_at, '%M'),
    year_y = YEAR(started_at);

-- Add new columns for month 
ALTER TABLE CYCLIST_WORKING
ADD month_int INT;

UPDATE CYCLIST_WORKING
SET month_int = MONTH(started_at);

-- Cast the datetime column to date
ALTER TABLE CYCLIST_WORKING
ADD `date_yyyy_mm_dd` DATE;

UPDATE CYCLIST_WORKING
SET `date_yyyy_mm_dd` = DATE(`started_at`);

commit;

---------------------------------------------
-- Tables for Visualization
---------------------------------------------

-- Trip Frequency: Analyze the frequency of bike trips taken by annual members and casual riders. Compare the average number of trips per month or per year for each group.
SELECT
    member_casual,
    YEAR(started_at) AS year,
    MONTH(started_at) AS month,
    AVG(trip_count) AS avg_trips
FROM
    (
        SELECT
            member_casual,
            COUNT(*) AS trip_count,
            started_at
        FROM
            cyclist_working
        GROUP BY
            member_casual,
            YEAR(started_at),
            MONTH(started_at),
            started_at
    ) AS subquery
GROUP BY
    member_casual,
    YEAR(started_at),
    MONTH(started_at)
ORDER BY
    member_casual,
    YEAR(started_at),
    MONTH(started_at);

-- Trip Duration: Determine the average duration of bike trips for annual members and casual riders. Compare the average trip length to identify any significant differences in how long each group tends to ride.
SELECT
    member_casual,
    AVG(ride_length) AS avg_trip_length
FROM
    cyclist_working
GROUP BY
    member_casual;
    
-- Popular Stations: Identify the most frequently used stations by annual members and casual riders. This analysis can provide insights into their preferred starting and ending points, which may indicate different usage patterns.
SELECT
    member_casual,
    start_station_name,
    COUNT(*) AS num_trips
FROM
    cyclist_working
GROUP BY
    member_casual,
    start_station_name
ORDER BY
    member_casual,
    num_trips DESC;

-- Trip Purpose: Explore the purpose of bike trips for annual members and casual riders. This could include commuting, leisure rides, exercise, or other specific categories. Understanding the different motivations for riding can provide insights into their usage patterns.
SELECT
    member_casual,
    start_station_name,
    end_station_name,
    COUNT(*) AS num_of_trips
FROM cyclist_working
GROUP BY member_casual, start_station_name, end_station_name
ORDER BY member_casual, num_of_trips DESC;

-- Peak Hours: Analyze the time of day when annual members and casual riders are most active. Determine if there are any specific peak hours or usage patterns that differentiate the two groups.
SELECT
    member_casual,
    HOUR(started_at) AS hour_of_day,
    COUNT(*) AS num_trips
FROM
    cyclist_working
GROUP BY
    member_casual,
    HOUR(started_at)
ORDER BY
    member_casual,
    num_trips DESC;

-- Trip Distance: Evaluate the average distance covered by annual members and casual riders. This analysis can help identify if one group tends to take longer rides compared to the other.
SELECT
    member_casual,
    AVG(
        2 * 3961 * ASIN(
            SQRT(
                POWER(SIN((end_lat - start_lat) * pi() / 180 / 2), 2) +
                COS(start_lat * pi() / 180) * COS(end_lat * pi() / 180) *
                POWER(SIN((end_lng - start_lng) * pi() / 180 / 2), 2)
            )
        )
    ) AS avg_distance
FROM cyclist_working
GROUP BY member_casual;

-- Seasonal Variations: Examine whether there are any seasonal variations in bike usage between annual members and casual riders. This could involve analyzing usage patterns during different months or seasons of the year.
SELECT
    DATE_FORMAT(started_at, '%Y-%m') AS month,
    member_casual,
    COUNT(*) AS num_of_trips
FROM cyclist_working
GROUP BY month, member_casual
ORDER BY month, member_casual;

-- Calculating Number of Riders Each Day by User Type and Creating View to store date for Further Visualization

-- Create new column trip duration secs
ALTER TABLE CYCLIST_WORKING
ADD TRIP_DURATION_SECS INT;

-- Calculate trip length
UPDATE CYCLIST_WORKING
SET TRIP_DURATION_SECS = TIME_TO_SEC(TIMEDIFF(ended_at, started_at));

COMMIT;

-- Number of rides for casual and members
CREATE TABLE MEM_CAS_RIDES AS
SELECT MEMBER_CASUAL, COUNT(*) AS NO_OF_RIDES
FROM CYCLIST_WORKING
GROUP BY MEMBER_CASUAL
ORDER BY COUNT(*) DESC;
    
-- Count of rides for each bike type
CREATE TABLE BIKES_RIDES AS
SELECT RIDEABLE_TYPE, COUNT(*) AS NO_OF_RIDES
FROM CYCLIST_WORKING
GROUP BY RIDEABLE_TYPE
ORDER BY COUNT(*) DESC;
    
-- Distribution of members and casuals for each bike type
CREATE TABLE MEM_CAS_BIKES_RIDES AS
SELECT RIDEABLE_TYPE, MEMBER_CASUAL, COUNT(*) AS NO_OF_RIDES
FROM CYCLIST_WORKING
GROUP BY RIDEABLE_TYPE,MEMBER_CASUAL
ORDER BY RIDEABLE_TYPE ASC, COUNT(*) DESC;
    
-- Count round trips for each bike type and membership type
CREATE TABLE ROUND_RIDES AS
SELECT START_STATION_NAME, COUNT(*) AS NO_OF_ROUND_TRIPS, RIDEABLE_TYPE, MEMBER_CASUAL
FROM CYCLIST_WORKING
WHERE START_STATION_ID = END_STATION_ID
GROUP BY START_STATION_NAME, RIDEABLE_TYPE, MEMBER_CASUAL
ORDER BY START_STATION_NAME, COUNT(*) DESC, RIDEABLE_TYPE;
    
-- Distribution of casual and member rides across the year
CREATE TABLE YEAR_RIDES AS
SELECT DATE_FORMAT(STARTED_AT, '%b-%Y') AS MON_YEAR, MEMBER_CASUAL, COUNT(*) AS NO_OF_RIDES
FROM CYCLIST_WORKING
GROUP BY DATE_FORMAT(STARTED_AT, '%b-%Y'), MEMBER_CASUAL;

CREATE TABLE YEAR_RIDES_CASUAL AS
SELECT DATE_FORMAT(STARTED_AT, '%b-%Y') AS MON_YEAR, MEMBER_CASUAL, COUNT(*) AS NO_OF_RIDES
FROM CYCLIST_WORKING
WHERE MEMBER_CASUAL = 'casual'
GROUP BY DATE_FORMAT(STARTED_AT, '%b-%Y'), MEMBER_CASUAL;

CREATE TABLE YEAR_RIDES_MEMBERS AS
SELECT DATE_FORMAT(STARTED_AT, '%b-%Y') AS MON_YEAR, MEMBER_CASUAL, COUNT(*) AS NO_OF_RIDES
FROM CYCLIST_WORKING
WHERE MEMBER_CASUAL = 'member'
GROUP BY DATE_FORMAT(STARTED_AT, '%b-%Y'), MEMBER_CASUAL;

CREATE VIEW users_per_day AS
SELECT
    COUNT(CASE WHEN member_casual = 'member' THEN 1 ELSE NULL END) AS num_of_members,
    COUNT(CASE WHEN member_casual = 'casual' THEN 1 ELSE NULL END) AS num_of_casual,
    COUNT(*) AS num_of_users,
    DAYNAME(started_at) AS day_of_week
FROM cyclist_working
GROUP BY DAYNAME(started_at);

-- Calculating Average Ride Length for Each User Type and Creating View to store data for further Data Visualization
CREATE VIEW avg_ride_length AS
SELECT member_casual AS user_type, AVG(ride_length) AS avg_ride_length
FROM cyclist_working
GROUP BY member_casual;

-- Creating tables exclusively for Members & Casual Users
CREATE TABLE member_table (
    ride_id VARCHAR(50),
    rideable_type VARCHAR(50),
    member_casual VARCHAR(50),
    ride_length INT,
    day_of_week VARCHAR(50),
    month_m VARCHAR(50),
    year_y INT);

INSERT INTO member_table (ride_id, rideable_type, member_casual, ride_length, day_of_week, month_m, year_y)
SELECT ride_id, rideable_type, member_casual, ride_length, day_of_week, month_m, year_y
FROM cyclist_working
WHERE member_casual = 'member';

SELECT *
FROM member_table;

CREATE TABLE casual_table (
    ride_id VARCHAR(50),
    rideable_type VARCHAR(50),
    member_casual VARCHAR(50),
    ride_length INT,
    day_of_week VARCHAR(50),
    month_m VARCHAR(50),
    year_y INT);

INSERT INTO casual_table (ride_id, rideable_type, member_casual, ride_length, day_of_week, month_m, year_y)
SELECT ride_id, rideable_type, member_casual, ride_length, day_of_week, month_m, year_y
FROM cyclist_working
WHERE member_casual = 'casual';

SELECT *
FROM casual_table;

-- Calculating User Traffic Every Month Since Startup
CREATE TABLE user_traffic_per_month
AS
SELECT
    month_int AS Month_Num,
    month_m AS Month_Name,
    year_y AS Year_Y,
    COUNT(CASE WHEN member_casual = 'member' THEN 1 ELSE NULL END) AS num_of_member,
    COUNT(CASE WHEN member_casual = 'casual' THEN 1 ELSE NULL END) AS num_of_casual,
    COUNT(member_casual) AS total_num_of_users
FROM cyclist_working
GROUP BY year_y, month_int, month_m
ORDER BY year_y, month_int, month_m;

-- Calculating Daily Traffic Since Startup 
CREATE TABLE Daily_Traffic 
AS
SELECT
    COUNT(CASE WHEN member_casual = 'member' THEN 1 ELSE NULL END) AS num_of_members,
    COUNT(CASE WHEN member_casual = 'casual' THEN 1 ELSE NULL END) AS num_of_casual,
    COUNT(*) AS num_of_users,
    date_yyyy_mm_dd AS date_d
FROM cyclist_working
GROUP BY date_yyyy_mm_dd
ORDER BY date_yyyy_mm_dd;

-- Calculating User Traffic Hour Wise
ALTER TABLE cyclist_working
ADD hour_of_day INT;

UPDATE cyclist_working
SET hour_of_day = HOUR(started_at);

CREATE TABLE user_traffic_hourly
AS
SELECT
    hour_of_day AS Hour_of_day,
    COUNT(CASE WHEN member_casual = 'member' THEN 1 ELSE NULL END) AS num_of_members,
    COUNT(CASE WHEN member_casual = 'casual' THEN 1 ELSE NULL END) AS num_of_casual,
    COUNT(*) AS num_of_users
FROM cyclist_working
GROUP BY hour_of_day
ORDER BY hour_of_day;

CREATE TABLE popular_stations
AS
SELECT
    start_station_name AS Station_name,
    COUNT(CASE WHEN member_casual = 'casual' THEN 1 ELSE NULL END) AS num_of_casual
FROM cyclist_working
GROUP BY start_station_name
ORDER BY num_of_casual DESC
LIMIT 20;

commit;
