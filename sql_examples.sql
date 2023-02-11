'''1) Query Desc: GROUP BY with single column/variable and extracting date information from datetime column'''
--Data Table Desc: an app installation table containing event_time, user_id, platform(IOS/ANDROID), network(download channel), and country
--Data Table Name: table_app_install
--Functions: EXTRACT(), GROUP BY, ORDER BY
--Output: daily app install and user counts
SELECT EXTRACT(DATE FROM event_time) AS event_date, --"group by" column
COUNT(*) AS install, COUNT(DISTINCT user_id) AS user -- calculated columns for "group by"
FROM table_app_install -- DB table for installations
GROUP BY EXTRACT(DATE FROM event_time)
ORDER BY EXTRACT(DATE FROM event_time) -- Sorting the results by date


'''2) Query Desc: GROUP BY with multi-columns and extracting date information from datetime column'''
--Data Table Desc: an app installation table containing event_time, user_id, platform(IOS/ANDROID), network(download channel), and country
--Data Table Name: table_app_install
--Functions: EXTRACT(), GROUP BY
--Output: app install and user counts for day, platform, network, and country (similar to multi-indexing)
SELECT EXTRACT(DATE FROM event_time) AS event_date, platform, network, country, -- "group by" columns
COUNT(*) AS install, COUNT(DISTINCT user_id) AS user -- calculated columns for "group by"
FROM table_app_install -- DB table for installations
WHERE platform IN ('android', 'ios') -- filtering the platform column/variable
GROUP BY EXTRACT(DATE FROM event_time), platform, network, country


'''3) Query Desc: Aggregating daily revenue and cost amounts from 2 different tables and JOIN them'''
--Data Table 1 Desc: daily marketting cost table containing date(day), platform(IOS/ANDROID), network(download channel), country, cost(in dollar)
--Data Table 1 Name: table_marketing_cost
--Data Table 2 Desc: user in-app purchases table containing event_time, revenue, user_id
--Data Table 2 Name: table_revenue
--Functions: LEFT JOIN(), SUM(), EXTRACT(), GROUP BY
--Output: daily total cost and revenue totals
SELECT * FROM 
 (SELECT date, SUM(cost) AS total_cost FROM table_marketing_cost GROUP BY date) table_cost_v2 -- first table
-- 
LEFT JOIN -- left join or inner join may be used for these tables.
-- 
 (SELECT EXTRACT(DATE FROM event_time) AS revenue_event_date, SUM(CAST(revenue AS INT)) AS daily_revenue_total 
 FROM table_revenue
 GROUP BY EXTRACT(DATE FROM event_time)) table_revenue_v2 -- second table
--
ON table_cost_v2.date = table_revenue_v2.revenue_event_date -- join column (similar to vlookup columns in Excel)
ORDER BY date -- Sorting the results by date


'''4) Query Desc: Calculating 1-day and 7-day user retentions after app installs by using TIME_DIFF, ROW_NUMBER and JOIN functions'''
--Data Table 1 Desc: an app installation table containing event_time, user_id, platform(IOS/ANDROID), network(download channel), and country
--Data Table 1 Name: table_app_install
--Data Table 2 Desc: users' app sessions table containing event_time, user_id, platform, session_duration(in minutes)
--Data Table 2 Name: table_session
--Highlighted Functions: ROW_NUMBER()-OVER(PARTITION BY), CASE-WHEN, TIMESTAMP_DIFF()
--Output: Daily app install, day-1 retention, and day-7 retention counts
SELECT install_date, -- "group by" column
COUNT(DISTINCT install_user_id) AS install_count,  -- calculated columns for "group by" 
COUNT(DISTINCT calendar_day_diff_1d) AS calendar_retention_1d, COUNT(DISTINCT calendar_day_diff_7d) AS calendar_retention_7d -- calculated columns for "group by" 
FROM  
 (SELECT *,
 TIMESTAMP_DIFF(session_event_date, install_date, DAY) AS calendar_day_diff, -- Calculating the day difference of 2 different date formatted columns
  --creating conditional new columns by using CASE-WHEN function
 CASE WHEN TIMESTAMP_DIFF(session_event_date, install_date, DAY) = 1 THEN install_user_id ELSE NULL END AS calendar_day_diff_1d, -- flag 1-day retention for upcoming count operation
 CASE WHEN TIMESTAMP_DIFF(session_event_date, install_date, DAY) = 7 THEN install_user_id ELSE NULL END AS calendar_day_diff_7d -- flag 7-day retention for upcoming count operation
 FROM  
  (SELECT install_date, event_time AS install_time_min, user_id AS install_user_id, platform, network, country FROM  
   (SELECT  
   --creating row numbers by user_id and event_time with ROW_NUMBER()-OVER(PARTITION BY) functions. Depending on use cases, it is more powerful than GROUP BY function
   ROW_NUMBER() OVER(PARTITION BY user_id ORDER BY event_time) AS rown,
   EXTRACT(DATE FROM event_time) AS install_date, 
   * 
   FROM table_app_install) table_tmp 
  WHERE rown=1) table_install -- by filtering row number (rown), the first rows were obtained for corresponding user_id.
 --
 LEFT JOIN 
 --
 (SELECT user_id, EXTRACT(DATE FROM event_time) AS session_event_date, event_time AS session_time
 FROM table_session
 WHERE platform IN ('android', 'ios')) table_session_v2
 -- joining and filterin since we are looking for user sessions after installs 
 ON table_install.install_user_id = table_session_v2.user_id AND table_install.install_time_min <= table_session_v2.session_time 
 ) table_retention 
GROUP BY install_date


'''5) Query Desc: Calculating number of churn customers and detecting the most challenging game id by using LEAD-OVER(PARTITION BY) function'''
--Data Table 1 Desc: an in-app game start log table containing event_time, user_id, platform(IOS/ANDROID), game_id
--Data Table 1 Name: table_game_start
--Data Table 2 Desc: an in-app game end log table containing event_time, user_id, platform(IOS/ANDROID), game_id, status(fail/win/quit), game_duration
--Data Table 2 Name: table_game_end
--Highlighted Functions: LEAD()-OVER(PARTITION BY)
--Output: number of users who stop playing the game after a fail result
SELECT game_id, --"group by" column
COUNT(DISTINCT user_id) AS churn_user FROM  -- calculation for "group by"
(SELECT *, 
--LEAD function is used to get the next game status, but if there is no game played, then we will get NULL value.
LEAD(status) OVER(PARTITION BY user_id ORDER BY event_time) AS lead_status 
FROM  
(SELECT *, 'start' AS status, NULL AS game_duration 
FROM table_game_start  
UNION ALL 
SELECT * FROM table_game_end  
WHERE platform IS NOT NULL AND game_duration IS NOT NULL)tmp)table_lead 
WHERE status='fail' AND lead_status IS NULL 
GROUP BY game_id 
ORDER BY game_id



