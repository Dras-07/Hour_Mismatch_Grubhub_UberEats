CREATE TEMP FUNCTION jsonObjectKeys(input STRING)
RETURNS ARRAY<STRING>
LANGUAGE js AS """
  return Object.keys(JSON.parse(input));
""";

CREATE TEMP FUNCTION extractStartValueFromJSON(json STRING, key STRING)
RETURNS STRING
LANGUAGE js AS """
  const obj = JSON.parse(json);
  return obj[key]['sections'][0]['regularHours'][0]['startTime'];
""";

CREATE TEMP FUNCTION extractEndValueFromJSON(json STRING, key STRING)
RETURNS STRING
LANGUAGE js AS """
  const obj = JSON.parse(json);
  return obj[key]['sections'][0]['regularHours'][0]['endTime'];
""";

CREATE TEMP FUNCTION extractDayFromJson(json STRING, key STRING, dayIndex INT64)
RETURNS BOOL
LANGUAGE js AS """
  const obj = JSON.parse(json);
  return obj[key]['sections'][0]['regularHours'][0]['daysBitArray'][dayIndex];
""";


CREATE TEMP FUNCTION calcdiff(ue_start STRING, ue_end STRING, gh_start STRING, gh_end STRING)
RETURNS INT64
LANGUAGE js AS """
  function timeToMinutes(timeStr) {
    const [hours, minutes] = timeStr.split(':').map(Number);
    return hours * 60 + minutes;
  }

  const ueStartMinutes = timeToMinutes(ue_start);
  const ueEndMinutes = timeToMinutes(ue_end);
  const ghStartMinutes = timeToMinutes(gh_start);
  const ghEndMinutes = timeToMinutes(gh_end);

  const ueDuration = ueEndMinutes - ueStartMinutes;
  const ghDuration = ghEndMinutes - ghStartMinutes;

  return Math.abs(ueStartMinutes - ghStartMinutes) + Math.abs(ueEndMinutes - ghEndMinutes);
""";


WITH 
  -- Grubhub Hours CTE
  grubhub_hours AS (
    SELECT 
      grubhub_slug AS gh_slug,
      grubhub_vb_name AS gh_vb_name,
      grubhub_b_name AS gh_b_name,
      grubhub_days,
      ARRAY(
        SELECT 
          STRUCT(
              TRIM(SPLIT(grubhub_day, '-')[OFFSET(1)])
             AS start_time,
             
              TRIM(SPLIT(grubhub_day, '-')[OFFSET(2)])
             AS end_time
          )
        FROM 
          UNNEST(grubhub_days) AS grubhub_day
      ) AS gh_times
    FROM (
      SELECT 
        slug AS grubhub_slug,
        vb_name AS grubhub_vb_name,
        b_name AS grubhub_b_name,
        ARRAY(
          SELECT CONCAT(day_abbr, '-', startTime, '-', endTime)
          FROM 
            UNNEST(response_data) AS day_info
        ) AS grubhub_days
      FROM (
        SELECT 
          slug,
          vb_name,
          b_name,
          timestamp,
          response,
          ARRAY(
            SELECT STRUCT(
              CASE day_index
                WHEN 0 THEN 'Monday'
                WHEN 1 THEN 'Tuesday'
                WHEN 2 THEN 'Wednesday'
                WHEN 3 THEN 'Thursday'
                WHEN 4 THEN 'Friday'
                WHEN 5 THEN 'Saturday'
                WHEN 6 THEN 'Sunday'
              END AS day_abbr,
              SUBSTR(JSON_EXTRACT_SCALAR(day_info, '$.from'), 1, 5) AS startTime,
              SUBSTR(JSON_EXTRACT_SCALAR(day_info, '$.to'), 1, 5) AS endTime
            )
            FROM UNNEST(JSON_EXTRACT_ARRAY(response, '$.today_availability_by_catalog.STANDARD_DELIVERY')) AS day_info
            CROSS JOIN UNNEST(GENERATE_ARRAY(0, 6)) AS day_index
          ) AS response_data,
          ROW_NUMBER() OVER (PARTITION BY slug ORDER BY timestamp DESC) AS row_num
        FROM 
          `arboreal-vision-339901.take_home_v2.virtual_kitchen_grubhub_hours`
      ) AS ranked_data
      WHERE 
        row_num = 1
    )
  ),
  
  -- UberEats Hours CTE
  ubereats_hours AS (
    WITH latest_timestamp_per_slug AS (
      SELECT
        slug AS ue_slug,
        vb_name AS ue_vb_name,
        b_name AS ue_b_name,
        MAX(timestamp) AS latest_timestamp
      FROM
        `arboreal-vision-339901.take_home_v2.virtual_kitchen_ubereats_hours`
      GROUP BY
        slug,
        vb_name,
        b_name
    )
    SELECT
      keys.ubereats_slug AS ue_slug,
      keys.ubereats_vb_name AS ue_vb_name,
      keys.ubereats_b_name AS ue_b_name,
      ue_days,
      ARRAY(
        SELECT 
          STRUCT(
           CASE 
            WHEN TRIM(SPLIT(ue_day, '-')[OFFSET(1)]) = 'Closed' THEN '25:00'
            ELSE TRIM(SPLIT(ue_day, '-')[OFFSET(1)])
          END AS start_time,
          CASE 
            WHEN TRIM(SPLIT(ue_day, '-')[OFFSET(1)]) = 'Closed' THEN '25:00'
            ELSE TRIM(SPLIT(ue_day, '-')[OFFSET(2)])
          END AS end_time
          )
        FROM 
          UNNEST(ue_days) AS ue_day
      ) AS ue_times
    FROM
      (
        SELECT
          slug AS ubereats_slug,
          vb_name AS ubereats_vb_name,
          b_name AS ubereats_b_name,
          ARRAY(
            SELECT
              CASE
                WHEN extractDayFromJson(json_menus, ubereats_keys, index) = true THEN
                  CONCAT(
                    CASE index
                      WHEN 0 THEN 'Monday'
                      WHEN 1 THEN 'Tuesday'
                      WHEN 2 THEN 'Wednesday'
                      WHEN 3 THEN 'Thursday'
                      WHEN 4 THEN 'Friday'
                      WHEN 5 THEN 'Saturday'
                      WHEN 6 THEN 'Sunday'
                    END,
                    '-',
                    extractStartValueFromJSON(json_menus, ubereats_keys),
                    '-',
                    extractEndValueFromJSON(json_menus, ubereats_keys)
                  )
                ELSE
                  CONCAT(
                    CASE index
                      WHEN 0 THEN 'Monday'
                      WHEN 1 THEN 'Tuesday'
                      WHEN 2 THEN 'Wednesday'
                      WHEN 3 THEN 'Thursday'
                      WHEN 4 THEN 'Friday'
                      WHEN 5 THEN 'Saturday'
                      WHEN 6 THEN 'Sunday'
                    END,
                    '- Closed'
                  )
              END
            FROM
              UNNEST(GENERATE_ARRAY(0, 6)) AS index
          ) AS ue_days
        FROM
          (
            SELECT
              slug,
              vb_name,
              b_name,
              TO_JSON_STRING(response.data.menus) AS json_menus
            FROM
              `arboreal-vision-339901.take_home_v2.virtual_kitchen_ubereats_hours`
            WHERE
              response.data.menus IS NOT NULL
          ) AS menus_data,
          UNNEST(jsonObjectKeys(json_menus)) AS ubereats_keys
      ) AS keys
    LEFT JOIN
      latest_timestamp_per_slug
    ON
      keys.ubereats_slug = latest_timestamp_per_slug.ue_slug
      AND keys.ubereats_vb_name = latest_timestamp_per_slug.ue_vb_name
      AND keys.ubereats_b_name = latest_timestamp_per_slug.ue_b_name
  ),
  
  mismatch AS (
    SELECT
      gh_slug,
      grubhub_days,
      ue_slug,
      ue_days,
      ARRAY(
        SELECT 
          CASE 
            WHEN calcdiff(ue_time.start_time, ue_time.end_time, gh_time.start_time, gh_time.end_time) = 0 THEN 'In Range'
            WHEN calcdiff(ue_time.start_time, ue_time.end_time, gh_time.start_time, gh_time.end_time) <= 5 THEN 'Out of Range with difference 5'
            ELSE 'Out Of Range'
          END
        FROM 
          UNNEST(gh_times) AS gh_time WITH OFFSET gh_index
          JOIN UNNEST(ue_times) AS ue_time WITH OFFSET ue_index
          ON gh_index = ue_index
      ) AS mismatched
    FROM 
      grubhub_hours
    JOIN 
      ubereats_hours
    ON 
      CONCAT(gh_vb_name, gh_b_name) = CONCAT(ue_vb_name, ue_b_name)
  )
  
-- Select final result
SELECT 
  gh_slug AS Grubhub_Slug,
  grubhub_days AS Virtual_Restuarant_Business_Hours,
  ue_slug AS Uber_Eats_Slug,
  ue_days AS Uber_Eats_Business_Hours,
  mismatched AS is_out_range
FROM 
  mismatch
;
