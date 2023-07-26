from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
from datetime import datetime

credentials = service_account.Credentials.from_service_account_file(".\\hsicc-eblasts-analytics-1d54ac154638.json", scopes=["https://www.googleapis.com/auth/cloud-platform"],)

bq_client = bigquery.Client(credentials=credentials, project=credentials.project_id,)

get_table_sql = """SELECT
  geo.country,
  geo.region,
  geo.city,
  geo.metro,
  device.category,
  device.mobile_brand_name,
  device.mobile_model_name,
  device.operating_system as os_system,
  device.operating_system_version as os_system_version,
  device.language as language,
  device.web_info.browser as web_info_browser,
  device.web_info.browser_version as web_info_browser_version,
  event_date,
  event_timestamp,
  (
  SELECT
    value.string_value
  FROM
    UNNEST(event_params)
  WHERE
    KEY = "page_location") AS page_url,
  (
  SELECT
    value.int_value
  FROM
    UNNEST(event_params)
  WHERE
    KEY = "timing_page_load_time") AS page_load_time_ms,
  (
  SELECT
    value.int_value
  FROM
    UNNEST(event_params)
  WHERE
    KEY = "timing_server_response_time") AS server_response_time_ms
FROM
  `hsicc-eblasts-analytics.analytics_249990458.events_*`
WHERE
  event_name = "performance_timing"
  AND (geo.country = "United States" OR geo.country = "American Samoa" OR geo.country = "Micronesia" OR geo.country = "Guam" OR geo.country = "Marshall Islands"  OR geo.country = "Northern Mariana Islands" OR geo.country = "Palau" OR geo.country = "Puerto Rico" OR geo.country = "U.S. Virgin Islands")
  AND (_TABLE_SUFFIX BETWEEN "20230527" and "20230724")"""

df = bq_client.query(get_table_sql).to_dataframe()

df.to_csv("raw_data_20230528_20230709.csv")