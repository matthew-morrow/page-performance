from google.cloud import bigquery
from google.oauth2 import service_account

credentials = service_account.Credentials.from_service_account_file(
    ".\\hsicc-eblasts-analytics-1d54ac154638.json", scopes=["https://www.googleapis.com/auth/cloud-platform"],
)

bq_client = bigquery.Client(credentials=credentials, project=credentials.project_id,)

table_string = "hsicc-eblasts-analytics.eclkc_advanced_analytics.page_performance_results"

job_config = bigquery.QueryJobConfig(destination=table_string)
job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE


modify_table_sql = """ SELECT
  geo.country,
  geo.region,
  geo.city,
  geo.metro,
  device.category,
  event_date,
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
  AND (_TABLE_SUFFIX BETWEEN "20230201" and "20230522")"""

query_job = bq_client.query(modify_table_sql, job_config=job_config)  # Make an API request.
query_job.result()  # Wait for the job to complete.

print("Query results loaded to the table {}".format(table_string))