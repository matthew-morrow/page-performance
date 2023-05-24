from google.cloud import bigquery
from google.oauth2 import service_account

credentials = service_account.Credentials.from_service_account_file(
    "C:\\Users\\MatthewMorrow\\OneDrive - Hendall Inc\\Everything Else\\Documents\\dev\\page-performance\\hsicc-eblasts-analytics-1d54ac154638.json", scopes=["https://www.googleapis.com/auth/cloud-platform"],
)

bq_client = bigquery.Client(credentials=credentials, project=credentials.project_id,)

bq_project = "hsicc-eblasts-analytics"
bq_dataset_id = "eclkc_advanced_analytics"
bq_table_id = "page_performance_results"

bucket_name = "eclkc_advanced_analytics"

destination_uri = "gs://{}/{}".format(bucket_name, "page_performance_results.csv")
dataset_ref = bigquery.DatasetReference(bq_project, bq_dataset_id)
table_ref = dataset_ref.table(bq_table_id)

extract_job = bq_client.extract_table(
    table_ref,
    destination_uri,
    # Location must match that of the source table.
    location="US",
)  # API request
extract_job.result()  # Waits for job to complete.

print(
    "Exported {}:{}.{} to {}".format(bq_project, bq_dataset_id, bq_table_id, destination_uri)
)