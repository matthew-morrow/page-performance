from boxsdk import OAuth2, Client
import pandas as pd
from decouple import config

auth = OAuth2(
    client_id = config("client_id"),
    client_secret = config("client_secret"),
    access_token= config("access_token"),
)
client = Client(auth)

user = client.user().get()
raw_bq_results_id = config("raw_big_query_results_box_id")
eclkc_active_urls_id = config("eclkc_active_urls_id")

raw_results_file_url = client.file(raw_bq_results_id).get_download_url()
test_dataframe = pd.read_csv(raw_results_file_url)

eclkc_active_urls_file_url = client.file(eclkc_active_urls_id).get_download_url()
eclkc_active_urls_dataframe = pd.read_csv(eclkc_active_urls_file_url, encoding="latin-1")
print(test_dataframe.describe)