"""
@python_version 3.11.1
@program_version 1.3.2
@program_publish_date 2023.04.26
@program_modified_date 2023.07.26
@program_author Matthew Morrow
@program_description This program generates the various page performance reports based on user dictated start dates, timeframe window, and input file with the raw performance data.
"""
import datetime
import pandas as pd
import sys
import argparse

# from decouple import config
# from boxsdk import OAuth2, Client

from styleframe import StyleFrame, Styler, utils

from google.cloud import bigquery
from google.oauth2 import service_account

credentials = service_account.Credentials.from_service_account_file(
    ".\\hsicc-eblasts-analytics-1d54ac154638.json",
    scopes=["https://www.googleapis.com/auth/cloud-platform"],
)

previous_raw_results = pd.DataFrame()
current_raw_results = pd.DataFrame()
previous_grouped_by_url = pd.DataFrame()
current_grouped_by_url = pd.DataFrame()

top_level_summary = pd.DataFrame()
external_comparison_summary = pd.DataFrame()
calculated_grouped_by_page_path = pd.DataFrame()
calculated_grouped_by_page_url = pd.DataFrame()
top_pageview_changes = pd.DataFrame()
change_outliers = pd.DataFrame()
current_outliers = pd.DataFrame()

# Raw results and active URL locations in GCS
bucket_location_for_raw_data = (
    "gs://eclkc_advanced_analytics/page_performance_results.csv"
)
bucket_location_for_active_urls = (
    "gs://eclkc_advanced_analytics/eclkc_urls_200_status_code.csv"
)

"""
Helper function for percent difference and returns N/A if previous value is null

@params previous, current: The previous and current values to calculate the % diff, respectively
"""
def per_diff(previous, current):
    percent_difference = 0

    try:
        percent_difference = (current - previous) / previous
        return percent_difference
    except ZeroDivisionError:
        return "N/A"


"""
Helper function for percentile calculation

@params n: represents the percentile of interest to be calculated
"""
def calc_percentile(n):
    def percentile_(x):
        return x.quantile(n)

    percentile_.__name__ = "percentile_{:2.0f}".format(n * 100)
    return percentile_


"""
Helper function for weighted average

@params values, weights: data values to be averaged and weights applied to the values, respectively
"""
def weighted_avg(values, weights):
    return (values * weights).sum() / weights.sum()


def main():
    program_start_time = datetime.datetime.now()

    # Authentication for uploading results to Box
    # TODO Rewrite to GCS if wanted
    """ auth = OAuth2(
        client_id = config("client_id"),
        client_secret = config("client_secret"),
        access_token= config("access_token"),
    )
    client = Client(auth) """
    # ArgParser module for creating command line arguments needed to run script
    parser = argparse.ArgumentParser(
        description="Calculate page performance metrics",
        epilog="Contact HSICC for further help or troubleshooting: hseclkc@gmail.com",
    )
    parser.add_argument(
        "-p",
        "--previous_start_date",
        metavar="previousdate",
        type=str,
        nargs=1,
        help="String representation for the start date of the previous timeframe as (yyyymmdd) format",
        required=True,
    )
    parser.add_argument(
        "-c",
        "--current_start_date",
        metavar="currentdate",
        type=str,
        nargs=1,
        help="String representation for the start date of the current timeframe as (yyyymmdd) format",
        required=True,
    )
    parser.add_argument(
        "-tf",
        "--time_frame",
        metavar="timeframe",
        type=int,
        nargs="?",
        help="Optionally specify the window of time for each dataset. Default is two weeks (14 days inclusive of start date)",
        default=13,
    )
    parser.add_argument(
        "-i",
        "--input_file",
        metavar="inputfile",
        nargs="?",
        type=argparse.FileType("r"),
        help="Override default file found on GCS with a user specified dataset",
    )
    parser.add_argument(
        "-o",
        "--output_file",
        metavar="outputfile",
        nargs="?",
        type=str,
        default="./page_performance_calculator_results.xlsx",
        help="Override default output path of current directory to user specified destination",
    )
    parser.add_argument(
        "-a",
        "--active_urls_file",
        metavar="activeurlfile",
        nargs="?",
        type=str,
        help="Override default file found on GCS with a user specified active URLs dataset",
    )
    parser.add_argument(
        "-rd",
        "--raw_datasets",
        metavar="rawdatasets",
        nargs="?",
        type=str,
        help="If a path is specified, will write out the two raw timeframe datasets to a seperate file for inspection",
    )
    args = parser.parse_args()

    # If the user did not specify
    if args.active_urls_file is None:
        """print("Getting active URLs file from Box")
        eclkc_active_urls_id = eclkc_active_urls_id = config("eclkc_active_urls_id")
        eclkc_active_urls_file_url = client.file(eclkc_active_urls_id).get_download_url()
        eclkc_active_urls = pd.read_csv(eclkc_active_urls_file_url, encoding="latin-1")
        print("Active URLs file read from Box")"""

        print("Getting active URLs file from GCS")
        eclkc_active_urls = pd.read_csv(
            bucket_location_for_active_urls,
            encoding="latin-1",
            storage_options={"token": credentials},
        )
        print("Active URLs file read from GCS")

    else:
        print("Reading active URLs file from path")
        eclkc_active_urls = pd.read_csv(args.active_urls_file, encoding="latin-1")

    if args.input_file is None:
        """print("Getting source file from Box")
        raw_bq_results_id = config("raw_big_query_results_box_id")
        raw_results_file_url = client.file(raw_bq_results_id).get_download_url()
        source_dataset = pd.read_csv(raw_results_file_url, encoding="latin-1",
            usecols=[
                "event_date",
                "page_url",
                "page_load_time_ms",
                "server_response_time_ms",
            ],
        )
        print("Source file read from Box")"""

        print("Getting source file from GCS")
        source_dataset = pd.read_csv(
            bucket_location_for_raw_data,
            encoding="latin-1",
            storage_options={"token": credentials},
            usecols=[
                "event_date",
                "event_timestamp",
                "country",
                "region",
                "city",
                "metro",
                "category",
                "mobile_brand_name",
                "mobile_model_name",
                "os_system",
                "os_system_version",
                "language",
                "web_info_browser",
                "web_info_browser_version",
                "page_url",
                "page_load_time_ms",
                "server_response_time_ms",
            ],
        )
        print("Source file read from GCS")
    else:
        print("Reading source file from path")
        source_dataset = pd.read_csv(
            args.input_file,
            encoding="latin-1",
            usecols=[
                "event_date",
                "event_timestamp",
                "country",
                "region",
                "city",
                "metro",
                "category",
                "mobile_brand_name",
                "mobile_model_name",
                "os_system",
                "os_system_version",
                "language",
                "web_info_browser",
                "web_info_browser_version",
                "page_url",
                "page_load_time_ms",
                "server_response_time_ms",
            ],
        )
    source_dataset["event_date"] = pd.to_datetime(
        source_dataset["event_date"], format="%Y%m%d"
    )
    source_dataset["event_timestamp"] = pd.to_datetime(source_dataset["event_timestamp"], unit="us")

    source_dataset.loc[source_dataset['page_load_time_ms'] > 90000, 'page_load_time_ms'] = 90000
    source_dataset.loc[source_dataset['server_response_time_ms'] > 90000, 'server_response_time_ms'] = 90000

    print("\nCalculating results:")
    previous_raw_results = calculate_time_frame(
        args.previous_start_date[0], args.time_frame, source_dataset, eclkc_active_urls
    )
    previous_grouped_by_url = group_by_page_url(previous_raw_results)

    current_raw_results = calculate_time_frame(
        args.current_start_date[0], args.time_frame, source_dataset, eclkc_active_urls
    )
    current_grouped_by_url = group_by_page_url(current_raw_results)

    calculated_grouped_by_page_url = merge_groups_by_page_url(
        previous_grouped_by_url, current_grouped_by_url
    )

    top_level_summary = create_top_results(
        previous_raw_results,
        previous_grouped_by_url,
        current_raw_results,
        current_grouped_by_url,
    )

    styled_top_level_summary = StyleFrame(top_level_summary)
    style_header_row(styled_top_level_summary)
    styled_top_level_summary.apply_column_style(
        cols_to_style=["Metrics"],
        styler_obj=Styler(
            number_format=utils.number_formats.general,
            horizontal_alignment=utils.horizontal_alignments.left,
        ),
    )
    styled_top_level_summary.apply_column_style(
        cols_to_style=["Pages"],
        styler_obj=Styler(horizontal_alignment=utils.horizontal_alignments.right),
    )
    styled_top_level_summary.apply_column_style(
        cols_to_style=[
            "Pageview % Change",
            "Page Load Time % Change",
            "Server Response Time % Change",
        ],
        styler_obj=Styler(
            number_format=utils.number_formats.percent,
            horizontal_alignment=utils.horizontal_alignments.right,
        ),
    )
    styled_top_level_summary.apply_column_style(
        cols_to_style=["Page Load Times (sec)", "Server Response Times (sec)"],
        styler_obj=Styler(
            number_format=utils.number_formats.general_float,
            horizontal_alignment=utils.horizontal_alignments.right,
        ),
    )

    external_comparison_summary = create_external_metrics(
        current_raw_results, current_grouped_by_url
    )
    styled_external_comparison_summary = StyleFrame(external_comparison_summary)
    style_header_row(styled_external_comparison_summary)
    styled_external_comparison_summary.apply_column_style(
        cols_to_style=["Metrics"],
        styler_obj=Styler(
            number_format=utils.number_formats.general,
            horizontal_alignment=utils.horizontal_alignments.left,
        ),
    )
    styled_external_comparison_summary.apply_column_style(
        cols_to_style=["Percent of Total Page URLs", "Percent of Total Pageviews"],
        styler_obj=Styler(
            number_format=utils.number_formats.percent,
            horizontal_alignment=utils.horizontal_alignments.right,
        ),
    )
    styled_external_comparison_summary.apply_column_style(
        cols_to_style=["Number of Page URLs", "Number of Pageviews"],
        styler_obj=Styler(
            number_format=utils.number_formats.general_integer,
            horizontal_alignment=utils.horizontal_alignments.right,
        ),
    )

    previous_grouped_by_page_path = group_by_page_path(previous_raw_results)
    current_grouped_by_page_path = group_by_page_path(current_raw_results)

    calculated_grouped_by_page_path = create_grouped_by_page_path(
        previous_grouped_by_page_path, current_grouped_by_page_path
    )
    styled_calculated_grouped_by_page_path = StyleFrame(calculated_grouped_by_page_path)
    style_header_row(styled_calculated_grouped_by_page_path)
    styled_calculated_grouped_by_page_path.apply_column_style(
        cols_to_style=["page_path_one"],
        styler_obj=Styler(
            number_format=utils.number_formats.general,
            horizontal_alignment=utils.horizontal_alignments.left,
        ),
    )
    styled_calculated_grouped_by_page_path.apply_column_style(
        cols_to_style=["pages", "pv"],
        styler_obj=Styler(
            number_format=utils.number_formats.general_integer,
            horizontal_alignment=utils.horizontal_alignments.right,
        ),
    )
    styled_calculated_grouped_by_page_path.apply_column_style(
        cols_to_style=["pv_percent_of_total","pv_percent_change", "plt_percent_change", "srt_percent_change"],
        styler_obj=Styler(
            number_format=utils.number_formats.percent,
            horizontal_alignment=utils.horizontal_alignments.right,
        ),
    )
    styled_calculated_grouped_by_page_path.apply_column_style(
        cols_to_style=["plt_avg","srt_avg"],
        styler_obj=Styler(
            number_format=utils.number_formats.general_float,
            horizontal_alignment=utils.horizontal_alignments.right,
        ),
    )

    top_pageview_changes = create_top_pageview_changes(calculated_grouped_by_page_url)
    styled_top_pageview_changes = StyleFrame(top_pageview_changes)
    style_header_row(styled_top_pageview_changes)
    styled_top_pageview_changes.apply_column_style(
        cols_to_style=["page_url_cleaned"],
        styler_obj=Styler(
            number_format=utils.number_formats.general,
            horizontal_alignment=utils.horizontal_alignments.left,
        ),
    )
    styled_top_pageview_changes.apply_column_style(
        cols_to_style=["pv_current"],
        styler_obj=Styler(
            number_format=utils.number_formats.general_integer,
            horizontal_alignment=utils.horizontal_alignments.right,
        ),
    )
    styled_top_pageview_changes.apply_column_style(
        cols_to_style=["pv_percent_of_total","pv_percent_change"],
        styler_obj=Styler(
            number_format=utils.number_formats.percent,
            horizontal_alignment=utils.horizontal_alignments.right,
        ),
    )

    change_outliers = create_change_outliers(calculated_grouped_by_page_url)
    styled_change_outliers = StyleFrame(change_outliers)
    style_header_row(styled_change_outliers)
    styled_change_outliers.apply_column_style(
        cols_to_style=["page_url_cleaned"],
        styler_obj=Styler(
            number_format=utils.number_formats.general,
            horizontal_alignment=utils.horizontal_alignments.left,
        ),
    )
    styled_change_outliers.apply_column_style(
        cols_to_style=["pv_current"],
        styler_obj=Styler(
            number_format=utils.number_formats.general_integer,
            horizontal_alignment=utils.horizontal_alignments.right,
        ),
    )
    styled_change_outliers.apply_column_style(
        cols_to_style=["plt_avg_current","srt_avg_current"],
        styler_obj=Styler(
            number_format=utils.number_formats.general_float,
            horizontal_alignment=utils.horizontal_alignments.right,
        ),
    )
    styled_change_outliers.apply_column_style(
        cols_to_style=["pv_percent_of_total","plt_percent_change", "srt_percent_change"],
        styler_obj=Styler(
            number_format=utils.number_formats.percent,
            horizontal_alignment=utils.horizontal_alignments.right,
        ),
    )
    styled_change_outliers.apply_column_style(
        cols_to_style=["outlier_value"],
        styler_obj=Styler(
            number_format=utils.number_formats.general,
            horizontal_alignment=utils.horizontal_alignments.right,
        ),
    )

    current_outliers = create_current_outliers(
        current_raw_results, calculated_grouped_by_page_url
    )
    styled_current_outliers = StyleFrame(current_outliers)
    style_header_row(styled_current_outliers)
    styled_current_outliers.apply_column_style(
        cols_to_style=["page_url_cleaned"],
        styler_obj=Styler(
            number_format=utils.number_formats.general,
            horizontal_alignment=utils.horizontal_alignments.left,
        ),
    )
    styled_current_outliers.apply_column_style(
        cols_to_style=["pv_current"],
        styler_obj=Styler(
            number_format=utils.number_formats.general_integer,
            horizontal_alignment=utils.horizontal_alignments.right,
        ),
    )
    styled_current_outliers.apply_column_style(
        cols_to_style=["plt_avg_current","srt_avg_current"],
        styler_obj=Styler(
            number_format=utils.number_formats.general_float,
            horizontal_alignment=utils.horizontal_alignments.right,
        ),
    )
    styled_current_outliers.apply_column_style(
        cols_to_style=["pv_percent_of_total","plt_percent_change", "srt_percent_change"],
        styler_obj=Styler(
            number_format=utils.number_formats.percent,
            horizontal_alignment=utils.horizontal_alignments.right,
        ),
    )
    styled_current_outliers.apply_column_style(
        cols_to_style=["outlier_value"],
        styler_obj=Styler(
            number_format=utils.number_formats.general,
            horizontal_alignment=utils.horizontal_alignments.right,
        ),
    )

    styled_calculated_grouped_by_page_url = StyleFrame(calculated_grouped_by_page_url)
    style_header_row(styled_calculated_grouped_by_page_url)
    styled_calculated_grouped_by_page_url.apply_style_by_indexes(
        indexes_to_style=styled_calculated_grouped_by_page_url[
            styled_calculated_grouped_by_page_url["outlier_value"] != "N/A"
        ],
        styler_obj=Styler(bg_color="#F25454", bold=True, horizontal_alignment=utils.horizontal_alignments.right),
    )
    styled_calculated_grouped_by_page_url.apply_style_by_indexes(
        indexes_to_style=styled_calculated_grouped_by_page_url[
            styled_calculated_grouped_by_page_url["outlier_value"] == "N/A"
        ],
        styler_obj=Styler(horizontal_alignment=utils.horizontal_alignments.right),
    )

    if args.raw_datasets is None:
        print("\nSkipped writing raw datasets to file\n")

    else:
        with pd.ExcelWriter(args.raw_datasets) as writer:
            print("\nWriting datasets to file:")
            previous_raw_results.to_excel(
                writer,
                sheet_name="previous_raw_results",
                index=False,
                freeze_panes=(1, 0),
            )
            print("Previous Raw Results written")

            current_raw_results.to_excel(
                writer,
                sheet_name="current_raw_results",
                index=False,
                freeze_panes=(1, 0),
            )
            print("Current Raw Results written")

    # Write out all of the dataframe results to their respective sheets in an excel file
    with StyleFrame.ExcelWriter(args.output_file) as writer:
        print("\nWriting results to file:")
        styled_top_level_summary.to_excel(
            writer,
            sheet_name="top_level",
            index=False,
            freeze_panes=(1, 0),
            row_to_add_filters=0,
            best_fit=list(styled_top_level_summary.columns),
        )
        print("Top Level Summary Results written")

        styled_external_comparison_summary.to_excel(
            writer,
            sheet_name="external_comparison",
            index=False,
            freeze_panes=(1, 0),
            row_to_add_filters=0,
            best_fit=list(styled_external_comparison_summary.columns),
        )
        print("External Comparison Summary Results written")

        styled_calculated_grouped_by_page_path.to_excel(
            writer,
            sheet_name="grouped_by_page_path",
            index=False,
            freeze_panes=(1, 0),
            row_to_add_filters=0,
            best_fit=list(styled_calculated_grouped_by_page_path.columns),
        )
        print("Grouped by Page Path Results written")

        styled_calculated_grouped_by_page_url.to_excel(
            writer,
            sheet_name="grouped_by_page_url",
            index=False,
            freeze_panes=(1, 0),
            row_to_add_filters=0,
            best_fit=list(styled_calculated_grouped_by_page_url.columns),
        )
        print("Grouped by Page URL Results written")

        styled_top_pageview_changes.to_excel(
            writer,
            sheet_name="top_pageview_changes",
            index=False,
            freeze_panes=(1, 0),
            row_to_add_filters=0,
            best_fit=list(styled_top_pageview_changes.columns),
        )
        print("Top Pageview Change Results written")

        styled_change_outliers.to_excel(
            writer,
            sheet_name="change_outliers",
            index=False,
            freeze_panes=(1, 0),
            row_to_add_filters=0,
            best_fit=list(styled_change_outliers.columns),
        )
        print("Change Outlier Results written")

        styled_current_outliers.to_excel(
            writer,
            sheet_name="current_outliers",
            index=False,
            freeze_panes=(1, 0),
            row_to_add_filters=0,
            best_fit=list(styled_current_outliers.columns),
        )
        print("Current Outlier Results written")

    print("Results finalized.")

    # upload_file = client.folder(config("box_folder_for_uploads")).upload(args.output_file, file_name="results_{start_value}-{end_value}-{datetime_now}.xlsx".format(start_value = args.previous_start_date[0], end_value= args.current_start_date[0], datetime_now = datetime.datetime.now().strftime("%H%M%S")), file_description="Sample Description")
    program_end_time = datetime.datetime.now()
    # print("Results uploaded to Box here: https://app.box.com/file/{file_id}".format(file_id = upload_file.id))
    print(
        "\nProgram Runtime Duration: {}\n".format(program_end_time - program_start_time)
    )


"""
Function for calculating the timeframe based on the start date, timeframe window, and source file

@param start_date: string representation of a datetime
@param window: timeframe lookup (default is two weeks)
@params source, active_urls: the raw source file and file with all the 200 status codes URLs
"""
def calculate_time_frame(start_date, window, source, active_urls):
    end_date = pd.to_datetime(start_date) + pd.DateOffset(days=window)
    time_frame_result = source[
        source["event_date"].between(
            start_date,
            end_date,
            inclusive="both",
        )
    ].sort_values("event_date", ascending=False)

    time_frame_result = cleanup_input_raw_results(time_frame_result, active_urls)

    return time_frame_result


"""
Helper function for removing non-active URLs from the cleaned dataframe

@param to_clean_dataframe: raw dataframe of a given timeframe
@param active_urls: the raw source file and file with all the 200 status codes URLs
"""
def cleanup_input_raw_results(to_clean_dataframe, active_urls):
    to_clean_dataframe = to_clean_dataframe.rename(
        columns={"page_load_time_ms": "plt_ms", "server_response_time_ms": "srt_ms"}
    )

    to_clean_dataframe["page_url_cleaned"] = to_clean_dataframe["page_url"].replace(
        "https://eclkc.ohs.acf.hhs.gov", "", regex=True
    )
    to_clean_dataframe["page_url_cleaned"] = (
        to_clean_dataframe["page_url_cleaned"].str.split("?").str[0]
    )
    to_clean_dataframe["page_path_one"] = (
        "/" + to_clean_dataframe["page_url_cleaned"].str.split("/").str[1]
    )
    to_clean_dataframe["plt_sec"] = to_clean_dataframe["plt_ms"] / 1000
    to_clean_dataframe["srt_sec"] = to_clean_dataframe["srt_ms"] / 1000
    to_clean_dataframe = to_clean_dataframe[
        to_clean_dataframe["page_url_cleaned"].isin(active_urls["URLs"])
    ].reset_index()
    return to_clean_dataframe


"""
Function for grouping values in the dataframe by the URL

@param to_group_dataframe: dataframe that needs grouping by URL
"""
def group_by_page_url(to_group_dataframe):
    grouped = (
        to_group_dataframe.sort_values(["page_url_cleaned"], ascending=False)
        .groupby("page_url_cleaned")
        .agg(
            pv=("page_url_cleaned", "count"),
            plt_sum=("plt_sec", "sum"),
            plt_avg=("plt_sec", "mean"),
            srt_sum=("srt_sec", "sum"),
            srt_avg=("srt_sec", "mean"),
        )
        .reset_index()
    )
    return grouped


"""
Function for grouping values in the dataframe by the page path

@param to_group_dataframe: dataframe that needs grouping by page path
"""
def group_by_page_path(to_group_dateframe):
    grouped = (
        to_group_dateframe.sort_values(["page_path_one"], ascending=False)
        .groupby("page_path_one")
        .agg(
            pages=("page_url_cleaned", "nunique"),
            pv=("page_path_one", "count"),
            plt_sum=("plt_sec", "sum"),
            plt_avg=("plt_sec", "mean"),
            srt_sum=("srt_sec", "sum"),
            srt_avg=("srt_sec", "mean"),
        )
        .reset_index()
    )
    return grouped


def merge_groups_by_page_url(previous_group, current_group):
    merged_group = (
        current_group.set_index("page_url_cleaned")
        .join(
            previous_group.set_index("page_url_cleaned"),
            lsuffix="_current",
            rsuffix="_previous",
        )
        .sort_values("pv_current", ascending=False)
    )

    merged_group["pv_percent_of_total"] = (
        merged_group["pv_current"] / merged_group["pv_current"].sum()
    )

    merged_group["pv_percent_change"] = per_diff(
        merged_group["pv_previous"],
        merged_group["pv_current"],
    )

    merged_group = merged_group.reset_index()

    return merged_group


def create_top_results(previous_raw, previous_group, current_raw, current_group):
    print("Calculating Top Level Results")
    metrics = [
        "Total (Pageviews) / Average (Timing):",
        "Min:",
        "25th:",
        "50th:",
        "75th:",
        "Max:",
        "Threshold:",
    ]

    # Calculates the various pageview metrics for the current and previous period
    current_total_pv = sum(current_group["pv"])
    current_min_pv = min(current_group["pv"])
    current_25_quartile_pv = current_group["pv"].quantile(0.25)
    current_50_quartile_pv = current_group["pv"].quantile(0.50)
    current_75_quartile_pv = current_group["pv"].quantile(0.75)
    current_max_pv = max(current_group["pv"])

    previous_total_pv = sum(previous_group["pv"])
    previous_min_pv = min(previous_group["pv"])
    previous_25_quartile_pv = previous_group["pv"].quantile(0.25)
    previous_50_quartile_pv = previous_group["pv"].quantile(0.50)
    previous_75_quartile_pv = previous_group["pv"].quantile(0.75)
    previous_max_pv = max(previous_group["pv"])

    # Calculates the various page load time metrics for the current and previous period
    current_avg_plt = sum(current_raw["plt_sec"]) / sum(current_group["pv"])
    current_min_plt = min(current_raw["plt_sec"])
    current_25_quartile_plt = current_raw["plt_sec"].quantile(0.25)
    current_50_quartile_plt = current_raw["plt_sec"].quantile(0.5)
    current_75_quartile_plt = current_raw["plt_sec"].quantile(0.75)
    current_max_plt = max(current_raw["plt_sec"])
    current_threshold_plt = current_raw["plt_sec"].quantile(0.75) + 1.5 * (
        current_raw["plt_sec"].quantile(0.75) - current_raw["plt_sec"].quantile(0.25)
    )

    previous_avg_plt = sum(previous_raw["plt_sec"]) / sum(previous_group["pv"])
    previous_min_plt = min(previous_raw["plt_sec"])
    previous_25_quartile_plt = previous_raw["plt_sec"].quantile(0.25)
    previous_50_quartile_plt = previous_raw["plt_sec"].quantile(0.5)
    previous_75_quartile_plt = previous_raw["plt_sec"].quantile(0.75)
    previous_max_plt = max(previous_raw["plt_sec"])
    previous_threshold_plt = previous_raw["plt_sec"].quantile(0.75) + 1.5 * (
        previous_raw["plt_sec"].quantile(0.75) - previous_raw["plt_sec"].quantile(0.25)
    )

    # Calculates the various server response time metrics for the current and previous period
    current_avg_srt = sum(current_raw["srt_sec"]) / sum(current_group["pv"])
    current_min_srt = min(current_raw["srt_sec"])
    current_25_quartile_srt = current_raw["srt_sec"].quantile(0.25)
    current_50_quartile_srt = current_raw["srt_sec"].quantile(0.5)
    current_75_quartile_srt = current_raw["srt_sec"].quantile(0.75)
    current_max_srt = max(current_raw["srt_sec"])
    current_threshold_srt = current_raw["srt_sec"].quantile(0.75) + 1.5 * (
        current_raw["srt_sec"].quantile(0.75) - current_raw["srt_sec"].quantile(0.25)
    )

    previous_avg_srt = sum(previous_raw["srt_sec"]) / sum(previous_group["pv"])
    previous_min_srt = min(previous_raw["srt_sec"])
    previous_25_quartile_srt = previous_raw["srt_sec"].quantile(0.25)
    previous_50_quartile_srt = previous_raw["srt_sec"].quantile(0.5)
    previous_75_quartile_srt = previous_raw["srt_sec"].quantile(0.75)
    previous_max_srt = max(previous_raw["srt_sec"])
    previous_threshold_srt = previous_raw["srt_sec"].quantile(0.75) + 1.5 * (
        previous_raw["srt_sec"].quantile(0.75) - previous_raw["srt_sec"].quantile(0.25)
    )
    # Creates the pageview values column using the calculated pageview metrics
    current_page_values = [
        current_total_pv,
        current_min_pv,
        current_25_quartile_pv,
        current_50_quartile_pv,
        current_75_quartile_pv,
        current_max_pv,
        "N/A",
    ]

    # Creates the percent change column for pageviews
    percent_change_pv = [
        per_diff(previous_total_pv, current_total_pv),
        per_diff(previous_min_pv, current_min_pv),
        per_diff(previous_25_quartile_pv, current_25_quartile_pv),
        per_diff(previous_50_quartile_pv, current_50_quartile_pv),
        per_diff(previous_75_quartile_pv, current_75_quartile_pv),
        per_diff(previous_max_pv, current_max_pv),
        "N/A",
    ]

    # Creates the load time values column using the calculated load time metrics
    current_page_load_time_values = [
        current_avg_plt,
        current_min_plt,
        current_25_quartile_plt,
        current_50_quartile_plt,
        current_75_quartile_plt,
        current_max_plt,
        current_threshold_plt,
    ]

    # Creates the percent change column for page load time
    percent_change_plt_sec = [
        per_diff(previous_avg_plt, current_avg_plt),
        per_diff(previous_min_plt, current_min_plt),
        per_diff(previous_25_quartile_plt, current_25_quartile_plt),
        per_diff(previous_50_quartile_plt, current_50_quartile_plt),
        per_diff(previous_75_quartile_plt, current_75_quartile_plt),
        per_diff(previous_max_plt, current_max_plt),
        per_diff(previous_threshold_plt, current_threshold_plt),
    ]

    # Creates the server response values column using the calculated server response metrics
    current_server_response_time_values = [
        current_avg_srt,
        current_min_srt,
        current_25_quartile_srt,
        current_50_quartile_srt,
        current_75_quartile_srt,
        current_max_srt,
        current_threshold_srt,
    ]

    # Creates the percent change column for server response time
    percent_change_srt_sec = [
        per_diff(previous_avg_srt, current_avg_srt),
        per_diff(previous_min_srt, current_min_srt),
        per_diff(
            previous_25_quartile_srt,
            current_25_quartile_srt,
        ),
        per_diff(
            previous_50_quartile_srt,
            current_50_quartile_srt,
        ),
        per_diff(
            previous_75_quartile_srt,
            current_75_quartile_srt,
        ),
        per_diff(previous_max_srt, current_max_srt),
        per_diff(previous_threshold_srt, current_threshold_srt),
    ]

    # Combine all the metrics and values into a new DataFrame for top_level_summary
    top_level = pd.DataFrame(
        {
            "Metrics": metrics,
            "Pages": current_page_values,
            "Pageview % Change": percent_change_pv,
            "Page Load Times (sec)": current_page_load_time_values,
            "Page Load Time % Change": percent_change_plt_sec,
            "Server Response Times (sec)": current_server_response_time_values,
            "Server Response Time % Change": percent_change_srt_sec,
        }
    )

    return top_level


def create_external_metrics(current_raw, current_group):
    print("Calculating External Metrics Results")
    external_metrics = [
        "90 secs:",
        ">30 - <90 secs:",
        ">10 - <=30 secs:",
        ">5 - <=10 secs:",
        ">2.9 - <=5 secs:",
        ">1.7 - <=2.9 secs:",
        ">0.8 - <=1.7 secs:",
        "<= 0.8 secs:",
        "WA: {:0.2f} secs".format(
            weighted_avg(
                current_group["plt_avg"],
                current_group["pv"],
            )
        ),
    ]

    page_url_counts = [
        current_group["plt_avg"].ge(90).sum(),
        (current_group["plt_avg"].lt(90) & current_group["plt_avg"].gt(30)).sum(),
        (current_group["plt_avg"].le(30) & current_group["plt_avg"].gt(10)).sum(),
        (current_group["plt_avg"].le(10) & current_group["plt_avg"].gt(5)).sum(),
        (current_group["plt_avg"].le(5) & current_group["plt_avg"].gt(2.9)).sum(),
        (current_group["plt_avg"].le(2.9) & current_group["plt_avg"].gt(1.7)).sum(),
        (current_group["plt_avg"].le(1.7) & current_group["plt_avg"].gt(0.8)).sum(),
        current_group["plt_avg"].le(0.8).sum(),
        current_group["page_url_cleaned"].count(),
    ]

    percent_urls = [
        current_group["plt_avg"].ge(90).sum()
        / current_group["page_url_cleaned"].count(),
        (current_group["plt_avg"].lt(90) & current_group["plt_avg"].gt(30)).sum()
        / current_group["page_url_cleaned"].count(),
        (current_group["plt_avg"].le(30) & current_group["plt_avg"].gt(10)).sum()
        / current_group["page_url_cleaned"].count(),
        (current_group["plt_avg"].le(10) & current_group["plt_avg"].gt(5)).sum()
        / current_group["page_url_cleaned"].count(),
        (current_group["plt_avg"].le(5) & current_group["plt_avg"].gt(2.9)).sum()
        / current_group["page_url_cleaned"].count(),
        (current_group["plt_avg"].le(2.9) & current_group["plt_avg"].gt(1.7)).sum()
        / current_group["page_url_cleaned"].count(),
        (current_group["plt_avg"].le(1.7) & current_group["plt_avg"].gt(0.8)).sum()
        / current_group["page_url_cleaned"].count(),
        current_group["plt_avg"].le(0.8).sum()
        / current_group["page_url_cleaned"].count(),
        current_group["page_url_cleaned"].count()
        / current_group["page_url_cleaned"].count(),
    ]

    pageview_counts = [
        current_raw["plt_sec"].ge(90).sum(),
        (current_raw["plt_sec"].lt(90) & current_raw["plt_sec"].gt(30)).sum(),
        (current_raw["plt_sec"].le(30) & current_raw["plt_sec"].gt(10)).sum(),
        (current_raw["plt_sec"].le(10) & current_raw["plt_sec"].gt(5)).sum(),
        (current_raw["plt_sec"].le(5) & current_raw["plt_sec"].gt(2.9)).sum(),
        (current_raw["plt_sec"].le(2.9) & current_raw["plt_sec"].gt(1.7)).sum(),
        (current_raw["plt_sec"].le(1.7) & current_raw["plt_sec"].gt(0.8)).sum(),
        current_raw["plt_sec"].le(0.8).sum(),
        current_raw["page_url"].count(),
    ]

    percent_pages = [
        current_raw["plt_sec"].ge(90).sum() / current_raw["page_url"].count(),
        (current_raw["plt_sec"].lt(90) & current_raw["plt_sec"].gt(30)).sum()
        / current_raw["page_url"].count(),
        (current_raw["plt_sec"].le(30) & current_raw["plt_sec"].gt(10)).sum()
        / current_raw["page_url"].count(),
        (current_raw["plt_sec"].le(10) & current_raw["plt_sec"].gt(5)).sum()
        / current_raw["page_url"].count(),
        (current_raw["plt_sec"].le(5) & current_raw["plt_sec"].gt(2.9)).sum()
        / current_raw["page_url"].count(),
        (current_raw["plt_sec"].le(2.9) & current_raw["plt_sec"].gt(1.7)).sum()
        / current_raw["page_url"].count(),
        (current_raw["plt_sec"].le(1.7) & current_raw["plt_sec"].gt(0.8)).sum()
        / current_raw["page_url"].count(),
        current_raw["plt_sec"].le(0.8).sum() / current_raw["page_url"].count(),
        current_raw["page_url"].count() / current_raw["page_url"].count(),
    ]

    external_results = pd.DataFrame(
        {
            "Metrics": external_metrics,
            "Number of Page URLs": page_url_counts,
            "Percent of Total Page URLs": percent_urls,
            "Number of Pageviews": pageview_counts,
            "Percent of Total Pageviews": percent_pages,
        }
    )

    return external_results


def create_grouped_by_page_path(previous_group, current_group):
    grouped_result = current_group
    grouped_result["pv_percent_of_total"] = (
        current_group["pv"] / current_group["pv"].sum()
    )
    grouped_result["pv_percent_change"] = per_diff(
        previous_group["pv"],
        current_group["pv"],
    )
    grouped_result["plt_percent_change"] = per_diff(
        previous_group["plt_sum"],
        current_group["plt_sum"],
    )
    grouped_result["srt_percent_change"] = per_diff(
        previous_group["srt_sum"],
        current_group["srt_sum"],
    )

    # Drop unneeded columns
    grouped_result = grouped_result[
        [
            "page_path_one",
            "pages",
            "pv",
            "pv_percent_of_total",
            "pv_percent_change",
            "plt_avg",
            "plt_percent_change",
            "srt_avg",
            "srt_percent_change",
        ]
    ].sort_values("pv", ascending=False)

    return grouped_result


def create_top_pageview_changes(merged_group):
    # Filter for the top and bottom 25 pageview percent changes and concatenate the reults into a single table
    print("Calculating Top Pageview Change Results")
    pageview_changes = pd.concat(
        [
            merged_group.nlargest(25, "pv_percent_change"),
            merged_group.nsmallest(25, "pv_percent_change"),
        ]
    ).reset_index()

    # Drop unneeded columns
    pageview_changes = pageview_changes[
        ["page_url_cleaned", "pv_current", "pv_percent_of_total", "pv_percent_change"]
    ]

    return pageview_changes


def create_change_outliers(merged_group):
    print("Calculating Change Outliers Results")
    outliers = merged_group

    outliers["plt_percent_change"] = per_diff(
        outliers["plt_avg_previous"], outliers["plt_avg_current"]
    )
    outliers["srt_percent_change"] = per_diff(
        outliers["srt_avg_previous"], outliers["srt_avg_current"]
    )
    current_threshold_plt_percent_change = outliers["plt_percent_change"].quantile(
        0.75
    ) + 1.5 * (
        outliers["plt_percent_change"].quantile(0.75)
        - outliers["plt_percent_change"].quantile(0.25)
    )
    current_threshold_srt_percent_change = outliers["srt_percent_change"].quantile(
        0.75
    ) + 1.5 * (
        outliers["srt_percent_change"].quantile(0.75)
        - outliers["srt_percent_change"].quantile(0.25)
    )
    # Classify each result as non-outlier, PLT outlier, SRT outlier, or both types of outlier
    outliers.loc[
        (
            (outliers["plt_percent_change"] >= current_threshold_plt_percent_change)
            & (outliers["srt_percent_change"] >= current_threshold_srt_percent_change)
        ),
        "outlier_value",
    ] = "PLT and SRT"
    outliers.loc[
        (
            (outliers["plt_percent_change"] > current_threshold_plt_percent_change)
            & (outliers["srt_percent_change"] < current_threshold_srt_percent_change)
        ),
        "outlier_value",
    ] = "PLT"
    outliers.loc[
        (
            (outliers["plt_percent_change"] < current_threshold_plt_percent_change)
            & (outliers["srt_percent_change"] >= current_threshold_srt_percent_change)
        ),
        "outlier_value",
    ] = "SRT"
    outliers.loc[
        (
            (outliers["plt_percent_change"] < current_threshold_plt_percent_change)
            & (outliers["srt_percent_change"] < current_threshold_srt_percent_change)
        ),
        "outlier_value",
    ] = "N/A"

    # Drop results that are not an outlier type
    outliers = outliers[
        outliers["outlier_value"].isin(["PLT", "PLT and SRT", "SRT"])
    ].reset_index()

    # Drop unneeded columns
    outliers = outliers[
        [
            "page_url_cleaned",
            "pv_current",
            "pv_percent_of_total",
            "plt_avg_current",
            "plt_percent_change",
            "srt_avg_current",
            "srt_percent_change",
            "outlier_value",
        ]
    ]

    # Sort by descending pageview values and then group the top 25 results of each outlier type
    # TODO Should all the values be listed rather than the top 25?
    outliers = outliers.sort_values(
        ["outlier_value", "pv_current"], ascending=[True, False]
    )
    return outliers


def create_current_outliers(current_raw, merged_group):
    print("Calculating Current Outliers Results")
    outliers = merged_group
    current_threshold_plt = current_raw["plt_sec"].quantile(0.75) + 1.5 * (
        current_raw["plt_sec"].quantile(0.75) - current_raw["plt_sec"].quantile(0.25)
    )
    current_threshold_srt = current_raw["srt_sec"].quantile(0.75) + 1.5 * (
        current_raw["srt_sec"].quantile(0.75) - current_raw["srt_sec"].quantile(0.25)
    )
    outliers.loc[
        (outliers["plt_avg_current"] >= current_threshold_plt)
        & (outliers["srt_avg_current"] >= current_threshold_srt),
        "outlier_value",
    ] = "PLT and SRT"
    outliers.loc[
        (outliers["plt_avg_current"] > current_threshold_plt)
        & (outliers["srt_avg_current"] < current_threshold_srt),
        "outlier_value",
    ] = "PLT"
    outliers.loc[
        (outliers["plt_avg_current"] < current_threshold_plt)
        & (outliers["srt_avg_current"] >= current_threshold_srt),
        "outlier_value",
    ] = "SRT"
    outliers.loc[
        (outliers["plt_avg_current"] < current_threshold_plt)
        & (outliers["srt_avg_current"] < current_threshold_srt),
        "outlier_value",
    ] = "N/A"

    # Drop results that are not an outlier type
    outliers = outliers[
        outliers["outlier_value"].isin(["PLT", "PLT and SRT", "SRT"])
    ].reset_index()

    outliers["plt_percent_change"] = per_diff(
        outliers["plt_avg_previous"], outliers["plt_avg_current"]
    )
    outliers["srt_percent_change"] = per_diff(
        outliers["srt_avg_previous"], outliers["srt_avg_current"]
    )

    # Drop unneeded columns
    outliers = outliers[
        [
            "page_url_cleaned",
            "pv_current",
            "pv_percent_of_total",
            "plt_avg_current",
            "plt_percent_change",
            "srt_avg_current",
            "srt_percent_change",
            "outlier_value",
        ]
    ]

    # Sort by descending pageview values and then group the top 25 results of each outlier type
    outliers = outliers.sort_values(
        ["outlier_value", "pv_current"], ascending=[True, False]
    )

    return outliers


def style_header_row(input):
    input.apply_headers_style(
        styler_obj=Styler(
            border_type=utils.borders.double,
            bg_color="#336A90",
            font_color=utils.colors.white,
            bold=True,
        )
    )


if __name__ == "__main__":
    main()
