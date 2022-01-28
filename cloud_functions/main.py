"""Cloud Function for SAKA ETL pipeline: gAds search terms to SA360 keywords."""
import json
import os

from typing import Dict, Tuple

from google.cloud import secretmanager

from lib import google_ads_client as google_ads_client_lib
from lib import search_term_transformer as search_term_transformer_lib

_DEFAULT_CLICKS_THRESHOLD = 5
_DEFAULT_CONVERSIONS_THRESHOLD = 0
_DEFAULT_SEARCH_TERM_TOKENS_THRESHOLD = 3
_DEFAULT_SA_ACCOUNT_TYPE = 'Google'
_DEFAULT_SA_LABEL = 'SA_add'


def saka_etl_function(request) -> None:
  """Cloud Function ("CF") triggered by Cloud Scheduler.

     This function orchestrates an ETL pipeline from Google Ads API to SA360.

  Args:
      request: The request sent to the Cloud Function.

  Raises:
    RuntimeError: A dependency was not found, requiring this CF to exit.

  Returns:
      None. The output is written to Cloud logging.
  """
  del request
  (gcp_project_id, customer_id, campaign_ids, clicks_threshold,
   conversions_threshold, search_term_tokens_threshold, sa_account_type,
   sa_label) = retrieve_environment_variables()
  gads_credentials = retrieve_gads_credentials(gcp_project_id)
  google_ads_client = google_ads_client_lib.GoogleAdsClient(gads_credentials)

  # TODO(akort) validate/convert types of env vars to ints if necessary

  search_terms_df = google_ads_client.get_search_terms(customer_id,
                                                       campaign_ids)

  search_term_transformer = search_term_transformer_lib.SearchTermTransformer(
      clicks_threshold, conversions_threshold, search_term_tokens_threshold,
      sa_account_type, sa_label)
  sa360_bulksheet_df = search_term_transformer.transform_search_terms_to_keywords(
      search_terms_df)

  print(f'Search terms df: {search_terms_df}')

  print(f'SA360 Bulksheet df: {sa360_bulksheet_df}')

  print(f'SA360 Bulksheet df CSV: {sa360_bulksheet_df.to_csv()}')

  return 'Exiting SAKA Cloud Function.'


def retrieve_gads_credentials(gcp_project_id: str) -> Dict[str, str]:
  """Helper function that gets Google Ads API credentials from Secret Manager."""
  secret_manager_client = secretmanager.SecretManagerServiceClient()

  # Access the secret version.
  secret_name = (
      f'projects/{gcp_project_id}/secrets/gads_api_yaml_creds/versions/latest')
  secret_response = secret_manager_client.access_secret_version(
      request={'name': secret_name})
  secret_contents = secret_response.payload.data.decode('UTF-8')
  gads_credentials = json.loads(secret_contents)

  print('The credentials retrieved from Secret Manager was: {}'.format(
      gads_credentials))

  return gads_credentials


def retrieve_environment_variables() -> Tuple[str, ...]:
  """Helper function that reads in the CF's environment variables."""
  gcp_project_id = os.environ.get('GCP_PROJECT_ID',
                                  'GCP_PROJECT_ID is not set.')
  customer_id = os.environ.get('CUSTOMER_ID', 'CUSTOMER_ID is not set.')
  campaign_ids = os.environ.get('CAMPAIGN_IDS', '')
  clicks_threshold = os.environ.get('CLICKS_THRESHOLD',
                                    _DEFAULT_CLICKS_THRESHOLD)
  conversions_threshold = os.environ.get('CONVERSIONS_THRESHOLD',
                                         _DEFAULT_CONVERSIONS_THRESHOLD)
  search_term_tokens_threshold = os.environ.get(
      'SEARCH_TERM_TOKENS_THRESHOLD', _DEFAULT_SEARCH_TERM_TOKENS_THRESHOLD)
  sa_account_type = os.environ.get('SA_ACCOUNT_TYPE', _DEFAULT_SA_ACCOUNT_TYPE)
  sa_label = os.environ.get('SA_LABEL', _DEFAULT_SA_LABEL)

  return gcp_project_id, customer_id, campaign_ids, clicks_threshold, conversions_threshold, search_term_tokens_threshold, sa_account_type, sa_label
