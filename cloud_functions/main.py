"""Cloud Function to add Google Ads search terms as keywords via SA360."""
import json
import os
from typing import Any, Dict

from google.cloud import functions
from google.cloud import secretmanager
from lib import google_ads_client as google_ads_client_lib
from lib import sa360_client as sa360_client_lib
from lib import search_term_transformer as search_term_transformer_lib

_DEFAULT_CLICKS_THRESHOLD = 5
_DEFAULT_CONVERSIONS_THRESHOLD = 0
_DEFAULT_SEARCH_TERM_TOKENS_THRESHOLD = 3
_DEFAULT_SA360_ACCOUNT_NAME = 'Google'
_DEFAULT_SA360_LABEL = 'SA_add'

_SA360_SFTP_HOSTNAME = 'partnerupload.google.com'
_SA360_SFTP_PORT = 19321

# Secrets
_GOOGLE_ADS_API_CREDENTIALS = 'google_ads_api_credentials'
_SA360_SFTP_PASSWORD = 'sa360_sftp_password'

# Environment variables
_GPC_PROJECT_ID = 'GCP_PROJECT_ID'
_CUSTOMER_ID = 'CUSTOMER_ID'
_SA360_SFTP_USERNAME = 'SA360_SFTP_USERNAME'
_SA360_ACCOUNT_NAME = 'SA360_ACCOUNT_NAME'
_SA360_LABEL = 'SA360_LABEL'

_CLICKS_THRESHOLD = 'CLICKS_THRESHOLD'
_CONVERSIONS_THRESHOLD = 'CONVERSIONS_THRESHOLD'
_SEARCH_TERMS_TOKENS_THRESHOLD = 'SEARCH_TERMS_TOKENS_THRESHOLD'

_CAMPAIGN_IDS = 'CAMPAIGN_IDS'

_REQUIRED_STR_SETTINGS = {
    _GPC_PROJECT_ID: '',
    _CUSTOMER_ID: '',
    _SA360_SFTP_USERNAME: '',
    _SA360_ACCOUNT_NAME: _DEFAULT_SA360_ACCOUNT_NAME,
    _SA360_LABEL: _DEFAULT_SA360_LABEL,
}

_REQUIRED_NUMERIC_SETTINGS = {
    _CLICKS_THRESHOLD: _DEFAULT_CLICKS_THRESHOLD,
    _CONVERSIONS_THRESHOLD: _DEFAULT_CONVERSIONS_THRESHOLD,
    _SEARCH_TERMS_TOKENS_THRESHOLD: _DEFAULT_SEARCH_TERM_TOKENS_THRESHOLD,
}

_OPTIONAL_SETTINGS = [
    _CAMPAIGN_IDS,
]


def extract_and_upload_keywords(event: Dict[Any, Any],
                                context: functions.Context) -> str:
  """Cloud Function ("CF") triggered by Cloud Scheduler.

     This function orchestrates an ETL pipeline to read search terms from
     Google Ads API and upload them as Ad Group keywords to SA360 via Bulksheet.

  Args:
    event (dict): The dictionary with data specific to this type of event. The
      `@type` field maps to
      `type.googleapis.com/google.pubsub.v1.PubsubMessage`. The `data` field
      maps to the PubsubMessage data in a base64-encoded string. The
      `attributes` field maps to the PubsubMessage attributes if any is present.
    context (google.cloud.functions.Context): Metadata of triggering event
      including `event_id` which maps to the PubsubMessage messageId,
      `timestamp` which maps to the PubsubMessage publishTime, `event_type`
      which maps to `google.pubsub.topic.publish`, and `resource` which is a
      dictionary that describes the service API endpoint pubsub.googleapis.com,
      the triggering topic's name, and the triggering event type
      `type.googleapis.com/google.pubsub.v1.PubsubMessage`.

  Returns:
      The response string. Required for Google Cloud Functions.
  """
  del event
  del context

  settings = _load_settings()
  _sanitize_settings(settings)

  # Fetches search terms from Google Ads API.
  google_ads_api_credentials = _retrieve_secret(settings[_GPC_PROJECT_ID],
                                                _GOOGLE_ADS_API_CREDENTIALS)

  if not google_ads_api_credentials:
    raise ValueError(f'Secret not found in Secret Manager. '
                     f'Project: "{settings[_GPC_PROJECT_ID]}",'
                     f'Secret Name: "{_GOOGLE_ADS_API_CREDENTIALS}".')

  google_ads_api_credentials = json.loads(google_ads_api_credentials)

  google_ads_client = google_ads_client_lib.GoogleAdsClient(
      google_ads_api_credentials)

  search_terms_df = google_ads_client.get_search_terms(settings[_CUSTOMER_ID],
                                                       settings[_CAMPAIGN_IDS])

  print(f'Fetched {len(search_terms_df)} search term row(s) from Google Ads.')

  # Fetches Ad Group stats from Google Ads API.
  ad_groups_df = google_ads_client.get_ad_groups(settings[_CUSTOMER_ID],
                                                 settings[_CAMPAIGN_IDS])

  print(f'Fetched {len(ad_groups_df)} Ad Group row(s) from Google Ads.')

  # Filters search terms for uploading to SA 360.
  search_term_transformer = search_term_transformer_lib.SearchTermTransformer(
      settings[_CLICKS_THRESHOLD], settings[_CONVERSIONS_THRESHOLD],
      settings[_SEARCH_TERMS_TOKENS_THRESHOLD], settings[_SA360_ACCOUNT_NAME],
      settings[_SA360_LABEL])

  sa360_bulksheet_df = search_term_transformer.transform_search_terms_to_keywords(
      search_terms_df, ad_groups_df)

  if sa360_bulksheet_df.empty:
    # No keywords found after filtering: exits the function.
    return 'Finished: No keywords found to upload to SA 360.'

  print(f'Found {len(sa360_bulksheet_df)} row(s) to upload to SA 360.')

  # Uploads data to SA 360 via Bulksheet.
  sa_360_sftp_password = _retrieve_secret(settings[_GPC_PROJECT_ID],
                                          _SA360_SFTP_PASSWORD)

  if not sa_360_sftp_password:
    raise ValueError(f'Secret not found in Secret Manager. '
                     f'Project: "{settings[_GPC_PROJECT_ID]}",'
                     f'Secret Name: "{_SA360_SFTP_PASSWORD}".')

  sa360_client = sa360_client_lib.SA360Client(_SA360_SFTP_HOSTNAME,
                                              _SA360_SFTP_PORT,
                                              settings[_SA360_SFTP_USERNAME],
                                              sa_360_sftp_password)

  sa360_client.upload_keywords_to_sa360(sa360_bulksheet_df)

  return f'Success: Uploaded bulksheet with {len(sa360_bulksheet_df)} row(s).'


def _load_settings() -> Dict[str, str]:
  """Loads Cloud Function environment variables into settings.

  Returns:
    A dictionary of setting names to values.
  """
  settings = {}

  for str_setting_name, default in _REQUIRED_STR_SETTINGS.items():
    settings[str_setting_name] = os.environ.get(str_setting_name, default)

  for numeric_setting_name, default in _REQUIRED_NUMERIC_SETTINGS.items():
    settings[numeric_setting_name] = os.environ.get(numeric_setting_name,
                                                    default)

  for optional_setting in _OPTIONAL_SETTINGS:
    settings[optional_setting] = os.environ.get(optional_setting, '')

  return settings


def _sanitize_settings(settings: Dict[str, str]) -> None:
  """Checks and sanitizes Cloud Function settings.

  Args:
    settings: The settings for this function loaded from environment variables.

  Raises:
    ValueError: If a setting was not set correctly.
  """
  # Checks and sanitizes String settings.
  for required_str_setting in _REQUIRED_STR_SETTINGS:
    str_setting_value = settings[required_str_setting].strip()
    if not str_setting_value:
      raise ValueError(
          f'Environment variable not set: "{required_str_setting}"')
    else:
      settings[required_str_setting] = str_setting_value

  # Checks and converts numeric settings.
  for required_numeric_setting in _REQUIRED_NUMERIC_SETTINGS:
    try:
      numeric_setting_value = float(settings[required_numeric_setting])
    except ValueError as value_error:
      raise ValueError(f'Environment variable could not be converted to float: '
                       f'"{required_numeric_setting}"') from value_error

    settings[required_numeric_setting] = numeric_setting_value

  # Sanitizes campaign ids.
  campaign_ids = settings[_CAMPAIGN_IDS].strip()

  # Strips trailing comma.
  if campaign_ids and campaign_ids[-1] == ',':
    settings[_CAMPAIGN_IDS] = campaign_ids[:-1]


def _retrieve_secret(gcp_project_id: str, secret_name: str) -> str:
  """Retrieves the value of the specified secret from Secret Manager.

  Args:
    gcp_project_id: The ID for this GCP project.
    secret_name: The name of the secret to retrieve.

  Returns:
    A string containing the secret value.
  """
  secret_manager_client = secretmanager.SecretManagerServiceClient()

  secret_name = (
      f'projects/{gcp_project_id}/secrets/{secret_name}/versions/latest')
  secret_response = secret_manager_client.access_secret_version(
      request={'name': secret_name})

  return secret_response.payload.data.decode('UTF-8').strip()
