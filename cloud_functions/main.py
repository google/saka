# coding=utf-8
# Copyright 2022 Google LLC.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Cloud Function to add Google Ads search terms as keywords via SA360."""
import json
import os
from typing import Any, Dict, List

import constants

from google.cloud import secretmanager

from lib import google_ads_client as google_ads_client_lib
from lib import sa360_client as sa360_client_lib
from lib import search_term_transformer as search_term_transformer_lib

_REQUIRED_STR_SETTINGS: Dict[str, str] = {
    constants.GCP_PROJECT_ID: '',
    constants.CUSTOMER_ID: '',
    constants.SA360_SFTP_USERNAME: '',
    constants.SA360_ACCOUNT_NAME: constants.DEFAULT_SA360_ACCOUNT_NAME,
    constants.SA360_LABEL: constants.DEFAULT_SA360_LABEL,
}

_REQUIRED_NUMERIC_SETTINGS: Dict[str, Any] = {
    constants.CLICKS_THRESHOLD:
        constants.DEFAULT_CLICKS_THRESHOLD,
    constants.CONVERSIONS_THRESHOLD:
        constants.DEFAULT_CONVERSIONS_THRESHOLD,
    constants.SEARCH_TERMS_TOKENS_THRESHOLD:
        constants.DEFAULT_SEARCH_TERM_TOKENS_THRESHOLD,
}

_OPTIONAL_SETTINGS: List[str] = [
    constants.CAMPAIGN_IDS,
    constants.KEYWORD_LANDING_PAGE,
    constants.KEYWORD_MAX_CPC,
]


def extract_and_upload_keywords(
    event: Dict[Any, Any], context: 'google.cloud.functions.Context') -> str:
  """Cloud Function ("CF") triggered by Cloud Scheduler.

     This function orchestrates an ETL pipeline to read search terms from
     Google Ads API and upload them as Ad Group keywords to SA360 via Bulksheet.

  Args:
    event (dict): (Unused) The dictionary with data specific to this type of
      event. The `@type` field maps to
      `type.googleapis.com/google.pubsub.v1.PubsubMessage`. The `data` field
      maps to the PubsubMessage data in a base64-encoded string. The
      `attributes` field maps to the PubsubMessage attributes if any is present.
    context (google.cloud.functions.Context): (Unused) Metadata of triggering
      event including `event_id` which maps to the PubsubMessage messageId,
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
  google_ads_api_credentials = _retrieve_secret(
      settings[constants.GCP_PROJECT_ID], constants.GOOGLE_ADS_API_CREDENTIALS)

  if not google_ads_api_credentials:
    raise ValueError(f'Secret not found in Secret Manager. '
                     f'Project: "{settings[constants.GCP_PROJECT_ID]}",'
                     f'Secret Name: "{constants.GOOGLE_ADS_API_CREDENTIALS}".')

  google_ads_api_credentials = json.loads(google_ads_api_credentials)

  google_ads_client = google_ads_client_lib.GoogleAdsClient(
      google_ads_api_credentials)

  search_terms_df = google_ads_client.get_search_terms(
      settings[constants.CUSTOMER_ID], settings[constants.CAMPAIGN_IDS])

  print(f'Fetched {len(search_terms_df)} search term row(s) from Google Ads.')

  # Fetches Ad Group stats from Google Ads API.
  ad_groups_df = google_ads_client.get_ad_groups(
      settings[constants.CUSTOMER_ID], settings[constants.CAMPAIGN_IDS])

  print(f'Fetched {len(ad_groups_df)} Ad Group row(s) from Google Ads.')

  # Filters search terms for uploading to SA 360.
  search_term_transformer = search_term_transformer_lib.SearchTermTransformer(
      settings[constants.CLICKS_THRESHOLD],
      settings[constants.CONVERSIONS_THRESHOLD],
      settings[constants.SEARCH_TERMS_TOKENS_THRESHOLD],
      settings[constants.SA360_ACCOUNT_NAME],
      settings[constants.SA360_LABEL],
      settings[constants.KEYWORD_LANDING_PAGE],
      settings[constants.KEYWORD_MAX_CPC])

  sa360_bulksheet_df = search_term_transformer.transform_search_terms_to_keywords(
      search_terms_df, ad_groups_df)

  if sa360_bulksheet_df.empty:
    # No keywords found after filtering: exits the function.
    no_keywords_found_message = (
        'Finished: No keywords found to upload to SA 360.')
    print(no_keywords_found_message)
    return no_keywords_found_message

  print(f'Found {len(sa360_bulksheet_df)} row(s) to upload to SA 360.')

  # Uploads data to SA 360 via Bulksheet.
  sa_360_sftp_password = _retrieve_secret(settings[constants.GCP_PROJECT_ID],
                                          constants.SA360_SFTP_PASSWORD)

  if not sa_360_sftp_password:
    raise ValueError(f'Secret not found in Secret Manager. '
                     f'Project: "{settings[constants.GCP_PROJECT_ID]}",'
                     f'Secret Name: "{constants.SA360_SFTP_PASSWORD}".')

  sa360_client = sa360_client_lib.SA360Client(
      constants.SA360_SFTP_HOSTNAME, constants.SA360_SFTP_PORT,
      settings[constants.SA360_SFTP_USERNAME], sa_360_sftp_password)

  sa360_client.upload_keywords_to_sa360(sa360_bulksheet_df)

  success_message = f'Success: Uploaded bulksheet with {len(sa360_bulksheet_df)} row(s).'
  print(success_message)
  return success_message


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
  campaign_ids = settings[constants.CAMPAIGN_IDS].strip()

  # Strips trailing comma.
  if campaign_ids and campaign_ids[-1] == ',':
    settings[constants.CAMPAIGN_IDS] = campaign_ids[:-1]


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
