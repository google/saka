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

"""Defines the GoogleAdsClient class for the SAKA Cloud Function.

See class docstring for more details.
"""

import logging
from typing import Any, Dict

from google.ads.googleads import client as google_ads_client
from google.ads.googleads import errors as google_ads_errors
import pandas as pd

_CUSTOMER_ID_LENGTH = 10
_DATE_RANGE = 'LAST_30_DAYS'
_GOOGLE_ADS_SERVICE_NAME = 'GoogleAdsService'
_SEARCH_REQUEST_TYPE = 'SearchGoogleAdsStreamRequest'
_SEARCH_REPORT_COLUMNS = [
    'search_term', 'status', 'conversions', 'clicks', 'ad_group_name',
    'campaign_id', 'campaign_name', 'ctr', 'keyword_text'
]
_AD_GROUP_COLUMNS = ['ad_group_name', 'ctr']

_SEARCH_REPORT_QUERY = f"""
  SELECT
    search_term_view.search_term,
    search_term_view.status,
    metrics.conversions,
    metrics.clicks,
    ad_group.name,
    campaign.id,
    campaign.name,
    metrics.ctr,
    segments.keyword.info.text
   FROM
    search_term_view
   WHERE
    segments.date DURING {_DATE_RANGE}
    AND search_term_view.status IN ('NONE', 'UNKNOWN')
"""

_AD_GROUP_QUERY = f"""
  SELECT
    ad_group.name,
    metrics.ctr
  FROM
    ad_group
  WHERE
    segments.date DURING {_DATE_RANGE}
"""


class Error(Exception):
  """Module-level error."""


class SAGoogleAdsClientError(Error):
  """Raised when error occurs in the Google Ads API Client."""


class GoogleAdsClient():
  """Client for fetching search terms report via Google Ads API.

  Using the client requires a Google Ads API OAuth2 dictionary to be defined in
  Secret Manager. The format of this dictionary is defined here:
  https://developers.google.com/google-ads/api/docs/client-libs/python/configuration#configuration_using_a_dict
  """

  def __init__(self, gads_credentials: Dict[str, str]) -> None:
    """Initializes the GoogleAdsClient.

    Args:
      gads_credentials: A dictionary containing credential information that can
        be used to authenticate with Google Ads API. See the following page for
        details on dictionary format:
        https://developers.google.com/google-ads/api/docs/client-libs/python/configuration#configuration_using_a_dict
    """

    try:
      self._gads_client = google_ads_client.GoogleAdsClient.load_from_dict(
          gads_credentials)
    except google_ads_errors.GoogleAdsException as google_ads_exception:
      logging.error(
          RuntimeError('Failed to initialize API client. '
                       'Check API credentials are correct. '
                       f'Error: {google_ads_exception}'))
      raise SAGoogleAdsClientError(
          'Failed to initialize API client. '
          'Check API credentials are correct. '
          f'Error: {google_ads_exception}') from google_ads_exception

    self._gads_service = self._gads_client.get_service(_GOOGLE_ADS_SERVICE_NAME)

    print('Initialized Google Ads API client.')

  def get_search_terms(self,
                       customer_id: str,
                       campaign_ids: str = None) -> pd.DataFrame:
    """Returns a search term report in a Pandas DataFrame.

    Args:
      customer_id: A 10-digit string representation of a Google Ads customer ID.
      campaign_ids: A comma-separated string of campaign_ids to query when
      retrieving search terms. If empty, all campaigns will be queried.

    Returns:
      A search term report in a Pandas DataFrame with the following columns:
      search_term, status, conversions, clicks, ad_group_id, campaign_id,
      campaign_name, ctr, keyword_text.

    Raises:
      SAGoogleAdsClientError: if args are incorrect or an error is encountered
        while processing the request.
    """
    self._validate_customer_id(customer_id)

    search_request = self._build_search_request(_SEARCH_REPORT_QUERY,
                                                customer_id,
                                                campaign_ids)

    try:
      stream = self._gads_service.search_stream(search_request)

      results = []

      for batch in stream:
        for row in batch.results:
          result = {
              'search_term': row.search_term_view.search_term,
              'status': row.search_term_view.status,
              'conversions': float(row.metrics.conversions),
              'clicks': float(row.metrics.clicks),
              'ad_group_name': row.ad_group.name,
              'campaign_id': row.campaign.id,
              'campaign_name': row.campaign.name,
              'ctr': float(row.metrics.ctr),
              'keyword_text': row.segments.keyword.info.text,
          }

          results.append(result)
    except google_ads_errors.GoogleAdsException as google_ads_exception:
      logging.error(RuntimeError(
          f'Error: Customer: {customer_id}, campaigns: {campaign_ids}. '
          f'Failed to get search terms report.Error: {google_ads_exception}'))
      raise SAGoogleAdsClientError(
          f'Error: Customer: {customer_id}, campaigns: {campaign_ids}. '
          f'Failed to get search terms report.Error: {google_ads_exception}'
      ) from google_ads_exception

    print('Successfully fetched search terms report.')

    return pd.DataFrame(results, columns=_SEARCH_REPORT_COLUMNS)

  def get_ad_groups(self,
                    customer_id: str,
                    campaign_ids: str = None) -> pd.DataFrame:
    """Returns an ad group report in a Pandas DataFrame.

    Args:
      customer_id: A 10-digit string representation of a Google Ads customer ID.
      campaign_ids: A comma-separated string of campaign_ids to query when
        retrieving ad groups. If empty, all campaigns will be queried.

    Returns:
      An ad group report in a Pandas DataFrame with the following columns:
      ad group name, ctr.

    Raises:
      SAGoogleAdsClientError: if args are incorrect or an error is encountered
        while processing the request.
    """
    self._validate_customer_id(customer_id)

    search_request = self._build_search_request(_AD_GROUP_QUERY,
                                                customer_id,
                                                campaign_ids)

    try:
      stream = self._gads_service.search_stream(search_request)

      results = []

      for batch in stream:
        for row in batch.results:
          result = {
              'ad_group_name': row.ad_group.name,
              'ctr': float(row.metrics.ctr),
          }
          results.append(result)
    except google_ads_errors.GoogleAdsException as google_ads_exception:
      logging.error(RuntimeError(
          f'Error: Customer: {customer_id}, campaigns: {campaign_ids}. '
          f'Failed to get ad group report.Error: {google_ads_exception}'))
      raise SAGoogleAdsClientError(
          f'Error: Customer: {customer_id}, campaigns: {campaign_ids}. '
          f'Failed to get ad group report.Error: {google_ads_exception}'
      ) from google_ads_exception

    print('Successfully fetched ad group report.')

    return pd.DataFrame(results, columns=_AD_GROUP_COLUMNS)

  def _validate_customer_id(self, customer_id: str) -> None:
    """Raises an exception if the customer id is not valid.

    Args:
      customer_id: A 10-digit string representation of a Google Ads customer ID.
    """
    if len(customer_id) != _CUSTOMER_ID_LENGTH or not customer_id.isdigit():
      raise SAGoogleAdsClientError(
          f'Error: customer_id should be a 10-digit string: {customer_id}')

  def _build_search_request(self,
                            query: str,
                            customer_id: str,
                            campaign_ids: str = None) -> Any:
    """Builds and returns a search request that can be sent to Google Ads API.

    Args:
      query: The Google Ads API query.
      customer_id: A 10-digit string representation of a Google Ads customer ID.
      campaign_ids: A comma-separated string of campaign_ids to query. If empty,
        all campaigns will be queried.

    Returns:
      A search request that can be sent to Google Ads API.
    """

    if campaign_ids:
      query += f' AND campaign.id IN ({campaign_ids})'

    search_request = self._gads_client.get_type(_SEARCH_REQUEST_TYPE)
    search_request.customer_id = customer_id
    search_request.query = query

    return search_request
