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

"""Defines the SearchTermTransformer class for the SAKA Cloud Function.

See class docstring for more details.
"""

import decimal
from typing import Tuple

import constants
import pandas as pd


class SearchTermTransformer():
  """Class with logic for deciding keyword types of gAds search queries."""

  def __init__(self, clicks_threshold: int, conversions_threshold: int,
               search_term_tokens_threshold: int, sa_account_name: str,
               sa_label: str, keyword_landing_page: str,
               keyword_max_cpc: str) -> None:
    """Initializes the SearchTermTransformer.

    Args:
      clicks_threshold: The threshold of # of clicks that determines if the
        search term should be included in the SA360 bulksheet or not.
      conversions_threshold: The threshold of conversions that determines
        whether to skip checking CTR and clicks or not.
      search_term_tokens_threshold: The number of tokens in the search term that
        determines if the it should be included in the SA360 bulksheet or not.
      sa_account_name: The name of account for the keywords, e.g. "Google".
      sa_label: The label to add to this keyword entry, e.g. "SA_add".
      keyword_landing_page: The webpage where people end up after they click the
        ad.
      keyword_max_cpc: The maximum cost-per-click that will be added to the
        bulksheet.
    """
    self._clicks_threshold = clicks_threshold
    self._conversions_threshold = conversions_threshold
    self._search_term_tokens_threshold = search_term_tokens_threshold
    self._sa_account_name = sa_account_name
    self._sa_label = sa_label
    self._keyword_landing_page = keyword_landing_page

    if keyword_max_cpc:
      self._keyword_max_cpc = round(decimal.Decimal(keyword_max_cpc), 2)
    else:
      self._keyword_max_cpc = None

    print('Initialized Search Term Transformer class.')

  def transform_search_terms_to_keywords(self, search_results_df: pd.DataFrame,
                                         ad_groups: pd.Series) -> pd.DataFrame:
    """Filters search terms based on biz criteria and creates SA360 keywords.

    Args:
      search_results_df: The gAds search terms report in DataFrame format.
      ad_groups: A DataFrame containing Ad Group name and CTR data.

    Returns:
      A DataFrame of keywords that are intended to be uploaded to SA360.
    """
    rows = []
    bulksheet_columns = constants.SA_360_BULKSHEET_COLUMNS.copy()
    optional_columns = {}

    if self._keyword_landing_page:
      bulksheet_columns.append(constants.SA_360_COLUMN_KEYWORD_LANDING_PAGE)
      optional_columns[constants.SA_360_COLUMN_KEYWORD_LANDING_PAGE] = (
          self._keyword_landing_page)

    if self._keyword_max_cpc:
      bulksheet_columns.append(constants.SA_360_COLUMN_KEYWORD_MAX_CPC)
      optional_columns[constants.SA_360_COLUMN_KEYWORD_MAX_CPC] = (
          self._keyword_max_cpc)

    for _, search_term_row in search_results_df.iterrows():

      match_types = self._get_match_type(search_term_row, ad_groups)

      if not any(match_types):
        continue

      for match_type in match_types:
        if not match_type:
          continue

        row_to_append = {
            constants.SA_360_COLUMN_ROW_TYPE: 'keyword',
            constants.SA_360_COLUMN_ACTION: 'create',
            constants.SA_360_COLUMN_ACCOUNT: self._sa_account_name,
            constants.SA_360_COLUMN_CAMPAIGN: search_term_row['campaign_name'],
            constants.SA_360_COLUMN_AD_GROUP: search_term_row['ad_group_name'],
            constants.SA_360_COLUMN_KEYWORD: search_term_row['search_term'],
            constants.SA_360_COLUMN_KEYWORD_MATCH_TYPE: match_type,
            constants.SA_360_COLUMN_LABEL: self._sa_label,
        }

        row_to_append.update(optional_columns)
        rows.append(row_to_append)

    return pd.DataFrame(rows, columns=bulksheet_columns)

  def _get_match_type(self, search_term_row: pd.Series,
                      ad_groups: pd.Series) -> Tuple[str, str]:
    """Helper method that determines if the search term row is a keyword.

    The business logic in this method is defined per customer requirements, so
    update the logic as necessary for determining what qualifies as a keyword.

    Args:
      search_term_row: A single search term entry in a Pandas Dataframe.
      ad_groups: A DataFrame containing Ad Group name and CTR data.

    Returns:
      A Tuple representing the type of keyword to add via SA360: "broad",
      "phrase" or "exact".
    """
    ad_group_name = search_term_row['ad_group_name']

    if ad_group_name not in ad_groups['ad_group_name'].values:
      print(f'No ad group CTR data found for {ad_group_name}'
            f' (likely ad group CTR is 0).')
      ad_group_ctr = 0
    else:
      # Looks up matching row in the Ad Group DataFrame for this search
      # term row, and parses out the CTR value.
      ad_group_ctr = ad_groups.loc[ad_groups['ad_group_name'] ==
                                   ad_group_name]['ctr'].values[0]

    if search_term_row['conversions'] > self._conversions_threshold or (
        search_term_row['ctr'] > ad_group_ctr and
        search_term_row['clicks'] > self._clicks_threshold):
      search_term_tokens = search_term_row['search_term'].split()
      if len(search_term_tokens) > self._search_term_tokens_threshold:
        return constants.MATCH_TYPE_BROAD, ''
      else:
        return constants.MATCH_TYPE_EXACT, constants.MATCH_TYPE_PHRASE

    # Empty string tuple indicates that the keyword should not be added.
    return '', ''
