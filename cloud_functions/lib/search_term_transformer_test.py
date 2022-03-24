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
"""Unit tests for the Search Term Transformer Class."""

import decimal
from typing import List

from absl.testing import parameterized
import constants
import pandas as pd
import search_term_transformer as search_term_transformer_lib

_TEST_SEARCH_REPORT_COLUMNS = [
    'search_term', 'status', 'conversions', 'clicks', 'ad_group_name',
    'campaign_id', 'campaign_name', 'ctr', 'keyword_text'
]

_TEST_AD_GROUP_COLUMNS = ['ad_group_name', 'ctr']

_EXPECTED_SA_360_BULKSHEET_COLUMNS = [
    'Row type',
    'Action',
    'Account',
    'Campaign',
    'Ad group',
    'Keyword',
    'Keyword match type',
    'Label',
]

_TEST_AD_GROUP_DF = pd.DataFrame([{
    'ad_group_name': 'test_ad_group',
    'ctr': 0.5,
}], columns=_TEST_AD_GROUP_COLUMNS)


def _build_expected_df(keyword: str,
                       match_types: List[str],
                       keyword_landing_page='',
                       keyword_max_cpc='') -> pd.DataFrame:
  """Builds a DataFrame for test assertions.

  Args:
    keyword: The expected keyword in the DF transformed from a search term.
    match_types: A list of one of 'broad', 'exact', or 'phrase' in the DF.
    keyword_landing_page: The webpage where people end up after they click the
      ad.
    keyword_max_cpc: The maximum cost-per-click that will be added to the
      bulksheet.

  Returns:
    A DataFrame representing the expected test output.
  """
  rows = []

  bulksheet_columns = constants.SA_360_BULKSHEET_COLUMNS.copy()
  optional_columns = {}

  if keyword_landing_page:
    bulksheet_columns.append(constants.SA_360_COLUMN_KEYWORD_LANDING_PAGE)
    optional_columns[constants.SA_360_COLUMN_KEYWORD_LANDING_PAGE] = (
        keyword_landing_page)

  if keyword_max_cpc:
    bulksheet_columns.append(constants.SA_360_COLUMN_KEYWORD_MAX_CPC)
    keyword_max_cpc = round(decimal.Decimal(keyword_max_cpc), 2)
    optional_columns[constants.SA_360_COLUMN_KEYWORD_MAX_CPC] = (
        keyword_max_cpc)

  for match_type in match_types:
    row_to_append = {
        constants.SA_360_COLUMN_ROW_TYPE: 'keyword',
        constants.SA_360_COLUMN_ACTION: 'create',
        constants.SA_360_COLUMN_ACCOUNT: 'Google',
        constants.SA_360_COLUMN_CAMPAIGN: 'test_campaign',
        constants.SA_360_COLUMN_AD_GROUP: 'test_ad_group',
        constants.SA_360_COLUMN_KEYWORD: keyword,
        constants.SA_360_COLUMN_KEYWORD_MATCH_TYPE: match_type,
        constants.SA_360_COLUMN_LABEL: 'SA_add',
    }

    row_to_append.update(optional_columns)

    rows.append(row_to_append)

  return pd.DataFrame(rows, columns=bulksheet_columns)


class SearchTermTransformerTest(parameterized.TestCase):

  @parameterized.named_parameters([{
      'testcase_name': 'cv > 0 and more than three tokens',
      'clicks': 6,
      'conversions': 1,
      'ctr': 1.0,
      'search_term': 'more than three tokens',
      'status': 'NONE',
  }, {
      'testcase_name': 'cv <= 0, ctr and clicks over threshold, and tokens > 3',
      'clicks': 6,
      'conversions': 0,
      'ctr': 1.0,
      'search_term': 'more than three tokens',
      'status': 'UNKNOWN',
  }])
  def test_transform_search_terms_to_keywords_returns_broad_keywords(
      self, status, search_term, ctr, conversions, clicks):
    # Arrange
    test_search_report = {
        'search_term': search_term,
        'status': status,
        'conversions': conversions,
        'clicks': clicks,
        'ad_group_name': 'test_ad_group',
        'campaign_id': '12345',
        'campaign_name': 'test_campaign',
        'ctr': ctr,
        'keyword_text': '',
    }
    test_search_report_df = pd.DataFrame([test_search_report],
                                         columns=_TEST_SEARCH_REPORT_COLUMNS)
    keyword_landing_page = ''
    keyword_max_cpc = ''
    sa_360_account_name = 'Google'

    search_term_transformer = search_term_transformer_lib.SearchTermTransformer(
        constants.DEFAULT_CLICKS_THRESHOLD,
        constants.DEFAULT_CONVERSIONS_THRESHOLD,
        constants.DEFAULT_SEARCH_TERM_TOKENS_THRESHOLD,
        sa_360_account_name,
        constants.DEFAULT_SA360_LABEL,
        keyword_landing_page,
        keyword_max_cpc)
    expected_df = _build_expected_df(search_term, [constants.MATCH_TYPE_BROAD])

    # Act
    actual_df = search_term_transformer.transform_search_terms_to_keywords(
        test_search_report_df, _TEST_AD_GROUP_DF)

    # Assert
    pd.testing.assert_frame_equal(actual_df, expected_df)

  @parameterized.named_parameters([{
      'testcase_name': 'cv > 0 but less than 4 tokens',
      'clicks': 6,
      'conversions': 1,
      'ctr': 1.0,
      'search_term': 'under four tokens',
      'status': 'ADDED',
  }, {
      'testcase_name': 'cv <= 0, ctr and clicks over threshold, and tokens < 4',
      'clicks': 6,
      'conversions': 0,
      'ctr': 1.0,
      'search_term': 'under four tokens',
      'status': 'EXCLUDED',
  }])
  def test_transform_search_terms_to_keywords_returns_exact_and_phrase_keywords(
      self, status, search_term, ctr, conversions, clicks):
    # Arrange
    test_search_report = {
        'search_term': search_term,
        'status': status,
        'conversions': conversions,
        'clicks': clicks,
        'ad_group_name': 'test_ad_group',
        'campaign_id': '12345',
        'campaign_name': 'test_campaign',
        'ctr': ctr,
        'keyword_text': '',
    }
    test_search_report_df = pd.DataFrame([test_search_report],
                                         columns=_TEST_SEARCH_REPORT_COLUMNS)
    keyword_landing_page = ''
    keyword_max_cpc = ''
    sa_360_account_name = 'Google'
    test_transformer = search_term_transformer_lib.SearchTermTransformer(
        constants.DEFAULT_CLICKS_THRESHOLD,
        constants.DEFAULT_CONVERSIONS_THRESHOLD,
        constants.DEFAULT_SEARCH_TERM_TOKENS_THRESHOLD,
        sa_360_account_name,
        constants.DEFAULT_SA360_LABEL,
        keyword_landing_page,
        keyword_max_cpc)
    expected_df = _build_expected_df(
        search_term, [constants.MATCH_TYPE_EXACT, constants.MATCH_TYPE_PHRASE])

    # Act
    actual_df = test_transformer.transform_search_terms_to_keywords(
        test_search_report_df, _TEST_AD_GROUP_DF)

    # Assert
    pd.testing.assert_frame_equal(actual_df, expected_df)

  @parameterized.named_parameters([{
      'testcase_name': 'cv <= 0 and ctr >= threshold but clicks < 6',
      'clicks': 4,
      'conversions': 0,
      'ctr': 1.0,
      'search_term': 'under four tokens',
      'status': 'ADDED',
  }, {
      'testcase_name': 'cv <= 0 but ctr < threshold',
      'clicks': 6,
      'conversions': 0,
      'ctr': 0.4,
      'search_term': 'under four tokens',
      'status': 'EXCLUDED',
  }])
  def test_transform_search_terms_to_keywords_skips_unqualified_rows(
      self, status, search_term, ctr, conversions, clicks):
    # Arrange
    test_search_report = {
        'search_term': search_term,
        'status': status,
        'conversions': conversions,
        'clicks': clicks,
        'ad_group_name': 'test_ad_group',
        'campaign_id': '12345',
        'campaign_name': 'test_campaign',
        'ctr': ctr,
        'keyword_text': '',
    }
    test_search_report_df = pd.DataFrame([test_search_report],
                                         columns=_TEST_SEARCH_REPORT_COLUMNS)
    keyword_landing_page = ''
    keyword_max_cpc = ''
    sa_360_account_name = 'Google'
    test_transformer = search_term_transformer_lib.SearchTermTransformer(
        constants.DEFAULT_CLICKS_THRESHOLD,
        constants.DEFAULT_CONVERSIONS_THRESHOLD,
        constants.DEFAULT_SEARCH_TERM_TOKENS_THRESHOLD,
        sa_360_account_name,
        constants.DEFAULT_SA360_LABEL,
        keyword_landing_page,
        keyword_max_cpc)

    # Act
    results = test_transformer.transform_search_terms_to_keywords(
        test_search_report_df, _TEST_AD_GROUP_DF)

    # Assert
    self.assertTrue(results.empty)

  def test_transform_search_terms_to_keywords_empty_input(self):
    # Arrange
    keyword_landing_page = ''
    keyword_max_cpc = ''
    sa_360_account_name = 'Google'
    test_transformer = search_term_transformer_lib.SearchTermTransformer(
        constants.DEFAULT_CLICKS_THRESHOLD,
        constants.DEFAULT_CONVERSIONS_THRESHOLD,
        constants.DEFAULT_SEARCH_TERM_TOKENS_THRESHOLD,
        sa_360_account_name,
        constants.DEFAULT_SA360_LABEL,
        keyword_landing_page,
        keyword_max_cpc)

    # Act
    results = test_transformer.transform_search_terms_to_keywords(
        pd.DataFrame(columns=_TEST_SEARCH_REPORT_COLUMNS),
        pd.DataFrame(columns=_TEST_AD_GROUP_COLUMNS))

    # Assert
    self.assertTrue(results.empty)

  @parameterized.named_parameters([{
      'testcase_name': 'landing page set, max cpc empty',
      'keyword_landing_page': 'www.google.com/saka',
      'keyword_max_cpc': '',
  }, {
      'testcase_name': 'max cpc set, landing page empty',
      'keyword_landing_page': '',
      'keyword_max_cpc': '5',
  }, {
      'testcase_name': 'landing page set, max cpc set',
      'keyword_landing_page': 'www.google.com/saka',
      'keyword_max_cpc': '5',
  }])
  def test_transform_search_terms_to_keywords_optional_columns(
      self, keyword_landing_page, keyword_max_cpc):
    # Arrange
    search_term = 'more than three tokens'
    test_search_report = {
        'search_term': search_term,
        'status': 'NONE',
        'conversions': 1,
        'clicks': 1.0,
        'ad_group_name': 'test_ad_group',
        'campaign_id': '12345',
        'campaign_name': 'test_campaign',
        'ctr': 2.0,
        'keyword_text': '',
    }
    sa_360_account_name = 'Google'
    test_search_report_df = pd.DataFrame([test_search_report],
                                         columns=_TEST_SEARCH_REPORT_COLUMNS)

    search_term_transformer = search_term_transformer_lib.SearchTermTransformer(
        constants.DEFAULT_CLICKS_THRESHOLD,
        constants.DEFAULT_CONVERSIONS_THRESHOLD,
        constants.DEFAULT_SEARCH_TERM_TOKENS_THRESHOLD,
        sa_360_account_name,
        constants.DEFAULT_SA360_LABEL,
        keyword_landing_page,
        keyword_max_cpc)
    expected_df = _build_expected_df(search_term,
                                     [constants.MATCH_TYPE_BROAD],
                                     keyword_landing_page,
                                     keyword_max_cpc)

    # Act
    actual_df = search_term_transformer.transform_search_terms_to_keywords(
        test_search_report_df, _TEST_AD_GROUP_DF)

    # Assert
    pd.testing.assert_frame_equal(actual_df, expected_df)
