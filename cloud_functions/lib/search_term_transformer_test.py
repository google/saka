"""Unit tests for the Search Term Transformer Class."""
import os
import pandas as pd

import unittest.mock as mock

from absl.testing import parameterized
from google.api_core import exceptions
import search_term_transformer

_MATCH_TYPE_BROAD = 'broad'
_MATCH_TYPE_EXACT = 'exact'
_MATCH_TYPE_PHRASE = 'phrase'

_TEST_SEARCH_REPORT_COLUMNS = [
    'search_term', 'status', 'conversions', 'clicks', 'ad_group_name',
    'campaign_id', 'campaign_name', 'ctr', 'keyword_text'
]


class SearchTermTransformerTest(parameterized.TestCase):

  def setUp(self):
    super().setUp()

  @parameterized.named_parameters([{
    'testcase_name': 'cv > 0 and more than three tokens',
    'clicks': 6,
    'conversions': 1,
    'ctr': 2,
    'search_term': 'more than three tokens',
    'status': 'NONE'
  }, {
    'testcase_name': 'cv <= 0 but ctr and clicks over threshold and tokens > 3',
    'clicks': 6,
    'conversions': 0,
    'ctr': 2,
    'search_term': 'more than three tokens',
    'status': 'UNKNOWN'
  }])
  def test_import_transform_search_terms_to_keywords_returns_broad_keywords(
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
        'keyword_text': ''
    }
    test_search_report_df = pd.DataFrame(
        [test_search_report], columns=_TEST_SEARCH_REPORT_COLUMNS)
    test_transformer = search_term_transformer.SearchTermTransformer()

    #Act
    results = test_transformer.transform_search_terms_to_keywords(
        test_search_report_df)

    #Assert
    self.assertEqual(results.loc[0].at['Keyword match type'], _MATCH_TYPE_BROAD)

  @parameterized.named_parameters([{
    'testcase_name': 'cv > 0 but less than 4 tokens',
    'clicks': 6,
    'conversions': 1,
    'ctr': 2,
    'search_term': 'under four tokens',
    'status': 'ADDED'
  }, {
    'testcase_name': 'cv <= 0 but ctr and clicks over threshold and tokens < 4',
    'clicks': 6,
    'conversions': 0,
    'ctr': 2,
    'search_term': 'under four tokens',
    'status': 'EXCLUDED'
  }])
  def test_import_transform_search_terms_to_keywords_returns_phrase_and_exact_keywords(
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
      'keyword_text': ''
    }
    test_search_report_df = pd.DataFrame(
      [test_search_report], columns=_TEST_SEARCH_REPORT_COLUMNS)
    test_transformer = search_term_transformer.SearchTermTransformer()

    # Act
    results = test_transformer.transform_search_terms_to_keywords(
      test_search_report_df)

    # Assert
    self.assertEqual(results.loc[0].at['Keyword match type'], _MATCH_TYPE_PHRASE)

  @parameterized.named_parameters([{
    'testcase_name': 'cv <= 0 and ctr >= threshold but clicks < 6',
    'clicks': 4,
    'conversions': 0,
    'ctr': 2,
    'search_term': 'under four tokens',
    'status': 'ADDED'
  }, {
    'testcase_name': 'cv <= 0 but ctr < threshold',
    'clicks': 6,
    'conversions': 0,
    'ctr': -2,
    'search_term': 'under four tokens',
    'status': 'EXCLUDED'
  }])
  def test_import_transform_search_terms_to_keywords_skips_unqualified_rows(
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
      'keyword_text': ''
    }
    test_search_report_df = pd.DataFrame(
      [test_search_report], columns=_TEST_SEARCH_REPORT_COLUMNS)
    test_transformer = search_term_transformer.SearchTermTransformer()

    # Act
    results = test_transformer.transform_search_terms_to_keywords(
      test_search_report_df)

    # Assert
    self.assertTrue(results.empty)