"""Defines the SearchTermTransformer class for the SAKA Cloud Function.

See class docstring for more details.
"""

import logging
from typing import Tuple

import pandas as pd

_CLICKS_THRESHOLD = 5
_CONVERSIONS_THRESHOLD = 0
_SEARCH_TERM_TOKENS_THRESHOLD = 3

_MATCH_TYPE_BROAD = 'broad'
_MATCH_TYPE_EXACT = 'exact'
_MATCH_TYPE_PHRASE = 'phrase'

_SA_360_BULKSHEET_COLUMNS = [
    'Row Type',
    'Action',
    'Account',
    'Campaign',
    'Ad Group',
    'Keyword',
    'Keyword match type',
    'Label',
]

_SA_ACCOUNT_TYPE = 'Google'
_SA_ADD_LABEL = 'SA_add'


class Error(Exception):
  """Module-level error."""


class SASearchTermTransformerError(Error):
  """Raised when error occurs in the Google Ads API Client."""


class SearchTermTransformer():
  """Class with logic for deciding keyword types of gAds search queries."""

  def __init__(self) -> None:
    """Initializes the SearchTermTransformer."""
    logging.info('Initialized Search Term Transformer class.')

  def transform_search_terms_to_keywords(
      self, search_results_df: pd.DataFrame) -> pd.DataFrame:
    """Filters search terms based on biz criteria and creates SA360 keywords.

    Args:
      search_results_df: The gAds search terms report in DataFrame format.

    Returns:
      A DataFrame of keywords that are intended to be uploaded to SA360.
    """
    sa_360_bulksheet_df = pd.DataFrame(columns=_SA_360_BULKSHEET_COLUMNS)

    for _, search_term_row in search_results_df.iterrows():

      match_types = self._get_match_type(search_term_row)

      if not any(match_types):
        continue

      for match_type in match_types:
        row = {}
        row['Row Type'] = 'keyword'
        row['Action'] = 'create'
        row['Account'] = _SA_ACCOUNT_TYPE
        row['Campaign'] = search_term_row['campaign_id']
        row['Ad Group'] = search_term_row['ad_group_name']
        row['Keyword'] = search_term_row['search_term']
        row['Keyword match type'] = match_type
        row['Label'] = _SA_ADD_LABEL

        sa_360_bulksheet_df = sa_360_bulksheet_df.append(row, ignore_index=True)

    return sa_360_bulksheet_df

  def _get_match_type(self, search_term_row) -> Tuple[str, str]:
    """Helper method that determines if the search term row is a keyword."""
    avg_ad_group_ctr = -1  # TODO: Replace this with a calculated 30-day ctr average.
    if search_term_row['conversions'] > _CONVERSIONS_THRESHOLD or (
        search_term_row['ctr'] > avg_ad_group_ctr and
        search_term_row['clicks'] > _CLICKS_THRESHOLD):
      search_term_tokens = search_term_row['search_term'].split()
      if len(search_term_tokens) > _SEARCH_TERM_TOKENS_THRESHOLD:
        return _MATCH_TYPE_BROAD, ''
      else:
        return _MATCH_TYPE_PHRASE, _MATCH_TYPE_EXACT

    # Empty string tuple indicates that the keyword should not be added.
    return '', ''
