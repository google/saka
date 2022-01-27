"""Defines the SearchTermTransformer class for the SAKA Cloud Function.

See class docstring for more details.
"""

from typing import Tuple

import pandas as pd

_AVG_AD_GROUP_CTR = -1

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


class SearchTermTransformer():
  """Class with logic for deciding keyword types of gAds search queries."""

  def __init__(self, clicks_threshold: int, conversions_threshold: int,
               search_term_tokens_threshold: int, sa_account_type: str,
               sa_label: str) -> None:
    """Initializes the SearchTermTransformer.

    Args:
      clicks_threshold: The threshold of # of clicks that determines if the
        search term should be included in the SA360 bulksheet or not.
      conversions_threshold: The threshold of conversions that determines
        whether to skip checking CTR and clicks or not.
      search_term_tokens_threshold: The number of tokens in the search term that
        determines if the it should be included in the SA360 bulksheet or not.
      sa_account_type: The type of account for the keyword, e.g. "Google".
      sa_label: The label to add to this keyword entry, e.g. "SA_add".
    """
    self._clicks_threshold = clicks_threshold
    self._conversions_threshold = conversions_threshold
    self._search_term_tokens_threshold = search_term_tokens_threshold
    self._sa_account_type = sa_account_type
    self._sa_label = sa_label

    print('Initialized Search Term Transformer class.')

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
        if not match_type:
          continue

        row = {
            'Row Type': 'keyword',
            'Action': 'create',
            'Account': self._sa_account_type,
            'Campaign': search_term_row['campaign_id'],
            'Ad Group': search_term_row['ad_group_name'],
            'Keyword': search_term_row['search_term'],
            'Keyword match type': match_type,
            'Label': self._sa_label
        }

        sa_360_bulksheet_df = sa_360_bulksheet_df.append(row, ignore_index=True)

    return sa_360_bulksheet_df

  def _get_match_type(self, search_term_row: pd.Series) -> Tuple[str, str]:
    """Helper method that determines if the search term row is a keyword.

    The business logic in this method is defined per customer requirements, so
    update the logic as necessary for determining what qualifies as a keyword.

    Args:
      search_term_row: A single search term entry in a Pandas Dataframe.

    Returns:
      A Tuple representing the type of keyword to add via SA360: "broad",
      "phrase" or "exact".
    """
    if search_term_row['conversions'] > self._conversions_threshold or (
        search_term_row['ctr'] > _AVG_AD_GROUP_CTR and
        search_term_row['clicks'] > self._clicks_threshold):
      search_term_tokens = search_term_row['search_term'].split()
      if len(search_term_tokens) > self._search_term_tokens_threshold:
        return _MATCH_TYPE_BROAD, ''
      else:
        return _MATCH_TYPE_EXACT, _MATCH_TYPE_PHRASE

    # Empty string tuple indicates that the keyword should not be added.
    return '', ''