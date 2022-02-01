"""Defines class fakes for use in testing.

See class docstrings for more details.
"""

from typing import List
from unittest import mock


class FakeStreamRow:
  """A fake row from a Google Ads batch stream result."""

  def __init__(self,
               search_term: str = '',
               status: int = -1,
               conversions: int = -1,
               clicks: int = -1,
               ctr: int = -1,
               ad_group_name: str = '',
               campaign_id: str = '',
               campaign_name: str = '',
               keyword_text: str = '') -> None:
    """Initializes the FakeStreamRow."""
    self.search_term_view = mock.MagicMock()
    self.metrics = mock.MagicMock()
    self.ad_group = mock.MagicMock()
    self.campaign = mock.MagicMock()
    self.segments = mock.MagicMock()

    self.search_term_view.search_term = search_term
    self.search_term_view.status = status

    self.metrics.conversions = conversions
    self.metrics.clicks = clicks
    self.metrics.ctr = ctr

    self.ad_group.name = ad_group_name

    self.campaign.id = campaign_id
    self.campaign.name = campaign_name

    self.segments.keyword.info.text = keyword_text


class FakeStream:
  """A fake Google Ads API stream that contains a list of fake rows."""

  def __init__(self, rows: List[FakeStreamRow]) -> None:
    """Initializes the FakeStream."""
    self.results = rows
