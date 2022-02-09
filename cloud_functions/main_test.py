"""Unit tests for the SAKA ETL main Cloud Function."""
import os
import unittest.mock as mock

from absl.testing import parameterized
import main
import pandas as pd

_TEST_GCP_PROJECT_ID = 'test-gcp-project-id'
_TEST_CUSTOMER_ID = '12345'
_TEST_CAMPAIGN_IDS = '123,456'
_TEST_CLICKS_THRESHOLD = '1'
_TEST_CONVERSIONS_THRESHOLD = '1'
_SEARCH_TERM_TOKENS_THRESHOLD = '3'
_TEST_SA360_ACCOUNT_TYPE = 'Google'
_TEST_SA360_LABEL = 'SA_add'
_TEST_SA360_SFTP_USERNAME = 'Test Username'

_TEST_SEARCH_TERM_DF = pd.DataFrame([{
    'search_term': 'Fake search term',
    'status': 1,
    'conversions': 5.0,
    'clicks': 10.0,
    'ad_group_name': 'Fake ad group',
    'campaign_id': 12345,
    'campaign_name': 'Fake campaign',
    'ctr': 0.5,
    'keyword_text': 'Fake keyword'
}], columns=['search_term', 'status', 'conversions', 'clicks', 'ad_group_name',
             'campaign_id', 'campaign_name', 'ctr', 'keyword_text'])

_TEST_AD_GROUP_DF = pd.DataFrame([{
    'ad_group_name': 'Fake ad group',
    'ctr': 0.3
}], columns=['ad_group_name', 'ctr'])

_TEST_TRANSFORMED_DF = pd.DataFrame([{
    'Row Type': 'keyword',
    'Action': 'create',
    'Account': 'Google',
    'Campaign': '12345',
    'Ad Group': 'test_ad_group',
    'Keyword': 'Fake keyword',
    'Keyword match type': 'BROAD',
    'Label': 'SA_add'
}], columns=['Row Type', 'Action', 'Account', 'Campaign', 'Ad Group', 'Keyword',
             'Keyword match type', 'Label'])


@mock.patch.dict(
    os.environ, {
        'GCP_PROJECT_ID': _TEST_GCP_PROJECT_ID,
        'CUSTOMER_ID': _TEST_CUSTOMER_ID,
        'CAMPAIGN_IDS': _TEST_CAMPAIGN_IDS,
        'CLICKS_THRESHOLD': _TEST_CLICKS_THRESHOLD,
        'CONVERSIONS_THRESHOLD': _TEST_CONVERSIONS_THRESHOLD,
        'SEARCH_TERM_TOKENS_THRESHOLD': _SEARCH_TERM_TOKENS_THRESHOLD,
        'SA360_ACCOUNT_TYPE': _TEST_SA360_ACCOUNT_TYPE,
        'SA360_LABEL': _TEST_SA360_LABEL,
        'SA360_SFTP_USERNAME': _TEST_SA360_SFTP_USERNAME,
    })
class MainTest(parameterized.TestCase):

  @parameterized.named_parameters([
      {
          'testcase_name': 'Rows uploaded to SA360',
          'transformed_df': _TEST_TRANSFORMED_DF,
          'expected_sa360_call_count': 1,
          'expected_response':
              f'Success: Uploaded bulksheet with {len(_TEST_TRANSFORMED_DF)} '
              'row(s).'
      },
      {
          'testcase_name': 'No rows found does not upload to SA360',
          'transformed_df': pd.DataFrame(),
          'expected_sa360_call_count': 0,
          'expected_response':
              'Finished: No keywords found to upload to SA 360.'
      },
  ])
  @mock.patch('lib.google_ads_client.GoogleAdsClient')
  @mock.patch('lib.search_term_transformer.SearchTermTransformer')
  @mock.patch('lib.sa360_client.SA360Client')
  @mock.patch('main._retrieve_secret')
  def test_extract_and_upload_keywords(self,
                                       mock_retrieve_secret,
                                       mock_sa360_client,
                                       mock_search_term_transformer,
                                       mock_google_ads_client,
                                       transformed_df,
                                       expected_sa360_call_count,
                                       expected_response):
    """Tests extract_and_upload_keywords."""
    # Arrange
    data = {'name': 'test'}
    test_request = mock.Mock(get_json=mock.Mock(return_value=data), args=data)

    mock_retrieve_secret.return_value = '["Test Secret"]'
    mock_google_ads_client.return_value.get_search_terms.return_value = _TEST_SEARCH_TERM_DF
    mock_google_ads_client.return_value.get_ad_groups.return_value = _TEST_AD_GROUP_DF
    mock_search_term_transformer.return_value.transform_search_terms_to_keywords.return_value = transformed_df

    # Act
    actual_response = main.extract_and_upload_keywords(test_request)
    actual_sa360_call_count = (
        mock_sa360_client.return_value.upload_keywords_to_sa360.call_count)

    # Assert
    self.assertEqual(expected_sa360_call_count, actual_sa360_call_count)
    self.assertEqual(expected_response, actual_response)

  @parameterized.named_parameters([
      {
          'testcase_name': 'Google Ads API Credentials Missing',
          'get_secret_call_results': ['', 'Test SFTP password'],
      },
      {
          'testcase_name': 'SA360 SFTP password Missing',
          'get_secret_call_results': ['["Test Secret"]', ''],
      }
  ])
  @mock.patch('lib.google_ads_client.GoogleAdsClient')
  @mock.patch('lib.search_term_transformer.SearchTermTransformer')
  @mock.patch('main._retrieve_secret')
  def test_extract_and_upload_keywords_secrets_not_found(
      self,
      mock_retrieve_secret,
      mock_search_term_transformer,
      mock_google_ads_client,
      get_secret_call_results):
    """Tests error raised when secrets are not set."""
    # Arrange
    data = {'name': 'test'}
    test_request = mock.Mock(get_json=mock.Mock(return_value=data), args=data)

    mock_google_ads_client.return_value.get_search_terms.return_value = _TEST_SEARCH_TERM_DF
    mock_google_ads_client.return_value.get_ad_groups.return_value = _TEST_AD_GROUP_DF
    mock_search_term_transformer.return_value.transform_search_terms_to_keywords.return_value = _TEST_TRANSFORMED_DF

    mock_retrieve_secret.side_effect = get_secret_call_results

    # Act / Assert
    with self.assertRaises(ValueError):
      main.extract_and_upload_keywords(test_request)

  @parameterized.named_parameters([
      {
          'testcase_name': 'GCP Project ID empty',
          'env_var': 'GCP_PROJECT_ID',
          'value': ''
      },
      {
          'testcase_name': 'Customer ID empty',
          'env_var': 'CUSTOMER_ID',
          'value': ''
      },
      {
          'testcase_name': 'SA360 SFTP Username empty',
          'env_var': 'SA360_SFTP_USERNAME',
          'value': ''
      },
      {
          'testcase_name': 'Clicks threshold empty',
          'env_var': 'CLICKS_THRESHOLD',
          'value': ''
      },
      {
          'testcase_name': 'Clicks threshold non-numeric',
          'env_var': 'CLICKS_THRESHOLD',
          'value': 'three'
      },
      {
          'testcase_name': 'Conversions threshold empty',
          'env_var': 'CONVERSIONS_THRESHOLD',
          'value': ''
      },
      {
          'testcase_name': 'Conversions threshold non-numeric',
          'env_var': 'CONVERSIONS_THRESHOLD',
          'value': 'one'
      },
      {
          'testcase_name': 'Search term tokens threshold empty',
          'env_var': 'SEARCH_TERMS_TOKENS_THRESHOLD',
          'value': ''
      },
      {
          'testcase_name': 'Search term tokens threshold non-numeric',
          'env_var': 'SEARCH_TERMS_TOKENS_THRESHOLD',
          'value': 'five'
      },
  ])
  def test_extract_and_upload_keywords_invalid_env_vars(self,
                                                        env_var,
                                                        value):
    """Tests invalid environment variables / settings are handled correctly."""
    # Arrange
    data = {'name': 'test'}
    test_request = mock.Mock(get_json=mock.Mock(return_value=data), args=data)

    os.environ[env_var] = value

    # Act / Assert
    with self.assertRaises(ValueError):
      main.extract_and_upload_keywords(test_request)
