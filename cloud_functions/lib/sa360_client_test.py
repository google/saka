import unittest
from unittest import mock

import sa360_client as sa360_client_lib


class SA360ClientTest(unittest.TestCase):

  def test_upload_keywords_to_sa360(self):
    """Tests DataFrame written as CSV to remote file."""
    # Arrange
    mock_df = mock.MagicMock()
    mock_sftp_client = mock.MagicMock()
    mock_remote_file = mock.MagicMock()

    # Sets up the mock SFTP client to return a mock remote file.
    mock_sftp_client.open.return_value = mock_remote_file

    with mock.patch('paramiko.SSHClient') as mock_ssh_client:
      sa360_client = sa360_client_lib.SA360Client('fake_host',
                                                  12345,
                                                  'fake_user',
                                                  'fake_pass')

      mock_ssh_client.return_value.open_sftp = mock_sftp_client

      # Act
      sa360_client.upload_keywords_to_sa360(mock_df)

      # Assert
      mock_df.to_csv.called_with(mock_remote_file, False)


if __name__ == '__main__':
  unittest.main()
