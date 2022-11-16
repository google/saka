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

"""Unit tests for the SA360 Client Class."""

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
