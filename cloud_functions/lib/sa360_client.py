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

"""Defines the SA360 client to upload the finalized keywords file via SFTP.

See class docstring for more details.
"""
import datetime
import pandas as pd
import paramiko


class SA360Client():
  """Class for uploading SAKA keywords file to SA360 via SFTP."""

  def __init__(self, hostname: str, port: int, username: str,
               password: str) -> None:
    """Initializes the SA360Client.

    Args:
      hostname: The SFTP server hostname.
      port: The SFTP server port.
      username: The SFTP server username.
      password: The SFTP server password.
    """
    self._hostname = hostname
    self._port = port
    self._username = username
    self._password = password

    print('Initialized SA360 Client class.')

  def upload_keywords_to_sa360(self, sa360_bulksheet_df: pd.DataFrame) -> None:
    """Method that uploads the keywords DataFrame as a file to SA360.

    Args:
      sa360_bulksheet_df: Dataframe of keywords to be added to Ad Groups.
    """
    ssh_client = paramiko.SSHClient()
    print(f'Hostname:{self._hostname}, Username: {self._username}')
    ssh_client.load_system_host_keys()
    ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh_client.connect(
        hostname=self._hostname,
        port=self._port,
        username=self._username,
        password=self._password)
    sftp_client = ssh_client.open_sftp()
    bulksheet_filename = (
        f'saka_bulkfile_{datetime.date.today().strftime("%b_%d_%Y")}.csv')
    with sftp_client.open(bulksheet_filename, 'w') as bulksheet_file:
      print(f'Opened SFTP client for writing file {bulksheet_filename}.')
      sa360_bulksheet_df.to_csv(bulksheet_file, index=False)
      print(f'Wrote file {bulksheet_filename} to SA360 SFTP server.')
