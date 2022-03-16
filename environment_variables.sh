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

#!/bin/bash

# Set Environment Variables to install SAKA.
# Please change the values for your project.

GCP_PROJECT_ID="" # GCP Project ID
SOURCE_REPO="" # Name of the Git Cloud Source Repository to create
LOCATION="us-central1" # Location of GCP resources that will be installed
CUSTOMER_ID="" # Customer ID
PUBSUB_TOPIC="trigger-extract-and-upload-keywords-cloud-function" # Cloud Pub/Sub topic that triggers the Cloud Function

GADS_CLIENT_ID="" # Google Ads Client ID
GADS_DEVELOPER_TOKEN="" # Google Ads Developer Token
GADS_REFRESH_TOKEN="" # Google Ads Refresh Token
GADS_CLIENT_SECRET="" # Google Ads Client Secret

SA360_SFTP_HOSTNAME="" # SA360 SFTP Hostname
SA360_SFTP_PORT="" # SA360 SFTP Port
SA360_SFTP_USERNAME="" # SA360 SFTP Username
SA360_SFTP_PASSWORD="" # SA360 SFTP Password
SA_ACCOUNT_NAME="" # SA360 Account Name
SA_LABEL="" # [OPTIONAL] SA360 Keyword Label

CAMPAIGN_IDS="" # [OPTIONAL] List of Campaign IDs to run Google Ads Report for
CLICKS_THRESHOLD="" # [OPTIONAL. Default=5] Number of clicks for a search term that is used to determine keyword eligibility
CONVERSIONS_THRESHOLD="" # [OPTIONAL. Default=0] Number of conversions for a search term that is used to determine keyword eligibility
SEARCH_TERM_TOKENS_THRESHOLD="" # [OPTIONAL. Default=3] Number of tokens for a search term that is used to determine keyword eligibility
KEYWORD_LANDING_PAGE="" # [OPTIONAL] The webpage where people end up after they click your ad
KEYWORD_MAX_CPC="" # [OPTIONAL] The maximum cost-per-click that will be added to the bulksheet
