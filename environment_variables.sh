#!/bin/bash

GCP_PROJECT_ID=#[GCP Project ID]
SOURCE_REPO=#[Name of the Git Cloud Source Repository to create]
LOCATION=us-central1
CUSTOMER_ID=#[Customer ID]

GADS_CLIENT_ID=#[Google Ads Client ID]
GADS_DEVELOPER_TOKEN=#[Google Ads Developer Token]
GADS_REFRESH_TOKEN=#[Google Ads Refresh Token]
GADS_CLIENT_SECRET=#[Google Ads Client Secret]

SA360_SFTP_HOSTNAME=#[SA360 SFTP Hostname]
SA360_SFTP_PORT=#[SA360 SFTP Port]
SA360_SFTP_USERNAME=#[SA360 SFTP Username]
SA360_SFTP_PASSWORD=#[SA360 SFTP Password]
SA_ACCOUNT_TYPE=#[SA360 Account Type]
SA_LABEL=#[SA360 Keyword Label]

CAMPAIGN_IDS=#[List of Campaign IDs to run Google Ads Report for]
CLICKS_THRESHOLD=#[Number of clicks for a search term that is used to determine keyword eligibility]
CONVERSIONS_THRESHOLD=#[Number of conversions for a search term that is used to determine keyword eligibility]
SEARCH_TERM_TOKENS_THRESHOLD=#[Number of tokens for a search term that is used to determine keyword eligibility]
