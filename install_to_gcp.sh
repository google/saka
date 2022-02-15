#!/bin/bash
source ./environment_variables.sh

if echo "$OSTYPE" | grep -q darwin
then
  GREEN='\x1B[1;32m'
  NOCOLOR='\x1B[0m'
  HYPERLINK='\x1B]8;;'
else
  GREEN='\033[1;32m'
  NOCOLOR='\033[0m'
  HYPERLINK='\033]8;;'
fi

print_green() {
  echo -e "${GREEN}$1${NOCOLOR}"
}

print_green "Initiating installation of SA360 Keyword Automator (SAKA) to GCP..."

# Enable the Secret Manager API if it is not enabled yet.
print_green "Enabling Cloud APIs if necessary..."
REQUIRED_SERVICES=(
  cloudbuild.googleapis.com
  cloudfunctions.googleapis.com
  logging.googleapis.com
  secretmanager.googleapis.com
  sourcerepo.googleapis.com
)

ENABLED_SERVICES=$(gcloud services list)
for SERVICE in "${REQUIRED_SERVICES[@]}"
do
  if echo "$ENABLED_SERVICES" | grep -q "$SERVICE"
  then
    echo "$SERVICE is already enabled."
  else
    gcloud services enable "$SERVICE" \
      && echo "$SERVICE has been successfully enabled."
    sleep 1
  fi
done

# Create a new Cloud Source Repository for Git
print_green "Creating the Git repository..."
EXISTING_REPOS="$(gcloud source repos list --filter="$SOURCE_REPO")"
if echo "$EXISTING_REPOS" | grep -q -w "$SOURCE_REPO"
then
  echo "Cloud Source Repository $SOURCE_REPO already exists."
else
  gcloud source repos create "$SOURCE_REPO"
fi

# Grant permissions to the Cloud Function to manage required APIs.
CF_SERVICE_ACCOUNT="$GCP_PROJECT_ID"@appspot.gserviceaccount.com
gcloud projects add-iam-policy-binding "$GCP_PROJECT_ID" \
    --member=serviceAccount:"$CF_SERVICE_ACCOUNT" \
    --role=roles/secretmanager.secretAccessor

# Delete Secrets before re-creating them.
gcloud --quiet secrets delete google_ads_api_credentials
gcloud --quiet secrets delete sa360_sftp_password

# Create API Secrets in GCP Secret Manager.
print_green "Storing Google Ads and SA360 credentials into Cloud Secret Manager..."

GOOGLE_ADS_API_CREDENTIALS=$"{\n  \"developer_token\": \"$GADS_DEVELOPER_TOKEN\",\n  \"refresh_token\": \"$GADS_REFRESH_TOKEN\",\n  \"client_id\": \"$GADS_CLIENT_ID\",\n  \"client_secret\": \"$GADS_CLIENT_SECRET\",\n  \"login_customer_id\": \"$GADS_MANAGER_ACCOUNT_CUSTOMER_ID\",\n  \"use_proto_plus\": \"True\"\n}"
echo "$GOOGLE_ADS_API_CREDENTIALS" | gcloud secrets create google_ads_api_credentials \
    --replication-policy="automatic" \
    --data-file=-

echo "$SA360_SFTP_PASSWORD" | gcloud secrets create sa360_sftp_password \
    --replication-policy="automatic" \
    --data-file=-

# Setup Service Accounts and grant permissions
print_green "Setting up service account permissions..."
PROJECT_NUMBER=$(gcloud projects list --filter="PROJECT_ID=$GCP_PROJECT_ID" --format="value(PROJECT_NUMBER)")
gcloud projects add-iam-policy-binding "$GCP_PROJECT_ID" \
  --member serviceAccount:"$PROJECT_NUMBER"@cloudbuild.gserviceaccount.com \
  --role roles/iam.serviceAccountUser
gcloud projects add-iam-policy-binding "$GCP_PROJECT_ID" \
  --member serviceAccount:"$PROJECT_NUMBER"@cloudbuild.gserviceaccount.com \
  --role roles/cloudfunctions.developer
gcloud projects add-iam-policy-binding "$GCP_PROJECT_ID" \
  --member serviceAccount:"$PROJECT_NUMBER"@cloudbuild.gserviceaccount.com \
  --role roles/editor

# Setup Cloud Build trigger for CICD.
CreateTrigger() {
  TARGET_TRIGGER=$1
  DESCRIPTION=$2
  ENV_VARIABLES=$3
  gcloud alpha builds triggers create cloud-source-repositories \
  --build-config=cicd/"$TARGET_TRIGGER" \
  --repo="$SOURCE_REPO" \
  --branch-pattern=main \
  --description="$DESCRIPTION" \
  --substitutions ^::^"$ENV_VARIABLES"
}

# Recreate the Cloud Build triggers by deleting them all first.
print_green "Removing and re-creating SAKA Cloud Build trigger..."
EXISTING_TRIGGERS=$(gcloud alpha builds triggers list --filter=name:SAKA | grep "id:" | awk '{printf("%s\n", $2)}')
for TRIGGER in $(echo "$EXISTING_TRIGGERS")
do
  gcloud alpha builds triggers -q delete "$TRIGGER"
  sleep 1
done

CreateTrigger deploy_saka_cf_to_gcp.yaml \
  "SAKA Deploy Cloud Function" \
  _GCP_PROJECT_ID="$GCP_PROJECT_ID"::_CUSTOMER_ID="$CUSTOMER_ID"::_SA360_SFTP_USERNAME="$SA360_SFTP_USERNAME"::_SA360_ACCOUNT_NAME="$SA360_ACCOUNT_NAME"::_SA360_LABEL="$SA360_LABEL"::_CAMPAIGN_IDS="$CAMPAIGN_IDS"::_CLICKS_THRESHOLD="$CLICKS_THRESHOLD"::_CONVERSIONS_THRESHOLD="$CONVERSIONS_THRESHOLD"::_SEARCH_TERM_TOKENS_THRESHOLD="$SEARCH_TERM_TOKENS_THRESHOLD"


# Create the Cloud Scheduler entry to be able to trigger the HTTP function.
$TRIGGER_URL="https://${LOCATION}-${GCP_PROJECT_ID}.cloudfunctions.net/extract_and_upload_keywords"
gcloud scheduler jobs create http triggerSakaFunction --schedule="0 12 * * *" --uri="$TRIGGER_URL" --oidc-service-account-email="$CF_SERVICE_ACCOUNT"

print_green "Installation and setup of SAKA finished. Please deploy via Cloud Build by pushing the code to your source repository at ${HYPERLINK}https://source.cloud.google.com/$GCP_PROJECT_ID/$SOURCE_REPO\ahttps://source.cloud.google.com/$GCP_PROJECT_ID/$SOURCE_REPO${HYPERLINK}\a"
