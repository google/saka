#!/bin/bash
print_green() {
  echo -e "${GREEN}$1${NOCOLOR}"
}

print_green "Initiating installation of SA360 Keyword Automator (SAKA) to GCP..."

source ./environment_variables.sh

print_green "Environment variables loaded."

# Enable the Secret Manager API if it is not enabled yet.
print_green "Enabling Cloud APIs if necessary..."
REQUIRED_SERVICES=(
  secretmanager.googleapis.com
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
gcloud --quiet secrets delete gads_api_developer_token
gcloud --quiet secrets delete gads_api_refresh_token
gcloud --quiet secrets delete gads_api_client_secret



# Create API Secrets in GCP Secret Manager.
print_green "Storing Google Ads credentials into Cloud Secret Manager..."

GADS_YAML_CREDS="{\n  \"developer_token\": $GADS_DEVELOPER_TOKEN,\n  \"refresh_token\": $GADS_REFRESH_TOKEN,\n  \"client_id\": $GADS_CLIENT_ID,\n  \"client_secret\": $GADS_CLIENT_SECRET,\n  \"login_customer_id\": $CUSTOMER_ID,\n  \"use_proto_plus\": \"True\"\n}"
echo -n "$GADS_YAML_CREDS" | gcloud secrets create gads_api_yaml_creds \
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
  _GCP_PROJECT_ID="$GCP_PROJECT_ID"::_CUSTOMER_ID="$CUSTOMER_ID"::_SA360_ACCOUNT_ID="$SA360_ACCOUNT_ID"::_SA360_ACCOUNT_NAME="$SA360_ACCOUNT_NAME"::_SA360_SFTP_URL="$SA360_SFTP_URL"::_SA_ACCOUNT_TYPE="$SA_ACCOUNT_TYPE"::_SA_LABEL="$SA_LABEL"::_CAMPAIGN_IDS="$CAMPAIGN_IDS"::_CLICKS_THRESHOLD="$CLICKS_THRESHOLD"::_CONVERSIONS_THRESHOLD="$CONVERSIONS_THRESHOLD"::_SEARCH_TERM_TOKENS_THRESHOLD="$SEARCH_TERM_TOKENS_THRESHOLD"

print_green "Installation and setup of SAKA finished. Please deploy via Cloud Build by pushing the code to your source repository at ${HYPERLINK}https://source.cloud.google.com/$GCP_PROJECT_ID/$SOURCE_REPO\ahttps://source.cloud.google.com/$GCP_PROJECT_ID/$SOURCE_REPO${HYPERLINK}\a"

