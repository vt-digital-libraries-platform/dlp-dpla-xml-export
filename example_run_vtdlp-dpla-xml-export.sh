#!/bin/bash

# Set only the environment variables needed for the export script
export COLLECTION_IDENTIFIER="<FILL-IN-COLLECTION_IDENTIFIER>"
export REGION="<FILL-IN-REGION>"
export DYNAMODB_TABLE_SUFFIX="<FILL-IN-DYNAMODB_TABLE_SUFFIX>"
export LONG_URL_PATH="<FILL-IN-LONG_URL_PATH>"
export TYPE="<FILL-IN-TYPE>"
# Set ENV to "prod" or "preprod"
ENV="<FILL-IN-ENV>"

if [ "$ENV" = "prod" ]; then
  export LONG_URL_PATH="<FILL-IN-PROD-LONG_URL_PATH>"
else
  export LONG_URL_PATH="<FILL-IN-PREPROD-LONG_URL_PATH>"
fi
# Run the export script
python3 /home/padmadlp/dpla-va/dlp-dpla-xml-export/dlp-dpla-xml-export.py
