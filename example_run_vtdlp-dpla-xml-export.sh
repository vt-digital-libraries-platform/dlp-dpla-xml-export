#!/bin/bash


# Set the environment variables 
export REGION="<FILL-IN-REGION>"
export DYNAMODB_TABLE_SUFFIX="<FILL-IN-DYNAMODB_TABLE_SUFFIX>"
export LONG_URL_PATH="<FILL-IN-LONG_URL_PATH>"
export TYPE="<FILL-IN-TYPE>"
export LANGUAGE_CODES_TABLE="<FILL-IN-LANGUAGE_CODES_TABLE>"
# Set ENV to "prod" or "preprod"
ENV="<FILL-IN-ENV>"

if [ "$ENV" = "prod" ]; then
  export LONG_URL_PATH="<FILL-IN-PROD-LONG_URL_PATH>"
else
  export LONG_URL_PATH="<FILL-IN-PREPROD-LONG_URL_PATH>"
fi
# Run the export script
python3 /home/padmadlp/dpla-va/dlp-dpla-xml-export/dlp-dpla-xml-export.py
# Run the language codes script
# python3 /home/padmadlp/dpla-va/dlp-dpla-xml-export/populate_language_codes.py
# Run the multi-valued format or dimension format script
# python3 /home/padmadlp/dpla-va/dlp-dpla-xml-export/print_multi_valued_or_dimension_format.py
