#!/bin/bash


# Set the environment variables 
export REGION="<FILL-IN-REGION>"
export DYNAMODB_TABLE="<FILL-IN-DYNAMODB_TABLE_SUFFIX>"
export COLLECTION_TABLE="<FILL-IN-COLLECTION_TABLE>"
export TYPE="<FILL-IN-TYPE>"
# S3 bucket and prefix
export S3_BUCKET="<FILL-IN-S3_BUCKET>"
export S3_PREFIX="<FILL-IN-S3_PREFIX>"
# Folder lookup table in DynamoDB
export FOLDER_LOOKUP_TABLE="<FILL-IN-FOLDER_LOOKUP_TABLE>"
# Set the identifier for which xml export is to be run
export IDENTIFIER_PREFIX="SQI"
# Set the language codes table
export LANGUAGE_CODES_TABLE="<FILL-IN-LANGUAGE_CODES_TABLE>"
# Set ENV to "prod" or "preprod"
ENV="<FILL-IN-ENV>"

if [ "$ENV" = "prod" ]; then
  export LONG_URL_PATH="<FILL-IN-PROD-LONG_URL_PATH>"
else
  export LONG_URL_PATH="<FILL-IN-PREPROD-LONG_URL_PATH>"
fi
# Run the export script
# python3 /home/padmadlp/dpla-va/dlp-dpla-xml-export/dlp-dpla-xml-export.py
# Run the language codes script
# python3 /home/padmadlp/dpla-va/dlp-dpla-xml-export/populate_language_codes.py
# Run the multi-valued format or dimension format script
# python3 /home/padmadlp/dpla-va/dlp-dpla-xml-export/print_multi_valued_or_dimension_format.py
# Run the get unique collection folders script
# python3 /home/padmadlp/dpla-va/dlp-dpla-xml-export/get_unique_collection_folders.py
# Run the format script to get the format of the records in the s3 bucket collection using an identifier
# python3 /home/padmadlp/dpla-va/dlp-dpla-xml-export/detect_s3_object_formats.py
# Run the folder extraction script to populate the folder lookup table in DynamoDB
python3 /home/padmadlp/dpla-va/dlp-dpla-xml-export/extract_and_store_folder_names_from_s3.py
exit 0  

#Delete existing DynamoDB table
echo "Deleting existing DynamoDB table: $FOLDER_LOOKUP_TABLE"
aws dynamodb delete-table --table-name $FOLDER_LOOKUP_TABLE --region $REGION 2>/dev/null || echo "Table does not exist or already deleted"      
# Wait for table to be deleted
echo "Waiting for table deletion to complete..."
aws dynamodb wait table-not-exists --table-name $FOLDER_LOOKUP_TABLE --region
$REGION
# Create new table
echo "Creating new DynamoDB table: $FOLDER_LOOKUP_TABLE"
aws dynamodb create-table \
  --table-name $FOLDER_LOOKUP_TABLE \
  --attribute-definitions AttributeName=identifier_prefix,AttributeType=S AttributeName=file_name,AttributeType=S \
  --key-schema AttributeName=identifier_prefix,KeyType=HASH AttributeName=file_name,KeyType=RANGE \
  --billing-mode PAY_PER_REQUEST \
  --region $REGION
# Wait for table to be created
echo "Waiting for table creation to complete..."
aws dynamodb wait table-exists --table-name $FOLDER_LOOKUP_TABLE --region $REGION