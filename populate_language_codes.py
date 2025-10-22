"""
Script to download ISO 639-2 language codes from the Library of Congress,
parse the HTML table, and populate a DynamoDB table with ISO 639-1 and ISO 639-2 codes.

Requirements:
- boto3
- requests
- beautifulsoup4

Usage:
  python populate_language_codes.py

Set AWS credentials and region in your environment or ~/.aws/credentials.
"""
import boto3
import requests
from bs4 import BeautifulSoup
import os

# Configuration
LANGUAGE_CODES_TABLE = os.environ.get('LANGUAGE_CODES_TABLE')
REGION = os.environ.get('REGION')

# Download the code list
url = "https://www.loc.gov/standards/iso639-2/php/code_list.php"
print(f"Downloading language code list from {url} ...")
response = requests.get(url)
response.raise_for_status()
soup = BeautifulSoup(response.text, "html.parser")

# Connect to DynamoDB
print(f"Connecting to DynamoDB table '{LANGUAGE_CODES_TABLE}' in region '{REGION}' ...")
dynamodb = boto3.resource('dynamodb', region_name=REGION)

def create_table_if_not_exists():
    existing_tables = [t.name for t in dynamodb.tables.all()]
    if LANGUAGE_CODES_TABLE not in existing_tables:
        print(f"Table '{LANGUAGE_CODES_TABLE}' does not exist. Creating ...")
        table = dynamodb.create_table(
            TableName=LANGUAGE_CODES_TABLE,
            KeySchema=[{'AttributeName': 'iso_639_1', 'KeyType': 'HASH'}],
            AttributeDefinitions=[{'AttributeName': 'iso_639_1', 'AttributeType': 'S'}],
            BillingMode='PAY_PER_REQUEST',
        )
        table.wait_until_exists()
        print(f"Table '{LANGUAGE_CODES_TABLE}' created.")
    else:
        print(f"Table '{LANGUAGE_CODES_TABLE}' already exists.")

create_table_if_not_exists()
table = dynamodb.Table(LANGUAGE_CODES_TABLE)

# Parse and insert
rows = soup.find_all('tr')[1:]  # skip header
count = 0

for row in rows:
    cols = row.find_all('td')
    if len(cols) >= 3:
        iso_639_2 = cols[0].text.strip()
        iso_639_1 = cols[1].text.strip()
        english_name = cols[2].text.strip()
        if iso_639_1:  # Only insert if 2-letter code exists
            table.put_item(Item={
                'iso_639_2': iso_639_2,
                'iso_639_1': iso_639_1,
                'english_name': english_name
            })
            count += 1
            print(f"Inserted: iso_639_2={iso_639_2}, iso_639_1={iso_639_1}, english_name={english_name}")

print(f"Done. Inserted {count} language code mappings.")
