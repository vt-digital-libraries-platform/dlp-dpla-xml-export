
import os
import boto3
import re
from boto3.dynamodb.conditions import Attr
from datetime import datetime

# Set up DynamoDB resource
region = os.environ.get('REGION')
table_name = os.environ.get('DYNAMODB_TABLE_SUFFIX')

dynamodb = boto3.resource('dynamodb', region_name=region)
table = dynamodb.Table(table_name)

dimension_pattern = re.compile(r'\b\d+\s*(in\.|cm|mm|ft|inches|feet)\b', re.IGNORECASE)

# Scan the table
response = table.scan()
items = response.get('Items', [])

matching_items = []
output_lines = []

while 'LastEvaluatedKey' in response:
    response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
    items.extend(response.get('Items', []))



for item in items:
    format_field = item.get('format')
    match = False
    matched_value = None
    if isinstance(format_field, list):
        for val in format_field:
            if isinstance(val, str) and dimension_pattern.search(val):
                match = True
                matched_value = val
                break
    elif isinstance(format_field, str):
        if dimension_pattern.search(format_field):
            match = True
            matched_value = format_field
    if match:
        identifier = item.get('identifier')
        matching_items.append(identifier)
        output_lines.append('---')
        output_lines.append(f"Identifier: {identifier}")
        output_lines.append(f"Format: {matched_value}")

summary = [
    f"\nTotal matching items: {len(matching_items)}",
    f"Unique identifiers: {len(set(matching_items))}"
]


# Add timestamp to output file name
timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
base_output = os.environ.get('OUTPUT_PATH', 'multi_valued_or_dimension_format_results')
if base_output.endswith('.txt'):
    base_output = base_output[:-4]
output_path = f"{base_output}_{timestamp}.txt"

with open(output_path, 'w', encoding='utf-8') as f:
    for line in output_lines:
        f.write(line + '\n')
    for line in summary:
        f.write(line + '\n')

print(f"Results written to {output_path}")
