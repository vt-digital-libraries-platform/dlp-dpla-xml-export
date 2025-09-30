
import os
import boto3
from datetime import datetime

# Set up DynamoDB resource

region = os.environ.get('REGION')
table_name = os.environ.get('COLLECTION_TABLE')
print(f"DEBUG: REGION={region}, COLLECTION_TABLE={table_name}")


dynamodb = boto3.resource('dynamodb', region_name=region)
table = dynamodb.Table(table_name)
print("DEBUG: DynamoDB resource and table initialized.")

# Scan the table for all items (with pagination)

items = []
response = table.scan()
print(f"DEBUG: First scan returned {len(response.get('Items', []))} items.")
items.extend(response.get('Items', []))
while 'LastEvaluatedKey' in response:
    print("DEBUG: Fetching next page of results...")
    response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
    print(f"DEBUG: Next scan returned {len(response.get('Items', []))} items.")
    items.extend(response.get('Items', []))
print(f"DEBUG: Total items retrieved: {len(items)}")

# Extract unique titles

# Map folder names to identifiers
folder_to_identifiers = {}

# Collect all Ms identifiers in a single set
ms_identifiers = set()
for idx, item in enumerate(items):
    title = item.get('title')
    identifier = item.get('identifier', 'NO_IDENTIFIER')
    print(f"DEBUG: Processing item {idx+1}/{len(items)}: title={title}, identifier={identifier}")
    if identifier.startswith('Ms'):
        ms_identifiers.add(identifier)
    elif title:
        folder_name = ''.join(e for e in title if e.isalnum())
        print(f"DEBUG: Generated folder name: {folder_name}")
        if folder_name not in folder_to_identifiers:
            folder_to_identifiers[folder_name] = set()
        folder_to_identifiers[folder_name].add(identifier)

# Add Ms folder only once if any Ms identifiers found
if ms_identifiers:
    folder_to_identifiers['Ms'] = ms_identifiers

# Print the unique folder names and their identifiers

print("Unique folder names and their identifiers:")
for folder in sorted(folder_to_identifiers.keys()):
    print(f"{folder}: {', '.join(sorted(folder_to_identifiers[folder]))}")

print(f"Total unique folders: {len(folder_to_identifiers)}")

# Log to timestamped file
timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
output_path = f"unique_collection_folders_{timestamp}.txt"
print(f"DEBUG: Writing unique folder names and identifiers to {output_path}")
with open(output_path, 'w', encoding='utf-8') as f:
    for folder in sorted(folder_to_identifiers.keys()):
        f.write(f"{folder}: {', '.join(sorted(folder_to_identifiers[folder]))}\n")
    f.write(f"Total unique folders: {len(folder_to_identifiers)}\n")
print(f"DEBUG: Results written to {output_path}")
