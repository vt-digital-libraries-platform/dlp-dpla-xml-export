import boto3
import os
from datetime import datetime
from collections import defaultdict

# --- CONFIGURATION ---
region = os.environ.get('REGION')
folder_lookup_table = os.environ.get('FOLDER_LOOKUP_TABLE')
bucket_name = os.environ.get('S3_BUCKET')
prefix = os.environ.get('S3_PREFIX')
print(f"DEBUG: REGION={region}, FOLDER_LOOKUP_TABLE={folder_lookup_table}")
print(f"DEBUG: Scanning S3 bucket '{bucket_name}' with prefix '{prefix}'")

s3 = boto3.client('s3')
paginator = s3.get_paginator('list_objects_v2')
page_iterator = paginator.paginate(Bucket=bucket_name, Prefix=prefix)

# Process files in all folders under federated
folder_files = defaultdict(list)
for page in page_iterator:
    for obj in page.get('Contents', []):
        key = obj['Key'][len(prefix):] if obj['Key'].startswith(prefix) else obj['Key']
        if not key:  # Skip empty keys
            continue
            
        # Extract the identifier from folder structure
        if '/' in key:
            path_parts = key.split('/')
            #----------------------------------------------------------
            # Only process items that start with SQI_PO
            # if not path_parts[0].startswith('SQI_PO'):
            #     continue
            #----------------------------------------------------------
            if len(path_parts) >= 3:  # Need at least top_folder/identifier_folder/Access or more
                # Find "Access" folder and get the folder one level above it
                if 'Access' in path_parts:
                    access_index = path_parts.index('Access')
                    if access_index > 0:  # Make sure there's a folder before Access
                        identifier = path_parts[access_index - 1]  # Folder one level above Access
                        folder_path_parts = path_parts[:access_index]  # Everything up to (but not including) Access
                        folder_path = '/'.join(folder_path_parts)
                        
                        # Get top-level folder for mapping
                        top_level_folder = path_parts[0]
                        
                        if identifier:  # Only add if identifier is not empty
                            folder_files[top_level_folder].append({
                                'identifier': identifier,
                                'folder_path': f"{prefix}{folder_path}",
                                'full_key': key
                            })
                            print(f"DEBUG: Found identifier '{identifier}' in path '{prefix}{folder_path}'")
                        else:
                            print(f"WARNING: Skipping empty identifier for key: {key}")
                    else:
                        print(f"WARNING: Access folder found but no parent folder for key: {key}")
                else:
                    print(f"SKIPPING: No Access folder found in key: {key}")
                    continue  # Skip items without Access folder
            else:
                print(f"SKIPPING: Path too short for key: {key}")
                continue  # Skip items with short paths
        else:
            print(f"SKIPPING: No folder structure found for key: {key}")
            continue  # Skip items without folder structure

print("DEBUG: All folders and their files found:", {k: len(v) for k, v in folder_files.items()})

# --- Folder mapping ---
folder_mapping = {
    'BTR': 'barter',
    'CRW': 'crewe',
    'CIDA': 'christiansburg-institute-digital-archive',
    'COST': 'costume-and-textile-collection',
    'FCHS': 'floyd-county-historic-society',
    'SQI_PO': 'squires'
}

# --- Write all folder names and their files to DynamoDB ---
dynamodb = boto3.resource('dynamodb', region_name=region)
table = dynamodb.Table(folder_lookup_table)
print("DEBUG: DynamoDB resource and table initialized.")

for folder, file_list in folder_files.items():
    if not folder:  # Skip empty folder names
        print(f"WARNING: Skipping empty folder name")
        continue
    
    # Map folder to proper name
    mapped_folder = folder_mapping.get(folder, folder)
    
    for file_info in file_list:
        identifier = file_info['identifier']
        folder_path = file_info['folder_path']
        
        print(f"DEBUG: Writing identifier_prefix='{folder}', folder='{mapped_folder}', identifier='{identifier}', folder_path='{folder_path}' to DynamoDB")
        table.put_item(Item={
            'identifier_prefix': folder,        # Partition key (e.g., FCHS, SQI, etc.)
            'file_name': identifier,            # Sort key (the identifier, e.g., fchs_1950_001_001)
            'folder': mapped_folder,            # Mapped folder name
            'folder_path': folder_path,         # Full S3 path
            'created_at': datetime.now().isoformat()
        })

print("DEBUG: Finished writing all folder names and files to DynamoDB.")