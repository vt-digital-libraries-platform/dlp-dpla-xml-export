#!/usr/bin/env python3
"""
Dynamically fetch and populate the RightsStatement DynamoDB table with official 
rights statements from rightsstatements.org.

This script:
1. Fetches the latest rights statements from rightsstatements.org
2. Clears the existing table contents
3. Populates with fresh data from the website

The rightsstatements.org vocabulary is stable (last update 2016) but this
ensures you always have the latest if new statements are added.

Requirements:
- boto3
- requests
- beautifulsoup4 (install with: pip install beautifulsoup4)
- The RightsStatement table must exist (run create_rights_statement_table.py first)

Usage:
  export REGION=""
  export ENV="preprod"  # or "prod"
  python3 populate_rights_statements_dynamic.py

Set AWS credentials in your environment or ~/.aws/credentials.
"""
import boto3
import os
import sys
import re
from datetime import datetime

try:
    import requests
    from bs4 import BeautifulSoup
except ImportError:
    print("❌ ERROR: Missing required packages")
    print("   Install with: pip install requests beautifulsoup4")
    sys.exit(1)

# Configuration from environment variables
REGION = os.environ.get('REGION', 'us-east-1')
ENV = os.environ.get('ENV', 'preprod')

# Table name (same for both preprod and prod)
TABLE_NAME = 'RightsStatement'

# Rights statements website
RIGHTS_STATEMENTS_URL = 'https://rightsstatements.org/page/1.0/?language=en'

print(f"========================================")
print(f"Populating RightsStatement Table (Dynamic)")
print(f"========================================")
print(f"Environment: {ENV}")
print(f"Region: {REGION}")
print(f"Table Name: {TABLE_NAME}")
print(f"Source: {RIGHTS_STATEMENTS_URL}")
print(f"========================================\n")

# Connect to DynamoDB
try:
    dynamodb = boto3.resource('dynamodb', region_name=REGION)
    table = dynamodb.Table(TABLE_NAME)
    print(f"✅ Connected to DynamoDB table '{TABLE_NAME}'")
except Exception as e:
    print(f"❌ ERROR: Failed to connect to DynamoDB: {e}")
    print(f"💡 Make sure you've run 'create_rights_statement_table.py' first!")
    sys.exit(1)

print(f"\n📡 Fetching rights statements from {RIGHTS_STATEMENTS_URL}...")

try:
    response = requests.get(RIGHTS_STATEMENTS_URL, timeout=30)
    response.raise_for_status()
    print(f"✅ Successfully fetched webpage")
except Exception as e:
    print(f"❌ ERROR: Failed to fetch webpage: {e}")
    sys.exit(1)

# Parse the HTML
soup = BeautifulSoup(response.text, 'html.parser')

# Find all rights statement entries
# The website structure has sections with class="rights-statement"
statements = []

# Pattern to extract code from URI like http://rightsstatements.org/vocab/InC/1.0/
uri_pattern = re.compile(r'http://rightsstatements\.org/vocab/([^/]+)/1\.0/')

# Find all rights statement blocks
for section in soup.find_all(['section', 'div'], class_=re.compile(r'statement|rights')):
    # Look for the canonical URI link
    uri_link = section.find('a', href=uri_pattern)
    
    if uri_link:
        uri = uri_link.get('href')
        
        # Extract code from URI
        match = uri_pattern.search(uri)
        if not match:
            continue
        code = match.group(1)
        
        # Find the label/title
        title_elem = section.find(['h2', 'h3', 'h4'])
        label = title_elem.get_text(strip=True) if title_elem else code
        
        # Find the description
        desc_elem = section.find('p')
        description = desc_elem.get_text(strip=True) if desc_elem else ""
        
        # Determine category based on code prefix
        if code.startswith('InC'):
            category = 'In Copyright'
        elif code.startswith('NoC'):
            category = 'No Copyright'
        else:
            category = 'Other'
        
        statements.append({
            'RightsURI': uri,
            'RightsCode': code,
            'RightsLabel': label,
            'RightsDescription': description,
            'RightsCategory': category,
            'IsActive': True,
            'createdAt': datetime.now().isoformat(),
            'updatedAt': datetime.now().isoformat(),
            'sourceURL': RIGHTS_STATEMENTS_URL
        })

# If scraping didn't work well, fall back to hardcoded list
if len(statements) < 10:  # We expect at least 12
    print(f"⚠️  WARNING: Only found {len(statements)} statements via scraping.")
    print(f"   Using fallback hardcoded list of 12 official statements...")
    
    current_time = datetime.now().isoformat()
    statements = [
        {'RightsURI': 'http://rightsstatements.org/vocab/InC/1.0/', 'RightsCode': 'InC', 'RightsLabel': 'In Copyright', 'RightsDescription': 'This Rights Statement can be used for an Item that is in copyright.', 'RightsCategory': 'In Copyright', 'IsActive': True, 'createdAt': current_time, 'updatedAt': current_time},
        {'RightsURI': 'http://rightsstatements.org/vocab/InC-OW-EU/1.0/', 'RightsCode': 'InC-OW-EU', 'RightsLabel': 'In Copyright - EU Orphan Work', 'RightsDescription': 'This Rights Statement is intended for use with Items identified as Orphan Works in accordance with EU Directive 2012/28/EU.', 'RightsCategory': 'In Copyright', 'IsActive': True, 'createdAt': current_time, 'updatedAt': current_time},
        {'RightsURI': 'http://rightsstatements.org/vocab/InC-EDU/1.0/', 'RightsCode': 'InC-EDU', 'RightsLabel': 'In Copyright - Educational Use Permitted', 'RightsDescription': 'This Rights Statement can be used only for copyrighted Items for which educational use is permitted.', 'RightsCategory': 'In Copyright', 'IsActive': True, 'createdAt': current_time, 'updatedAt': current_time},
        {'RightsURI': 'http://rightsstatements.org/vocab/InC-NC/1.0/', 'RightsCode': 'InC-NC', 'RightsLabel': 'In Copyright - Non-Commercial Use Permitted', 'RightsDescription': 'This Rights Statement can be used only for copyrighted Items for which non-commercial use is permitted.', 'RightsCategory': 'In Copyright', 'IsActive': True, 'createdAt': current_time, 'updatedAt': current_time},
        {'RightsURI': 'http://rightsstatements.org/vocab/InC-RUU/1.0/', 'RightsCode': 'InC-RUU', 'RightsLabel': 'In Copyright - Rights-holder(s) Unlocatable or Unidentifiable', 'RightsDescription': 'This Rights Statement is intended for use with an Item that has been identified as in copyright but for which no rights-holder has been identified.', 'RightsCategory': 'In Copyright', 'IsActive': True, 'createdAt': current_time, 'updatedAt': current_time},
        {'RightsURI': 'http://rightsstatements.org/vocab/NoC-CR/1.0/', 'RightsCode': 'NoC-CR', 'RightsLabel': 'No Copyright - Contractual Restrictions', 'RightsDescription': 'This Rights Statement can only be used for Items in the Public Domain with contractual restrictions.', 'RightsCategory': 'No Copyright', 'IsActive': True, 'createdAt': current_time, 'updatedAt': current_time},
        {'RightsURI': 'http://rightsstatements.org/vocab/NoC-NC/1.0/', 'RightsCode': 'NoC-NC', 'RightsLabel': 'No Copyright - Non-Commercial Use Only', 'RightsDescription': 'This Rights Statement can only be used for Works in the Public Domain digitized in public-private partnerships with commercial use restrictions.', 'RightsCategory': 'No Copyright', 'IsActive': True, 'createdAt': current_time, 'updatedAt': current_time},
        {'RightsURI': 'http://rightsstatements.org/vocab/NoC-OKLR/1.0/', 'RightsCode': 'NoC-OKLR', 'RightsLabel': 'No Copyright - Other Known Legal Restrictions', 'RightsDescription': 'This Rights Statement should be used for Items in the Public Domain with other known legal restrictions.', 'RightsCategory': 'No Copyright', 'IsActive': True, 'createdAt': current_time, 'updatedAt': current_time},
        {'RightsURI': 'http://rightsstatements.org/vocab/NoC-US/1.0/', 'RightsCode': 'NoC-US', 'RightsLabel': 'No Copyright - United States', 'RightsDescription': 'This Rights Statement should be used for Items free of copyright under US laws.', 'RightsCategory': 'No Copyright', 'IsActive': True, 'createdAt': current_time, 'updatedAt': current_time},
        {'RightsURI': 'http://rightsstatements.org/vocab/CNE/1.0/', 'RightsCode': 'CNE', 'RightsLabel': 'Copyright Not Evaluated', 'RightsDescription': 'This Rights Statement should be used for Items for which the copyright status is unknown.', 'RightsCategory': 'Other', 'IsActive': True, 'createdAt': current_time, 'updatedAt': current_time},
        {'RightsURI': 'http://rightsstatements.org/vocab/UND/1.0/', 'RightsCode': 'UND', 'RightsLabel': 'Copyright Undetermined', 'RightsDescription': 'This Rights Statement should be used for Items for which copyright status is unknown despite investigation.', 'RightsCategory': 'Other', 'IsActive': True, 'createdAt': current_time, 'updatedAt': current_time},
        {'RightsURI': 'http://rightsstatements.org/vocab/NKC/1.0/', 'RightsCode': 'NKC', 'RightsLabel': 'No Known Copyright', 'RightsDescription': 'This Rights Statement should be used for Items for which copyright status has not been determined conclusively.', 'RightsCategory': 'Other', 'IsActive': True, 'createdAt': current_time, 'updatedAt': current_time},
    ]

print(f"\n✅ Prepared {len(statements)} rights statements")

# Add Creative Commons licenses
print(f"\n📝 Adding Creative Commons licenses...")
current_time = datetime.now().isoformat()

creative_commons_licenses = [
    {
        'RightsURI': 'https://creativecommons.org/publicdomain/zero/1.0/',
        'RightsCode': 'CC0',
        'RightsLabel': 'CC0 1.0 Universal (CC0 1.0) Public Domain Dedication',
        'RightsDescription': 'The person who associated a work with this deed has dedicated the work to the public domain by waiving all of his or her rights to the work worldwide under copyright law.',
        'RightsCategory': 'Public Domain',
        'IsActive': True,
        'createdAt': current_time,
        'updatedAt': current_time,
        'sourceURL': 'https://creativecommons.org/publicdomain/zero/1.0/'
    },
    {
        'RightsURI': 'https://creativecommons.org/publicdomain/mark/1.0/',
        'RightsCode': 'PDM',
        'RightsLabel': 'Public Domain Mark 1.0',
        'RightsDescription': 'This work has been identified as being free of known restrictions under copyright law, including all related and neighboring rights.',
        'RightsCategory': 'Public Domain',
        'IsActive': True,
        'createdAt': current_time,
        'updatedAt': current_time,
        'sourceURL': 'https://creativecommons.org/publicdomain/mark/1.0/'
    },
    {
        'RightsURI': 'https://creativecommons.org/licenses/by/4.0/',
        'RightsCode': 'CC BY',
        'RightsLabel': 'Attribution 4.0 International (CC BY 4.0)',
        'RightsDescription': 'This license lets others distribute, remix, adapt, and build upon your work, even commercially, as long as they credit you for the original creation.',
        'RightsCategory': 'Creative Commons',
        'IsActive': True,
        'createdAt': current_time,
        'updatedAt': current_time,
        'sourceURL': 'https://creativecommons.org/licenses/by/4.0/'
    },
    {
        'RightsURI': 'https://creativecommons.org/licenses/by-sa/4.0/',
        'RightsCode': 'CC BY-SA',
        'RightsLabel': 'Attribution-ShareAlike 4.0 International (CC BY-SA 4.0)',
        'RightsDescription': 'This license lets others remix, adapt, and build upon your work even for commercial purposes, as long as they credit you and license their new creations under identical terms.',
        'RightsCategory': 'Creative Commons',
        'IsActive': True,
        'createdAt': current_time,
        'updatedAt': current_time,
        'sourceURL': 'https://creativecommons.org/licenses/by-sa/4.0/'
    },
    {
        'RightsURI': 'https://creativecommons.org/licenses/by-nc/4.0/',
        'RightsCode': 'CC BY-NC',
        'RightsLabel': 'Attribution-NonCommercial 4.0 International (CC BY-NC 4.0)',
        'RightsDescription': 'This license lets others remix, adapt, and build upon your work non-commercially, and although their new works must also acknowledge you and be non-commercial, they don\'t have to license their derivative works on the same terms.',
        'RightsCategory': 'Creative Commons',
        'IsActive': True,
        'createdAt': current_time,
        'updatedAt': current_time,
        'sourceURL': 'https://creativecommons.org/licenses/by-nc/4.0/'
    },
    {
        'RightsURI': 'https://creativecommons.org/licenses/by-nd/4.0/',
        'RightsCode': 'CC BY-ND',
        'RightsLabel': 'Attribution-NoDerivatives 4.0 International (CC BY-ND 4.0)',
        'RightsDescription': 'This license lets others reuse the work for any purpose, including commercially; however, it cannot be shared with others in adapted form, and credit must be provided to you.',
        'RightsCategory': 'Creative Commons',
        'IsActive': True,
        'createdAt': current_time,
        'updatedAt': current_time,
        'sourceURL': 'https://creativecommons.org/licenses/by-nd/4.0/'
    },
    {
        'RightsURI': 'https://creativecommons.org/licenses/by-nc-sa/4.0/',
        'RightsCode': 'CC BY-NC-SA',
        'RightsLabel': 'Attribution-NonCommercial-ShareAlike 4.0 International (CC BY-NC-SA 4.0)',
        'RightsDescription': 'This license lets others remix, adapt, and build upon your work non-commercially, as long as they credit you and license their new creations under the identical terms.',
        'RightsCategory': 'Creative Commons',
        'IsActive': True,
        'createdAt': current_time,
        'updatedAt': current_time,
        'sourceURL': 'https://creativecommons.org/licenses/by-nc-sa/4.0/'
    },
    {
        'RightsURI': 'https://creativecommons.org/licenses/by-nc-nd/4.0/',
        'RightsCode': 'CC BY-NC-ND',
        'RightsLabel': 'Attribution-NonCommercial-NoDerivatives 4.0 International (CC BY-NC-ND 4.0)',
        'RightsDescription': 'This license is the most restrictive of our six main licenses, only allowing others to download your works and share them with others as long as they credit you, but they can\'t change them in any way or use them commercially.',
        'RightsCategory': 'Creative Commons',
        'IsActive': True,
        'createdAt': current_time,
        'updatedAt': current_time,
        'sourceURL': 'https://creativecommons.org/licenses/by-nc-nd/4.0/'
    }
]

statements.extend(creative_commons_licenses)
print(f"✅ Added {len(creative_commons_licenses)} Creative Commons licenses")
print(f"   Total statements to populate: {len(statements)}")
print(f"   Expected entries: (12 rightsstatements + 8 creative commons) × 2 protocols = {len(statements) * 2}")

# Clear existing table data
print(f"\n🗑️  Clearing existing table data...")
try:
    scan = table.scan()
    with table.batch_writer() as batch:
        for item in scan['Items']:
            batch.delete_item(Key={'RightsURI': item['RightsURI']})
    print(f"✅ Cleared {len(scan['Items'])} existing items")
except Exception as e:
    print(f"⚠️  WARNING: Could not clear table: {e}")
    print(f"   Continuing with population (will overwrite existing items)...")

# Populate table with new data (both HTTP and HTTPS versions for all statements)
print(f"\n📝 Populating table with statements (both http:// and https:// for all)...\n")

success_count = 0
error_count = 0

# Add statements (both http:// and https:// for all URIs)
for idx, statement in enumerate(statements, 1):
    # Add both HTTP and HTTPS versions for all statements
    # Add HTTP version
    try:
        http_statement = statement.copy()
        http_statement['RightsURI'] = http_statement['RightsURI'].replace('https://', 'http://')
        table.put_item(Item=http_statement)
        success_count += 1
        print(f"✅ {idx}a: {statement['RightsCode']:15} - {statement['RightsLabel']} (http)")
    except Exception as e:
        error_count += 1
        print(f"❌ {idx}a: Failed to add {statement.get('RightsCode', 'UNKNOWN')} (http): {e}")
    
    # Add HTTPS version
    try:
        https_statement = statement.copy()
        https_statement['RightsURI'] = https_statement['RightsURI'].replace('http://', 'https://')
        table.put_item(Item=https_statement)
        success_count += 1
        print(f"✅ {idx}b: {statement['RightsCode']:15} - {statement['RightsLabel']} (https)")
    except Exception as e:
        error_count += 1
        print(f"❌ {idx}b: Failed to add {statement.get('RightsCode', 'UNKNOWN')} (https): {e}")

# Calculate total expected: all statements × 2 protocols (http and https)
rightsstatements_count = len([s for s in statements if 'rightsstatements.org' in s['RightsURI']])
cc_count = len([s for s in statements if 'creativecommons.org' in s['RightsURI']])
total_expected = len(statements) * 2  # All statements get both HTTP and HTTPS

# Summary
print(f"\n" + "="*70)
print(f"SUMMARY")
print(f"="*70)
print(f"✅ Successfully added: {success_count}")
print(f"❌ Errors: {error_count}")
print(f"📊 Total expected: {total_expected}")
print(f"   - RightsStatements.org: {rightsstatements_count} statements × 2 protocols = {rightsstatements_count * 2} entries")
print(f"   - Creative Commons: {cc_count} licenses × 2 protocols = {cc_count * 2} entries")
print(f"="*70)

if success_count == total_expected:
    print(f"\n✨ All rights statements successfully populated!")
    print(f"   Total entries: {success_count}")
    print(f"   Your validation will work for:")
    print(f"   - RightsStatements.org (both http:// and https://)")
    print(f"   - Creative Commons (both http:// and https://)")
else:
    print(f"\n⚠️  Some errors occurred. Check the output above.")
