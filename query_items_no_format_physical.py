import boto3
import csv
import os
from datetime import datetime

# Get configuration from environment variables
REGION = os.getenv("REGION")
DYNAMODB_TABLE = os.getenv("DYNAMODB_TABLE")

if not DYNAMODB_TABLE:
    print("ERROR: DYNAMODB_TABLE environment variable is not set!")
    print("Please run this script with the environment variables set, e.g.:")
    print("  source run_vtdlp-dpla-xml-export.sh && python query_items_no_format_physical.py")
    exit(1)

print(f"Using region: {REGION}")
print(f"Using DynamoDB table: {DYNAMODB_TABLE}")

# Initialize DynamoDB
dynamodb = boto3.resource("dynamodb", region_name=REGION)
dbtable = dynamodb.Table(DYNAMODB_TABLE)

def scan_items_without_format_physical():
    """
    Scan the DynamoDB table and find all items without format_physical field.
    Returns a list of items that either don't have the field or have it as null/empty.
    """
    all_items = []
    items_without_format_physical = []
    
    print("Starting scan of DynamoDB table...")
    
    # Initial scan
    response = dbtable.scan()
    all_items.extend(response['Items'])
    print(f"Scanned {len(response['Items'])} items...")
    
    # Continue scanning if there are more pages
    while 'LastEvaluatedKey' in response:
        response = dbtable.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
        all_items.extend(response['Items'])
        print(f"Scanned {len(all_items)} items so far...")
    
    print(f"Total items scanned: {len(all_items)}")
    
    # Filter items without format_physical
    for item in all_items:
        format_physical = item.get('format_physical')
        # Check if format_physical is missing, None, empty string, or empty list
        if not format_physical or (isinstance(format_physical, list) and len(format_physical) == 0):
            items_without_format_physical.append(item)
    
    print(f"Found {len(items_without_format_physical)} items without format_physical")
    return items_without_format_physical

def save_to_csv(items, filename=None):
    """
    Save the items to a CSV file.
    Includes key fields: identifier, title, format, type, medium and description
    """
    if not filename:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"items_no_format_physical_{timestamp}.csv"
    
    # Define the fields to export
    fieldnames = [
        'identifier', 
        'title',
        'format', 
        'type',
        'medium',
        'description'
    ]
    
    print(f"Writing results to {filename}...")
    
    with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames, extrasaction='ignore')
        writer.writeheader()
        
        for item in items:
            # Convert list fields to strings for CSV
            row = {}
            for field in fieldnames:
                value = item.get(field, '')
                if isinstance(value, list):
                    # Join list items with semicolons
                    row[field] = '; '.join(str(v) for v in value)
                elif isinstance(value, dict):
                    # Convert dict to string representation
                    row[field] = str(value)
                else:
                    row[field] = value
            writer.writerow(row)
    
    print(f"Successfully wrote {len(items)} items to {filename}")
    return filename

def main():
    """Main execution function"""
    print("=" * 60)
    print("Query Items Without format_physical")
    print("=" * 60)
    
    # Scan for items without format_physical
    items = scan_items_without_format_physical()
    
    if items:
        # Save to CSV
        filename = save_to_csv(items)
        print(f"\nResults saved to: {filename}")
        
        # Print some statistics
        print("\n" + "=" * 60)
        print("Statistics:")
        print("=" * 60)
        
        # Count by item_category
        categories = {}
        for item in items:
            cat = item.get('item_category', 'unknown')
            categories[cat] = categories.get(cat, 0) + 1
        
        print("\nBreakdown by item_category:")
        for cat, count in sorted(categories.items()):
            print(f"  {cat}: {count}")
        
        # Count by identifier prefix
        prefixes = {}
        for item in items:
            identifier = item.get('identifier', '')
            if identifier:
                # Get first 3 characters as prefix
                prefix = identifier[:3].upper()
                prefixes[prefix] = prefixes.get(prefix, 0) + 1
        
        print("\nTop 10 identifier prefixes:")
        for prefix, count in sorted(prefixes.items(), key=lambda x: x[1], reverse=True)[:10]:
            print(f"  {prefix}: {count}")
    else:
        print("\nNo items found without format_physical!")
    
    print("\n" + "=" * 60)
    print("Query complete!")
    print("=" * 60)

if __name__ == "__main__":
    main()
