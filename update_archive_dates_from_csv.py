#!/usr/bin/env python3
"""
Update DynamoDB 'date' attribute (list of strings) from a grouped CSV.

Input CSV format (produced by dynamodb_column_uniques.py):
- item_category, identifier, date_iso_list (JSON array), date_count

Usage examples:
  python update_archive_dates_from_csv.py \
    --csv dynamodb_item_dates_iso_grouped.csv \
    --table Archive-77eik3yv7rbdbjhjemas6h7dmi-vtdlppprd \
    --region us-east-1 \
    --identifier-index identifier-index \
    --dry-run

If --identifier-index is provided, the script queries the GSI to map
identifier -> primary key (assumes hash key 'id'). If not provided, the
script does a paginated Scan (slower). For tables with composite keys,
use --pk-name and optionally --sk-name with additional logic.

Notes:
- The script only updates items where the new ISO list is non-empty.
- Use --filter-category federated to limit updates to that category.
- Use --dry-run to preview without making changes.
"""

import argparse
import csv
import json
import sys
from typing import Optional, Dict, Any

import boto3
from boto3.dynamodb.conditions import Key


def parse_args():
    p = argparse.ArgumentParser(description='Update DynamoDB date list from CSV')
    p.add_argument('--csv', default='dynamodb_item_dates_iso_grouped.csv', help='Grouped input CSV')
    p.add_argument('--table', required=True, help='DynamoDB table name')
    p.add_argument('--region', default='us-east-1', help='AWS region')
    p.add_argument('--identifier-index', default=None, help='GSI name to query by identifier (e.g., identifier-index)')
    p.add_argument('--pk-name', default='id', help='Partition key attribute name (default: id)')
    p.add_argument('--filter-category', default=None, help='Only update rows with this item_category (e.g., federated)')
    p.add_argument('--dry-run', action='store_true', help='Show actions but do not update DynamoDB')
    return p.parse_args()


def get_item_key_by_identifier(table, identifier: str, gsi_name: Optional[str], pk_name: str) -> Optional[Dict[str, Any]]:
    """Resolve the primary key by identifier using a GSI if available, otherwise Scan.
    Assumes items store an 'identifier' top-level attribute.
    Returns {pk_name: value} or None if not found.
    """
    # Try GSI fast-path
    if gsi_name:
        resp = table.query(IndexName=gsi_name, KeyConditionExpression=Key('identifier').eq(identifier))
        items = resp.get('Items', [])
        if items:
            return {pk_name: items[0][pk_name]}
        return None

    # Slow fallback: scan
    scan_kwargs = {}
    while True:
        resp = table.scan(**scan_kwargs)
        for it in resp.get('Items', []):
            if it.get('identifier') == identifier:
                return {pk_name: it[pk_name]}
        if 'LastEvaluatedKey' not in resp:
            break
        scan_kwargs['ExclusiveStartKey'] = resp['LastEvaluatedKey']
    return None


def main():
    args = parse_args()
    ddb = boto3.resource('dynamodb', region_name=args.region)
    table = ddb.Table(args.table)

    updated = 0
    missing = 0
    skipped_empty = 0

    with open(args.csv, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            if args.filter_category and row.get('item_category') != args.filter_category:
                continue

            identifier = row.get('identifier')
            if not identifier:
                continue

            try:
                iso_list = json.loads(row.get('date_iso_list') or '[]')
            except Exception:
                iso_list = []

            if not iso_list:
                skipped_empty += 1
                continue

            key = get_item_key_by_identifier(table, identifier, args.identifier_index, args.pk_name)
            if not key:
                missing += 1
                print(f"WARN: Could not resolve key for identifier={identifier}")
                continue

            if args.dry_run:
                print(f"DRY RUN: would update {key} identifier={identifier} date -> {iso_list}")
                updated += 1
                continue

            table.update_item(
                Key=key,
                UpdateExpression='SET #d = :vals',
                ExpressionAttributeNames={'#d': 'date'},
                ExpressionAttributeValues={':vals': iso_list},
            )
            updated += 1

    print(f"Done. updated={updated} missing_key={missing} skipped_empty={skipped_empty}")


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)
