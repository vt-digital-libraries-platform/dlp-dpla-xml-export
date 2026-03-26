"""
Script to create RightsStatement lookup table in DynamoDB for validating
rights statement URIs used in DPLA metadata.

This table contains all 12 official rights statements from rightsstatements.org
and is used to validate that metadata only uses legitimate rights URIs.

Requirements:
- boto3

Usage:
  export REGION=""
  export ENV="preprod"  # or "prod"
  python3 create_rights_statement_table.py

Set AWS credentials in your environment or ~/.aws/credentials.
"""
import boto3
import os
import sys

# Configuration from environment variables
REGION = os.environ.get('REGION','us-east-1')
ENV = os.environ.get('ENV','prod')

# Table name (same for both preprod and prod)
TABLE_NAME = 'RightsStatement'

print(f"========================================")
print(f"Creating RightsStatement Table")
print(f"========================================")
print(f"Environment: {ENV}")
print(f"Region: {REGION}")
print(f"Table Name: {TABLE_NAME}")
print(f"========================================\n")

# Connect to DynamoDB
try:
    dynamodb = boto3.resource('dynamodb', region_name=REGION)
    print(f"✅ Connected to DynamoDB in region '{REGION}'")
except Exception as e:
    print(f"❌ ERROR: Failed to connect to DynamoDB: {e}")
    sys.exit(1)


def table_exists(table_name):
    """Check if a DynamoDB table exists"""
    try:
        existing_tables = [t.name for t in dynamodb.tables.all()]
        return table_name in existing_tables
    except Exception as e:
        print(f"❌ ERROR: Failed to list tables: {e}")
        return False


def create_table():
    """Create the RightsStatement table"""
    
    if table_exists(TABLE_NAME):
        print(f"⚠️  Table '{TABLE_NAME}' already exists.")
        response = input(f"Do you want to delete and recreate it? (yes/no): ")
        
        if response.lower() == 'yes':
            print(f"🗑️  Deleting existing table '{TABLE_NAME}'...")
            try:
                table = dynamodb.Table(TABLE_NAME)
                table.delete()
                # Wait for table to be deleted
                table.wait_until_not_exists()
                print(f"✅ Table deleted successfully")
            except Exception as e:
                print(f"❌ ERROR: Failed to delete table: {e}")
                sys.exit(1)
        else:
            print(f"ℹ️  Keeping existing table. Exiting.")
            sys.exit(0)
    
    print(f"\n📊 Creating table '{TABLE_NAME}'...")
    
    try:
        table = dynamodb.create_table(
            TableName=TABLE_NAME,
            KeySchema=[
                {
                    'AttributeName': 'RightsURI',
                    'KeyType': 'HASH'  # Partition key (primary key)
                }
            ],
            AttributeDefinitions=[
                {
                    'AttributeName': 'RightsURI',
                    'AttributeType': 'S'  # String
                }
            ],
            BillingMode='PAY_PER_REQUEST',  # No capacity planning needed
            Tags=[
                {
                    'Key': 'Environment',
                    'Value': ENV
                },
                {
                    'Key': 'Purpose',
                    'Value': 'DPLA Rights Statement Validation'
                },
                {
                    'Key': 'CreatedBy',
                    'Value': 'create_rights_statement_table.py'
                }
            ]
        )
        
        # Wait for table to be created
        print(f"⏳ Waiting for table to be created...")
        table.wait_until_exists()
        
        print(f"\n✅ SUCCESS! Table '{TABLE_NAME}' created successfully!")
        print(f"\n📋 Table Details:")
        print(f"   - Table Name: {TABLE_NAME}")
        print(f"   - Primary Key: RightsURI (String)")
        print(f"   - Billing Mode: PAY_PER_REQUEST")
        print(f"   - Environment: {ENV}")
        print(f"\n💡 Next Step: Run 'populate_rights_statements.py' to add the 12 rights statements")
        
        return table
        
    except Exception as e:
        print(f"❌ ERROR: Failed to create table: {e}")
        sys.exit(1)


if __name__ == "__main__":
    create_table()
