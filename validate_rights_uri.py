"""
Script to validate rights statement URIs against the RightsStatement lookup table.

This script provides:
1. A reusable validation function
2. A standalone validation tool for testing
3. Batch validation for multiple URIs
4. Integration examples for use in dlp-dpla-xml-export.py

Requirements:
- boto3
- The RightsStatement table must exist and be populated

Usage:
  # Standalone validation
  export REGION=""
  export ENV="preprod"  # or "prod"
  python3 validate_rights_uri.py

  # Or import into your script
  from validate_rights_uri import validate_rights_uri, get_rights_info

Set AWS credentials in your environment or ~/.aws/credentials.
"""
import boto3
import os
import sys
from typing import Tuple, Optional, Dict

# Configuration from environment variables
REGION = os.environ.get('REGION')
ENV = os.environ.get('ENV')

# Table name (same for both preprod and prod)
TABLE_NAME = 'RightsStatement'

# Initialize DynamoDB resource (lazy loading)
_dynamodb = None
_table = None


def get_dynamodb_table():
    """Get or create DynamoDB table connection"""
    global _dynamodb, _table
    
    if _table is None:
        try:
            _dynamodb = boto3.resource('dynamodb', region_name=REGION)
            _table = _dynamodb.Table(TABLE_NAME)
        except Exception as e:
            print(f"❌ ERROR: Failed to connect to DynamoDB: {e}")
            raise
    
    return _table


def normalize_rights_uri(rights_uri: str) -> str:
    """
    Normalize a rights URI by removing query parameters.
    
    Query parameters (like ?language=en) are valid for display purposes but should
    be stripped for validation since the canonical URI doesn't include them.
    
    Args:
        rights_uri: The rights statement URI to normalize
        
    Returns:
        Normalized URI without query parameters
        
    Examples:
        >>> normalize_rights_uri('https://rightsstatements.org/vocab/InC-NC/1.0/?language=en')
        'https://rightsstatements.org/vocab/InC-NC/1.0/'
    """
    if not rights_uri:
        return rights_uri
    
    # Strip query parameters (everything after '?')
    if '?' in rights_uri:
        rights_uri = rights_uri.split('?')[0]
    
    return rights_uri


def validate_rights_uri(rights_uri: str) -> Tuple[bool, Optional[str], Optional[str]]:
    """
    Validate a rights statement URI against the lookup table.
    
    Args:
        rights_uri: The rights statement URI to validate
        
    Returns:
        Tuple of (is_valid, rights_code, error_message)
        - is_valid: Boolean indicating if URI is valid and active
        - rights_code: The short code (e.g., 'InC', 'NoC-US') if valid, None otherwise
        - error_message: Error description if invalid, None if valid
        
    Examples:
        >>> is_valid, code, error = validate_rights_uri('https://rightsstatements.org/vocab/InC/1.0/')
        >>> print(f"Valid: {is_valid}, Code: {code}")
        Valid: True, Code: InC
        
        >>> is_valid, code, error = validate_rights_uri('https://example.com/fake')
        >>> print(f"Valid: {is_valid}, Error: {error}")
        Valid: False, Error: URI not found in RightsStatement table
    """
    if not rights_uri:
        return False, None, "Rights URI is empty or None"
    
    # Normalize the URI (remove query parameters like ?language=en)
    normalized_uri = normalize_rights_uri(rights_uri)
    
    # REJECT /page/ URIs - only /vocab/ URIs are valid for metadata
    if 'rightsstatements.org/page/' in normalized_uri:
        return False, None, "Invalid URI: rightsstatements.org must use /vocab/ not /page/ for metadata"
    
    try:
        table = get_dynamodb_table()
        
        # Query the table with normalized URI
        response = table.get_item(Key={'RightsURI': normalized_uri})
        
        if 'Item' not in response:
            return False, None, f"URI not found in RightsStatement table"
        
        item = response['Item']
        
        # Check if the statement is active
        if not item.get('IsActive', False):
            return False, item.get('RightsCode'), f"Rights statement is marked as inactive"
        
        # Valid!
        return True, item.get('RightsCode'), None
        
    except Exception as e:
        return False, None, f"Database error: {str(e)}"


def get_rights_info(rights_uri: str) -> Optional[Dict]:
    """
    Get complete information about a rights statement.
    
    Args:
        rights_uri: The rights statement URI to look up
        
    Returns:
        Dictionary with all rights statement information, or None if not found
        
    Example:
        >>> info = get_rights_info('http://rightsstatements.org/vocab/InC/1.0/')
        >>> print(info['RightsLabel'])
        In Copyright
    """
    if not rights_uri:
        return None
    
    # Normalize the URI (remove query parameters like ?language=en)
    normalized_uri = normalize_rights_uri(rights_uri)
    
    try:
        table = get_dynamodb_table()
        response = table.get_item(Key={'RightsURI': normalized_uri})
        
        if 'Item' in response:
            return dict(response['Item'])
        else:
            return None
            
    except Exception as e:
        print(f"❌ ERROR: Failed to get rights info: {e}")
        return None


def validate_batch(rights_uris: list) -> Dict:
    """
    Validate multiple rights URIs at once.
    
    Args:
        rights_uris: List of rights URIs to validate
        
    Returns:
        Dictionary with validation results:
        {
            'valid': [uri1, uri2, ...],
            'invalid': [uri3, uri4, ...],
            'errors': {uri: error_message, ...}
        }
    """
    results = {
        'valid': [],
        'invalid': [],
        'errors': {}
    }
    
    for uri in rights_uris:
        is_valid, code, error = validate_rights_uri(uri)
        
        if is_valid:
            results['valid'].append(uri)
        else:
            results['invalid'].append(uri)
            results['errors'][uri] = error
    
    return results



#=============================================================================
# Standalone CLI tool for testing
# ============================================================================

def main():
    """Interactive CLI tool for testing rights URI validation"""
    
    print(f"{'='*70}")
    print(f"  Rights Statement URI Validator")
    print(f"{'='*70}")
    print(f"  Environment: {ENV}")
    print(f"  Region: {REGION}")
    print(f"  Table: {TABLE_NAME}")
    print(f"{'='*70}\n")
    
    # Common test URIs
    test_uris = [
        'http://rightsstatements.org/vocab/InC/1.0/',
        'http://rightsstatements.org/vocab/NoC-US/1.0/',
        'http://rightsstatements.org/vocab/InC-EDU/1.0/',
        'http://rightsstatements.org/vocab/CNE/1.0/',
        'http://rightsstatements.org/vocab/FAKE/1.0/',  # Invalid
        'http://example.com/not-real',  # Invalid
        'http://rightsstatements.org/vocab/lnC/1.0/',  # Typo (lowercase L instead of I)
    ]
    
    print("🧪 Testing common URIs:\n")
    
    valid_count = 0
    invalid_count = 0
    
    for uri in test_uris:
        is_valid, code, error = validate_rights_uri(uri)
        
        if is_valid:
            print(f"✅ VALID:   {uri}")
            print(f"           Code: {code}\n")
            valid_count += 1
        else:
            print(f"❌ INVALID: {uri}")
            print(f"           Error: {error}\n")
            invalid_count += 1
    
    print(f"{'─'*70}")
    print(f"📊 Results: {valid_count} valid, {invalid_count} invalid")
    print(f"{'─'*70}\n")
    
    # Interactive mode
    print("💡 Interactive Mode - Enter URIs to validate (or 'quit' to exit):\n")
    
    while True:
        try:
            user_input = input("Enter URI to validate: ").strip()
            
            if user_input.lower() in ['quit', 'exit', 'q']:
                print("\n👋 Goodbye!")
                break
            
            if not user_input:
                continue
            
            is_valid, code, error = validate_rights_uri(user_input)
            
            if is_valid:
                info = get_rights_info(user_input)
                print(f"\n✅ VALID!")
                print(f"   Code:        {info.get('RightsCode')}")
                print(f"   Label:       {info.get('RightsLabel')}")
                print(f"   Category:    {info.get('RightsCategory')}")
                print(f"   Description: {info.get('RightsDescription')[:100]}...")
            else:
                print(f"\n❌ INVALID!")
                print(f"   Error: {error}")
            
            print()  # Blank line
            
        except KeyboardInterrupt:
            print("\n\n👋 Interrupted. Goodbye!")
            break
        except Exception as e:
            print(f"\n❌ Error: {e}\n")


if __name__ == "__main__":
    # Check if table exists
    try:
        table = get_dynamodb_table()
        # Try to access the table to verify it exists
        table.table_status
    except Exception as e:
        print(f"❌ ERROR: Cannot access table '{TABLE_NAME}'")
        print(f"   {e}")
        print(f"\n💡 Make sure you've run:")
        print(f"   1. create_rights_statement_table.py")
        print(f"   2. populate_rights_statements.py")
        sys.exit(1)
    
    main()
