import boto3
import xml.etree.ElementTree as ET
import os
from datetime import datetime
import logging

# Add a timestamp to the log file name
log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'logs')
os.makedirs(log_dir, exist_ok=True)
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
log_filename = os.path.join(log_dir, f'display_dates_debug_{timestamp}.log')
logging.basicConfig(
    filename=log_filename,
    level=logging.DEBUG,
    format='%(asctime)s %(levelname)s %(message)s'
)
# DEBUG: Script started
print('DEBUG: Starting dpla_xmloutput.py')

# DynamoDB setup
# Testing with preproduction first and then run it on production
# JLG 09/08/2025
# This script exports DynamoDB records to XML files for use in DPLA ingestion.
# It assumes a specific schema in DynamoDB and maps fields to XML elements.

env = {}
env["region_name"] = "set in .sh file"
env["COLLECTION_IDENTIFIER"] = os.getenv("COLLECTION_IDENTIFIER")
env["REGION"] = os.getenv("REGION")
env["DYNAMODB_TABLE"] = os.getenv("DYNAMODB_TABLE")
env["LONG_URL_PATH"] = os.getenv("LONG_URL_PATH")
env["TYPE"] = os.getenv("TYPE")

# DEBUG: Print environment variables
print(f'DEBUG: Environment variables loaded: {env}')

# Check for missing environment variables
for key in ["COLLECTION_IDENTIFIER", "REGION", "DYNAMODB_TABLE", "LONG_URL_PATH", "TYPE"]:
    if not env[key]:
        print(f'WARNING: Environment variable {key} is not set!')

# Setup DynamoDB resource
try:
    dynamodb = boto3.resource("dynamodb", env["REGION"])
    dbtable = dynamodb.Table(env["DYNAMODB_TABLE"])
    print(f'DEBUG: Connected to DynamoDB table: {env["DYNAMODB_TABLE"]}')
    # Add folder lookup table connection
    folder_table_name = os.getenv("FOLDER_LOOKUP_TABLE")
    folder_table = dynamodb.Table(folder_table_name)
    print(f'DEBUG: Connected to folder lookup table: {folder_table_name}')
except Exception as e:
    print(f'ERROR: Failed to connect to DynamoDB: {e}')
    raise


# Namespace mapping
NSMAP = {
    "dc": "http://purl.org/dc/elements/1.1/",
    "dcterms": "http://purl.org/dc/terms/",
    "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
    "edm": "http://www.europeana.eu/schemas/edm/",
    None: "http://dplava.lib.virginia.edu"
}
# DEBUG: Registering XML namespaces
for prefix, uri in NSMAP.items():
    if prefix:  # skip default namespace
        ET.register_namespace(prefix, uri)
        #rint(f'DEBUG: Registered namespace {prefix}: {uri}')

# Function to look up ISO 639-2 code from language codes DynamoDB table
def get_iso_639_2_code(iso_639_1):
    """
    Look up the ISO 639-2 code from the DynamoDB table.
    Returns the 3-letter code if found, else returns the original value.
    """
    try:
        region = os.getenv("REGION")
        lang_table_name = os.getenv("LANGUAGE_CODES_TABLE")
        dynamodb_lang = boto3.resource("dynamodb", region_name=region)
        lang_table = dynamodb_lang.Table(lang_table_name)
        response = lang_table.get_item(Key={'iso_639_1': iso_639_1})
        return response['Item']['iso_639_2']
    except Exception as e:
        print(f"WARNING: Could not map language code '{iso_639_1}': {e}")
        return iso_639_1

def get_permalink(item):
    long_url_path = os.getenv("LONG_URL_PATH")
    item_type = os.getenv("TYPE")
    custom_key = item.get("custom_key", "")
    noid = custom_key.split("/")[-1] if custom_key else ""
    if long_url_path and item_type and noid:
        return f"{long_url_path.rstrip('/')}/{item_type}/{noid}"
    print(f"WARNING: Could not construct permalink for item: long_url_path={long_url_path}, item_type={item_type}, noid={noid}")
    return ""

def is_likely_date(s):
    # Accepts YYYY-MM-DD, YYYY-MM, YYYY/MM/DD, YYYY/MM, or 4-digit year
    s = s.strip()
    if len(s) == 4 and s.isdigit():
        return True
    try:
        datetime.strptime(s, "%Y-%m-%d")
        return True
    except Exception:
        pass
    try:
        datetime.strptime(s, "%Y-%m")
        return True
    except Exception:
        pass
    try:
        datetime.strptime(s, "%Y/%m/%d")
        return True
    except Exception:
        pass
    try:
        datetime.strptime(s, "%Y/%m")
        return True
    except Exception:
        pass
    return False

def parse_display_date(date_str):
    """
    Parse a date string and return it in YYYY-MM-DD format if possible.
    - If the input is already YYYY-MM-DD, return as is.
    - If only year is present, return just the year.
    - If year and month, return YYYY-MM.
    - If blank or unparseable, return 'undated'.
    - For ambiguous MM/DD/YYYY or DD/MM/YYYY, prefer U.S. style (MM/DD/YYYY), but if day > 12, treat as DD/MM/YYYY.
    - Parse a date string and return a list of YYYY-MM-DD formatted dates.
    - Handles single dates, date ranges (two dates), and multi-date lists separated by '/'.
    """
    if not date_str or not date_str.strip():
        return ["undated"]
    date_str = date_str.strip()

    # Handle multi-date lists like '1984-02-23/1984-05-31/1984-09-27/1985-01-31/1985-04-25'
    if "/" in date_str and not any(c.isalpha() for c in date_str):
        parts = [p.strip() for p in date_str.split("/")]
        # Only treat as multi-date if ALL parts are likely dates
        if len(parts) > 1 and all(is_likely_date(p) for p in parts):
            dates = []
            for part in parts:
                single = parse_display_date(part)
                if isinstance(single, list):
                    dates.extend(single)
                else:
                    dates.append(single)
            return dates

    # If already in YYYY-MM-DD, return as is
    try:
        dt = datetime.strptime(date_str, "%Y-%m-%d")
        return [dt.strftime("%Y-%m-%d")]
    except Exception:
        pass

    # Try YYYY/MM/DD
    try:
        dt = datetime.strptime(date_str, "%Y/%m/%d")
        return [dt.strftime("%Y-%m-%d")]
    except Exception:
        pass

    # Try MM/DD/YYYY vs DD/MM/YYYY (U.S. style, check for day > 12)
    if "/" in date_str:
        parts = [p.strip() for p in date_str.split("/")]
        if len(parts) == 3 and all(p.isdigit() for p in parts):
            month, day, year = int(parts[0]), int(parts[1]), int(parts[2])
            # If month > 12, it's likely DD/MM/YYYY
            if month > 12:
                try:
                    dt = datetime.strptime(date_str, "%d/%m/%Y")
                    return [dt.strftime("%Y-%m-%d")]
                except Exception:
                    pass
            else:
                try:
                    dt = datetime.strptime(date_str, "%m/%d/%Y")
                    return [dt.strftime("%Y-%m-%d")]
                except Exception:
                    pass

    # Try MM-DD-YYYY vs DD-MM-YYYY (U.S. style preferred, but check for day > 12)
    if "-" in date_str:
        parts = date_str.split("-")
        if len(parts) == 3 and all(p.isdigit() for p in parts):
            month, day, year = int(parts[0]), int(parts[1]), int(parts[2])
            # If day > 12, it's likely DD-MM-YYYY
            if day > 12:
                try:
                    dt = datetime.strptime(date_str, "%d-%m-%Y")
                    return [dt.strftime("%Y-%m-%d")]
                except Exception:
                    pass
            else:
                try:
                    dt = datetime.strptime(date_str, "%m-%d-%Y")
                    return [dt.strftime("%Y-%m-%d")]
                except Exception:
                    pass

    # Try other common formats
    fmts = [
        "%Y-%d-%m", "%Y.%m.%d", "%d.%m.%Y",
        "%B %d, %Y", "%b %d, %Y"
    ]
    for fmt in fmts:
        try:
            dt = datetime.strptime(date_str, fmt)
            return [dt.strftime("%Y-%m-%d")]
        except Exception:
            continue

    # Handle YYYY-MM or MM-YYYY
    try:
        dt = datetime.strptime(date_str, "%Y-%m")
        return [date_str]  # Only year and month, return as YYYY-MM
    except Exception:
        pass
    try:
        dt = datetime.strptime(date_str, "%m-%Y")
        return [dt.strftime("%Y-%m")]
    except Exception:
        pass

    # Handle only year
    try:
        dt = datetime.strptime(date_str, "%Y")
        return [date_str]  # Only year
    except Exception:
        pass

    # Handle custom cases like YYYY--DD
    if "--" in date_str:
        parts = date_str.split("--")
        if len(parts) == 2 and parts[0].isdigit() and parts[1].isdigit():
            return [f"{parts[0]}--{parts[1].zfill(2)}"]

    return ["undated"]

def build_xml(item):
    """
    Build XML for a single DynamoDB row.
    """
    print(f'DEBUG: Building XML for item: {item.get("identifier", "NO IDENTIFIER FOUND")}')
        # Create root with minimal attributes, custom serialization will handle formatting
    root = ET.Element("mdRecord")

    # Loop over expected dcterms fields
    dcterms_fields = [
        "identifier", "title", "description", "language", "contributor",
        "subject", "type", "isPartOf", "spatial", "medium", "format",
        "rights", "provenance"
    ]


    for field in dcterms_fields:
        if field in item:
            value = item[field]
            if isinstance(value, list):
                for v in value:
                    if field == "language":
                        v = get_iso_639_2_code(v)
                    #print(f'DEBUG: Adding multi-value for field "{field}": {v}')
                    ET.SubElement(root, f"{{{NSMAP['dcterms']}}}{field}").text = str(v)
            else:
                if field == "language":
                    value = get_iso_639_2_code(value)
                ET.SubElement(root, f"{{{NSMAP['dcterms']}}}{field}").text = str(value)

    # Always add provenance as required by DPLA
    ET.SubElement(
        root,
        f"{{{NSMAP['dcterms']}}}provenance"
    ).text = "Virginia Polytechnic Institute and State University. University Libraries"


    # edm fields
    # edm:isShownAt (permalink)
    permalink = get_permalink(item)
    if permalink:
        ET.SubElement(root, f"{{{NSMAP['edm']}}}isShownAt").text = permalink

    # edm:preview (thumbnail)
    thumbnail_path = item.get("thumbnail_path", "")
    if thumbnail_path:
        ET.SubElement(root, f"{{{NSMAP['edm']}}}preview").text = thumbnail_path

    # Add creator element if present
    creator = item.get("creator")
    if creator:
        if isinstance(creator, list):
            for c in creator:
                ET.SubElement(root, f"{{{NSMAP['dcterms']}}}creator").text = str(c)
        else:
            ET.SubElement(root, f"{{{NSMAP['dcterms']}}}creator").text = str(creator)

    # Replace createdAt with display_date logic
    display_dates = item.get("display_date", [])
    if not isinstance(display_dates, list):
        display_dates = [display_dates]
    print(f"DEBUG: Raw display_dates for {item.get('identifier', 'NO IDENTIFIER FOUND')}: {display_dates}")
    logging.debug(f"Raw display_dates for {item.get('identifier', 'NO IDENTIFIER FOUND')}: {display_dates}")
    if display_dates:
        for date_str in display_dates:
            formatted_dates = parse_display_date(date_str)
            print(f"DEBUG: Parsed display_date for {item.get('identifier', 'NO IDENTIFIER FOUND')}: {date_str} -> {formatted_dates}")
            logging.debug(f"Parsed display_date for {item.get('identifier', 'NO IDENTIFIER FOUND')}: {date_str} -> {formatted_dates}")
            # Only add date tags if not undated
            if formatted_dates != ["undated"]:
                for formatted_date in formatted_dates:
                    ET.SubElement(root, f"{{{NSMAP['dcterms']}}}date").text = formatted_date
# Do nothing if undated or no display_dates

    print(f'DEBUG: Finished building XML for item: {item.get("identifier", "NO IDENTIFIER FOUND")}')
    return root

# Query all items from DynamoDB (scan example, not efficient for big tables)
# Scan for all Federated and do each collection individually and put in collection folders
# JLG 09/08/2025

print('DEBUG: Scanning DynamoDB table for items (with pagination)...')
items = []
try:
    response = dbtable.scan()
    items.extend(response.get("Items", []))
    print(f'DEBUG: Retrieved {len(response.get("Items", []))} items from first scan.')
    while 'LastEvaluatedKey' in response:
        print('DEBUG: Fetching next page of results...')
        response = dbtable.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
        items.extend(response.get("Items", []))
        print(f'DEBUG: Retrieved {len(response.get("Items", []))} items from next scan. Total so far: {len(items)}')
    print(f'DEBUG: Total items retrieved from DynamoDB: {len(items)}')
except Exception as e:
    print(f'ERROR: Failed to scan DynamoDB table: {e}')
    items = []


# Output folder logic based on identifier
# Write directly to repo root
output_base_dir = os.path.dirname(os.path.abspath(__file__))
print(f'DEBUG: Output base directory set to repo root: {output_base_dir}')


# Filter and sort items for squires only
#items = [item for item in items if item.get("identifier", "").upper().startswith("SQI")]
#print(f'DEBUG: Filtered items for squires only, count: {len(items)}')
#items = sorted(items, key=lambda x: x.get("identifier", ""))
#print('DEBUG: Items sorted by identifier for output order.')
filter_prefix = os.getenv("IDENTIFIER_PREFIX", None)  # Set in your .sh script
if filter_prefix:
    items = [item for item in items if item.get("identifier", "").upper().startswith(filter_prefix.upper())]
    print(f'DEBUG: Filtered items for {filter_prefix} only, count: {len(items)}')
else:
    print(f'DEBUG: Processing all items, count: {len(items)}')
# Mapping for identifier prefixes to folders/subfolders
def get_output_subdir(identifier):
    """
    Look up folder mapping using GSI query on file_name.
    Falls back to hardcoded logic if DynamoDB lookup fails.
    """
    try:        
        # Query the GSI using file_name as partition key
        response = folder_table.query(
            IndexName='file_name-index',
            KeyConditionExpression=boto3.dynamodb.conditions.Key('file_name').eq(identifier.strip())
        )
        
        if response['Items']:
            folder = response['Items'][0].get('folder')
            print(f"DEBUG: Found folder mapping for identifier '{identifier}': {folder}")
            return folder
        else:
            print(f"WARNING: No DynamoDB mapping found for '{identifier}', using hardcoded fallback")
            return get_hardcoded_mapping(identifier)
            
    except Exception as e:
        print(f"WARNING: DynamoDB lookup failed for '{identifier}': {e}, using hardcoded fallback")
        return get_hardcoded_mapping(identifier)

def get_hardcoded_mapping(identifier):
    """Fallback to existing hardcoded logic"""
    identifier = identifier.upper()
    if identifier.startswith("SQI"):
        return "squires"
    if identifier.startswith("BTR"):
        return "barter"
    if identifier.startswith("CRW"):
        return "crewe"
    if identifier.startswith("MTG_MGM") or identifier.startswith("MTG_MGN"):
        return "montgomery"
    if identifier.startswith("TAU_ART"):
        return "taubman"
    # Currie subfolders by LJC pattern
    if identifier.startswith("LJC"):
        # Asia: LJC_118, LJC_120, LJC_121, LJC_135
        if any(identifier.startswith(f"LJC_{n}_") for n in ["118", "120", "121", "135"]):
            return "currie/currie-asia"
        # Central America: LJC_018
        if identifier.startswith("LJC_018_"):
            return "currie/currie-centralamerica"
        # CINVA: LJC_086
        if identifier.startswith("LJC_086_"):
            return "currie/currie-CINVA"
        # Colombia: LJC_019
        if identifier.startswith("LJC_019_"):
            return "currie/currie-colombia"
        # Egypt: LJC_020
        if identifier.startswith("LJC_020_"):
            return "currie/currie-egypt"
        # Europe: LJC_021
        if identifier.startswith("LJC_021_"):
            return "currie/currie-europe"
        # Italy: LJC_022
        if identifier.startswith("LJC_022_"):
            return "currie/currie-italy"
        # Japan: LJC_023
        if identifier.startswith("LJC_023_"):
            return "currie/currie-japan"
        # Mexico: LJC_024
        if identifier.startswith("LJC_024_"):
            return "currie/currie-mexico"
        # Nepal: LJC_025
        if identifier.startswith("LJC_025_"):
            return "currie/currie-nepal"
        # Panama: LJC_026
        if identifier.startswith("LJC_026_"):
            return "currie/currie-panama"
        # South America: LJC_027
        if identifier.startswith("LJC_027_"):
            return "currie/currie-southamerica"
        # Spain: LJC_028
        if identifier.startswith("LJC_028_"):
            return "currie/currie-spain"
        # United States: LJC_029
        if identifier.startswith("LJC_029_"):
            return "currie/currie-unitedstates"
    if identifier.startswith("NMCST"):
        return "nmcst"
    if identifier.startswith("SFDST"):
        return "sfdst"
    if identifier.startswith("XB17J67J"):
        return "xb17j67j"
    if identifier.startswith("MTG"):
        return "montgomery"
    if identifier.startswith("MS"):
        return "ms"
    if identifier.startswith("BHSST"):
        return "blacksburg-high-school"
    if identifier.startswith("VTCATALOG"):
        return "vt-catalog"
    if identifier.startswith("LDGST"):
        return "ldgst"
    if identifier.startswith("VTEC"):
        return "vtec"
    if identifier.startswith("EGG"):
        return "egg"
    if identifier.startswith("P6"):
        return "horse-teeth-clacker"
    if identifier.startswith("REY"):
        return "reynolds"
    if identifier.startswith("VA_AM"):
        return "va-amcollege-catalog"
    if identifier.startswith("VTGRAD"):
        return "vtgrad-catalog"
    if identifier.startswith("ITEM"):
        return "item"
    if identifier.startswith("699"):
        return "podcast"
    if identifier.startswith("PRADER"):
        return "prader-willi"
    # Default: None matched
    return None

def indent(elem, level=0):
    i = "\n" + level*"    "
    if len(elem):
        if not elem.text or not elem.text.strip():
            elem.text = i + "    "
        for e in elem:
            indent(e, level+1)
        if not e.tail or not e.tail.strip():
            e.tail = i
    else:
        if level and (not elem.tail or not elem.tail.strip()):
            elem.tail = i

for idx, item in enumerate(items):
    print(f'\nDEBUG: Processing item {idx+1}/{len(items)}')
    #rint(f'DEBUG: Raw item: {item}')
    xml_root = build_xml(item)

    identifier = item.get("identifier", f"item_{idx+1}")
    print(f'DEBUG: Current identifier: {identifier}')
    file_name = identifier + ".xml"

    output_subdir = get_output_subdir(identifier)
    print(f'DEBUG: Output subdir from mapping: {output_subdir}')

    if output_subdir:
        output_dir = os.path.join(output_base_dir, output_subdir)
        print(f'DEBUG: Output directory set to: {output_dir}')
    else:
        output_dir = output_base_dir
        print(f'DEBUG: Output directory defaulted to repo root: {output_dir}')

    os.makedirs(output_dir, exist_ok=True)
    print(f'DEBUG: Ensured output directory exists: {output_dir}')
    file_path = os.path.join(output_dir, file_name)
    print(f'DEBUG: Full file path for XML: {file_path}')

    indent(xml_root)
    print(f'DEBUG: Writing XML to file: {file_path}')
    try:
            # Custom serialization for exact root formatting
            def serialize_with_custom_root(root_elem):
                # Build the exact root opening tag
                root_tag = (
                    '<mdRecord xmlns="http://dplava.lib.virginia.edu"\n'
                    '    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"\n'
                    '    xmlns:dc="http://purl.org/dc/elements/1.1/" xmlns:dcterms="http://purl.org/dc/terms/"\n'
                    '    xmlns:edm="http://www.europeana.eu/schemas/edm/"\n'
                    '    xsi:schemaLocation="http://dplava.lib.virginia.edu\n'
                    'dplava.xsd">'
                )
                # Serialize children
                children = ET.tostring(root_elem, encoding="unicode", method="xml")
                # Remove the auto-generated root tag
                children = children[children.find('>')+1:]
                # Remove closing tag
                children = children[:children.rfind('</mdRecord>')]
                # Compose final XML
                return f'{root_tag}\n{children}</mdRecord>'

            xml_str = serialize_with_custom_root(xml_root)
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(xml_str)
            print(f"DEBUG: Successfully generated {file_path}")
    except Exception as e:
        print(f'ERROR: Failed to write XML file {file_path}: {e}')
        print(f'DEBUG: Identifier {identifier} mapped to folder: {output_dir}')
