import boto3
import xml.etree.ElementTree as ET
import os

# DEBUG: Script started
print('DEBUG: Starting dpla_xmloutput.py')

# DynamoDB setup
# Testing with preproduction first and then run it on production
# JLG 09/08/2025
# This script exports DynamoDB records to XML files for use in DPLA ingestion.
# It assumes a specific schema in DynamoDB and maps fields to XML elements.
#
# can you programmatically get all the collection identifiers and loop through them? to pass that as a variable?
#
env = {}
env["region_name"] = "set in .sh file"
env["COLLECTION_IDENTIFIER"] = os.getenv("COLLECTION_IDENTIFIER")
env["REGION"] = os.getenv("REGION")
env["DYNAMODB_TABLE_SUFFIX"] = os.getenv("DYNAMODB_TABLE_SUFFIX")
env["LONG_URL_PATH"] = os.getenv("LONG_URL_PATH")
env["TYPE"] = os.getenv("TYPE")

# DEBUG: Print environment variables
print(f'DEBUG: Environment variables loaded: {env}')

# Check for missing environment variables
for key in ["COLLECTION_IDENTIFIER", "REGION", "DYNAMODB_TABLE_SUFFIX", "LONG_URL_PATH", "TYPE"]:
    if not env[key]:
        print(f'WARNING: Environment variable {key} is not set!')

# Setup DynamoDB resource
try:
    dynamodb = boto3.resource("dynamodb", env["REGION"])
    dbtable = dynamodb.Table(env["DYNAMODB_TABLE_SUFFIX"])
    print(f'DEBUG: Connected to DynamoDB table: {env["DYNAMODB_TABLE_SUFFIX"]}')
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
        print(f'DEBUG: Registered namespace {prefix}: {uri}')

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
                    print(f'DEBUG: Adding multi-value for field "{field}": {v}')
                    ET.SubElement(root, f"{{{NSMAP['dcterms']}}}{field}").text = str(v)
            else:
                if field == "language":
                    value = get_iso_639_2_code(value)
                ET.SubElement(root, f"{{{NSMAP['dcterms']}}}{field}").text = str(value)

    # Always add format as image/tiff for display
    ET.SubElement(root, f"{{{NSMAP['dcterms']}}}format").text = "image/tiff"

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

    # Always add date element mapped from createdAt if present
    created_at = item.get("createdAt")
    if created_at:
        ET.SubElement(root, f"{{{NSMAP['dcterms']}}}date").text = str(created_at)

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
items = [item for item in items if item.get("identifier", "").upper().startswith("SQI")]
print(f'DEBUG: Filtered items for squires only, count: {len(items)}')
#items = sorted(items, key=lambda x: x.get("identifier", ""))
#print('DEBUG: Items sorted by identifier for output order.')

# Mapping for identifier prefixes to folders/subfolders
def get_output_subdir(identifier):
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
    print(f'DEBUG: Raw item: {item}')
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
