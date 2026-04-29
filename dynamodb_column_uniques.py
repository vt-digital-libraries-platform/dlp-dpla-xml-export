import boto3
import csv
from collections import defaultdict


# Initialize DynamoDB
dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
#dynamodb = boto3.resource('dynamodb')
dbtable = dynamodb.Table('Archive-77eik3yv7rbdbjhjemas6h7dmi-vtdlppprd')

#Facet columns to focus on for the DLP
columns_to_count = ['format','format_physical', 'language', 'medium','subject','type','date']  # Replace with your real columns
filter_values = ['federated', 'iawa']  # item_category values to run separate reports for

# scan the archive table
def full_scan():
    all_items = []
    response = dbtable.scan()
    all_items.extend(response['Items'])

    while 'LastEvaluatedKey' in response:
        response = dbtable.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
        all_items.extend(response['Items'])

    return all_items

# below is the section to count distinct values for selected columns
def group_counts(items, group_column, target_columns):
    """Count distinct values per target column grouped by group_column.
    Handles single values, lists, and simple dicts.
    """
    stats = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))

    def normalize_values(v):
        if v is None or v == '':
            return []
        if isinstance(v, list):
            return [str(x) for x in v if x not in (None, '')]
        if isinstance(v, dict):
            # Flatten simple dicts like {'start': '1990', 'end': '1995'}
            out = []
            for k, val in v.items():
                if val not in (None, ''):
                    out.append(f"{k}:{val}")
            return out
        return [str(v)]

    for item in items:
        group = item.get(group_column)
        if not group:
            continue  # Skip items without item_category

        for col in target_columns:
            if col not in item:
                continue
            for vv in normalize_values(item[col]):
                stats[group][col][vv] += 1

    return stats
# Step 3: Export grouped counts to CSV
def export_grouped_counts_to_csv(stats, filename):
    with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['item_category', 'archivecolumn_name', 'archivecolumn_value', 'count_per_value'])

        for item_category, column_data in stats.items():
            if item_category == "federated":
                for column, value_counts in column_data.items():
                    for value, count in value_counts.items():
                        writer.writerow([item_category, column, value, count])

# New: Export a per-item CSV listing items that have one or more date values
def export_item_dates(items, outfile='dynamodb_item_dates.csv', category_filter=None):
    """Write one row per date value for items that include a 'date' attribute.
    If category_filter is provided (e.g., 'federated'), only include that item_category.
    Columns: item_category, identifier, date_value
    """
    with open(outfile, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['item_category', 'identifier', 'date_value'])

        for it in items:
            if category_filter and it.get('item_category') != category_filter:
                continue
            if 'date' not in it or not it['date']:
                continue

            identifier = it.get('identifier') or it.get('custom_key') or it.get('id') or ''
            val = it['date']

            if isinstance(val, list):
                vals = val
            else:
                vals = [val]

            for v in vals:
                if v in (None, ''):
                    continue
                if isinstance(v, dict):
                    for k, vv in v.items():
                        if vv not in (None, ''):
                            writer.writerow([it.get('item_category', ''), identifier, f'{k}:{vv}'])
                else:
                    writer.writerow([it.get('item_category', ''), identifier, str(v)])

# Run all steps
items = full_scan()
stats = group_counts(items, group_column='item_category', target_columns=columns_to_count)
export_grouped_counts_to_csv(stats, 'dynamodb_column_unique_facets.csv')

# Also export a per-item list of date values wherever present (all categories)
export_item_dates(items, outfile='dynamodb_item_dates.csv', category_filter=None)

print("Export complete: dynamodb_column_unique_facets.csv and dynamodb_item_dates.csv")

# --- Date parsing helpers ---
import re
from datetime import datetime

MONTH_NAMES = {
    'jan': '01', 'january': '01',
    'feb': '02', 'february': '02',
    'mar': '03', 'march': '03',
    'apr': '04', 'april': '04',
    'may': '05',
    'jun': '06', 'june': '06',
    'jul': '07', 'july': '07',
    'aug': '08', 'august': '08',
    'sep': '09', 'sept': '09', 'september': '09',
    'oct': '10', 'october': '10',
    'nov': '11', 'november': '11',
    'dec': '12', 'december': '12',
}

def _clean_date_string(s: str) -> str:
    s = s.strip()
    # strip prefixes like 'start:' or 'end:'
    if ':' in s:
        parts = s.split(':', 1)
        if parts[0].lower() in ('start', 'end'):
            s = parts[1].strip()
    # remove circa prefixes
    s = re.sub(r'^(circa|c\.|ca\.|ca)\s+', '', s, flags=re.IGNORECASE)
    return s

def parse_date_to_iso(value: str) -> str:
    """Best-effort parse to ISO date string.
    Returns one of: YYYY, YYYY-MM, or YYYY-MM-DD. Empty string if not parseable.
    """
    if not value:
        return ''
    s = _clean_date_string(str(value))

    # 1) Exact ISO patterns
    if re.fullmatch(r"\d{4}", s):
        return s
    if re.fullmatch(r"\d{4}-\d{2}", s):
        return s
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", s):
        return s

    # 2) Slash-separated M/D/YYYY or MM/DD/YYYY
    if re.fullmatch(r"\d{1,2}/\d{1,2}/\d{4}", s):
        m, d, y = s.split('/')
        try:
            dt = datetime(int(y), int(m), int(d))
            return dt.strftime('%Y-%m-%d')
        except Exception:
            return ''

    # 3) Month name formats like 'August 20, 1947' or 'Aug 1947' or 'Aug 20 1947'
    m = re.match(r"^([A-Za-z]+)\s+(\d{1,2})(?:,)?\s+(\d{4})$", s)
    if m:
        mon, day, year = m.groups()
        mm = MONTH_NAMES.get(mon.lower())
        if mm:
            try:
                dt = datetime(int(year), int(mm), int(day))
                return dt.strftime('%Y-%m-%d')
            except Exception:
                return ''

    m = re.match(r"^([A-Za-z]+)\s+(\d{4})$", s)
    if m:
        mon, year = m.groups()
        mm = MONTH_NAMES.get(mon.lower())
        if mm:
            return f"{year}-{mm}"

    # 4) Year-month-day with time component — take date part
    m = re.match(r"^(\d{4}-\d{2}-\d{2})[T\s].*$", s)
    if m:
        return m.group(1)

    # 5) Year range handled elsewhere (we split on '/')
    if '/' in s:
        left = s.split('/', 1)[0].strip()
        return parse_date_to_iso(left)

    # 6) Fallback: try datetime.fromisoformat on lenient replacements
    try:
        dt = datetime.fromisoformat(s.replace(' ', 'T'))
        return dt.date().isoformat()
    except Exception:
        return ''

# Group ISO dates per identifier into a single row (list) for easier DynamoDB updates
import json

_ISO_INTERVAL_RE = re.compile(r"^\d{4}(?:-\d{2}(?:-\d{2})?)?/\d{4}(?:-\d{2}(?:-\d{2})?)?$")

def _is_iso_interval(s: str) -> bool:
    """Return True if s looks like an ISO interval (YYYY[/YYYY], YYYY-MM[/YYYY-MM], or YYYY-MM-DD[/YYYY-MM-DD]).
    Avoids treating MM/DD/YYYY as an interval.
    """
    if not s:
        return False
    return bool(_ISO_INTERVAL_RE.fullmatch(s.strip()))

def write_grouped_iso_csv(input_file='dynamodb_item_dates.csv', output_file='dynamodb_item_dates_iso_grouped.csv'):
    groups = {}
    with open(input_file, 'r', encoding='utf-8') as inf:
        reader = csv.DictReader(inf)
        for row in reader:
            ident = row.get('identifier', '')
            cat = row.get('item_category', '')
            original = row.get('date_value', '')
            if not ident:
                continue
            if ident not in groups:
                groups[ident] = {
                    'item_category': cat,
                    'identifier': ident,
                    'date_value_list': [],
                    'date_iso_list': []
                }
            # keep all provided values; we'll de-dup by iso below
            if original:
                groups[ident]['date_value_list'].append(original)
            # parse ISO for the original value
            iso_once = parse_date_to_iso(original)
            if iso_once:
                groups[ident]['date_iso_list'].append(iso_once)
            # If original is an ISO interval (start/end), expand into separate list entries
            if original and _is_iso_interval(original):
                parts = [p.strip() for p in original.split('/') if p and p.strip()]
                for p in parts:
                    iso_part = parse_date_to_iso(p)
                    if iso_part:
                        groups[ident]['date_iso_list'].append(iso_part)

    # De-duplicate while preserving all ISO endpoints from intervals.
    for ident, rec in groups.items():
        # Keep original values (optionally de-dup preserve order)
        dv_seen, dv_unique = set(), []
        for dv in rec['date_value_list']:
            if dv and dv not in dv_seen:
                dv_seen.add(dv)
                dv_unique.append(dv)
        rec['date_value_list'] = dv_unique

        # Unique ISO values, preserving order; include both start/end of intervals
        iso_seen, iso_unique = set(), []
        for di in rec['date_iso_list']:
            if di and di not in iso_seen:
                iso_seen.add(di)
                iso_unique.append(di)
        rec['date_iso_list'] = iso_unique

    with open(output_file, 'w', newline='', encoding='utf-8') as outf:
        writer = csv.writer(outf)
        writer.writerow(['item_category', 'identifier', 'date_value', 'date_value_list', 'date_iso_list'])
        for rec in groups.values():
            first_original = rec['date_value_list'][0] if rec['date_value_list'] else ''
            writer.writerow([
                rec['item_category'],
                rec['identifier'],
                first_original,
                json.dumps(rec['date_value_list'], ensure_ascii=False),
                json.dumps(rec['date_iso_list'], ensure_ascii=False)
            ])

write_grouped_iso_csv()
print("Export complete: dynamodb_item_dates_iso_grouped.csv")


