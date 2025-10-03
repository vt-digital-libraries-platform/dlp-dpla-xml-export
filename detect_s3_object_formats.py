import boto3
import os
from datetime import datetime
import mimetypes

# S3 bucket and prefix
bucket_name = os.environ.get('S3_BUCKET')
prefix = os.environ.get('S3_PREFIX')

print(f"DEBUG: Scanning S3 bucket '{bucket_name}' with prefix '{prefix}'")

s3 = boto3.client('s3')
paginator = s3.get_paginator('list_objects_v2')
page_iterator = paginator.paginate(Bucket=bucket_name, Prefix=prefix)

results = []
for page in page_iterator:
    for obj in page.get('Contents', []):
        key = obj['Key']
        # Detect format from file extension
        ext = os.path.splitext(key)[1].lower()
        mime, _ = mimetypes.guess_type(key)
        if mime:
            detected_fmt = mime
        elif ext in ['.tif', '.tiff']:
            detected_fmt = 'image/tiff'
        elif ext in ['.jpg', '.jpeg']:
            detected_fmt = 'image/jpeg'
        elif ext == '.png':
            detected_fmt = 'image/png'
        elif ext == '.pdf':
            detected_fmt = 'application/pdf'
        else:
            detected_fmt = 'unknown'
        results.append((key, detected_fmt))
        print(f"{key}: {detected_fmt}")

# Write results to a timestamped log file
timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
output_path = f"squires_s3_formats_{timestamp}.txt"
print(f"DEBUG: Writing formats to {output_path}")

with open(output_path, 'w', encoding='utf-8') as f:
    for key, detected_fmt in results:
        f.write(f"{key}: {detected_fmt}\n")
    print(f"DEBUG: Results written to {output_path}")