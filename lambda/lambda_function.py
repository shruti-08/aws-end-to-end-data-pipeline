import json
import boto3
import csv
from io import StringIO

s3 = boto3.client('s3')

def lambda_handler(event, context):

    # Get S3 details
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']

    print(f"Processing file: s3://{bucket}/{key}")

    # Read CSV file from S3
    response = s3.get_object(Bucket=bucket, Key=key)
    content = response['Body'].read().decode('utf-8')

    csv_reader = csv.DictReader(StringIO(content))

    output = StringIO()
    writer = None

    for row in csv_reader:

        # 🔹 Example processing logic
        # Keep only shipped or confirmed orders
        if row.get('Status') in ['shipped', 'confirmed']:

            # Convert amount to float (optional cleanup)
            if 'Amount' in row:
                try:
                    row['Amount'] = float(row['Amount'])
                except:
                    row['Amount'] = 0

            # Initialize writer
            if writer is None:
                writer = csv.DictWriter(output, fieldnames=row.keys())
                writer.writeheader()

            writer.writerow(row)

    # Save to processed folder
    output_key = key.replace("raw/", "processed/")

    s3.put_object(
        Bucket=bucket,
        Key=output_key,
        Body=output.getvalue()
    )

    print(f"Processed file saved to: s3://{bucket}/{output_key}")

    return {
        'statusCode': 200,
        'body': 'Processing complete'
    }