from flask import Flask
import boto3
import time

app = Flask(__name__)

ATHENA_DATABASE = "orders_db"
S3_OUTPUT = "s3://assign3-orders-pipeline/enriched/"
REGION = "us-east-2"  # change if needed

athena = boto3.client('athena', region_name=REGION)

def run_query(query):
    response = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': ATHENA_DATABASE},
        ResultConfiguration={'OutputLocation': S3_OUTPUT}
    )
    return response['QueryExecutionId']

@app.route('/')
def home():
    query = 'SELECT "Customer", SUM("Amount") AS total_sales FROM processed GROUP BY "Customer"'
    run_query(query)
    return "<h1>Athena Dashboard Running</h1><p>Query Executed Successfully</p>"

app.run(host='0.0.0.0', port=5000)