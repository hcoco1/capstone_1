import json
import io 
import boto3
import csv
from decimal import Decimal, InvalidOperation

def lambda_handler(event, context):
    s3 = boto3.client('s3')
    
    # Direct references for S3 bucket names and DynamoDB table name
    raw_bucket = 'capstone1stack-rawpropertieseb3d18db-0zbcz2kgk18i'
    processed_bucket = 'capstone1stack-processedproperties39d0d984-8qiiaclegnxp'
    table_name = 'Capstone1Stack-PropertiesTable324F3970-1VOIGTHD4R145'

    try:
        # Log the event details
        print("Event: ", json.dumps(event))
        
        # Retrieving the S3 object based on event triggers
        raw_bucket = event['Records'][0]['s3']['bucket']['name']
        csv_filename = event['Records'][0]['s3']['object']['key']
        
        # Log the bucket name and object key
        print(f"Bucket: {raw_bucket}, Key: {csv_filename}")
        
        obj = s3.get_object(Bucket=raw_bucket, Key=csv_filename)
        csv_data = obj['Body'].read().decode('utf-8').splitlines()
        
        processed_rows = []
        required_keys = ['zpid', 'streetAddress', 'unit', 'bedrooms', 
                         'bathrooms', 'homeType', 'priceChange', 'zipcode', 'city', 
                         'state', 'country', 'livingArea', 'taxAssessedValue', 
                         'priceReduction', 'datePriceChanged', 'homeStatus', 'price', 'currency']
        
        # Parsing CSV data
        reader = csv.DictReader(csv_data)
        for row in reader:
            filtered_row = {key: row[key] for key in required_keys if key in row}
            
            # Implement the currency conversion logic here
            if 'price' in filtered_row and 'currency' in filtered_row:
                try:
                    original_price = Decimal(filtered_row['price'])
                    filtered_row['price'] = convert_currency(original_price, filtered_row['currency'])
                    filtered_row['currency'] = 'USD'  # Update currency to USD after conversion
                except (InvalidOperation, ValueError) as e:
                    print(f"Error converting price for zpid {row.get('zpid', 'unknown')}: {e}")
                    continue
            
            processed_rows.append(filtered_row)
        
        # Debugging: Print processed rows before uploading
        print("Processed Rows: ", processed_rows)
        
        # Upload processed data to DynamoDB
        if processed_rows:
            upload_to_dynamodb(table_name, processed_rows)
            # Write processed CSV data to S3
            upload_processed_data_to_s3(processed_bucket, csv_filename, processed_rows)
            move_file_to_processed_bucket(raw_bucket, processed_bucket, csv_filename)
        
        return {
            'statusCode': 200,
            'body': json.dumps('Currency Standardization complete!')
        }

    except Exception as e:
        print(f"Error processing the file: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps(f"Error processing the file: {e}")
        }

# Complete the currency conversion function
def convert_currency(price, currency):
    conversion_rates = {
        'CAD': Decimal('0.75'),  # Example conversion rate from CAD to USD
        'USD': Decimal('1.00')
    }
    return price * conversion_rates.get(currency, Decimal('1.00'))

# Complete the function to upload data to DynamoDB
def upload_to_dynamodb(table_name, items):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(table_name)
    
    with table.batch_writer() as batch:
        for item in items:
            # Debugging: Print each item being uploaded
            print("Uploading item: ", item)
            batch.put_item(Item=item)

# Function to upload processed CSV data to S3
def upload_processed_data_to_s3(bucket_name, original_file_key, processed_rows):
    s3 = boto3.client('s3')
    processed_csv = convert_to_csv(processed_rows)
    processed_file_key = f"processed_{original_file_key}"
    
    s3.put_object(Bucket=bucket_name, Key=processed_file_key, Body=processed_csv)
    print(f"Processed file uploaded to {bucket_name}/{processed_file_key}")

def convert_to_csv(data):
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=data[0].keys())
    writer.writeheader()
    writer.writerows(data)
    return output.getvalue()

# Complete the function to move files within S3
def move_file_to_processed_bucket(raw_bucket_name, processed_bucket_name, file_key):
    s3 = boto3.resource('s3')
    copy_source = {'Bucket': raw_bucket_name, 'Key': file_key}
    
    s3.Object(processed_bucket_name, file_key).copy_from(CopySource=copy_source)
    s3.Object(raw_bucket_name, file_key).delete()