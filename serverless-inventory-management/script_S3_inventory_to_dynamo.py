import boto3
import csv
from datetime import datetime

# Configurations
s3_bucket_name = 'inventory-bucket-example'
s3_prefix = 'inventory_files'
ddb_table_name = 'Inventory'

# Connection to S3 and DynamoDB
s3 = boto3.client('s3')
ddb = boto3.resource('dynamodb')
table = ddb.Table(ddb_table_name)

# Function to import data from CSV files in S3 to DynamoDB
def import_inventory_data():
    print("Starting data import...")
    # List objects in the S3 bucket
    response = s3.list_objects_v2(Bucket=s3_bucket_name, Prefix=s3_prefix)
    
    # Loop through the objects
    for obj in response['Contents']:
        # Check if the object is a CSV file
        if obj['Key'].endswith('.csv'):
            # Read the CSV file from S3
            csv_obj = s3.get_object(Bucket=s3_bucket_name, Key=obj['Key'])
            lines = csv_obj['Body'].read().decode('utf-8').splitlines()
            
            # Read the CSV using tab delimiter
            reader = csv.DictReader(lines, delimiter='\t')
            
            # Flag to track if any data was inserted
            data_inserted = False
            
            # Loop through the rows of the CSV and insert into DynamoDB
            for row in reader:
                try:
                    # Format the timestamp for DynamoDB
                    timestamp = datetime.strptime(row['Timestamp'], '%Y-%m-%dT%H:%M:%S.%f%z')
                    timestamp_str = timestamp.strftime('%Y-%m-%dT%H:%M:%S')
                    
                    # Insert data into DynamoDB table
                    table.put_item(
                        Item={
                            'Timestamp': timestamp_str,
                            'WarehouseName': row['WarehouseName'],
                            'ItemId': row['ItemId'],
                            'ItemName': row['ItemName'],
                            'StockLevelChange': int(row['StockLevelChange'])
                        }
                    )
                    # Set flag to True if data is inserted
                    data_inserted = True
                except KeyError as e:
                    print(f"Failed to process CSV file. Missing column: {e}")
                except Exception as e:
                    print(f"Failed to insert data into DynamoDB: {e}")
            
            # Print the name of the file if data is inserted
            if data_inserted:
                print(f"Data from file '{obj['Key']}' inserted into DynamoDB.")
                    
    print("Data import completed.")

# Execute the import function
import_inventory_data()
