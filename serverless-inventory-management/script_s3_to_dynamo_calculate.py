import boto3
import csv
from datetime import datetime
from boto3.dynamodb.conditions import Key

# Configurations
s3_bucket_name = 'unique-name-for-inventory-bucket-example'
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
    for obj in response.get('Contents', []):
        # Check if the object is a CSV file
        if obj['Key'].endswith('.csv'):
            # Read the CSV file from S3
            csv_obj = s3.get_object(Bucket=s3_bucket_name, Key=obj['Key'])
            lines = csv_obj['Body'].read().decode('utf-8').splitlines()
            
            # Read the CSV using tab delimiter
            reader = csv.DictReader(lines, delimiter='\t')
            
            # Initialize a dictionary to store the latest stock level changes for each item
            latest_stock_changes = {}
            
            # Loop through the rows of the CSV to determine the latest stock level change for each item
            for row in reader:
                try:
                    # Format the timestamp for DynamoDB
                    timestamp = datetime.strptime(row['Timestamp'], '%Y-%m-%dT%H:%M:%S.%f%z')
                    timestamp_str = timestamp.strftime('%Y-%m-%dT%H:%M:%S')
                    
                    # Check if the item already exists in the latest_stock_changes dictionary
                    if row['ItemId'] in latest_stock_changes:
                        # If the item exists, update the stock level change and timestamp
                        existing_change = latest_stock_changes[row['ItemId']]
                        latest_stock_changes[row['ItemId']] = {
                            'Timestamp': max(existing_change['Timestamp'], timestamp),
                            'StockLevelChange': existing_change['StockLevelChange'] + int(row['StockLevelChange'])
                        }
                    else:
                        # If the item doesn't exist, add it to the dictionary
                        latest_stock_changes[row['ItemId']] = {
                            'Timestamp': timestamp,
                            'StockLevelChange': int(row['StockLevelChange']),
                            'ItemName': row['ItemName'],  # Store ItemName for updating purposes
                            'WarehouseName': row['WarehouseName']  # Store WarehouseName for updating purposes
                        }
                    
                except KeyError as e:
                    print(f"Failed to process CSV file. Missing column: {e}")
                except Exception as e:
                    print(f"Failed to process CSV row: {e}")
            
            # Loop through the items in latest_stock_changes and update DynamoDB
            for item_id, data in latest_stock_changes.items():
                try:
                    # Retrieve existing item from DynamoDB
                    existing_item = table.query(
                        KeyConditionExpression=Key('ItemId').eq(item_id)
                    )['Items']
                    
                    # Calculate the new stock level change
                    stock_level_change = data['StockLevelChange']
                    if existing_item:
                        previous_stock_level = int(existing_item[0]['StockLevelChange'])
                        stock_level_change += previous_stock_level
                    
                    # Insert or update data into DynamoDB table
                    table.put_item(
                        Item={
                            'ItemId': item_id,
                            'Timestamp': data['Timestamp'].strftime('%Y-%m-%dT%H:%M:%S'),
                            'WarehouseName': data['WarehouseName'],
                            'ItemName': data['ItemName'],
                            'StockLevelChange': stock_level_change
                        }
                    )
                    
                    print(f"Data for item '{item_id}' updated in DynamoDB.")
                    
                except Exception as e:
                    print(f"Failed to insert/update data into DynamoDB: {e}")
                    
            print(f"Data from file '{obj['Key']}' processed.")
                    
    print("Data import completed.")

# Execute the import function
import_inventory_data()
