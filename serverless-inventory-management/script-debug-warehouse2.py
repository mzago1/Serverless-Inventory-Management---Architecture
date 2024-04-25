import boto3
import csv
from datetime import datetime, timezone

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
            
            # Read the CSV using semicolon delimiter
            reader = csv.DictReader(lines, delimiter=';')
            
            # Initialize a dictionary to store the latest stock level changes for each item in each warehouse
            latest_stock_changes = {}
            
            # Loop through the rows of the CSV to determine the latest stock level change for each item in each warehouse
            for row in reader:
                try:
                    # Format the timestamp for DynamoDB
                    timestamp = datetime.strptime(row['Timestamp'], '%Y-%m-%dT%H:%M:%S.%f%z').astimezone(timezone.utc)
                    
                    # Check if the ItemName is not empty
                    if row['ItemName']:  
                        # Generate a unique key for each item in each warehouse
                        item_warehouse_key = (row['ItemId'], row['WarehouseName'])
                        
                        # Check if the item in this warehouse already exists in the latest_stock_changes dictionary
                        if item_warehouse_key in latest_stock_changes:
                            # If the item exists and the timestamp is more recent, update the stock level change
                            if timestamp > latest_stock_changes[item_warehouse_key]['Timestamp']:
                                latest_stock_changes[item_warehouse_key] = {
                                    'Timestamp': timestamp,
                                    'StockLevelChange': int(row['StockLevelChange']),
                                    'ItemName': row['ItemName'],  
                                    'WarehouseName': row['WarehouseName']  
                                }
                        else:
                            # If the item doesn't exist, add it to the dictionary
                            latest_stock_changes[item_warehouse_key] = {
                                'Timestamp': timestamp,
                                'StockLevelChange': int(row['StockLevelChange']),
                                'ItemName': row['ItemName'],  
                                'WarehouseName': row['WarehouseName']  
                            }
                    else:
                        print(f"Skipping item '{row['ItemId']}' because ItemName is empty.")

                except KeyError as e:
                    print(f"Failed to process CSV file '{obj['Key']}'. Missing column: {e}")
                except Exception as e:
                    print(f"Failed to process CSV row in file '{obj['Key']}': {e}")
            
            # Loop through the items in latest_stock_changes and update DynamoDB
            for item_warehouse_key, data in latest_stock_changes.items():
                try:
                    # Check if the item already exists in DynamoDB
                    existing_item = table.get_item(
                        Key={
                            'ItemId': item_warehouse_key[0],
                            'WarehouseName': item_warehouse_key[1]
                        }
                    ).get('Item')
                    
                    # If the item exists, update its stock level for the specific warehouse
                    if existing_item:
                        existing_stock_level = int(existing_item.get('StockLevelChange', 0))
                        new_stock_level = existing_stock_level + data['StockLevelChange']
                        
                        # Update the item in DynamoDB
                        table.put_item(
                            Item={
                                'ItemId': item_warehouse_key[0],
                                'Timestamp': data['Timestamp'].strftime('%Y-%m-%dT%H:%M:%S'),
                                'WarehouseName': data['WarehouseName'],
                                'ItemName': data['ItemName'],
                                'StockLevelChange': new_stock_level
                            }
                        )
                        
                        print(f"Data for item '{item_warehouse_key[0]}' in warehouse '{item_warehouse_key[1]}' updated in DynamoDB.")
                    else:
                        # If the item does not exist, insert it into DynamoDB
                        table.put_item(
                            Item={
                                'ItemId': item_warehouse_key[0],
                                'Timestamp': data['Timestamp'].strftime('%Y-%m-%dT%H:%M:%S'),
                                'WarehouseName': data['WarehouseName'],
                                'ItemName': data['ItemName'],
                                'StockLevelChange': data['StockLevelChange']
                            }
                        )
                        
                        print(f"Data for item '{item_warehouse_key[0]}' in warehouse '{item_warehouse_key[1]}' inserted into DynamoDB.")
                    
                except Exception as e:
                    print(f"Failed to insert/update data into DynamoDB for item '{item_warehouse_key[0]}' in warehouse '{item_warehouse_key[1]}': {e}")
                    
            print(f"Data from file '{obj['Key']}' processed.")
                    
    print("Data import completed.")

# Execute the import function
import_inventory_data()
