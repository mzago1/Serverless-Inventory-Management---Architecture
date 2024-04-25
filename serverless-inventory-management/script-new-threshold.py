import boto3
import csv
import json
from datetime import datetime, timezone

# Configurations
s3_bucket_name = 'unique-name-for-inventory-bucket-example'
s3_prefix_inventory = 'inventory_files'
s3_prefix_thresholds = 'restock_thresholds'
ddb_table_name = 'Inventory'
restock_list_file = 'items_to_restock.txt'  # File to save the list of items to restock

# Connection to S3 and DynamoDB
s3 = boto3.client('s3')
ddb = boto3.resource('dynamodb')
table = ddb.Table(ddb_table_name)

# Function to fetch restock thresholds from S3
def fetch_restock_thresholds():
    try:
        # Get the latest restock threshold file
        response = s3.list_objects_v2(Bucket=s3_bucket_name, Prefix=s3_prefix_thresholds)
        latest_file = max(response['Contents'], key=lambda x: x['LastModified'])
        thresholds_obj = s3.get_object(Bucket=s3_bucket_name, Key=latest_file['Key'])
        thresholds_data = thresholds_obj['Body'].read().decode('utf-8')
        return thresholds_data
    except Exception as e:
        print(f"Failed to fetch restock thresholds: {e}")
        return None

# Function to import data from CSV files in S3 to DynamoDB
def import_inventory_data():
    print("Starting data import...")
    
    # Initialize a list to store items that need restocking
    items_to_restock = []
    
    # List objects in the S3 bucket for inventory files
    response = s3.list_objects_v2(Bucket=s3_bucket_name, Prefix=s3_prefix_inventory)
    
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
            
            # Call fetch_restock_thresholds to get restock thresholds
            thresholds_data = fetch_restock_thresholds()
            if thresholds_data:
                # Convert the thresholds data to dictionary
                thresholds = json.loads(thresholds_data)
                
                # Loop through the items in latest_stock_changes and check if restocking is required
                for item_warehouse_key, data in latest_stock_changes.items():
                    try:
                        # Check if the item's stock falls below the threshold
                        for threshold in thresholds.get('ThresholdList', []):
                            if threshold['ItemId'] == item_warehouse_key[0] and data['StockLevelChange'] < threshold.get('RestockIfBelow', 10):
                                items_to_restock.append({
                                    'ItemID': item_warehouse_key[0],
                                    'WarehouseName': item_warehouse_key[1],
                                    'CurrentStockLevel': data['StockLevelChange'],
                                    'RestockThreshold': threshold.get('RestockIfBelow', 10)
                                })
                                break  # No need to check further thresholds for this item
                    except Exception as e:
                        print(f"Error while checking restock threshold for item '{item_warehouse_key[0]}': {e}")

            # Update DynamoDB with the latest stock changes
            for item_warehouse_key, data in latest_stock_changes.items():
                try:
                    existing_item = table.get_item(
                        Key={
                            'ItemId': item_warehouse_key[0],
                            'WarehouseName': item_warehouse_key[1]
                        }
                    ).get('Item')
                    
                    if existing_item:
                        existing_stock_level = int(existing_item.get('StockLevelChange', 0))
                        new_stock_level = existing_stock_level + data['StockLevelChange']
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
    
    # Write the list of items to restock to a text file
    write_restock_list_to_file(items_to_restock)
    
    print("Data import completed.")

# Function to write the list of items to restock to a text file
def write_restock_list_to_file(items_to_restock):
    try:
        with open(restock_list_file, 'w') as file:
            file.write("Items to Restock:\n")
            for item in items_to_restock:
                file.write(f"ItemID: {item['ItemID']}, Warehouse: {item['WarehouseName']}, Current Stock Level: {item['CurrentStockLevel']}, Restock Threshold: {item['RestockThreshold']}\n")
        print(f"List of items to restock saved to '{restock_list_file}'.")
    except Exception as e:
        print(f"Failed to save restock list: {e}")

# Execute the import function
import_inventory_data()
