import os
import csv
from datetime import datetime
import boto3

# DynamoDB settings
dynamodb = boto3.client('dynamodb')
table_name = 'Inventory'

# Function to insert an item into DynamoDB
def insert_item(item):
    response = dynamodb.put_item(
        TableName=table_name,
        Item=item
    )
    return response

# Function to process a CSV file
def process_csv(file_path):
    with open(file_path, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile, delimiter='\t')  # Setting delimiter to tab
        for row in reader:
            # Check if the 'Timestamp' key is present in the current row
            if 'Timestamp' in row:
                # Convert the date to the desired format for DynamoDB
                timestamp = datetime.strptime(row['Timestamp'], '%Y-%m-%dT%H:%M:%S.%fZ').isoformat()

                # Create the item in the format expected by DynamoDB
                item = {
                    'WarehouseName': {'S': row['WarehouseName']},
                    'ItemId': {'S': row['ItemId']},
                    'ItemName': {'S': row['ItemName']},
                    'StockLevelChange': {'N': row['StockLevelChange']},
                    'Timestamp': {'S': timestamp}
                }

                # Insert the item into DynamoDB
                response = insert_item(item)
                print(response)
            else:
                print('Key "Timestamp" not found in the row. Row ignored:', row)

# Main function to iterate over CSV files
def main():
    base_path = 'inventory_files'
    count = 0
    for root, dirs, files in os.walk(base_path):
        for file in files:
            if count >= 5:  # Check if the first 5 files have been processed
                return
            if file.endswith('.csv'):
                file_path = os.path.join(root, file)
                print('Processing file:', file_path)
                process_csv(file_path)
                count += 1

if __name__ == "__main__":
    main()
