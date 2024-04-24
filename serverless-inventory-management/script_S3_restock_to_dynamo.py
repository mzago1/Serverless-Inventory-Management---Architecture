import boto3
import json

# Configurations
s3_bucket_name = 'inventory-bucket-2'
s3_prefix = 'restock_thresholds'
ddb_table_name = 'Restock'

# Connection to S3 and DynamoDB
s3 = boto3.client('s3')
ddb = boto3.resource('dynamodb')
table = ddb.Table(ddb_table_name)

# Function to import data from JSON files in S3 to DynamoDB
def import_restock_data():
    print("Starting data import...")
    # List objects in the S3 bucket
    response = s3.list_objects_v2(Bucket=s3_bucket_name, Prefix=s3_prefix)
    
    # Loop through the objects
    for obj in response.get('Contents', []):
        # Check if the object is a JSON file
        if obj['Key'].endswith('.json'):
            # Read the JSON file from S3
            json_obj = s3.get_object(Bucket=s3_bucket_name, Key=obj['Key'])
            json_data = json_obj['Body'].read().decode('utf-8')
            
            # Convert JSON to Python dictionary
            data = json.loads(json_data)
            
            # Insert data into DynamoDB table
            try:
                # Insert each item from the ThresholdList
                for item in data.get('ThresholdList', []):
                    table.put_item(
                        Item={
                            'ItemId': item['ItemId'],
                            'RestockIfBelow': item['RestockIfBelow']
                            # Add more attributes as needed
                        }
                    )
                    print(f"File {obj['Key']} added successfully to DynamoDB.")
            except Exception as e:
                print(f"Failed to insert data from file {obj['Key']} into DynamoDB: {e}")
                    
    print("Data import completed.")

# Execute the import function
import_restock_data()
