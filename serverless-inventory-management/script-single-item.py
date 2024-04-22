import boto3

# Create a DynamoDB client
dynamodb = boto3.client('dynamodb')

# Define the data for the new item
new_item = {
    'WarehouseName': {'S': 'Warehouse A'},
    'ItemId': {'S': '004'},
    'ItemName': {'S': 'Item 5'},
    'StockLevelChange': {'N': '90'}
}

# Insert the new item into the table
response = dynamodb.put_item(
    TableName='Inventory',
    Item=new_item
)

print(response)
