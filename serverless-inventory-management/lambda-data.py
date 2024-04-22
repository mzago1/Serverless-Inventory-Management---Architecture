import boto3
import json
from datetime import datetime

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('Inventory')

def lambda_handler(event, context):
    timestamp = str(datetime.now())
    warehouse_name = event['WarehouseName']
    item_id = event['ItemId']
    item_name = event['ItemName']
    stock_level_change = event['StockLevelChange']
    
    table.put_item(
       Item={
            'Timestamp': timestamp,
            'WarehouseName': warehouse_name,
            'ItemId': item_id,
            'ItemName': item_name,
            'StockLevelChange': stock_level_change
        }
    )
    
    return {
        'statusCode': 200,
        'body': json.dumps('Data inserted successfully!')
    }
