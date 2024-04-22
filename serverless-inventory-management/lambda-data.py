import boto3
import json

# Crie um cliente Lambda
lambda_client = boto3.client('lambda')

# Defina os dados para o novo item
novo_item = {
    "WarehouseName": "Armazém C",
    "ItemId": "003",
    "ItemName": "Item 3",
    "StockLevelChange": 75
}

# Defina o evento com todas as chaves necessárias
evento = {
    "WarehouseName": novo_item["WarehouseName"],
    "ItemId": novo_item["ItemId"],
    "ItemName": novo_item["ItemName"],
    "StockLevelChange": novo_item["StockLevelChange"]
}

# Converta o evento em formato JSON
payload = json.dumps(evento)

# Nome da função Lambda definido no Terraform
function_name = "InsertDataLambda"

# Invocar a função Lambda com o evento modificado
response = lambda_client.invoke(
    FunctionName=function_name,
    InvocationType='RequestResponse',  # Você pode alterar isso conforme necessário
    Payload=payload
)

# Verifique a resposta
print(response['Payload'].read())
