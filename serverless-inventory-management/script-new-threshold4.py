import boto3
import csv
import json
from datetime import datetime, timezone

# Configurações
s3_bucket_name = 'unique-name-for-inventory-bucket-example'
s3_prefix_inventory = 'inventory_files'
s3_prefix_thresholds = 'restock_thresholds'
ddb_table_name = 'Inventory'
restock_list_folder = 'restock_lists'  # Pasta para salvar as listas de itens para reposição

# Conexão com o S3 e DynamoDB
s3 = boto3.client('s3')
ddb = boto3.resource('dynamodb')
table = ddb.Table(ddb_table_name)

# Função para buscar os limites de reposição do S3
def fetch_restock_thresholds(timestamp):
    try:
        # Extrair a data (ano, mês e dia) do timestamp do DynamoDB
        year = timestamp.year
        month = timestamp.month
        day = timestamp.day
        
        # Montar o prefixo do objeto no S3 com base na data do arquivo CSV processado
        prefix = f"{s3_prefix_thresholds}/{year}/{month:02d}/{day:02d}/"
        
        # Listar objetos no S3 com base no prefixo
        response = s3.list_objects_v2(Bucket=s3_bucket_name, Prefix=prefix)
        
        # Verificar se há objetos encontrados
        if 'Contents' in response:
            # Obter o último objeto da lista
            latest_file = max(response['Contents'], key=lambda x: x['LastModified'])
            
            # Buscar o conteúdo do arquivo de limite de reposição
            thresholds_obj = s3.get_object(Bucket=s3_bucket_name, Key=latest_file['Key'])
            thresholds_data = thresholds_obj['Body'].read().decode('utf-8')
            thresholds = json.loads(thresholds_data)
            
            return thresholds
        else:
            print(f"No restock thresholds found for date: {year}-{month:02d}-{day:02d}")
            return None
    except Exception as e:
        print(f"Failed to fetch restock thresholds: {e}")
        return None

# Função para importar dados dos arquivos CSV no S3 para o DynamoDB
def import_inventory_data():
    print("Starting data import...")
    
    # Listar objetos no bucket S3 para os arquivos de inventário
    response = s3.list_objects_v2(Bucket=s3_bucket_name, Prefix=s3_prefix_inventory)
    
    # Loop através dos objetos
    for obj in response.get('Contents', []):
        # Verificar se o objeto é um arquivo CSV
        if obj['Key'].endswith('.csv'):
            # Extrair a data do arquivo CSV processado
            timestamp_str = obj['LastModified'].strftime('%Y-%m-%d %H:%M:%S%z')
            
            # Converter a string de data para um objeto datetime
            timestamp = datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S%z')
            
            # Chamar fetch_restock_thresholds para obter os limites de reposição
            thresholds = fetch_restock_thresholds(timestamp)
            if thresholds:
                # Inicializar uma lista para armazenar os itens que precisam de reposição
                items_to_restock = []
                
                # Inicializar um dicionário para armazenar as últimas alterações no nível de estoque
                latest_stock_changes = {}
                
                # Ler o arquivo CSV do S3
                csv_obj = s3.get_object(Bucket=s3_bucket_name, Key=obj['Key'])
                lines = csv_obj['Body'].read().decode('utf-8').splitlines()
                
                # Ler o CSV usando o delimitador de ponto e vírgula
                reader = csv.DictReader(lines, delimiter=';')
                
                # Loop através das linhas do CSV para determinar a última alteração no nível de estoque
                for row in reader:
                    try:
                        # Formatar o timestamp para o DynamoDB
                        timestamp = datetime.strptime(row['Timestamp'], '%Y-%m-%dT%H:%M:%S.%f%z').astimezone(timezone.utc)
                        
                        # Gerar uma chave única para cada item em cada armazém
                        item_warehouse_key = (row['ItemId'], row['WarehouseName'])
                        
                        # Verificar se o item já existe no dicionário latest_stock_changes
                        if item_warehouse_key in latest_stock_changes:
                            # Se o item existe e o timestamp é mais recente, atualizar a alteração no nível de estoque
                            if timestamp > latest_stock_changes[item_warehouse_key]['Timestamp']:
                                latest_stock_changes[item_warehouse_key] = {
                                    'Timestamp': timestamp,
                                    'StockLevelChange': int(row['StockLevelChange']),
                                    'ItemName': row['ItemName'],  
                                    'WarehouseName': row['WarehouseName']  
                                }
                        else:
                            # Se o item não existe, adicioná-lo ao dicionário
                            latest_stock_changes[item_warehouse_key] = {
                                'Timestamp': timestamp,
                                'StockLevelChange': int(row['StockLevelChange']),
                                'ItemName': row['ItemName'],  
                                'WarehouseName': row['WarehouseName']  
                            }
                    except KeyError as e:
                        print(f"Failed to process CSV file '{obj['Key']}'. Missing column: {e}")
                    except Exception as e:
                        print(f"Failed to process CSV row in file '{obj['Key']}': {e}")
                
                # Loop através dos itens em latest_stock_changes e verificar se é necessário repor o estoque
                for item_warehouse_key, data in latest_stock_changes.items():
                    try:
                        # Verificar se o estoque do item está abaixo do limite de reposição
                        for threshold in thresholds.get('ThresholdList', []):
                            if threshold['ItemId'] == item_warehouse_key[0] and data['StockLevelChange'] < threshold.get('RestockIfBelow', 10):
                                items_to_restock.append({
                                    'ItemID': item_warehouse_key[0],
                                    'WarehouseName': item_warehouse_key[1],
                                    'CurrentStockLevel': data['StockLevelChange'],
                                    'RestockThreshold': threshold.get('RestockIfBelow', 10)
                                })
                                break  # Não é necessário verificar mais limites para este item
                    except Exception as e:
                        print(f"Error while checking restock threshold for item '{item_warehouse_key[0]}': {e}")
                
                # Escrever a lista de itens para reposição em um arquivo de texto separado por dia
                day = obj['LastModified'].strftime('%Y-%m-%d')
                write_restock_list_to_file(day, items_to_restock)
                
                # Loop através dos itens em latest_stock_changes e atualizar o DynamoDB com as últimas alterações no nível de estoque
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
        else:
            print(f"No CSV files found in prefix: {s3_prefix_inventory}")

    print("Data import completed.")

# Função para escrever a lista de itens para reposição em um arquivo de texto separado por dia
def write_restock_list_to_file(day, items_to_restock):
    try:
        file_name = f"{restock_list_folder}/items_to_restock_{day}.txt"
        with open(file_name, 'w') as file:
            file.write("Items to Restock:\n")
            for item in items_to_restock:
                file.write(f"ItemID: {item['ItemID']}, Warehouse: {item['WarehouseName']}, Current Stock Level: {item['CurrentStockLevel']}, Restock Threshold: {item['RestockThreshold']}\n")
        print(f"List of items to restock for day {day} saved to '{file_name}'.")
    except Exception as e:
        print(f"Failed to save restock list for day {day}: {e}")

# Executar a função de importação de dados
import_inventory_data()
