resource "aws_dynamodb_table" "inventory_table" {
  name           = "Inventory"
  billing_mode   = "PAY_PER_REQUEST"
  hash_key       = "ItemId"
  
  attribute {
    name = "ItemId"
    type = "S"
  }
  
  attribute {
    name = "Timestamp"
    type = "S"
  }
  
  attribute {
    name = "WarehouseName"
    type = "S"
  }
  
  attribute {
    name = "ItemName"
    type = "S"
  }
  
  attribute {
    name = "StockLevelChange"
    type = "N"
  }
  
  global_secondary_index {
    name               = "StockLevelChangeIndex"
    hash_key           = "StockLevelChange"
    projection_type    = "ALL"
  }
  
  global_secondary_index {
    name               = "ItemNameIndex"
    hash_key           = "ItemName"
    projection_type    = "ALL"
  }
  
  global_secondary_index {
    name               = "WarehouseNameIndex"
    hash_key           = "WarehouseName"
    projection_type    = "ALL"
  }
  
  global_secondary_index {
    name               = "TimestampIndex"
    hash_key           = "Timestamp"
    projection_type    = "ALL"
  }
}

resource "aws_dynamodb_table" "restock_table" {
  name           = "Restock"
  billing_mode   = "PAY_PER_REQUEST"
  hash_key       = "ItemId"

  attribute {
    name = "ItemId"
    type = "S"
  }

  attribute {
    name = "RestockIfBelow"
    type = "N"
  }

  tags = {
    Name = "Restock Table"
  }

  global_secondary_index {
    name               = "RestockIfBelowIndex"
    hash_key           = "RestockIfBelow"
    projection_type    = "ALL"
  }
}

