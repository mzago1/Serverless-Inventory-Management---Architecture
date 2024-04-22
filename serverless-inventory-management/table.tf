resource "aws_dynamodb_table" "inventory_files" {
  name           = "inventory_fils"
  billing_mode   = "PROVISIONED"
  read_capacity  = 5
  write_capacity = 5
  hash_key       = "ItemId"

  attribute {
    name = "ItemId"
    type = "S"
  }
provider "aws" {
  region = "eu-central-1"
}

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
}
