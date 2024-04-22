resource "aws_dynamodb_table" "inventory_files" {
  name           = "inventory_files"
  billing_mode   = "PROVISIONED"
  read_capacity  = 5
  write_capacity = 5
  hash_key       = "ItemId"

  attribute {
    name = "ItemId"
    type = "S"
  }
}

resource "aws_dynamodb_item" "example" {
  table_name = aws_dynamodb_table.inventory_files.name

  hash_key = "ItemId"
  range_key = "Timestamp"

  item {
    Timestamp       = "2024-04-22T12:00:00Z"
    WarehouseName   = "Example Warehouse"
    ItemId          = "unique-id-123"
    ItemName        = "Example Item"
    StockLevelChange = 10
  }
}
