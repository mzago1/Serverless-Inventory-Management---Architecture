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