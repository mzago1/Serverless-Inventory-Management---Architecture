# Creation of the S3 bucket
resource "aws_s3_bucket" "inventory_files" {
  bucket = "unique-name-for-inventory-bucket-example"  # Unique name for your S3 bucket
}

# Create restock_lists folder in S3 bucket
resource "aws_s3_object" "restock_lists_folder" {
  bucket = aws_s3_bucket.inventory_files.bucket
  key    = "restock_lists/"
  source = "/dev/null"  # Dummy source to create an empty object
}

# Upload the inventory files to the S3 bucket
resource "aws_s3_object" "inventory_files" {
  for_each = fileset(path.module, "inventory_files/**/*")  # Get the list of files in the "inventory_files" directory
  bucket   = aws_s3_bucket.inventory_files.bucket  # Reference to the ID of the created S3 bucket
  key      = each.value  # Set the object key as the local file name
  source   = each.value  # Set the source as the local file name
}

# Upload the restock thresholds to the S3 bucket
resource "aws_s3_object" "restock_thresholds" {
  for_each = fileset(path.module, "restock_thresholds/**/*")  # Get the list of files in the "restock_thresholds" directory
  bucket   = aws_s3_bucket.inventory_files.bucket  # Reference to the ID of the created S3 bucket
  key      = each.value  # Set the object key as the local file name
  source   = each.value  # Set the source as the local file name
}
