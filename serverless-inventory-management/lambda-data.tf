resource "aws_lambda_function" "insert_data_lambda" {
  filename         = "insert-data.zip" // O arquivo ZIP contendo o código da função Lambda
  function_name    = "InsertDataLambda"
  role             = aws_iam_role.lambda_role.arn
  handler          = "index.handler"
  source_code_hash = filebase64sha256("insert-data.zip")
  runtime          = "python3.8"
  
  environment {
    variables = {
      DYNAMODB_TABLE_NAME = aws_dynamodb_table.inventory_table.name
    }
  }
}

resource "aws_iam_role" "lambda_role" {
  name = "lambda_dynamodb_role"

  assume_role_policy = jsonencode({
    "Version" : "2012-10-17",
    "Statement" : [
      {
        "Effect" : "Allow",
        "Principal" : {
          "Service" : "lambda.amazonaws.com"
        },
        "Action" : "sts:AssumeRole"
      }
    ]
  })
}

resource "aws_iam_policy_attachment" "lambda_dynamodb_policy_attachment" {
  name       = "lambda_dynamodb_policy_attachment"
  roles      = [aws_iam_role.lambda_role.name]
  policy_arn = "arn:aws:iam::aws:policy/AmazonDynamoDBFullAccess"
}
