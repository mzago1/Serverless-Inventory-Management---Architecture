resource "aws_lambda_function" "lambda-data" {
  filename         = "lambda-data.zip" // Lambda file
  function_name    = "InsertDataLambda"
  role             = aws_iam_role.lambda_role.arn
  handler          = "lambda-data.handler"
  source_code_hash = filebase64sha256("lambda-data.zip")
  runtime          = "python3.8"
  timeout          = 10  # Aumente o tempo limite para 10 segundos ou mais, conforme necessário
  
  environment {
    variables = {
      DYNAMODB_TABLE_NAME = aws_dynamodb_table.inventory_table.name
    }
  }
}

resource "aws_lambda_permission" "allow_invoke" {
  statement_id  = "AllowInvokeFromLambda"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.lambda-data.function_name
  principal     = "lambda.amazonaws.com"
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

resource "aws_iam_policy_attachment" "lambda_policy_attachment" {
  name       = "lambda_policy_attachment"
  roles      = [aws_iam_role.lambda_role.name]
  policy_arn = "arn:aws:iam::aws:policy/AWSLambda_FullAccess"
}

