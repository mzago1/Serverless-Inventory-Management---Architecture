# Criação do tópico SNS
resource "aws_sns_topic" "restock_notifications" {
  name = "restock_notifications_topic"
}

# Assinatura do tópico para o seu email
resource "aws_sns_topic_subscription" "email_subscription" {
  topic_arn = aws_sns_topic.restock_notifications.arn
  protocol  = "email"
  endpoint  = "mjzagobooks@gmail.com"
}

# Criação do papel IAM para o Lambda
resource "aws_iam_role" "lambda_execution_role" {
  name               = "lambda_execution_role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect    = "Allow"
        Principal = {
          Service = "lambda.amazonaws.com"
        }
        Action    = "sts:AssumeRole"
      }
    ]
  })
}

# Anexar uma política que permite que a função do Lambda acesse o SNS
resource "aws_iam_policy_attachment" "lambda_sns_policy_attachment" {
  name       = "lambda_sns_policy_attachment"
  roles      = [aws_iam_role.lambda_execution_role.name]
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

# Criação da função Lambda
resource "aws_lambda_function" "notify_on_restock" {
  filename      = "notify_on_restock.zip"  # Arquivo zip contendo o código do Lambda
  function_name = "notify_on_restock"
  role          = aws_iam_role.lambda_execution_role.arn
  handler       = "lambda_function.lambda_handler"
  runtime       = "python3.8"
  timeout       = 10  # Tempo máximo de execução em segundos
  memory_size   = 128  # Tamanho da memória em MB
}

# Permissão para o Lambda acessar o S3
resource "aws_lambda_permission" "allow_s3" {
  statement_id  = "AllowExecutionFromS3"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.notify_on_restock.arn
  principal     = "s3.amazonaws.com"
  source_arn    = aws_s3_bucket.inventory_files.arn
}

# Criação do gatilho S3 para acionar o Lambda quando um objeto for criado no restock_lists_folder
resource "aws_s3_bucket_notification" "inventory_files_notification" {
  bucket = aws_s3_bucket.inventory_files.id

  lambda_function {
    lambda_function_arn = aws_lambda_function.notify_on_restock.arn
    events              = ["s3:ObjectCreated:*"]
    filter_prefix       = "restock_lists/"
  }
}
