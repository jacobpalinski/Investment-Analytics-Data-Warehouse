# Create SNS topic
resource "aws_sns_topic" "nasdaq_market_analytics" {
    name = var.sns_topic
}

# Create email subscription
resource "aws_sns_topic_subscription" "email_alerts" {
    topic_arn = aws_sns_topic.nasdaq_market_analytics.arn
    protocol = "email"
    endpoint = var.sns_email
}

# Store SNS topic arn in SSM
resource "aws_ssm_parameter" "nasdaq_market_analytics_topic_arn" {
    name = "/investment_analytics_data_warehouse/prd/SNS_TOPIC_ARN"
    description = "Secret parameter for SNS_TOPIC_ARN"
    type = "SecureString"
    value = aws_sns_topic.nasdaq_market_analytics.arn
    overwrite = true
}

