locals {
  ssm_prefix = "/investment_analytics_data_warehouse/prd/"  # change if needed
}

# A helper module-like pattern to avoid repetition
resource "aws_ssm_parameter" "ssm_params" {
  for_each = {
    access_key_id = var.aws_access_key_id
    secret = var.aws_secret
    s3_bucket = var.aws_s3_bucket
    s3_tst_bucket = var.aws_s3_tst_bucket
    airflow_uid = var.airflow_uid
    polygon_api_key = var.polygon_api_key
    finnhub_api_key = var.finnhub_api_key
    news_api_key = var.news_api_key
    reddit_client_id = var.reddit_client_id
    reddit_client_secret = var.reddit_client_secret
    reddit_user_agent = var.reddit_user_agent
    reddit_username = var.reddit_username
    reddit_password = var.reddit_password
    snowflake_user = var.snowflake_user
    snowflake_password = var.snowflake_password
    snowflake_account = var.snowflake_account
    snowflake_private_key_b64 = var.snowflake_private_key_b64
    snowflake_private_key_passphrase = var.snowflake_private_key_passphrase
    fred_api_key = var.fred_api_key
    sec_api_user_agent = var.sec_api_user_agent
    kafka_bootstrap_servers = var.kafka_bootstrap_servers
    kafka_topic = var.kafka_topic
    schema_registry_url = var.schema_registry_url
    metabase_private_key = var.metabase_private_key
  }

  name = local.ssm_prefix + each.key
  description = "Secret parameter for " + each.key
  type = "SecureString"
  value = each.value
  overwrite = true
}
