locals {
  ssm_prefix = "/investment_analytics_data_warehouse/prd/"  # change if needed
}

# A helper module-like pattern to avoid repetition
resource "aws_ssm_parameter" "ssm_params" {
  for_each = {
    ACCESS_KEY_ID = var.aws_access_key_id
    SECRET_ACCESS_KEY = var.aws_secret_access_key
    S3_BUCKET = var.aws_s3_bucket
    S3_TST_BUCKET = var.aws_s3_tst_bucket
    AIRFLOW_UID = var.airflow_uid
    _AIRFLOW_WWW_USER_USERNAME = var.airflow_username
    _AIRFLOW_WWW_USER_PASSWORD = var.airflow_password
    POLYGON_API_KEY = var.polygon_api_key
    FINNHUB_API_KEY = var.finnhub_api_key
    NEWS_API_KEY = var.news_api_key
    REDDIT_CLIENT_ID = var.reddit_client_id
    REDDIT_CLIENT_SECRET = var.reddit_client_secret
    REDDIT_USER_AGENT = var.reddit_user_agent
    REDDIT_USERNAME = var.reddit_username
    REDDIT_PASSWORD = var.reddit_password
    SNOWFLAKE_USER = var.snowflake_user
    SNOWFLAKE_PASSWORD = var.snowflake_password
    SNOWFLAKE_ACCOUNT = var.snowflake_account
    SNOWFLAKE_ROLE = var.snowflake_role
    SNOWFLAKE_PRIVATE_KEY_B64 = var.snowflake_private_key_b64
    SNOWFLAKE_PRIVATE_KEY_B64_FULL = var.snowflake_private_key_b64_full
    SNOWFLAKE_PRIVATE_KEY_PASSPHRASE = var.snowflake_private_key_passphrase
    FRED_API_KEY = var.fred_api_key
    SEC_API_USER_AGENT = var.sec_api_user_agent
    KAFKA_BOOTSTRAP_SERVERS = var.kafka_bootstrap_servers
    KAFKA_TOPIC = var.kafka_topic
    SCHEMA_REGISTRY_URL = var.schema_registry_url
    METABASE_PRIVATE_KEY = var.metabase_private_key
    METABASE_USERNAME = var.metabase_username
    METABASE_PASSWORD = var.metabase_password
  }

  name = "${local.ssm_prefix}${each.key}"
  description = "Secret parameter for ${each.key}"
  type = "SecureString"
  value = each.value
  overwrite = true
}
