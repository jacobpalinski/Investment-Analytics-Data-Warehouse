# Declare aws_access_key_id variable
variable "aws_access_key_id" {
  type = string
  sensitive = true
}

# Declare aws_secret variable
variable "aws_secret_access_key" {
  type = string
  sensitive = true
}

# Declare aws_region variable
variable "aws_region" {
  type = string
  sensitive = false
  default = "ap-southeast-1"
}

# Declare aws_s3_bucket_variable
variable "aws_s3_bucket" {
  type = string
  sensitive = true
}

# Declare aws_s3_tst_bucket variable
variable "aws_s3_tst_bucket" {
  type = string
  sensitive = true
}

# Declare airflow_uid variable
variable "airflow_uid" {
  type = string
  sensitive = true
}

# Declare airflow_username variable
variable "airflow_username" {
  type = string
  sensitive = true
}

# Declare airflow_password variable
variable "airflow_fernet_key" {
  type = string
  sensitive = true
}

# Declare airflow_fernet_key variable
variable "airflow_password" {
  type = string
  sensitive = true
}

# Declare polygon_api_key variable
variable "polygon_api_key" {
  type = string
  sensitive = true
}

# Declare finnhub_api_key variable
variable "finnhub_api_key" {
  type = string
  sensitive = true
}

# Declare news_api_key variable
variable "news_api_key" {
  type = string
  sensitive = true
}

# Declare reddit_client_id variable
variable "reddit_client_id" {
  type = string
  sensitive = true
}

# Declare reddit_client_secret variable
variable "reddit_client_secret" {
  type = string
  sensitive = true
}

# Declare reddit_user_agent variable
variable "reddit_user_agent" {
  type = string
  sensitive = true
}

# Declare reddit_username variable
variable "reddit_username" {
  type = string
  sensitive = true
}

# Declare reddit_password variable
variable "reddit_password" {
  type = string
  sensitive = true
}

# Declare snowflake_user variable
variable "snowflake_user" {
  type = string
  sensitive = true
}

# Declare snowflake_password variable
variable "snowflake_password" {
  type = string
  sensitive = true
}

# Declare snowflake_account variable
variable "snowflake_account" {
  type = string
  sensitive = true
}

# Declare snowflake_role variable
variable "snowflake_role" {
  type = string
  sensitive = true
}

# Declare snowflake_private_key_b64 variable
variable "snowflake_private_key_b64" {
  type = string
  sensitive = true
}

# Declare snowflake_private_key_passphrase variable
variable "snowflake_private_key_passphrase" {
  type = string
  sensitive = true
}

# Declare fred_api_key variable
variable "fred_api_key" {
  type = string
  sensitive = true
}

# Declare sec_api_user_agent variable
variable "sec_api_user_agent" {
  type = string
  sensitive = true
}

# Declare kafka_bootstrap_servers variable
variable "kafka_bootstrap_servers" {
  type = string
  sensitive = true
}

# Declare kafka_topic variable
variable "kafka_topic" {
  type = string
  sensitive = true
}

# Declare schema_registry_url variable
variable "schema_registry_url" {
  type = string
  sensitive = true
}

# Declare metabase_private_key variable
variable "metabase_private_key" {
  type = string
  sensitive = true
}

# Declare metabase_email variable
variable "metabase_email" {
  type = string
  sensitive = true
}

# Declare metabase_password variable
variable "metabase_password" {
  type = string
  sensitive = true
}

# Declare postgres_username variable
variable "postgres_username" {
  type = string
  sensitive = true
}

# Declare postgres_password variable
variable "postgres_password" {
  type = string
  sensitive = true
}

# Declare instance_type variable
variable "instance_type" {
  description = "EC2 instance type"
  type        = string
  default     = "t3.2xlarge"
}









