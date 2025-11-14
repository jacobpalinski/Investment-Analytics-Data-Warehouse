locals {
  buckets = {"investment-analytics-data-warehouse", "investment-analytics-data-warehouse-tst" }
}

# Not needed once created for the first time
resource "aws_s3_bucket" "buckets" {
  for_each = local.buckets
  bucket = each.value
}

resource "aws_s3_object" "metadata" {
  for_each = aws_s3_bucket.buckets

  bucket = each.key
  key = "metadata.json"
  content = "{}"

  depends_on = [aws_s3_bucket.buckets]
}
