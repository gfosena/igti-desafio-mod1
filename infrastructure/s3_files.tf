resource "aws_s3_object" "job_spark" {
  # bucket = aws_s3_bucket.datalake.id
  bucket = aws_s3_bucket.datalake.id

  key    = "emr-code/pyspark/job_spark_rais.py"
  acl    = "private"
  source = "../job_spark_rais.py"
  etag   = filemd5("../job_spark_rais.py")

}