resource "aws_s3_object" "job_spark_from_tf" {
  # bucket = aws_s3_bucket.datalake.id
  bucket = aws_s3_bucket.dl.id

  key    = "emr-code/pyspark/job_spark_rais.py"
  acl    = "private"
  source = "../job_spark_rais.py"
  etag   = filemd5("../job_spark_rais.py")

}