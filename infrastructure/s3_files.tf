# atualizando forma de declarar recursos conforme nova versão do Terraform:
# https://registry.terraform.io/providers/hashicorp/aws/latest/docs/guides/version-4-upgrade)

# resource "aws_s3_bucket_object" "job_spark" {
resource "aws_s3_object" "job_spark" {
  # bucket = aws_s3_bucket.datalake.id
  bucket = aws_s3_bucket.datalake.id

  key    = "emr-code/pyspark/job_spark_rais.py"
  acl    = "private"
  source = "../job_spark_rais.py"
  etag   = filemd5("../job_spark_rais.py")

}

