resource "aws_s3_bucket" "datalake" {
  # parâmetros de configuração do recurso escolhido
  bucket = "dl-gustavo-igti-edc-desafio-mod1"

  tags = {
    IES   = "IGTI",
    CURSO = "EDC"
  }
}


resource "aws_s3_bucket_acl" "datalake" {
  bucket = aws_s3_bucket.datalake.id
  acl = "private"
}

resource "aws_s3_bucket_server_side_encryption_configuration" "datalake" {
  bucket = aws_s3_bucket.datalake.id

  rule {
      apply_server_side_encryption_by_default {
        sse_algorithm = "AES256"
      }
  }  
}

