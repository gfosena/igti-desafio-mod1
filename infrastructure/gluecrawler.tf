resource "aws_glue_catalog_database" "rais" {
  name = "raisdb"
}

resource "aws_glue_crawler" "rais" {
  database_name = aws_glue_catalog_database.rais.name
  name          = "rais_s3_crawler"
  role          = aws_iam_role.glue_rais_role.arn

  s3_target {
    path = "s3://${aws_s3_bucket.dl.bucket}/staging-zone/rais/"
  }

  configuration = <<EOF
{
   "Version": 1.0,
   "Grouping": {
      "TableGroupingPolicy": "CombineCompatibleSchemas" }
}
EOF

  tags = {
    IES   = "IGTI",
    CURSO = "EDC"
  }
}