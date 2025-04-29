# This Terraform file sets up:
# 1. An S3 Table Bucket
# 2. A Glue Catalog Database
# 3. An IAM Role for Glue
# 4. A Glue 5.0 Job that reads/writes to native S3 Tables (Iceberg)

provider "aws" {
  region = "us-east-1"  # Change if needed
}

# 1. S3 Table Bucket
resource "aws_s3_bucket" "table_bucket" {
  bucket = "my-native-s3-table-bucket"
  force_destroy = true
}

# 2. Glue Catalog Database
resource "aws_glue_catalog_database" "s3tables_db" {
  name = "s3tables_db"
}

# 3. IAM Role for Glue Job
resource "aws_iam_role" "glue_service_role" {
  name = "glue5-s3tables-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect = "Allow",
        Principal = {
          Service = "glue.amazonaws.com"
        },
        Action = "sts:AssumeRole"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "glue_policies" {
  role       = aws_iam_role.glue_service_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_iam_role_policy" "custom_permissions" {
  name = "glue-custom-access"
  role = aws_iam_role.glue_service_role.id

  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect = "Allow",
        Action = [
          "s3:*",
          "glue:*",
          "logs:*",
          "lakeformation:*"
        ],
        Resource = "*"
      }
    ]
  })
}

# 4. Glue 5.0 Spark Job
resource "aws_glue_job" "iceberg_job" {
  name     = "glue5-iceberg-s3tables-job"
  role_arn = aws_iam_role.glue_service_role.arn

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.table_bucket.bucket}/scripts/iceberg_etl.py"
    python_version  = "3"
  }

  glue_version      = "5.0"
  worker_type       = "G.1X"
  number_of_workers = 2
  max_retries       = 0

  default_arguments = {
    "--TempDir"                              = "s3://${aws_s3_bucket.table_bucket.bucket}/tmp/"
    "--enable-continuous-cloudwatch-log"    = "true"
    "--enable-spark-ui"                     = "true"
    "--spark-event-logs-path"              = "s3://${aws_s3_bucket.table_bucket.bucket}/spark-logs/"
  }
}

# Sample Glue ETL Script: iceberg_etl.py
# Save locally and upload to: s3://my-native-s3-table-bucket/scripts/iceberg_etl.py
# --------------------------
# from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Glue5 Iceberg ETL") \
    .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.glue_catalog.warehouse", "s3://my-native-s3-table-bucket/") \
    .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
    .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
    .enableHiveSupport() \
    .getOrCreate()

# Sample data
from pyspark.sql import Row
rows = [Row(id=1, name="Alice"), Row(id=2, name="Bob")]
df = spark.createDataFrame(rows)

# DDL Operations - Create Table (if not exists)
spark.sql("""
    CREATE TABLE IF NOT EXISTS glue_catalog.s3tables_db.users_native (
        id INT,
        name STRING
    )
    USING ICEBERG
""")

# DML Operation - Insert
spark.sql("""
    INSERT INTO glue_catalog.s3tables_db.users_native VALUES (3, 'Charlie'), (4, 'Diana')
""")

# DML Operation - Update
spark.sql("""
    UPDATE glue_catalog.s3tables_db.users_native
    SET name = 'Alice Updated'
    WHERE id = 1
""")

# DML Operation - Delete
spark.sql("""
    DELETE FROM glue_catalog.s3tables_db.users_native WHERE id = 2
""")

# DML Overwrite or Append from DataFrame
df.writeTo("glue_catalog.s3tables_db.users_native").using("iceberg").overwritePartitions()

# Alter Table - Add Column
spark.sql("""
    ALTER TABLE glue_catalog.s3tables_db.users_native ADD COLUMN email STRING
""")

# Describe Table
spark.sql("DESCRIBE TABLE glue_catalog.s3tables_db.users_native").show()

# Read back and display
df_read = spark.read.format("iceberg").load("glue_catalog.s3tables_db.users_native")
df_read.show()
