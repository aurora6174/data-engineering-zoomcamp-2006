variable "aws_region" {
  description = "AWS region to deploy resources in"
  type = string
  default = "eu-north-1"

}

variable "bucket_name" {
  description = "S3 bucket name for storing data"
  type        = string
}


variable "dataset_name" {
  description = "Glue Catalog database name (logical dataset for Athena/Glue)"
  type        = string
  default = "ny_taxi"
}