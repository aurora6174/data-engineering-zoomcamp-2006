resource "aws_s3_bucket" "taxi_data" {
  bucket = "zoomcamp-yellow-taxi-${random_id.suffix.hex}"
}

resource "random_id" "suffix" {
  byte_length = 4
}