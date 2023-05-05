# AWS S3 CSV Merge by Month

This script will merge all CSV files in a given S3 bucket by month. It will then upload the merged CSV files back to S3.

## Usage

```bash
poetry run s3-csv-merger --bucket <bucket_name> --prefix <prefix> --output_bucket <output_bucket> --output_prefix <output_prefix>
```
