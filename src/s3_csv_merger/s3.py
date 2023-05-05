# use smart-open to read list of files from S3

import csv

from smart_open import open
from smart_open.s3 import _list_bucket

from s3_csv_merger.utils import extract_timestamp_from_key


def list_files(bucket, prefix):
    """
    List files in a S3 bucket with a given prefix
    """
    return list(_list_bucket(bucket, prefix=prefix))


def read_csv_from_s3(bucket, key, add_timestamp=False):
    """
    Read csv from S3
    """
    with open(f"s3://{bucket}/{key}", mode="r") as f:
        if add_timestamp:
            timestamp = extract_timestamp_from_key(key)
            for row in csv.DictReader(f):
                row["date"] = timestamp
                yield row
        else:
            yield from csv.DictReader(f)


def write_csv_to_s3(bucket, key, data):
    """
    Write csv to S3
    """
    with open(f"s3://{bucket}/{key}", mode="w") as f:
        writer = csv.DictWriter(f, fieldnames=data[0].keys())
        writer.writeheader()
        writer.writerows(data)
