"""Main module for s3_csv_merger.

Used to merge csv files from S3 into fewer files, based on the date in the key.
Should upload the merged files back to S3, in folder "bricks-merged/<month_name>/"
"""

from tqdm import tqdm

from s3_csv_merger.s3 import list_files, read_csv_from_s3, write_csv_to_s3
from s3_csv_merger.utils import extract_date_from_key, get_month_name_from_key


def main(bucket, prefix, output_bucket, output_prefix):
    """Main function.
    
    High level function, that orchestrates the merging of files.
    
    Args:
        bucket (str): S3 bucket name
        prefix (str): S3 prefix
        output_bucket (str): S3 bucket name, where to upload the merged files
        output_prefix (str): S3 prefix, where to upload the merged files
    """
    files = list_files(bucket, prefix)

    # remove files with no date in key
    files = filter_sort_files(files)

    # group files by month name
    files_by_month = group_files_by_month(files)

    # merge files based on date in key
    merge_files_by_month(bucket, output_bucket, output_prefix, files_by_month)


def merge_files_by_month(bucket, out_bucket, out_prefix, files_by_month):
    """Merge files by month.

    Merges all files for a given month into a single file, and uploads it to S3.

    Args:
        bucket (str): S3 bucket name
        out_bucket (str): S3 bucket name, where to upload the merged files
        out_prefix (str): S3 prefix, where to upload the merged files
        files_by_month (dict): dictionary with month names as keys, and list of CSV files as values
    """
    for month_name, files in tqdm(
        files_by_month.items(), desc="Merging files by month"
    ):
        data = []
        # read all files for a given month, progress bar
        for file in tqdm(files, desc=f"Reading files for {month_name}"):
            data.extend(read_csv_from_s3(bucket, file, add_timestamp=True))
        write_csv_to_s3(out_bucket, f"{out_prefix}/{month_name}/merged.csv", data)


def filter_sort_files(files):
    """Filter and sort files.
    
    Filters out files that do not have a date in the key, and sorts the files by date.
    
    Args:
        files (list): list of S3 keys
    
    Returns:
        list: list of S3 keys, sorted by date
    """
    files = (file for file in files if extract_date_from_key(file))
    # sort files by date in filename
    files = sorted(files, key=extract_date_from_key)
    return files


def group_files_by_month(files):
    """Group files by month.
    
    Groups files by month name, based on the date in the key.
    
    Args:
        files (list): list of S3 keys
        
    Returns:
        dict: dictionary with month names as keys, and list of CSV files as values
    """
    files_by_month = {}
    for file in files:
        month_name = get_month_name_from_key(file)
        if month_name not in files_by_month:
            files_by_month[month_name] = []
        files_by_month[month_name].append(file)
    return files_by_month


def cli():
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket", type=str, help="S3 bucket name", required=True)
    parser.add_argument("--prefix", type=str, help="S3 prefix", required=True)
    parser.add_argument(
        "--output_bucket",
        type=str,
        help="S3 bucket name, defaults to input bucket",
        default=None,
    )
    parser.add_argument(
        "--output_prefix",
        type=str,
        help="S3 prefix, defaults to input prefix",
        default=None,
    )
    args = parser.parse_args()

    if args.output_bucket is None:
        args.output_bucket = args.bucket
    if args.output_prefix is None:
        args.output_prefix = args.prefix

    main(args.bucket, args.prefix, args.output_bucket, args.output_prefix)
