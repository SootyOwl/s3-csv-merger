from datetime import datetime
from functools import lru_cache
import re

REGEX_DATE = re.compile(r"\d{4}-\d{2}-\d{2}")
REGEX_TIMESTAMP = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}")

@lru_cache(maxsize=256)
def extract_date_from_key(key):
    """
    Extract date from key

    key format: 0592bcb1_2023-04-13T06:23:10.308337+00:00.csv
    """
    try:
        return REGEX_DATE.findall(key)[0]
    except IndexError:
        return None
    
    
def extract_timestamp_from_key(key):
    """
    Extract timestamp from key

    key format: 0592bcb1_2023-04-13T06:23:10.308337+00:00.csv
    """
    try:
        return REGEX_TIMESTAMP.findall(key)[0]
    except IndexError:
        return None

def get_month_name_from_date(date):
    """
    Get month name from date

    date format: 2023-04-13
    """
    return datetime.strptime(date, "%Y-%m-%d").strftime("%B")

def get_month_name_from_key(key):
    """
    Get month name from key

    key format: bricks/0592bcb1_2023-04-13T06:23:10.308337+00:00.csv
    """
    date = extract_date_from_key(key)
    return get_month_name_from_date(date)
