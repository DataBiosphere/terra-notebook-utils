import os

WORKSPACE_NAME = os.environ.get('WORKSPACE_NAME', None)
WORKSPACE_GOOGLE_PROJECT = os.environ.get('GOOGLE_PROJECT', None)
WORKSPACE_BUCKET = os.environ.get('WORKSPACE_BUCKET', None)
if WORKSPACE_BUCKET is not None:
    WORKSPACE_BUCKET = WORKSPACE_BUCKET[5:]  # Chop off the bucket schema, "gs://"

def _iter_table(table: str):
    from firecloud import fiss
    for item in fiss.fapi.get_entities(WORKSPACE_GOOGLE_PROJECT, WORKSPACE_NAME, table).json():
        yield item

def _get_item_val(item: dict, key: str):
    if "name" == key:
        return item['name']
    else:
        return item['attributes'][key]

def fetch_table_attribute(table: str, filter_column: str, filter_val: str, attribute: str):
    """
    Fetch `attribute` from `table` from the same row containing `filter_val` in column `filter_column`
    """
    for item in _iter_table(table):
        if filter_val == _get_item_val(item, filter_column):
            return _get_item_val(item, attribute)
    else:
        raise ValueError(f"No row found for table {table}, filter_column {filter_column} filter_val {filter_val}")

def fetch_table_object_id(table: str, file_name: str):
    """
    Fetch `object_id` associated with `file_name` from `table`.
    DRS urls, when available, are stored in `object_id`.
    """
    return fetch_table_attribute(table, "file_name", file_name, "object_id")

def print_table_column(table: str, column: str):
    for item in _iter_table(table):
        print(_get_item_val(item, column))
