import os

WORKSPACE_NAME = os.environ.get('WORKSPACE_NAME', None)
WORKSPACE_GOOGLE_PROJECT = os.environ.get('GOOGLE_PROJECT', None)
WORKSPACE_BUCKET = os.environ.get('WORKSPACE_BUCKET', None)
if WORKSPACE_BUCKET is not None:
    WORKSPACE_BUCKET = WORKSPACE_BUCKET[5:]  # Chop off the bucket schema, "gs://"

def fetch_data_table_attribute(table: str, object_name: str, key: str):
    from firecloud import fiss
    row = fiss.fapi.get_entities(WORKSPACE_GOOGLE_PROJECT, WORKSPACE_NAME, table).json()
    for item in row:
        if object_name == item['name']:
            return item['attributes'][key]
    else:
        raise ValueError(f"No row found for table {table}, object {object_name}")

def fetch_data_table_object_id(table: str, object_name: str):
    return fetch_data_table_attribute(table, object_name, "object_id")
