"""
Terra data table commands
"""
from firecloud import fiss

from terra_notebook_utils import WORKSPACE_GOOGLE_PROJECT, WORKSPACE_NAME

def _iter_table(table: str, workspace_google_project: str=WORKSPACE_GOOGLE_PROJECT, workspace_name: str=WORKSPACE_NAME):
    resp = fiss.fapi.get_entities(workspace_google_project, workspace_name, table)
    if 200 != resp.status_code:
        print(resp.content)
        raise Exception(f"Expected status 200, got {resp.status_code}")
    for item in resp.json():
        yield item

def _get_item_val(item: dict, key: str):
    if "name" == key:
        return item['name']
    else:
        return item['attributes'][key]

def fetch_attribute(table: str, filter_column: str, filter_val: str, attribute: str):
    """
    Fetch `attribute` from `table` from the same row containing `filter_val` in column `filter_column`
    """
    for item in _iter_table(table):
        if filter_val == _get_item_val(item, filter_column):
            return _get_item_val(item, attribute)
    else:
        raise ValueError(f"No row found for table {table}, filter_column {filter_column} filter_val {filter_val}")

def fetch_object_id(table: str, file_name: str):
    """
    Fetch `object_id` associated with `file_name` from `table`.
    DRS urls, when available, are stored in `object_id`.
    """
    return fetch_attribute(table, "file_name", file_name, "object_id")

def fetch_drs_url(table: str, file_name: str):
    val = fetch_object_id(table, file_name)
    if not val.startswith("drs://"):
        raise ValueError(f"Expected DRS url in {table} for {file_name}, got {val} instead.")
    return val

def list_tables(workspace_google_project: str=WORKSPACE_GOOGLE_PROJECT, workspace_name: str=WORKSPACE_NAME):
    resp = fiss.fapi.list_entity_types(workspace_google_project, workspace_name)
    resp.raise_for_status()
    for ent_type, data in resp.json().items():
        yield ent_type, data['attributeNames']

def list_entities(ent_type: str,
                  workspace_google_project: str=WORKSPACE_GOOGLE_PROJECT,
                  workspace_name: str=WORKSPACE_NAME):
    resp = fiss.fapi.get_entities(workspace_google_project, workspace_name, ent_type)
    resp.raise_for_status()
    for ent in resp.json():
        yield ent

def get_row(table: str,
            entity_id: str,
            workspace_google_project: str=WORKSPACE_GOOGLE_PROJECT,
            workspace_name: str=WORKSPACE_NAME):
    resp = fiss.fapi.get_entity(workspace_google_project, workspace_name, table, entity_id)
    resp.raise_for_status()
    return resp.json()

def delete_entities(entities: list):
    ents = [dict(entityType=e['entityType'], entityName=e['name'])
            for e in entities]
    resp = fiss.fapi.delete_entities(WORKSPACE_GOOGLE_PROJECT, WORKSPACE_NAME, ents)
    resp.raise_for_status()

def delete_table(ent_type: str):
    delete_entities([e for e in list_entities(ent_type)])

def upload_entities(tsv_data,
                    workspace_google_project: str=WORKSPACE_GOOGLE_PROJECT,
                    workspace_name: str=WORKSPACE_NAME):
    resp = fiss.fapi.upload_entities(workspace_google_project, workspace_name, tsv_data, model="flexible")
    resp.raise_for_status()

def print_column(table: str, column: str):
    for item in _iter_table(table):
        print(_get_item_val(item, column))
