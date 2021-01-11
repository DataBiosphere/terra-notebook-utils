"""
Terra data table commands
"""
import os
from uuid import uuid4
from collections import defaultdict, namedtuple
from typing import Any, Dict, Generator, Iterable, List, Optional, Set, Tuple, Union

import requests
from firecloud import fiss

from terra_notebook_utils import WORKSPACE_GOOGLE_PROJECT, WORKSPACE_NAME
from terra_notebook_utils.utils import _AsyncContextManager


Row = namedtuple("Row", "name attributes")

VALUE = Union[str, int, float, bool]
ATTRIBUTES = Dict[str, Union[VALUE, Iterable[VALUE]]]
UPDATE_OPS = List[Dict[str, Any]]
COLUMN_HEADERS = Tuple[str, ...]
ROW_LIKE = Tuple[str, ATTRIBUTES]
ROW_OR_NAME = Union[Row, str]

# A note on types:
# As presented in Terra's UI, tables may contain string, integer, and boolean values.  However, Firecloud API clients
# must transmit tables using TSV formatted data, which is not typed. So, alas, we cannot transmit typed data to Terra
# data tables, and cannot transform uploaded types via the Firecloud API (as far as I know).
#
# It would be preferable if the Firecloud API exposed JSON endpoints for uploading table data (which may already exist
# on the backend?)

# It turns out google.auth.transport.requests.AuthorizedSession is not thread safe.
# Fortunately fiss.fapi._set_session caches the result. Call it once on the main thread.
fiss.fapi._set_session()

class Writer(_AsyncContextManager):
    """
    Distribute row uploads across as few API calls as possible.
    Uploads are performed in the background.
    Also transparently handles sequences, which the Firecloud API makes difficult. (These must be uploaded and modified
    via separate API calls.)
    """
    def __init__(self, name: str, **kwargs):
        self.name = name
        self._init_request_data()
        self._workspace_name = kwargs.get("workspace_name", WORKSPACE_NAME)
        self._workspace_google_project = kwargs.get("workspace_google_project", WORKSPACE_GOOGLE_PROJECT)

    def _init_request_data(self):
        self._tsvs: Dict[COLUMN_HEADERS, str] = defaultdict(list)
        self._row_update_request_data: Dict[COLUMN_HEADERS, List[Tuple[Row, UPDATE_OPS]]] = dict()

    def _get_row_update_request_data(self, row: Row) -> List[Dict[str, Any]]:
        request_data = list()
        for name, val in row.attributes.items():
            if isinstance(val, str):
                update_ops = list()  # No Firecloud update operations needed for string values
            elif isinstance(val, (int, float, bool)):
                update_ops = [dict(op="AddUpdateAttribute", attributeName=name, addUpdateAttribute=val)]
            elif hasattr(val, "__iter__"):
                update_ops = [dict(op="RemoveAttribute", attributeName=name)]
                types: Set[type] = set()
                for m in val:
                    update_ops.append(dict(op="AddListMember", attributeListName=name, newMember=m))
                    types.add(type(m))
                assert 1 == len(types)
            request_data.extend(update_ops)
        return request_data

    def put_row(self, item: Union[ROW_LIKE, ATTRIBUTES]) -> str:
        if isinstance(item, dict):
            row = Row(f"{uuid4()}", item)
        else:
            row = Row(*item)
        column_headers = tuple(sorted(row.attributes.keys()))
        if column_headers not in self._tsvs:
            self._tsvs[column_headers] = "\t".join([f"{self.name}_id", *column_headers])
            self._row_update_request_data[column_headers] = list()
        self._tsvs[column_headers] += (
            os.linesep
            + row.name + "\t"
            + "\t".join(row.attributes[c] if isinstance(row.attributes[c], str)
                        else "x"  # Dummy value to be replaced during row update API calls.
                        for c in column_headers)
        )
        update_request_data = self._get_row_update_request_data(row)
        if update_request_data:
            self._row_update_request_data[column_headers].append((row, update_request_data))
        self._upload(1024 * 20)
        return row.name

    def _upload(self, threshold: Optional[int]=None):
        """
        Schedule uploads for all TSVs of size equal to or greater than `threshold`.
        If `threshold` is None, schedule uploads for all TSVs.
        """
        for column_headers, tsv in self._tsvs.copy().items():
            if threshold is None or len(tsv) >= threshold:
                row_updates = self._row_update_request_data.get(column_headers, list())
                self.submit(self._do_fiss_upload, tsv, row_updates)
                del self._tsvs[column_headers]
                if row_updates:
                    del self._row_update_request_data[column_headers]

    def _do_fiss_upload(self, tsv: str, row_update_request_data: List[Tuple[Row, List[Dict[str, Any]]]]):
        fiss.fapi.upload_entities(self._workspace_google_project,
                                  self._workspace_name,
                                  tsv,
                                  model="flexible").raise_for_status()
        for row, request_data in row_update_request_data:
            if request_data:
                self.submit(self._do_fiss_updates, row, request_data)

    def _do_fiss_updates(self, row: Row, request_data: UPDATE_OPS, retry: int=0):
        try:
            fiss.fapi.update_entity(self._workspace_google_project,
                                    self._workspace_name,
                                    self.name,
                                    row.name,
                                    request_data).raise_for_status()
        except requests.exceptions.HTTPError as e:
            if 500 == e.response.status_code:
                # Firecloud occasionally throws 500 errors for successful update operations.
                # Check if we get the row we expect, retry otherwise.
                if 5 > retry:
                    cur_row = get_row(self.name, row.name)
                    if row != cur_row:
                        self.submit(self._do_fiss_updates, row, request_data, retry=retry + 1)
                else:
                    raise Exception(f"Ran out of retries updating row {row.name}") from e
            else:
                raise

    def _prepare_for_exit(self):
        if self._tsvs:
            self._upload()

class Deleter(_AsyncContextManager):
    """
    Distribute row deletes across as few API calls as possible.
    """
    def __init__(self, name: str, **kwargs):
        self.name = name
        self._workspace_name = kwargs.get("workspace_name", WORKSPACE_NAME)
        self._workspace_google_project = kwargs.get("workspace_google_project", WORKSPACE_GOOGLE_PROJECT)
        self._init_request_data()

    def _init_request_data(self):
        self._request_data: List[Dict[str, str]] = list()

    def del_row(self, item: Union[str, ROW_LIKE]):
        if isinstance(item, str):
            name = item
        elif isinstance(item, Row):
            name = item.name
        else:
            name = item[0]
        self._request_data.append(dict(entityType=self.name, entityName=name))
        if 500 <= len(self._request_data):
            self._delete()

    def _delete(self):
        self.submit(self._do_fiss_delete, self._request_data)
        self._init_request_data()

    def _do_fiss_delete(self, ents: List[Dict[str, str]]):
        try:
            fiss.fapi.delete_entities(self._workspace_google_project,
                                      self._workspace_name,
                                      ents).raise_for_status()
        except requests.exceptions.HTTPError as e:
            if 400 == e.response.status_code:
                pass
            else:
                raise

    def _prepare_for_exit(self):
        if self._request_data:
            self._delete()

def list_tables(**kwargs) -> Generator[str, None, None]:
    workspace_name = kwargs.get("workspace_name", WORKSPACE_NAME)
    workspace_google_project = kwargs.get("workspace_google_project", WORKSPACE_GOOGLE_PROJECT)
    resp = fiss.fapi.list_entity_types(workspace_google_project, workspace_name)
    resp.raise_for_status()
    for table_name in resp.json():
        yield table_name

def _get_rows_page(table: str,
                   page_number: int=1,
                   page_size: int=500,
                   sort_direction: str="asc",
                   filter_terms: Any=None,
                   **kwargs) -> Tuple[int, dict]:
    resp = fiss.fapi.get_entities_query(kwargs.get("workspace_google_project", WORKSPACE_GOOGLE_PROJECT),
                                        kwargs.get("workspace_name", WORKSPACE_NAME),
                                        table,
                                        page=page_number,
                                        page_size=page_size,
                                        sort_direction=sort_direction,
                                        filter_terms=filter_terms)
    resp.raise_for_status()
    body = resp.json()
    return body['resultMetadata']['filteredPageCount'], body['results']

def _attributes_from_fiss_response(attributes: Dict[str, Any]) -> ATTRIBUTES:
    return {key: val['items'] if isinstance(val, dict) and "items" in val else val
            for key, val in attributes.items()}

def list_rows(table: str, **kwargs) -> Generator[Row, None, None]:
    page_number = total_pages = 1
    while total_pages >= page_number:
        total_pages, rows = _get_rows_page(table, page_number, **kwargs)
        for item in rows:
            yield Row(item['name'], _attributes_from_fiss_response(item['attributes']))
        page_number += 1

def get_row(table: str, item: ROW_OR_NAME, **kwargs) -> Optional[Row]:
    row_name = item.name if isinstance(item, Row) else item
    resp = fiss.fapi.get_entity(kwargs.get("workspace_google_project", WORKSPACE_GOOGLE_PROJECT),
                                kwargs.get("workspace_name", WORKSPACE_NAME),
                                table,
                                row_name)
    try:
        resp.raise_for_status()
    except requests.exceptions.HTTPError as e:
        if 404 == e.response.status_code:
            return None
        else:
            raise
    data = resp.json()
    return Row(data['name'], _attributes_from_fiss_response(data['attributes']))

def put_rows(table: str, items: Iterable[Union[ROW_LIKE, ATTRIBUTES]], **kwargs) -> List[str]:
    with Writer(table, **kwargs) as tw:
        return [tw.put_row(item) for item in items]

def put_row(table: str, item: Union[ROW_LIKE, ATTRIBUTES], **kwargs) -> str:
    return put_rows(table, [item], **kwargs)[0]

def del_rows(table: str, items: Iterable[ROW_OR_NAME], **kwargs):
    with Deleter(table, **kwargs) as td:
        for row in items:
            td.del_row(row)

def del_row(table: str, item: ROW_OR_NAME, **kwargs):
    del_rows(table, [item], **kwargs)


def delete(table: str, **kwargs):
    with Deleter(table, **kwargs) as td:
        for row in list_rows(table, **kwargs):
            td.del_row(row)

def fetch_drs_url(table: str, file_name: str, **kwargs) -> str:
    """
    Fetch the first `object_id` associated with `pfb:file_name` from `table`.
    DRS urls, when available, are stored in `pfb:object_id`.
    Note: prior to 21-October, 2020, column headers omitted the "pfb:" prefix. For the time being, both formats are
          supported.
    """
    for pfx in ("pfb:", ""):
        try:
            for row in list_rows(table, **kwargs):
                if file_name == row.attributes[f"{pfx}file_name"]:
                    val = row.attributes[f"{pfx}object_id"]
                    if isinstance(val, str) and val.startswith("drs://"):
                        return val
                    else:
                        raise ValueError(f"Expected DRS url in '{table}' for '{file_name}'"
                                         f", got '{val}' instead.")
        except KeyError:
            pass
    else:
        raise KeyError(f"Unable to fetch DRS URL for table '{table}', file_name '{file_name}'")
