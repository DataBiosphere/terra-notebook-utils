#!/usr/bin/env python
"""Upload fixtures into Terra data model."""
import os
import sys
import json
from uuid import uuid4

from firecloud import fiss

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', ".."))  # noqa
sys.path.insert(0, pkg_root)  # noqa

import tests.config
WORKSPACE_GOOGLE_PROJECT = os.environ['GCLOUD_PROJECT']
WORKSPACE_NAME = os.environ['WORKSPACE_NAME']

def list_entities():
    resp = fiss.fapi.get_entities_with_type(WORKSPACE_GOOGLE_PROJECT, WORKSPACE_NAME)
    resp.raise_for_status()
    for ent in resp.json():
        yield ent

def delete_entities(entities):
    resp = fiss.fapi.delete_entities(WORKSPACE_GOOGLE_PROJECT, WORKSPACE_NAME, entities)
    resp.raise_for_status()

def delete_all_entities():
    entities_to_delete = [dict(entityType=e['entityType'], entityName=e['name'])
                          for e in list_entities()]
    delete_entities(entities_to_delete)

def upload_entities(tsv_data):
    resp = fiss.fapi.upload_entities(WORKSPACE_GOOGLE_PROJECT, WORKSPACE_NAME, tsv_data, model="flexible")
    print(resp.content)
    resp.raise_for_status()

delete_all_entities()
with open("tests/fixtures/workspace_manifest.json", "rb") as fh:
    manifest = json.loads(fh.read())
    for table_name, table_data in manifest.items():
        tsv = "\t".join([f"entity:{table_name}_id"] + table_data['column_headers'])
        for row in table_data['rows']:
            tsv += os.linesep + "\t".join([f"{uuid4()}"] + row)
        upload_entities(tsv)
