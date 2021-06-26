"""This defines a test notebook for TNU. It can be published to
'https://terra.biodatacatalyst.nhlbi.nih.gov/#workspaces/firecloud-cgl/terra-notebook-utils-tests' with the command
'make dev_scripts/test_notebook.py'.
"""
import os
import herzog

# Mock the notebook environment
os.environ['WORKSPACE_NAME'] = "terra-notebook-utils-tests"
os.environ['WORKSPACE_BUCKET'] = "gs://fc-9169fcd1-92ce-4d60-9d2d-d19fd326ff10"
os.environ['GOOGLE_PROJECT'] = "firecloud-cgl"

with herzog.Cell("markdown"):
    """
    # terra-notebook-utils test notebook
    This notebook tests TNU functionality in a notebook environment. It should be executed prior to TNU releases.

    *author: Brian Hannafious, Genomics Institute, University of California Santa Cruz*
    """

with herzog.Cell("python"):
    #%pip install --upgrade --no-cache-dir git+https://github.com/DataBiosphere/terra-notebook-utils
    pass

with herzog.Cell("python"):
    import logging
    from terra_notebook_utils.logger import logger
    logger.setLevel(logging.WARNING)

with herzog.Cell("python"):
    import os
    import json
    import logging
    from uuid import uuid4
    from terra_notebook_utils import drs, vcf, workspace, profile, table, workflows
    bucket_name = os.environ['WORKSPACE_BUCKET'][5:]
    DRS_URI_370_KB = "drs://dg.4503/6ffc2f59-2596-405c-befd-9634dc0ed837"  # 1000 Genomes, 370.38 KB
    DRS_URI_021_MB = "drs://dg.4503/48286908-b079-4407-8773-5ab8ab42df12"  # 1000 Genomes, 20.62 MB
    DRS_URI_240_MB = "drs://dg.4503/06ea6ade-f1cf-42b1-b6be-5a6f912ab965"  # 1000 Genomes, 240.53 MB
    DRS_URI_702_MB = "drs://dg.4503/5cc56e78-cb80-4e3c-aa41-63ea3297d1f3"  # 1000 Genomes, 702.57 MB
    DRS_URI_002_GB = "drs://dg.4503/076be06a-4251-4fe5-b02f-43600e909534"  # 1000 Genomes, 1.66 GB
    DRS_URI_006_GB = "drs://dg.4503/ccae5e23-014d-47b1-89d3-049745a10120"  # 1000 Genomes, 5.75 GB
    DRS_URI_025_GB = "drs://dg.4503/3e8438ec-9a7f-4215-8c23-de2c321aeb42"  # 1000 Genomes, 24.82 GB
    DRS_URI_069_GB = "drs://dg.4503/81f2efd4-20bc-44c9-bf04-2743275d21ac"  # 1000 Genomes, 68.54 GB
    DRS_URI_100_GB = "drs://dg.4503/6ff298c4-35fc-44aa-acb2-f0b4d98e407a"  # 1000 Genomes, 100 GB
    DRS_URI_TAR_GZ = "drs://dg.4503/da8cb525-4532-4d0f-90a3-4d327817ec73"  # GENOA, 198 GB

with herzog.Cell("python"):
    """Test drs info"""
    drs.info(DRS_URI_370_KB)

with herzog.Cell("python"):
    """Test drs head"""
    drs.head(DRS_URI_370_KB)

with herzog.Cell("python"):
    """Test drs copy to local disk"""
    drs.copy(DRS_URI_370_KB, ".")
    drs.copy(DRS_URI_240_MB, ".")

with herzog.Cell("python"):
    """Test drs copy to bucket"""
    drs.copy(DRS_URI_370_KB, f"gs://{bucket_name}/test-notebook-{uuid4()}")
    drs.copy(DRS_URI_240_MB, f"gs://{bucket_name}/test-notebook-{uuid4()}")
    drs.copy_to_bucket(DRS_URI_370_KB, f"test-notebook-{uuid4()}")
    drs.copy_to_bucket(DRS_URI_240_MB, f"test-notebook-{uuid4()}")

with herzog.Cell("python"):
    """Test drs copy batch"""
    manifest = [
        dict(drs_uri=DRS_URI_370_KB, dst=f"gs://{bucket_name}/test-notebook-{uuid4()}"),
        dict(drs_uri=DRS_URI_370_KB, dst=f"."),
        dict(drs_uri=DRS_URI_240_MB, dst=f"gs://{bucket_name}/test-notebook-{uuid4()}"),
        dict(drs_uri=DRS_URI_240_MB, dst=f"."),
    ]
    drs.copy_batch(manifest)

with herzog.Cell("python"):
    """Test drs extract tarball"""
    drs.extract_tar_gz(DRS_URI_TAR_GZ, ".")
    drs.extract_tar_gz(DRS_URI_TAR_GZ, f"gs://{bucket_name}/test-notebook-{uuid4()}")

with herzog.Cell("python"):
    """Test vcf info"""
    blob = drs.blob_for_url(DRS_URI_100_GB, os.environ['GOOGLE_PROJECT'])
    info = vcf.VCFInfo.with_blob(blob)
    assert 2504 == len(info.samples)
    assert "HG00096" == info.samples[0]

with herzog.Cell("python"):
    """Test workspace get"""
    workspace.get_workspace("terra-notebook-utils-tests")

with herzog.Cell("python"):
    """Test workspace list"""
    workspace.list_workspaces()

with herzog.Cell("python"):
    """Test workspace get bucket"""
    workspace.get_workspace_bucket("terra-notebook-utils-tests")

with herzog.Cell("python"):
    """Test workspace get namespace"""
    workspace.get_workspace_namespace("terra-notebook-utils-tests")

with herzog.Cell("python"):
    """Test profile list workspace namespaces"""
    profile.list_workspace_namespaces()

with herzog.Cell("python"):
    """Test table"""
    table_name = f"test-notebook-table-{uuid4()}"
    data = [dict(file_name=f"{uuid4()}", object_id=f"{DRS_URI_100_GB}"),
            dict(file_name=f"{uuid4()}", object_id=f"{DRS_URI_240_MB}"),
            dict(file_name=f"{uuid4()}", object_id=f"{DRS_URI_021_MB}"),
            dict(file_name=f"{uuid4()}", object_id=f"{DRS_URI_069_GB}")]
    assert table_name not in {table_name for table_name in table.list_tables()}
    try:
        table.put_rows(table_name, data)
        assert table_name in {table_name for table_name in table.list_tables()}
        for row in table.list_rows(table_name):
            assert row.attributes in data
        trow = table.get_row(table_name, row.name)
        assert trow.attributes == row.attributes
        drs_uri = table.fetch_drs_url(table_name, trow.attributes['file_name'])
        assert trow.attributes['object_id'] == drs_uri
        rows = [row for row in table.list_rows(table_name)]
        table.del_rows(table_name, [rows[0].name, rows[1].name])
        assert len(data) - 2 == len([row for row in table.list_rows(table_name)])
    finally:
        table.delete(table_name)
    assert table_name not in {table_name for table_name in table.list_tables()}

with herzog.Cell("python"):
    """Test workflows list"""
    for s in workflows.list_submissions():
        print(json.dumps(s, indent=2))

with herzog.Cell("python"):
    """Test workflows get submission"""
    submissions = [s for s in workflows.list_submissions()]
    workflows.get_submission(submissions[0]['submissionId'])

with herzog.Cell("python"):
    """Test workflows get workflow"""
    submissions = [s for s in workflows.list_submissions()]
    s = workflows.get_submission(submissions[0]['submissionId'])
    workflows.get_workflow(s['submissionId'], s['workflows'][0]['workflowId'])

with herzog.Cell("python"):
    """Test workflows estimate cost"""
    submissions = [s for s in workflows.list_submissions()]
    metadata = workflows.get_all_workflows(submissions[0]['submissionId'])
    for workflow_id, workflow_metadata in metadata.items():
        for item in workflows.estimate_workflow_cost(workflow_id, workflow_metadata):
            print(item)
