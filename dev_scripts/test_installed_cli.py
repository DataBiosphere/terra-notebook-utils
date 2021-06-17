#!/usr/bin/env python
"""This is a suite of CLI tests that should pass before releasing a new version of terra-notebook-utils.
The following steps are performed:
    1. A python virtual environment is created in a temporary location
    2. terra-notebook-utils is installed into the virtual environment using the local repo
    3. The installed tnu executable is used to run CLI tests
"""
import os
import sys
import json
import unittest
import tempfile
import subprocess
from uuid import uuid4

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from tests.infra import get_env


TNU_REPO = "https://github.com/DataBiosphere/terra-notebook-utils"
TNU_TEST_WORKSPACE = get_env("TNU_TEST_WORKSPACE")
TNU_TEST_WORKSPACE_NAMESPACE = get_env("TNU_TEST_WORKSPACE_NAMESPACE")
TNU_TEST_BUCKET = get_env("TNU_BLOBSTORE_TEST_GS_BUCKET")
WORKSPACE_ARGS = f"--workspace {TNU_TEST_WORKSPACE} --workspace-namespace {TNU_TEST_WORKSPACE_NAMESPACE}"

VENV_DIR = "venv"
VENV_BIN = os.path.join(VENV_DIR, "bin")
TNU = os.path.join(VENV_BIN, "tnu")

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

class TestTerraNotebookUtilsReleaseCLI(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        repo_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
        cls._tempdir = tempfile.TemporaryDirectory()
        os.chdir(cls._tempdir.name)
        run("python -m pip install virtualenv")
        run(f"python -m virtualenv {VENV_DIR}")
        run(f"{VENV_BIN}/pip install {repo_dir}")

    def test_drs_copy_to_local(self):
        run(f"{TNU} drs copy {WORKSPACE_ARGS} {DRS_URI_370_KB} .")
        run(f"{TNU} drs copy {WORKSPACE_ARGS} {DRS_URI_240_MB} .")

    def test_drs_copy_to_gs_bucket(self):
        run(f"{TNU} drs copy {WORKSPACE_ARGS} {DRS_URI_370_KB} gs://{TNU_TEST_BUCKET}")
        run(f"{TNU} drs copy {WORKSPACE_ARGS} {DRS_URI_240_MB} gs://{TNU_TEST_BUCKET}")

    def test_drs_copy_batch_to_local(self):
        run(f"{TNU} drs copy-batch {WORKSPACE_ARGS} --dst . {DRS_URI_370_KB} {DRS_URI_021_MB} {DRS_URI_240_MB}")

    def test_drs_copy_batch_to_gs_bucket(self):
        run(f"{TNU} drs copy-batch {WORKSPACE_ARGS} --dst gs://{TNU_TEST_BUCKET} "
            f"{DRS_URI_370_KB} {DRS_URI_021_MB} {DRS_URI_240_MB}")

    def test_drs_head(self):
        run(f"{TNU} drs head {WORKSPACE_ARGS} {DRS_URI_240_MB}")

    def test_drs_info(self):
        d = run_json(f"{TNU} drs info {DRS_URI_240_MB}")
        self.assertEqual("CCDG_13607_B01_GRM_WGS_2019-02-19_chr20.recalibrated_variants.annotated.vcf.gz",
                         d['name'])

    def test_drs_credentials(self):
        run(f"{TNU} drs credentials {DRS_URI_240_MB}")

    @unittest.skip("This takes a lot of resources. Test in workflow (WDL) instead")
    def test_drs_extract_tar_gz(self):
        run(f"{TNU} drs extract-tar-gz {WORKSPACE_ARGS} {DRS_URI_TAR_GZ}")

    def test_vcf_head(self):
        run(f"{TNU} vcf head {WORKSPACE_ARGS} {DRS_URI_240_MB}")

    def test_vcf_samples(self):
        d = run_json(f"{TNU} vcf samples {WORKSPACE_ARGS} {DRS_URI_100_GB}")
        self.assertEqual(2504, len(d))
        self.assertEqual("HG00096", d[0])

    def test_vcf_stats(self):
        d = run_json(f"{TNU} vcf stats {WORKSPACE_ARGS} {DRS_URI_100_GB}")
        self.assertEqual(107536394413, d['size'])

    def test_workspace_list(self):
        run_json(f"{TNU} workspace list")

    def test_workspace_get(self):
        run_json(f"{TNU} workspace get --workspace {TNU_TEST_WORKSPACE}")

    def test_workspace_get_bucket(self):
        run(f"{TNU} workspace get-bucket --workspace {TNU_TEST_WORKSPACE}")

    @unittest.skip("Awkward to test")
    def test_workspace_delete_workflow_logs(self):
        run_json(f"{TNU} workspace get-bucket --workspace {TNU_TEST_WORKSPACE}")

    def test_profile_list_workspace_namespaces(self):
        run_json(f"{TNU} profile list-workspace-namespaces")

    def test_table(self):
        with self.subTest("list"):
            run(f"{TNU} table list {WORKSPACE_ARGS}")

        table_name = f"release-test-cli-{uuid4()}"
        data = [dict(file_name=f"{uuid4()}", object_id=f"{DRS_URI_100_GB}"),
                dict(file_name=f"{uuid4()}", object_id=f"{DRS_URI_240_MB}"),
                dict(file_name=f"{uuid4()}", object_id=f"{DRS_URI_021_MB}"),
                dict(file_name=f"{uuid4()}", object_id=f"{DRS_URI_069_GB}")]

        with self.subTest("put-row"):
            for row in data:
                run_json(f"{TNU} table put-row {WORKSPACE_ARGS} --table {table_name} "
                         + " ".join(f"{k}={v}" for k, v in row.items()))

        with self.subTest("list-rows"):
            p = run(f"{TNU} table list-rows {WORKSPACE_ARGS} --table {table_name}")
            for line in p.stdout.decode("utf-8").split(os.linesep):
                line = line.strip()
                if not line:
                    continue
                row = json.loads(line)
                self.assertIn(dict(file_name=row['file_name'], object_id=row['object_id']), data)
            row_name = row[f'{table_name}_id']

        with self.subTest("get-row"):
            run_json(f"{TNU} table get-row {WORKSPACE_ARGS} --table {table_name} --row {row_name}")

        with self.subTest("fetch-drs-url"):
            fn = data[0]['file_name']
            p = run(f"{TNU} table fetch-drs-url {WORKSPACE_ARGS} --table {table_name} --file-name {fn}")
            self.assertEqual(data[0]['object_id'], p.stdout.decode("utf-8").strip())

        with self.subTest("delete-row"):
            run_json(f"{TNU} table get-row {WORKSPACE_ARGS} --table {table_name} --row {row_name}")

        with self.subTest("delete-table"):
            run(f"{TNU} table delete-table {WORKSPACE_ARGS} --table {table_name}")

    def test_workflows(self):
        with self.subTest("list-submissions"):
            p = run(f"{TNU} workflows list-submissions {WORKSPACE_ARGS}")
            for line in p.stdout.decode("utf-8").split(os.linesep):
                line = line.strip()
                if not line:
                    continue
                submission_id, _ = line.strip().split(" ", 1)

        with self.subTest("get-submission"):
            d = run_json(f"{TNU} workflows get-submission {WORKSPACE_ARGS} --submission-id {submission_id}")
            workflow_id = d['workflows'][0]['workflowId']

        with self.subTest("get-workflow"):
            d = run_json(f"{TNU} workflows get-workflow {WORKSPACE_ARGS}"
                         f" --submission-id {submission_id} --workflow-id {workflow_id}")

        with self.subTest("get-submission"):
            run(f"{TNU} workflows estimate-submission-cost {WORKSPACE_ARGS} --submission-id {submission_id}")

class BColors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

def run(cmd: str) -> subprocess.CompletedProcess:
    """This is meant intended mimic typical CLI usage, so execute in a shell."""
    p = subprocess.run(cmd, shell=True, capture_output=True)
    try:
        p.check_returncode()
        print(f"{BColors.OKGREEN}{cmd}{BColors.ENDC}")
    except subprocess.CalledProcessError:
        print(f"{BColors.FAIL}{cmd}{BColors.ENDC}")
        print(f"{BColors.FAIL}{p.stderr}{BColors.ENDC}")  # type: ignore # go home mypy you're drunk
        raise
    return p

def run_json(cmd: str) -> dict:
    p = run(cmd)
    return json.loads(p.stdout.strip())

if __name__ == '__main__':
    unittest.main()
