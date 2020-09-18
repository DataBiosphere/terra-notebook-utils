# terra-notebook-utils
Python API and CLI providing utilities for working with [DRS](https://support.terra.bio/hc/en-us/articles/360039330211)
objects, [VCF](https://samtools.github.io/hts-specs/VCFv4.1.pdf) files, and the
[Terra notebook environment](https://support.terra.bio/hc/en-us/articles/360027237871-Terra-s-Jupyter-Notebooks-environment-Part-I-Key-components).

## Installation

From the CLI:
```
pip install terra-notebook-utils
```

In a Jupyter notebook (note the ipython magic "[%pip](https://ipython.readthedocs.io/en/stable/interactive/magics.html#magic-pip)"):
```
%pip install terra-notebook-utils
```

To upgrade to the newest version:
```
pip install --upgrade --no-cache-dir terra-notebook-utils
```

## Credentials
Much of the terra-notebook-utilities functionality requires credentialed access through a Google Cloud Platform account.
Credentials are already available when running in a Terra notebook environment. Otherwise, credentials may be obtained
with the command
```
gcloud auth application-default login
```

## Usage

terra-notebook-utils exposes a Python API, as well as wrappers to execute some functionality on the CLI. The Python
API is best explored with Pythons great `help` function. For instance, issuing the follow commands into a Python
interpreter or Jupyter notebook will produce help and usage for the `drs` module.
```
import terra_notebook_utils as tnu
help(tnu.drs)
```

Similarly, the CLI may be explored using the typical `-h` argument. Try the following commands at a bash prompt.
```
# See the command groups available to the CLI
tnu -h
```

```
# See the commands available to the vcf group
tnu vcf -h
```

```
# Show your available billing projects
tnu profile list-billing-projects
```

### CLI Configuration

Several CLI commands target a workspace or require a Google billing project. Defaults can be configured using the
commands
```
tnu config set-workspace my-workspace
tnu config set-workspace-google-project my-billing-project
```

Alternatively, workspace and billing project can be passed in to individual commands instead of, or as overrides to,
the configured defaults. See command help, e.g. `tnu table get --help`, for usage information.

Finally, workspace and billing project can be specified with the environment variables
`WORKSPACE_NAME` and `GOOGLE_PROJECT`. These values are used with lowest precedence.

### The DRS API and CLI

terra-notebook-utils provides several methods and CLI commands useful for working with
[DRS](https://github.com/ga4gh/data-repository-service-schemas) resolved objects:

#### Python API

Copy drs object to local file system or bucket:
```
from terra_notebook_utils import drs
drs.copy("drs://my-drs-url", "gs://my-dst-bucket/my-dst-key")
drs.copy("drs://my-drs-url", "local_filepath")
drs.copy_batch(["drs://my-drs-url1", "drs://my-drs-url2"], "local_directory")
drs.copy_batch(["drs://my-drs-url1", "drs://my-drs-url2"], "gs://my-dst-bucket/prefix")
```

Head drs object:
```
from terra_notebook_utils import drs
drs.head("drs://my-drs-url", num_bytes=10)
```

#### CLI

Copy drs object to local or bucket:
```
tnu drs copy drs://my-drs-url gs://my-dst-bucket/my-dstkey
tnu drs copy drs://my-drs-url local_filepath
tnu drs copy-batch drs://my-drs-url1 drs://my-drs-url2 --dst local_directory
tnu drs copy-batch drs://my-drs-url1 drs://my-drs-url2 --dst gs://my-dst-bucket/prefix
```

Head drs object:
```
tnu drs head drs://my-drs-url --bytes 10
```

### The VCF API and CLI

terra-notebook-utils provides some CLI commands useful for getting information about VCF files.
These commands work for VCFs stored locally, in a Google Storage bucket, or at a DRS url.

Print VCF header:
```
tnu vcf head drs://my-vcf
tnu vcf head gs://my-vcf
tnu vcf head my.vcf.gz
```

Print VCF samples:
```
tnu vcf samples drs://my-vcf
tnu vcf samples gs://my-vcf
tnu vcf samples my.vcf.gz
```

Print VCF stats. This command executes quickly, and shows the length and file size of the VCF. If
the VCF is compressed, the compressed size is returned.
```
tnu vcf stats drs://my-vcf
tnu vcf stats gs://my-vcf
tnu vcf stats my.vcf.gz
```

While a Python API for working with VCFs is currently available, usage is more complex. Please contact the
maintainer for more information.

## Local Development
For local development:
1. Make the decision whether you want to run this using your local environment, or develop from within a docker image. 
Developing within a docker image is recommended, since that most closely models how users will use this. Additionally, there are some issues with installing the requirements.txt on mac.
If you don't wish to run this within a docker image, skip to step 5.
2. run `docker pull us.gcr.io/broad-dsp-gcr-public/terra-jupyter-python:0.0.12`
3. run the image from *one directory above* the root directory of this repo via `docker run -itd --entrypoint='/bin/bash' -v $PWD/terra-notebook-utils:/work -u root -e PIP_USER=false --name test-image us.gcr.io/broad-dsp-gcr-public/terra-jupyter-python:0.0.12`
4. Attach your terminal to the image via `docker exec -it test-image bash`, then navigate to the directory the code is mounted to via `cd /work`. Note that the above command ensures any changes you make to files in the repo will be updated in the image as well.
5. log in with your Google credentials using `gcloud auth application-default login`,
6. install requirements with `pip install -r requirements.txt`
7. set up the following environment variables, depending on what you will be using: 
  - `export GOOGLE_PROJECT=[validProject]`
  - `export WORKSPACE_NAME=[workspaceWithinProject]`
  - `export TERRA_DEPLOYMENT_ENV=dev` 
  - `export WORKSPACE_BUCKET=[bucketWithinWorkspace]`
  - `export GCLOUD_PROJECT=[valid google project]` (set this if your DRS uri does not return Google SA)
8. For Python API
  - run the python shell via `python`, and import any modules you wish to use. For example, `from terra_notebook_utils import drs`
  For CLI
  - run `pip install terra-notebook-utils`
  - run `import terra_notebook_utils as tnu`
  - run `scripts/tnu <command>`, for example `scripts/tnu drs copy drs://url/here local_path`

Sample DRS urls used in tests:
(you would need to get access to these before successfully resolving it)
  - `drs://dg.712C/fa640b0e-9779-452f-99a6-16d833d15bd0`: non-protected test DRS url that resolves to a small file in dev
  - `drs://jade.datarepo-dev.broadinstitute.org/v1_0c86170e-312d-4b39-a0a4-2a2bfaa24c7a_c0e40912-8b14-43f6-9a2f-b278144d0060`: Jade Dev test url
Make sure you are setting proper environment variables mentioned in step 7 for each DRS url


## Tests
To run tests, follow the same setup from Local Development till step 4. Then,
1. Your account must have access to the workspace `terra-notebook-utils-tests` 
2. install requirements with `pip install -r requirements-dev.txt`
3. Set `export WORKSPACE_NAME=terra-notebook-utils-tests`

Test Env: Prod 
This will run tests against Terra and Martha Prod (make sure you have proper access to DRS urls, workspace and Google bucket)
4. log in with your Google credentials using `gcloud auth application-default login` with your Terra Prod account
5. Set `export GOOGLE_PROJECT=firecloud-cgl; export TERRA_DEPLOYMENT_ENV=prod` 
6. run in package root:
  - `make test`: skips controlled and dev access tests
  - `make mypy controlled_access_test`: runs tests marked as `controlled_access`
  
Test Env: Dev (currently it has tests for DRS methods)
This will run tests against Terra and Martha Dev using Jade Dev DRS url (make sure your Terra Dev account has access to this url)
4. log in with your Google credentials using `gcloud auth application-default login` with your Terra Dev account
5. Set 
  - `export GOOGLE_PROJECT=[google project to be billed]`
  - `export TERRA_DEPLOYMENT_ENV=dev`
  - `export WORKSPACE_BUCKET=[bucketWithinWorkspace]` (or a bucket where you want to copy data resolved through DRS url)
6. run in package root:
  - `make mypy dev_env_access_test`: runs tests marked as `dev_env_access`


## Links
Project home page [GitHub](https://github.com/DataBiosphere/terra-notebook-utils)  
Package distribution [PyPI](https://pypi.org/project/terra-notebook-utils)

### Bugs
Please report bugs, issues, feature requests, etc. on [GitHub](https://github.com/DataBiosphere/terra-notebook-utils).

![](https://biodata-integration-tests.net/xbrianh/terra-notebook-utils/badges/master/pipeline.svg) ![](https://badge.fury.io/py/terra-notebook-utils.svg)
