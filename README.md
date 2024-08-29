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

## Upgrading

It is often useful to keep up to date with new features and bug fixes. Installing the latest version of
terra-notebook-utils depends on your host environment.

From any Jupyter notebook, use the following (and note the leading "%")
```
%pip install --upgrade --no-cache-dir terra-notebook-utils
```

From the CLI on standard Terra notebook runtimes, which are available using the terminal button in the Terra user
interface, use
```
/usr/local/bin/pip install --upgrade --no-cache-dir terra-notebook-utils
```
Note that all standard notebook runtimes on Terra are based on
[this Docker image](https://github.com/databiosphere/terra-docker#terra-base-images).

For other environments, it is often enough to do
```
pip install --upgrade --no-cache-dir terra-notebook-utils
```

## Credentials
Much of the terra-notebook-utilities functionality requires credentialed access through a Google Cloud Platform account.
Credentials are already available when running in a Terra Google notebook environment.
Otherwise, Google credentials may be obtained with the command
```
gcloud auth application-default login
```
The terra-notebook-utilities `drs` subcommands (only) run successfully in a Terra Azure
notebook environment using the Azure default credentials.

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
# Show your available workspace namespaces (also known as Google billing projects)
tnu profile list-workspace-namespaces
```

```
# Show version
tnu --version
```

### CLI Configuration

Several CLI commands target a workspace or require a workspace namespace. Defaults can be configured using the
commands
```
tnu config set-workspace my-workspace
tnu config set-workspace-google-project my-workspace-namespace
```

Note that workspace namespace is the same as Google billing project.

Alternatively, workspace and workspace namespace can be passed in to individual commands instead of, or as overrides to,
the configured defaults. See command help, e.g. `tnu table get --help`, for usage information.

Finally, workspace and workspace namespace can be specified with the environment variables
`WORKSPACE_NAME` and `GOOGLE_PROJECT`. These values are used with lowest precedence.

### The DRS API and CLI

terra-notebook-utils provides several methods and CLI commands useful for working with
[DRS](https://github.com/ga4gh/data-repository-service-schemas) resolved objects:

#### Python API

Return information about a DRS object:
```
from terra_notebook_utils import drs
drs.info("drs://my-drs-url")
```

Copy a DRS object to local file system or bucket:
```
from terra_notebook_utils import drs
drs.copy("drs://my-drs-url", "gs://my-dst-bucket/my-dst-key")
drs.copy("drs://my-drs-url", "local_filepath")
drs.copy_batch(["drs://my-drs-url1", "drs://my-drs-url2"], "local_directory")
drs.copy_batch(["drs://my-drs-url1", "drs://my-drs-url2"], "gs://my-dst-bucket/prefix")
```

Head a DRS object:
```
from terra_notebook_utils import drs
drs.head("drs://my-drs-url", num_bytes=10)
```

Return a signed URL to access a DRS object:
```
from terra_notebook_utils import drs
drs.access("drs://my-drs-url")
```

#### CLI

Information about a DRS object:
```
tnu drs info drs://my-drs-url
```

Copy a DRS object to local or bucket:
```
tnu drs copy drs://my-drs-url gs://my-dst-bucket/my-dstkey
tnu drs copy drs://my-drs-url local_filepath
tnu drs copy-batch drs://my-drs-url1 drs://my-drs-url2 --dst local_directory
tnu drs copy-batch drs://my-drs-url1 drs://my-drs-url2 --dst gs://my-dst-bucket/prefix
```

Head a DRS object:
```
tnu drs head drs://my-drs-url --bytes 10
```

Return a signed URL to access a DRS object:
```
tnu drs access drs://my-drs-url
```

The CLI outputs error messages, not strack traces. Stack traces are available by defining the environment variable
`TNU_CLI_DEBUG`.

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
2. Pull the `terra-jupyter-python` docker image currently used by the Terra UI (double-check the version [here](https://storage.googleapis.com/terra-docker-image-documentation/terra-docker-versions-candidate.json)):
   `docker pull us.gcr.io/broad-dsp-gcr-public/terra-jupyter-python:1.1.5`
3. run the image from *one directory above* the root directory of this repo via `docker run -itd --entrypoint='/bin/bash' -v $PWD/terra-notebook-utils:/work -u root -e PIP_USER=false --name test-image us.gcr.io/broad-dsp-gcr-public/terra-jupyter-python:1.1.5`
4. Attach your terminal to the image via `docker exec -it test-image bash`, then navigate to the directory the code is mounted to via `cd /work`. Note that the above command ensures any changes you make to files in the repo will be updated in the image as well.
5. log in with your Google credentials using `gcloud auth application-default login`,
6. install requirements with `pip install -r requirements.txt`
7. set up the following environment variables, depending on what you will be using:
    - `export GOOGLE_PROJECT=[validProject]`
    - `export WORKSPACE_NAME=[workspaceWithinProject]`
    - `export TERRA_DEPLOYMENT_ENV=dev`
    - `export WORKSPACE_BUCKET=[bucketWithinWorkspace]`
    - `export GCLOUD_PROJECT=[validGoogleProject]` (set this if your DRS uri does not return Google SA)

For Python API
  - run the python shell via `python`, and import any modules you wish to use. For example, `from terra_notebook_utils import drs`

For CLI
  - run `scripts/tnu <command>`, for example `scripts/tnu drs copy drs://url/here local_path`

Sample DRS urls used in tests:
(you would need to get access to these before successfully resolving them)
  - `drs://dg.712C/fa640b0e-9779-452f-99a6-16d833d15bd0`: non-protected test DRS url that resolves to a small file in dev
  - `drs://jade.datarepo-dev.broadinstitute.org/v1_0c86170e-312d-4b39-a0a4-2a2bfaa24c7a_c0e40912-8b14-43f6-9a2f-b278144d0060`: Jade Dev test url
Make sure you are setting proper environment variables mentioned in step 7 for each DRS url


## Tests
To run tests, follow the same setup from Local Development till step 4. Make sure your account has access to the workspace `terra-notebook-utils-tests`
1. install requirements with `pip install -r requirements-dev.txt`
2. set `export WORKSPACE_NAME=terra-notebook-utils-tests`

**Test Env: Dev** (currently it has tests for DRS methods)

This will run tests against Terra and DRSHub Dev using Jade Dev DRS url (make sure your Terra Dev account has access to the above mentioned url)

3. log in with your Google credentials using `gcloud auth application-default login` with your Terra Dev account
4. Set
    - `export GOOGLE_PROJECT=[googleProjectToBeBilled]`
    - `export TERRA_DEPLOYMENT_ENV=dev`
    - `export WORKSPACE_BUCKET=[bucketWithinWorkspace]` (or a bucket where you want to copy data resolved through DRS url)
5. run in package root:
    - `make dev_env_access_test`: runs tests marked as `dev_env_access`


**Test Env: Prod**

This will run tests against Terra and DRSHub Prod (make sure you have proper access to DRS urls, workspace and Google bucket)

3. log in with your Google credentials using `gcloud auth application-default login` with your Terra Prod account
4. set `export GOOGLE_PROJECT=firecloud-cgl; export TERRA_DEPLOYMENT_ENV=prod; export TNU_BLOBSTORE_TEST_GS_BUCKET=tnu-test-bucket`
5. run in package root:
    - `make test`: skips controlled and dev access tests
    - `make controlled_access_test`: runs tests marked as `controlled_access`
    - `make all_test`: runs all tests for Prod (controlled_access and workspace_access)

## Release
The commands mentioned in `common.mk` file are used for the release process.
Steps:
- if you don't have a [PyPI](https://pypi.org/) account, please create one
- you should be a collaborator in PyPI for Terra Notebook Utils. If you are not, please ask Lon Blauvelt (lblauvel at ucsc dot edu) to add
you as a collaborator
- follow the setup instructions as mentioned in `Tests` section above for env Prod; make sure you have access
to the DRS urls, workspaces and buckets
- run `make all_test` from inside the docker container created in `Local Development` section.
Once tests pass, you can move to the release step
- Release:
    - For non-breaking API changes, use `make release_patch`
    - For breaking API changes, use `make release_minor`
    - For a major release, use `make release_major`

If a release needs to be rolled back for some reason, please contact Lon Blauvelt (lblauvel at ucsc dot edu) for help.

## Links
Project home page [GitHub](https://github.com/DataBiosphere/terra-notebook-utils)
Package distribution [PyPI](https://pypi.org/project/terra-notebook-utils)

### Bugs
Please report bugs, issues, feature requests, etc. on [GitHub](https://github.com/DataBiosphere/terra-notebook-utils).

![](https://biodata-integration-tests.net/xbrianh/terra-notebook-utils/badges/master/pipeline.svg) ![](https://badge.fury.io/py/terra-notebook-utils.svg)
