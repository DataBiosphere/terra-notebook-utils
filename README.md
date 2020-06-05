# terra-notebook-utils
Python API and CLI providing utilties for working with [DRS](https://support.terra.bio/hc/en-us/articles/360039330211)
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

### The DRS API and CLI

terra-notebook-utils provides several methods and CLI commands useful for working with
[DRS](https://github.com/ga4gh/data-repository-service-schemas) resolved objects:

#### Python API

Copy drs object to local filesystem or bucket:
```
from terra_notebook_utils import drs
drs.copy("drs://my-drs-url", "gs://my-dst-bucket/my-dst-key")
drs.copy("drs://my-drs-url", "local_filepath")
```

#### CLI

Copy drs object to local or bucket:
```
tnu drs copy drs://my-drs-url gs://my-dst-bucket/my-dstkey
tnu drs copy drs://my-drs-url local_filepath
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

## Tests
To run tests,
1. log in with your Google credentials using `gcloud auth application-default login`,
1. install requirements with `pip install -r requirements-dev.txt`,
1. run `make test` in the package root.

## Links
Project home page [GitHub](https://github.com/DataBiosphere/terra-notebook-utils)  
Package distribution [PyPI](https://pypi.org/project/terra-notebook-utils)

### Bugs
Please report bugs, issues, feature requests, etc. on [GitHub](https://github.com/DataBiosphere/terra-notebook-utils).

![](https://biodata-integration-tests.net/xbrianh/terra-notebook-utils/badges/master/pipeline.svg) ![](https://badge.fury.io/py/terra-notebook-utils.svg)
