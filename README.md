# terra-notebook-utils
Utilities for the [Terra notebook environment](https://support.terra.bio/hc/en-us/articles/360027237871-Terra-s-Jupyter-Notebooks-environment-Part-I-Key-components).

- Fetch a DRS url from a Terra data table:

  `drs_url = table.fetch_drs_url("data table name", "file name")`

- Download a DRS object to your workspace VM file system:

  `drs.download(drs_url, "file name")`

- Copy a DRS object to your workspace bucket:

  `drs.copy(drs_url, "my_key", bucket=[bucket name])`

## Credentials
When running in a Terra notebook environment, no user action is required to obtain credentials. For applications outside of the Terra notebook
environment, users should obtain "application default credentials" using the command `gcloud auth application-default login`.

### Testing
The account used to run the test suite should be authorized to access [TopMED data](https://www.nhlbiwgs.org/) through [Gen3](https://gen3.datastage.io/).
Credentials can be made available by logging in with the command `gcloud auth application-default login`.

## Tests
To run tests,
1. log in with your Google credentials using `gcloud auth application-default login`,
1. install requirements with `pip install -r requirements-dev.txt`,
1. run `make test` in the package root.
