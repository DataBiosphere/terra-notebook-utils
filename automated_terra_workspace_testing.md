# Automated Terra Workspace Testing

Automated testing of workspace features requires service account access from a virtual machine.
This machine should be co-located in the same data warehouse (google compute region) as the Terra workspace compute environment.

Note that the firecloud API cannot be accessed from a VM using personal GCP account credentials. Why is this?

## Configuring a GCP service account with access privileges to a Terra Workspace
  1. Create a GCP service account
  1. Register the account with firecloud using [the firecloud service account registration script](https://github.com/broadinstitute/firecloud-tools/blob/master/scripts/register_service_account/register_service_account.py), which can be executed in Docker with the command shown below:

       `docker run --rm -it -v "$HOME"/.config:/.config -v [local service account credentials filepath]:/svc.json broadinstitute/firecloud-tools python /scripts/register_service_account/register_service_account.py -j /svc.json -e [your organizationl email address]`

     See [this thread](https://gatkforums.broadinstitute.org/firecloud/discussion/12981/running-the-firecloud-api-from-a-google-compute-instance) for discussion on firecloud service account access.
  1. Give your service account read/write access to the Terra workspace

## Configuring a GCP service account with access to BDCatalyst data

Before automated testing can begin, the service account will need to be authorized to access BDCatalyst data through Gen3. This is WIP.
