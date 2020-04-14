# Automated Terra Workspace Testing

Service accounts currently cannot be granted access to controlled access data, so automated testing against controlled
access data cannot be configured using service accounts. Instead, tests must be executed using personal developer
credentials.

It is possible to grant a service account access to workspace data and buckets, as described in the section below,
which would allow automated testing against services that do not involve accessing controlled access data.

## Configuring a GCP service account with access privileges to a Terra Workspace
  1. Create a GCP service account
  1. Register the account with firecloud using [the firecloud service account registration script](https://github.com/broadinstitute/firecloud-tools/blob/master/scripts/register_service_account/register_service_account.py), which can be executed in Docker with the command shown below:

       `docker run --rm -it -v "$HOME"/.config:/.config -v [local service account credentials filepath]:/svc.json broadinstitute/firecloud-tools python /scripts/register_service_account/register_service_account.py -j /svc.json -e [your organizationl email address]`

     See [this thread](https://gatkforums.broadinstitute.org/firecloud/discussion/12981/running-the-firecloud-api-from-a-google-compute-instance) for discussion on firecloud service account access.
  1. Give your service account read/write access to the Terra workspace
