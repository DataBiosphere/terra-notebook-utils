# Changes for v0.5.0 (2020-10-02)
Remove prepare_merge_workflow_input (#169)
Add support for martha_v3 [WA-320][WA-348] (#117)
Update tests for exception raised. (#114)
Swap subprocess Popen for subprocess run. (#116)
Use absolute paths for tnu head testing. (#115)
Fix drs batch copy test (#112)
Add batch drs copy instructions to readme (#111)
Disallow trailing / in drs batch copy bucket dest (#110)
Make tnu drs head compatible with python notebooks. (#109)
Add a tnu drs head function.
Prevent test cruft from littering repo dir (#105)
Improve gs url parser with better output to user (#103)
Fix bump gs-chunked-io error in tarball extraction (#102)
Bump and fix usage for google-crc32c (#104)

# Changes for v0.3.0 (2020-07-06)
Avoid enabling requester pays before copies (#87)
Add info and credentials drs commands (#86)
Use DRS file name when copying to local dir (#85)
Track DRS info with namedtuple (#84)
Propagate workspace/namespace args to drs methods (#82)
Consistently order workspace and namespace args (#81)
Split drs info resolution into separate method (#77)
Improve multipart copy (#76)

# Changes for v0.2.1 (2020-06-22)
CLI usage and expressive errors (#75)
Improve test concurrency (#74)
CLI config correctly falls back to defaults (#73)
Config override for tests (#72)

# Changes for v0.2.0 (2020-06-16)
[IA-1919] Adding requester pays and ENV support. Documentation added as well (#69)

## 0.2.1 - 06/12/2020
- Added requester pays set-up to drs module
- Added ENV variable for local testing to __init__
- Added some print messages to drs module to let user know what is happening