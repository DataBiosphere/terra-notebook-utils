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