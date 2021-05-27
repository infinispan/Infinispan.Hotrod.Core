# Infinispan.Hotrod.Core
This is a 100% C# .NET Core Hotrod client for Infinispan data grid.

# Try It
A sample Application is in the [Infinispan.Hotrod.Application](Infinispan.Hotrod.Application) folder, it refers the latest nuget package published.

# Status
This is a work in progress.

# Features
All the basic Hotrod request/response operations are supported over [Hotrod 3.0](https://infinispan.org/docs/stable/titles/hotrod_protocol/hotrod_protocol.html#hot_rod_protocol_3_0):
- GET, GETWITHVERSION, GETWITHMETADATA
- PUT
- REPLACE, REPLACEWITHVERSION
- REMOVE, REMOVEWITHVERSION
- CONTAINSKEY
- CLEAR
- SIZE
- STATS

For an updated list, all the implemented commands can be found in the [InfinispanCommands](src/InfinispanCommands) folder.
## Security
TLS is working (certificats are not verified), authentication ca be performed via SASL (PLAIN and DIGEST-MD5).

# Plans
The master plan is this:
- code consolidation
- queries
- PING command
- client intelligence
- events
- ...
things can be added or moved up/down basing on community requests.

# Interact
Please open an issue if you're interested in prioritize a feature (if you are _really_ interested consider to do it yourself and provide a pull request, yeah that would be great!)

# Credits
This project took some inspiration from the [set of extensions](https://github.com/beetlex-io/BeetleX#extended-components) of the [BeetleX](https://github.com/beetlex-io/BeetleX) component on which it's also based.
