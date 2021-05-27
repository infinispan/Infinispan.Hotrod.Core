# Infinispan.Hotrod.Core
This is a 100% C# .NET Core Hotrod client for Infinispan data grid.

# Try It
A sample Application is in the [Infinispan.Hotrod.Application](Infinispan.Hotrod.Application) folder, it uses the latest [nuget package](https://www.nuget.org/packages/Infinispan.Hotrod.Core) published.

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
TLS is working (certificates are not verified), authentication is available via SASL (PLAIN and DIGEST-MD5).

## Hotrod dotnet-client comparison
The testsuite folder [Infinispan.Hotrod.Core.XUnitTest](Infinispan.Hotrod.Core.XUnitTest) is structured like the testsuite of the [official .NET client](https://github.com/infinispan/dotnet-client) so to make make it easier to compare the two products.

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
