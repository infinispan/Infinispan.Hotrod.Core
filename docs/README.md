# Infinispan.Hotrod.Core

This package allows the user application to perform operation on a remote Infinispan cluster via the Hot Rod protocol.

The main use case is the following:

1. import this package in the project description
\include Infinispan.Hotrod.Application.csproj
2. Create a cluster
\snippet /Program.cs Create a cluster object
3. Create a cache
\snippet /Program.cs Create a cache
4. Write application code
\snippet /Program.cs Application code
