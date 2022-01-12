# Infinispan.Hotrod.Core - Demo Room

This is a demo for the Infinispan .NET Core Hotrod client.
For more info related to the client itself please go to the [main branch](https://github.com/infinispan/Infinispan.Hotrod.Core).

## How to run Infinispan Queries with .NET Core
### Prerequisites
- .Net
- Java 11

or
- docker/podman

### On PC
A running Infinispan server is needed, you can:
- run `scripts/setup.sh` to download and setup the server;
- run (in a different windows) `scripts/run-ispn.sh`.

### In container

- `cd demo-container`
- `podman run -it --mount type=bind,src=$PWD/container-data,dst=/home/host-data quay.io/rigazilla/netcore-demo:1.0 /bin/bash -c "cd home/host-data && ./setup-run.sh"`

Alternatively you can build the image using the provided `Dockerfile`

### The demo
#### What does
This demo does the following:
- read the `data/app.json` file containing a list of apps (more than 9k) in json;
- for all the json entry:
  - converts json->C# creating an Application object
  - and sends it to the Infinispan cache called `market`;
- checks the content of the `market` cache against the initial json file (useless but... it's a demo);
- queries Infinispan to select a list of Application object;
- queries to select a list of projections;
- queries to select an aggregate value.
#### How to run it
It should be easy as `dotnet run`
#### Some lovely facts
- The user application can work with plain C# data type. Once the cache is configured with a suitable marshaller, protobuf details are handled by the client. ([link](https://github.com/infinispan/Infinispan.Hotrod.Core/blob/e2efac6591741d23ff92c6253bf1257a60ea8879/demo/Query/Program.cs#L35-L38))
- client API is asynchronous by default and concurrency is well supported. This demo runs all the PUT commands in one shot. ([link](https://github.com/infinispan/Infinispan.Hotrod.Core/blob/e2efac6591741d23ff92c6253bf1257a60ea8879/demo/Query/Program.cs#L111-L122))
### The tutorial
Here some more info on how to work with protobuf/queries.
#### Server setup
All the protobuf schemas need to be stored on the server in the `___protobuf_metadata` system cache. This is how the user makes Infinispan aware of his data model. ([link](https://github.com/infinispan/Infinispan.Hotrod.Core/blob/e2efac6591741d23ff92c6253bf1257a60ea8879/demo/Query/Program.cs#L69))
#### Client setup
A marshaller for the whole data model must be implemented. This sounds scary, but actually it isn't cause most of the work can be easily delegated to the parsers provied by protobuf. What is need in the marshaller is the logic that pick up the right parser in the right time.
In practice:
- in the protobuf schema a unique TypeId is assigned to each protobuf message; ([link](https://github.com/infinispan/Infinispan.Hotrod.Core/blob/a648993db9cd97ebff2186a6f3f5ef64b37517da/demo/Query/Protos/app.proto#L5)) and ([link](https://github.com/infinispan/Infinispan.Hotrod.Core/blob/a648993db9cd97ebff2186a6f3f5ef64b37517da/demo/Query/Protos/review.proto#L5))
- in the marshaller the TypeId is used to identify which parser needs to be selected. ([link](https://github.com/infinispan/Infinispan.Hotrod.Core/blob/a648993db9cd97ebff2186a6f3f5ef64b37517da/demo/Query/Marshaller.cs#L87))

### Conclusion
With the Infinispan.Hotrod.Core it's easy for a C# .NET application to query the Infinispan data grid.
