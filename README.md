# Infinispan Hot Rod .NET Core Demo

This branch provides a demonstration of the Hot Rod .NET Core client with remote caches on an Infinispan Server instance.

For the latest client code, use the [main branch](https://github.com/infinispan/Infinispan.Hotrod.Core/tree/main).

## Querying Infinispan caches with the .NET Core client

This demonstration shows you how to you use the .NET Core client to store and query entries in remote caches on Infinispan Server instance by:

1. Taking a list of more than 9k applications in JSON format (`data/app.json`) and creates a C# Application object for each entry.
2. Storing all the entries in the `market` cache on Infinispan Server and then checking the entries against the `data/app.json` file.
3. Queries the `market` cache to select a list of Application objects, a list of projections, and an aggregate value.

### Requirements

To run this demo you need:

- [.NET Core](https://docs.microsoft.com/it-it/dotnet/core/install)
- Java 11 or later

Alternatively you can run the demo in a container image using [Podman](https://podman.io/) or Docker.

### Running the demo with .NET Core

If you install .NET you can run the demo as follows:

1. Set up Infinispan Server for the demo. This step creates a server instance in the filesystem of this repository. Be sure to stop any other locally running Infinispan Server instances.
```bash
scripts/setup.sh
```
2. Start Infinispan Server and create the demo caches.
```bash
scripts/run-ispn.sh
```
3. Run the .NET Core client demo.
```bash
dotnet run
```

### Running the demo with Podman

If you want to run the demo with Podman or Docker, do the following:

1. Start the container image.
```bash
podman run --network=host --name query_demo -it --mount type=bind,src=$PWD,dst=/home/hostfs quay.io/rigazilla/netcore-demo:1.0 /bin/bash -c "cd home/hostfs && scripts/container-setup.sh"
```
2. Connect to the container image in a new terminal window.
```bash
podman exec -it query_demo /bin/bash
```
3. Run the .NET Core client demo.
```bash
cd home/hostfs && dotnet run
```
4. Stop the container when you are finished with the demo.
```bash
podman stop query_demo
```

### Some highlights

- .NET applications can use plain C# data types. By configuring Infinispan caches with the ProtoStream marshaller the Hot Rod .NET Core client handles all the Protobuf to C# conversion ([link](https://github.com/infinispan/Infinispan.Hotrod.Core/blob/e2efac6591741d23ff92c6253bf1257a60ea8879/demo/Query/Program.cs#L35-L38)).
- The Hot Rod client API is asynchronous by default and concurrency is supported. This demo runs all PUT commands in a single call ([link](https://github.com/infinispan/Infinispan.Hotrod.Core/blob/e2efac6591741d23ff92c6253bf1257a60ea8879/demo/Query/Program.cs#L111-L122)).

### Protobuf and remote cache queries

To query remote caches you need to provide Infinispan Server with Protobuf schema that describe your data model ([link](https://github.com/infinispan/Infinispan.Hotrod.Core/blob/e2efac6591741d23ff92c6253bf1257a60ea8879/demo/Query/Program.cs#L69)).

You also need to implement a marshaller for your data model.
That sounds scary but Protobuf gives you some parsers that really simplify the entire process.
The main thing in the marshaller is the logic that selects the right parser for the type.

- In the Protobuf schema, a unique `TypeId` is assigned to each Protobuf message ([link](https://github.com/infinispan/Infinispan.Hotrod.Core/blob/a648993db9cd97ebff2186a6f3f5ef64b37517da/demo/Query/Protos/app.proto#L5)) and ([link](https://github.com/infinispan/Infinispan.Hotrod.Core/blob/a648993db9cd97ebff2186a6f3f5ef64b37517da/demo/Query/Protos/review.proto#L5)).
- In the marshaller, the `TypeId` identifies which parser to select ([link](https://github.com/infinispan/Infinispan.Hotrod.Core/blob/a648993db9cd97ebff2186a6f3f5ef64b37517da/demo/Query/Marshaller.cs#L87)).

### Conclusion

With the Infinispan Hot Rod .NET Core client it's easy for .NET applications to store and query remote caches while working with C# data types.
