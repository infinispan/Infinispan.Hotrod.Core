# Infinispan Hot Rod .NET Core Demo

This branch provides a demonstration of the Hot Rod .NET Core client with remote caches on an Infinispan Server instance.
### Goal
This demo aims to show you the current maturity level of the Hot Rod .NET Core client as well as how to:

- Include ithe client in your application.
- Store and query entries in a remote cache.
- Handle Protobuf serialization of C# data types.

For the latest client code, use the [main branch](https://github.com/infinispan/Infinispan.Hotrod.Core/tree/main).

## Querying Infinispan caches with the .NET Core client

The demo shows a typical use case with queries by:
1. Taking a list of more than 9k applications in JSON format (`data/app.json`) and creates a C# Application object for each entry.
2. Storing all the entries in the `market` cache on Infinispan Server and then checking the entries against the `data/app.json` file.
3. Queries the `market` cache to select a list of Application objects, a list of projections, and an aggregate value.

### Requirements

To run this demo you need:

- [.NET](https://docs.microsoft.com/dotnet/core/install/)
- Java 11 or later

Alternatively you can run the demo in a container image using [Podman](https://podman.io/) or Docker.

### Running the demo with .NET

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

- Concurrency: The Hot Rod client API is asynchronous by default and supports concurrency. To show these capabilites the demo runs all PUT commands asynchronously ([link](https://github.com/infinispan/Infinispan.Hotrod.Core/blob/e2efac6591741d23ff92c6253bf1257a60ea8879/demo/Query/Program.cs#L111-L122)).

- Simplicity: The client is shipped as a `.nuget` package that you can easily import ([link](https://github.com/infinispan/Infinispan.Hotrod.Core/blob/234362df176512f23d0eaef171a26b6f5ccf9489/Query.csproj#L7)).
- Protobuf support: Using the default Protobuf/GRPC tools ([link](https://github.com/infinispan/Infinispan.Hotrod.Core/blob/234362df176512f23d0eaef171a26b6f5ccf9489/Query.csproj#L8-L9)), the C# data types hierarchy can be generated and used in the application.


### Infinispan Query

#### Server setup
To run queries the Infinispan server needs a `.proto` description of the queriable data types. This demo takes care of uploading the `.proto` file onto the server. ([link](https://github.com/infinispan/Infinispan.Hotrod.Core/blob/e2efac6591741d23ff92c6253bf1257a60ea8879/demo/Query/Program.cs#L69))

#### Client Marshaller
Infinispan uses the `application/x-protostream` media type to serialize data for remote queries. [ProtoStream](https://github.com/infinispan/protostream) is a Protobuf-based protocol that allows multiple object types to be serialized in the same data stream. Simplifying a little bit, the client and server share a map that assigns an integer number to each query-able object type; this is usually done defining the `TypeId` in the `.proto` schema file. (see [link](https://github.com/infinispan/Infinispan.Hotrod.Core/blob/a648993db9cd97ebff2186a6f3f5ef64b37517da/demo/Query/Protos/app.proto#L5) and [link](https://github.com/infinispan/Infinispan.Hotrod.Core/blob/a648993db9cd97ebff2186a6f3f5ef64b37517da/demo/Query/Protos/review.proto#L5)). This `TypeId` is sent in front of each Protobuf message transmitted across the wire.

On the client side the application must provide a Protostream marshaller for its own data types. That could sound scary, but Protobuf can do most of the hard work for this.
A protostream essentially must:
- set the correct `typeId` in the protostream message when marshalling data to the server ([see](https://github.com/infinispan/Infinispan.Hotrod.Core/blob/234362df176512f23d0eaef171a26b6f5ccf9489/Marshaller.cs#L17-L24));
- use the correct Protobuf parser when unmarshalling from the server ([see](https://github.com/infinispan/Infinispan.Hotrod.Core/blob/234362df176512f23d0eaef171a26b6f5ccf9489/Marshaller.cs#L31-L37)).
Once the marshaller has been implemented, the plain C# classes generated by the Protobuf framework can be used in the application development.

Maybe it worth to highlight that, using protostream, the data format is independent from the specific programming language; this means that data store in the cache can be accessed by C#, Java, C++ clients. Actually the demo shows a round trip interoperability between different data representations:
- [json->C#](https://github.com/infinispan/Infinispan.Hotrod.Core/blob/234362df176512f23d0eaef171a26b6f5ccf9489/Program.cs#L117) marshalling;
- [C#->protostream](https://github.com/infinispan/Infinispan.Hotrod.Core/blob/234362df176512f23d0eaef171a26b6f5ccf9489/Program.cs#L118) marshalling;
- [protostream->C#](https://github.com/infinispan/Infinispan.Hotrod.Core/blob/234362df176512f23d0eaef171a26b6f5ccf9489/Program.cs#L46-L51) unmarshalling;
- [C#->json](https://github.com/infinispan/Infinispan.Hotrod.Core/blob/234362df176512f23d0eaef171a26b6f5ccf9489/Program.cs#L52) marshalling.

### Conclusion

With the Infinispan Hot Rod .NET Core client it's easy for .NET applications to store and query remote caches, in an language independent format, while working with native C# data types.
