# Infinispan Hot Rod .NET Core Demo (Events Listener)

This an Hot Rod .NET Core demo application that shows how to register an event listener with Infinispan work with remote events.
### Goal
This demo aims to show you the current maturity level of the Hot Rod .NET Core client as well as how to:

- Include the client in your application.
- Register listeners and process incoming events from the server.
- Handle Protobuf serialization of C# data types.

For the latest client code, use the [main branch](https://github.com/infinispan/Infinispan.Hotrod.Core/tree/main).

## Listen to remote cache events with the .NET Core client

The demo shows a typical use case with listeners by:
1. Running a client app that registers a listeners and count creation and removal events.
2. Running the [Query demo](https://github.com/infinispan/Infinispan.Hotrod.Core/tree/query-demo) as a feeder client.

### Requirements

To run this demo you need:

- [.NET](https://docs.microsoft.com/dotnet/core/install/)
- Java 11 or later

Alternatively you can run the demo in a container image using [Podman](https://podman.io/) or Docker.

### The demo

A feeder application populates the cache, a listener is registered on the server to get Infinispan events.
Both the applications count how many entries have been created and how many of them have the string "Video" in the key. Feeder get these numbers via Size operation and query, while listener counts the CREATED events to do the job. Numbers should match at the end.

### Running the demo with .NET

If you install .NET you can run the demo as follows:

1. Be sure to have this branch checked out:
```bash
git clone git@github.com:infinispan/Infinispan.Hotrod.Core.git -b listener-demo
```
```bash
cd Infinispan.Hotrod.Core
```
2. Set up Infinispan Server for the demo. This step creates a server instance in the filesystem of this repository. Be sure to stop any other locally running Infinispan Server instances.
```bash
scripts/setup.sh
```
3. Start Infinispan Server and create the demo caches.
```bash
scripts/run-ispn.sh
```
4. Build and Run the listener client (open new terminal)
```bash
dotnet build Listener/Listener.csproj
dotnet run -p Listener/Listener.csproj
```
5. Build and Run the feeder client (open new terminal)
```bash
cd feeder
dotnet build Feeder/Feeder.csproj
dotnet run -p Feeder/Feeder.csproj
```
## What happens

In the Listener terminal the number of created and removed events received by the listener is
printed on the screen each second. Plus all the keys containing the "Video" substring are counted and printed.

In the Feeder terminal the number of the entries put into the cache and the number of operations failed due to overflow are printed (see below why overflow happens).
When all the PUT operations complete the execution, the cache is queried to get all the entries containing the "Video" substring in their keys. The result set, its size and the size of the whole cache is printed as output. The Feeder then terminates.

In the Listener terminal is possible to:
- press `Enter` to clear the cache, after that the REMOVED event counter should match the CREATE).
The Listener terminates when:
- a `Ctrl-C` is pressed in the terminal or
- the Infinispan server is switched off. In this case the listener task completes and the main program terminates.

### Highlights

  - Listener/Listener.cs contains the sample code to create a listener for CREATE, REMOVE and ERROR events, MODIFIED and EXPIRED can be added the same way. The CREATE action also shows how to use event data to write application code.
  - Feeder/Feeder.cs sends all the data (around 9k entries) in parallel to show how the client performs under heavy load. The burst of requests is handled this way:
    - requests are sent to the server up to the max number of connections per host, then
    - requests queue by the request pool up to the max size of the queue pool, then
    - for the remaining request and exception is returned.
    - final count should be about 8223 success and 1379 exceptions returned to the user.

### Conclusion

The Infinispan Hot Rod .NET Core client now supports event listeners, this demo has shown a simple application that process remote events.
