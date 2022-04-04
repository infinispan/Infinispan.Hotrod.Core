using System;
using Infinispan.Hotrod.Core;
using System.Text;
using System.IO;
using System.Reflection;
using AppDB;
using Newtonsoft.Json;
using Org.Infinispan.Query.Remote.Client;
using System.Collections.Generic;
using Org.Infinispan.Protostream;
using Google.Protobuf;
using System.Threading.Tasks;

namespace Events
{
    class FeederClient
    {
        public const String ERRORS_KEY_SUFFIX = ".errors";
        public const String PROTOBUF_METADATA_CACHE_NAME = "___protobuf_metadata";
        public static InfinispanHost host;

        public static void Main(string[] args)
        {
            // Setup the cluster
            var ispnCluster = new InfinispanDG();
            // Settings for the client
            ispnCluster.Version = 0x1f;
            ispnCluster.ClientIntelligence = 0x03;
            ispnCluster.ForceReturnValue = false;
            // Add the server endpoint address
            host = ispnCluster.AddHost("127.0.0.1", 11222);
            // Install the proto definition of the AppEntry objects
            installProto(ispnCluster, "Common.Protos.app.proto");
            installProto(ispnCluster, "Common.Protos.review.proto");
            var mv = new BasicTypesProtoStreamMarshaller();
            var mk = new StringMarshaller();
            // Create a cache proxy for the remote cache named "market"
            var cache = ispnCluster.NewCache<string, Object>(mk, mv, "market");
            // Populate the cache
            populateApp(ispnCluster, cache);
            // Verify that all the keys are there
            System.Threading.Thread.Sleep(10000);
            // Now play with queries
            // Query 1: select projection
            Console.Write("Running query: select a.App, a.Installs, a.Price from AppDB.AppEntry a where a.App like '%Video%'... ");
            List<Object> projection = cache.Query("select a.App, a.Installs, a.Price from AppDB.AppEntry a where a.App like '%Video%'").Result;
            Console.WriteLine(" done.");
            foreach (var item in projection)
            {
                Console.Write("Result: ");
                object[] values = (Object[])item;
                Console.WriteLine(JsonConvert.SerializeObject(values));
            }
            Console.WriteLine("Above result for query: select a.App, a.Installs, a.Price from AppDB.AppEntry a where a.App like '%Video%'");
            Console.WriteLine("ResultSet size: " + projection.Count);

            // Query 2: select aggregate value
            Console.Write("Running query: select count(a.App) from AppDB.AppEntry a where a.App like '%Video%'... ");
            List<Object> count = cache.Query("select count(a.App) from AppDB.AppEntry a where a.App like '%Video%'").Result;
            Console.WriteLine(" done.");
            Object[] values1 = (Object[])count[0];
            Console.WriteLine("count(*) result: " + values1[0]);
            Console.WriteLine("Cache size: {0}", cache.Size().Result);
        }
        private static void installProto(InfinispanDG ispnCluster, string resourceName)
        {
            // Setup __protobuf_metadata cache
            // <string,string> cache with text/plain media type
            Console.Write("Installing proto files... ");
            var metaCache = ispnCluster.NewCache(new StringMarshaller(), new StringMarshaller(), PROTOBUF_METADATA_CACHE_NAME);
            MediaType kvMediaType = new MediaType();
            kvMediaType.CustomMediaType = Encoding.ASCII.GetBytes("text/plain");
            kvMediaType.InfoType = 2;
            metaCache.KeyMediaType = kvMediaType;
            metaCache.ValueMediaType = kvMediaType;

            // Cleanup previous errors
            metaCache.Remove(ERRORS_KEY_SUFFIX).Wait();

            var assembly = typeof(BasicTypesProtoStreamMarshaller).Assembly;
            var arr = assembly.GetManifestResourceNames();
            string protoDef;
            using (Stream stream = assembly.GetManifestResourceStream(resourceName))
            using (StreamReader reader = new StreamReader(stream))
            {
                protoDef = reader.ReadToEnd();
            }

            metaCache.Put(resourceName, protoDef).Wait();
            if (metaCache.ContainsKey(ERRORS_KEY_SUFFIX).Result)
            {
                throw new Exception("Error");
            }
            Console.WriteLine(" done.");
        }

        private static void populateApp(InfinispanDG ispnCluster, Cache<string, object> cache)
        {
            Console.Write("Populating cache... ");
            var assembly = typeof(BasicTypesProtoStreamMarshaller).Assembly;
            var arr = assembly.GetManifestResourceNames();
            using (Stream stream = assembly.GetManifestResourceStream("Common.data.app.json"))
            using (StreamReader reader = new StreamReader(stream))
            {
                using (JsonReader jReader = new JsonTextReader(reader))
                {
                    var serializer = new JsonSerializer();
                    serializer.DefaultValueHandling = DefaultValueHandling.Ignore;
                    List<Task<object>> reqs = new List<Task<object>>();
                    // Just put all the data concurrently
                    while (jReader.Read())
                    {
                        if (jReader.TokenType == JsonToken.StartObject)
                        {
                            AppEntry a = serializer.Deserialize<AppEntry>(jReader);
                            var res = cache.Put(a.App, a);
                            // Uncomment this to slowdown the storm
                            // System.Threading.Thread.Sleep(1);
                            reqs.Add(res);
                        }
                    }
                    // wait for all the commands to complete
                    try
                    {
                        Task.WaitAll(reqs.ToArray());
                    }
                    catch (AggregateException ex)
                    {
                        // Handle errors
                        // In this demo, just count operations lost due to pool overflow.
                        int errNum = 0, errPoolFull = 0;
                        Console.Write(" Got exceptions ");
                        foreach (var iex in ex.InnerExceptions)
                        {
                            var ispnEx = iex as InfinispanException;
                            if (ispnEx.Result.IsError)
                            {
                                ++errNum;
                                if (ispnEx.Result.Results.Count > 0 && ispnEx.Result.Results[0].Messge.Contains("exceeding"))
                                    ++errPoolFull;
                            }
                        }
                        if (errNum != 0)
                        {
                            Console.Write(" {0} operations failed. {0} for conn pool full. ", errNum, errPoolFull);
                        }
                    }
                }
            }
            Console.WriteLine(" done.");
        }
    }
}
