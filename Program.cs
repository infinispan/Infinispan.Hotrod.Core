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

namespace Query
{
    class Program
    {
        public const String ERRORS_KEY_SUFFIX = ".errors";
        public const String PROTOBUF_METADATA_CACHE_NAME = "___protobuf_metadata";
        public static InfinispanHost host;

        static void Main(string[] args)
        {
            // Setup the cluster
            var ispnCluster = new InfinispanDG();
            // Settings for the client
            ispnCluster.Version = 0x1f;
            ispnCluster.ClientIntelligence = 0x03;
            ispnCluster.ForceReturnValue = false;
            // Add the server endpoint address
            host = ispnCluster.AddHost("127.0.0.1", 11222, false);
            // Install the proto definition of the AppEntry objects
            installProto(ispnCluster, "Query.Protos.app.proto");
            installProto(ispnCluster, "Query.Protos.review.proto");
            var mv = new BasicTypesProtoStreamMarshaller();
            var mk = new StringMarshaller();
            // Create a cache proxy for the remote cache named "market"
            var cache = ispnCluster.newCache<string, Object>(mk, mv, "market");
            // Populate the cache
            populateApp(ispnCluster, cache);
            // Verify that all the keys are there
            verifyApp(ispnCluster, cache);

            // Now play with queries
            // Query 1: select object
            List<Object> apps = cache.Query("from AppDB.AppEntry a where a.Rating=\"4.6\"").Result;
            Console.WriteLine("ResultSet size: " + apps.Count);
            Console.WriteLine("ResultSet");
            foreach (var a in apps)
            {
                AppEntry entry = (AppEntry)a;
                Console.WriteLine("Result: " + JsonConvert.SerializeObject(entry));
            }
            // Query 2: select projection
            List<Object> projection = cache.Query("select a.App, a.Installs, a.Price from AppDB.AppEntry a where a.Rating=\"4.6\"").Result;
            Console.WriteLine("ResultSet");
            Console.WriteLine("ResultSet size: " + projection.Count);
            foreach (var item in projection)
            {
                Console.Write("Result: ");
                object[] values = (Object[])item;
                Console.WriteLine(JsonConvert.SerializeObject(values));
            }

            // Query 3: select aggregate value
            List<Object> count = cache.Query("select count(a.App) from AppDB.AppEntry a where a.Rating=\"4.6\"").Result;
            Object[] values1 = (Object[])count[0];
            Console.WriteLine("ResultSet count: " + values1[0]);
        }
        private static void installProto(InfinispanDG ispnCluster, string resourceName)
        {
            // Setup __protobuf_metadata cache
            // <string,string> cache with text/plain media type
            var metaCache = ispnCluster.newCache(new StringMarshaller(), new StringMarshaller(), PROTOBUF_METADATA_CACHE_NAME);
            MediaType kvMediaType = new MediaType();
            kvMediaType.CustomMediaType = Encoding.ASCII.GetBytes("text/plain");
            kvMediaType.InfoType = 2;
            metaCache.KeyMediaType = kvMediaType;
            metaCache.ValueMediaType = kvMediaType;

            // Cleanup previous errors
            metaCache.Remove(ERRORS_KEY_SUFFIX).Wait();

            var assembly = Assembly.GetExecutingAssembly();
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
        }

        private static void populateApp(InfinispanDG ispnCluster, Cache<string, object> cache)
        {
            var assembly = Assembly.GetExecutingAssembly();
            var arr = assembly.GetManifestResourceNames();
            using (Stream stream = assembly.GetManifestResourceStream("Query.data.app.json"))
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
                            // System.Threading.Thread.Sleep(5);
                            reqs.Add(res);
                        }
                    }
                    // wait for all the commands to complete
                    Task.WhenAll(reqs.ToArray());
                }
            }
        }
        private static void verifyApp(InfinispanDG ispnCluster, Cache<string, object> cache)
        {
            var assembly = Assembly.GetExecutingAssembly();
            var arr = assembly.GetManifestResourceNames();
            using (Stream stream = assembly.GetManifestResourceStream("Query.data.app.json"))
            using (StreamReader reader = new StreamReader(stream))
            {
                using (JsonReader jReader = new JsonTextReader(reader))
                {
                    var serializer = new JsonSerializer();
                    serializer.DefaultValueHandling = DefaultValueHandling.Ignore;
                    var mv = new BasicTypesProtoStreamMarshaller();
                    while (jReader.Read())
                    {
                        if (jReader.TokenType == JsonToken.StartObject)
                        {
                            AppEntry a = serializer.Deserialize<AppEntry>(jReader);
                            var c = cache.Get(a.App).Result;
                            var ab = mv.marshall(a);
                            var cb = mv.marshall(c);
                            if (!Google.Protobuf.WireFormat.Equals(a, c))
                            {
                                Console.WriteLine("------------------------------------------");
                                Console.WriteLine(JsonConvert.SerializeObject(a));
                                Console.WriteLine("--");
                                Console.WriteLine(JsonConvert.SerializeObject(c));
                            }
                        }
                    }
                }
            }
        }
    }
}
