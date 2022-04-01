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
    class ClientListener : AbstractClientListener
    {
        public override string ListenerID { get => "listenerDemo"; set => throw new NotImplementedException(); }

        public override void OnError(Exception ex = null)
        {
            System.Console.WriteLine("Listener received an error event. Stacktrace follows\n{0}", ex?.StackTrace.ToString());
        }
        private int created;
        private int withVideoInKey;
        private StringMarshaller mk;

        public ClientListener(StringMarshaller mk)
        {
            this.mk = mk;
        }

        public override void OnEvent(Event e)
        {
            switch (e.Type)
            {
                case EventType.CREATED:
                    string strKey;
                    lock (this)
                    {
                        Console.WriteLine("Created {0}", ++created);
                        strKey = mk.unmarshall(e.Key);
                    }
                    if (strKey.Contains("Video"))
                    {
                        Console.WriteLine("Created key with Video {0}: {1}", ++withVideoInKey, strKey);
                    }
                    break;
            }
        }
    }
    class Program
    {
        public static InfinispanHost host;
        private static ClientListener listener;

        static void Main(string[] args)
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
            //installProto(ispnCluster, "Query.Protos.app.proto");
            //installProto(ispnCluster, "Query.Protos.review.proto");
            var mv = new BasicTypesProtoStreamMarshaller();
            var mk = new StringMarshaller();
            listener = new ClientListener(mk);
            // Create a cache proxy for the remote cache named "market"
            var cache = ispnCluster.NewCache<string, Object>(mk, mv, "market");
            cache.AddListener(listener, false).Wait();
            System.Threading.Thread.Sleep(20000);
            Console.WriteLine("Size of the cache is {0}", cache.Size().Result);
            listener.Wait();
        }
    }
}
