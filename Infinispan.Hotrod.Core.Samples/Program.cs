using System;
using System.Threading.Tasks;
using BeetleX.Tracks;
using Infinispan.Hotrod.Core;
namespace Infinispan.Hotrod.Core.Samples
{
    class Program
    {
        static async Task Main(string[] args)
        {
            CodeTrackFactory.Level = CodeTrackLevel.Code;
            var ispnCluster = DefaultInfinispan.Instance;

            // Configuration section
            ispnCluster.User="reader";
            ispnCluster.Password="password";
            ispnCluster.AuthMech="PLAIN"; // "DIGEST-MD5";
            ispnCluster.Version = 0x30;
            ispnCluster.ClientIntelligence = 0x01;
            ispnCluster.ForceReturnValue = false;

            var host = ispnCluster.AddHost("127.0.0.1", 11222, true);
            System.Threading.Thread.Sleep(1000);
            using (CodeTrackFactory.TrackReport("Test", CodeTrackLevel.Bussiness, null))
            {
                await Test();
            }
            Console.WriteLine(CodeTrackFactory.Activity?.GetReport());
        }
        static async Task Test()
        {
            var km = new StringMarshaller();
            var vm = new StringMarshaller();
            var cache = DefaultInfinispan.Instance.newCache(km, vm, "default");
            cache.ForceReturnValue = true;
            string result = await cache.Set<string,string>(km, vm, "key1", "value1");
            Console.WriteLine("Result is: "+result);
            string getResult = await cache.Get<string,string>(km, vm, "key1");
            Console.WriteLine("Get Result is: "+getResult);
        }
    }
}
