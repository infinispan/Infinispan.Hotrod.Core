using System;
using System.Threading.Tasks;
using BeetleX.Tracks;
using BeetleX.Infinispan;
namespace BeetleX.Infinispan.Samples
{
    class Program
    {
        static async Task Main(string[] args)
        {
            CodeTrackFactory.Level = CodeTrackLevel.Code;
            var host = DefaultInfinispan.Instance.Host.AddHost("127.0.0.1", 11222, true);
            host.User="reader";
            host.Password="password";
            host.AuthMech="PLAIN"; // "DIGEST-MD5";
            System.Threading.Thread.Sleep(1000);
            using (CodeTrackFactory.TrackReport("Test", CodeTrackLevel.Bussiness, null))
            {
                await Test();
            }
            Console.WriteLine(CodeTrackFactory.Activity?.GetReport());
        }

        static async Task Test()
        {
            var cache = DefaultInfinispan.Instance.newCache("default");
            cache.ForceReturnValue = true;
            var km = new StringMarshaller();
            var vm = new StringMarshaller();
            string result = await DefaultInfinispan.Set<string,string>(km, vm, cache, "key1","value1");
            Console.WriteLine("Result is: "+result);
            string getResult = await DefaultInfinispan.Get<string,string>(km, vm, cache, "key1");
            Console.WriteLine("Get Result is: "+getResult);
        }
    }
}
