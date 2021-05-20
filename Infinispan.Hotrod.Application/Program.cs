using System;
using System.Threading.Tasks;
using Infinispan.Hotrod.Core;
namespace Infinispan.Hotrod.Application
{

    class Program
    {
        static string checkValue = "";
        static async Task Main(string[] args)
        {
            var myGreetings = "Hello World!";
            Console.WriteLine(myGreetings);
            InfinispanDG dg = DefaultInfinispan.Instance;
            // Use a non-authenticated non-encrypted cluster;
            dg.AddHost("127.0.0.1",11222, false);
            var cache = dg.newCache<string,string>(new StringMarshaller(), new StringMarshaller(), "default");
            await cache.Put("greetings",myGreetings);
            string result = await cache.Get("greetings");
            Console.WriteLine("Greetings from cache: "+result);
        }
    }
}
