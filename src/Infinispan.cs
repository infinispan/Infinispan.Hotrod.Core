using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace BeetleX.Infinispan
{
    public static class InfinispanDGEx
    {
        public static InfinispanDG Instance(this InfinispanDG dg)
        {
            return dg ?? InfinispanDG.Default;
        }
    }


    public class DefaultInfinispan
    {


        public static InfinispanDG Instance
        {
            get
            {
                return InfinispanDG.Default;
            }

        }

        public static async ValueTask<V> Get<K,V>(Marshaller<K> km, Marshaller<V> vm, Cache cache, K key)
        {
            return await Instance.Get<K,V>(km, vm, cache, key);
        }

        public static async ValueTask<V> Set<K,V>(Marshaller<K> km, Marshaller<V> vm, Cache cache, K key, V value)
        {
            return await Instance.Set(km, vm, cache, key, value);
        }

    }
}
