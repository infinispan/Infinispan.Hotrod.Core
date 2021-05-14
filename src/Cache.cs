using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace Infinispan.Hotrod.Core
{
    public class Cache {
        public Cache(InfinispanDG ispnCluster, string name) {
            Cluster = ispnCluster;
            Name = name;
            MessageId=1;
            NameAsBytes = Encoding.ASCII.GetBytes(Name);
            if (Cluster!=null) {
                Version = Cluster.Version;
                ClientIntelligence = Cluster.ClientIntelligence;
                TopologyId = Cluster.TopologyId;
                ForceReturnValue = Cluster.ForceReturnValue;
            }
            codec = Codec30.getCodec(Version);
        }

        public static Cache NullCache = new Cache(null, "");
        private InfinispanDG Cluster;
        public string Name {get;}
        public byte[] NameAsBytes {get;}
        public byte Version {get;}
        public UInt64 MessageId {get;}
        public byte ClientIntelligence {get;}
        public UInt32 TopologyId {get; set;}
         public bool ForceReturnValue;
       public MediaType KeyMediaType {get; set;}
        public MediaType ValueMediaType {get; set;}
        public Codec30 codec;

        public async ValueTask<V> Get<K,V>(Marshaller<K> km, Marshaller<V> vm, K key)
        {
            return await Cluster.Get<K,V>(km, vm, this, key);
        }

        public async ValueTask<V> Set<K,V>(Marshaller<K> km, Marshaller<V> vm, K key, V value)
        {
            return await Cluster.Set(km, vm, this, key, value);
        }


    }
}