using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace Infinispan.Hotrod.Core
{

    public class UntypedCache {
        public static UntypedCache NullCache = new UntypedCache(null, "");
        protected InfinispanDG Cluster;
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
        public UntypedCache(InfinispanDG ispnCluster, string name) {
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

    }
    public class Cache<K,V> : UntypedCache {
        public Cache(InfinispanDG ispnCluster, Marshaller<K> keyM, Marshaller<V> valM, string name) : base(ispnCluster, name) {
            KeyMarshaller = keyM;
            ValueMarshaller = valM;
        }
        Marshaller<K> KeyMarshaller;
        Marshaller<V> ValueMarshaller;

        public async ValueTask<V> Get(K key)
        {
            return await Cluster.Get(KeyMarshaller, ValueMarshaller, (UntypedCache)this, key);
        }

        public async ValueTask<V> Put(K key, V value)
        {
            return await Cluster.Put(KeyMarshaller, ValueMarshaller, this, key, value);
        }
        public async ValueTask<UInt32> Size()
        {
            return await Cluster.Size(this);
        }
        public async ValueTask<Boolean> ContainsKey(K key)
        {
            return await Cluster.ContainsKey(KeyMarshaller, (UntypedCache)this, key);
        }
        public async ValueTask<V> Remove(K key)
        {
            return await Cluster.Remove(KeyMarshaller,  ValueMarshaller, (UntypedCache)this, key);
        }
        public async ValueTask Clear()
        {
            await Cluster.Clear(this);
        }
        public async ValueTask<Boolean> IsEmpty()
        {
            return await Cluster.Size(this)==0;
        }

    }
}