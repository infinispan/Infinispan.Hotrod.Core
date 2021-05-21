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
        public bool UseCacheDefaultLifespan;
        public bool UseCacheDefaultMaxIdle;

        public UInt32 Flags {get {return getFlags();}}

        private uint getFlags()
        {
            uint retVal=0;
            if (ForceReturnValue) retVal+=1;
            if (UseCacheDefaultLifespan) retVal+=2;
            if (UseCacheDefaultMaxIdle) retVal+=4;
            return retVal;
        }

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
            codec = Codec.getCodec(Version);
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
        public async ValueTask<ValueWithVersion<V>> GetWithVersion(K key)
        {
            return await Cluster.GetWithVersion(KeyMarshaller, ValueMarshaller, (UntypedCache)this, key);
        }
        public async ValueTask<ValueWithMetadata<V>> GetWithMetadata(K key)
        {
            return await Cluster.GetWithMetadata(KeyMarshaller, ValueMarshaller, (UntypedCache)this, key);
        }

        public async ValueTask<V> Put(K key, V value, ExpirationTime lifespan =null, ExpirationTime maxidle=null)
        {
            return await Cluster.Put(KeyMarshaller, ValueMarshaller, this, key, value, lifespan, maxidle);
        }
        public async ValueTask<UInt32> Size()
        {
            return await Cluster.Size(this);
        }
        public async ValueTask<Boolean> ContainsKey(K key)
        {
            return await Cluster.ContainsKey(KeyMarshaller, (UntypedCache)this, key);
        }
        public async ValueTask<(V PrevValue, Boolean Removed)> Remove(K key)
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
        public async ValueTask<ServerStatistics> Stats()
        {
            return await Cluster.Stats(this);
        }
        public async ValueTask<(V PrevValue, Boolean Replaced)> Replace(K key, V value, ExpirationTime lifespan =null, ExpirationTime maxidle=null)
        {
            return await Cluster.Replace(KeyMarshaller, ValueMarshaller, this, key, value, lifespan, maxidle);
        }
        public async ValueTask<Boolean> ReplaceWithVersion(K key, V value, UInt64 version, ExpirationTime lifeSpan = null, ExpirationTime maxIdle = null)
        {
            return await Cluster.ReplaceWithVersion(KeyMarshaller, ValueMarshaller, (UntypedCache)this, key, value, version, lifeSpan, maxIdle);
        }

        public async ValueTask<(V V, Boolean Removed)> RemoveWithVersion(K key, UInt64 version)
        {
            return await Cluster.RemoveWithVersion(KeyMarshaller, ValueMarshaller, (UntypedCache)this, key, version);
        }


    }
    public class ValueWithVersion<V> {
        public V Value;
        public UInt64 Version;
    }

    public class ValueWithMetadata<V> : ValueWithVersion<V> {
        public Int64 Created = -1;
        public Int32 Lifespan = -1;
        public Int64 LastUsed = -1;
        public Int32 MaxIdle = -1;
    }
    public class VersionedResponse<V> {
    }

    public class ServerStatistics
    {
        public ServerStatistics(Dictionary<string,string> stats) {
            this.stats = stats;
        }
        /// <summary>
        ///   Number of seconds since Hot Rod started.
        /// </summary>
        public const String TIME_SINCE_START = "timeSinceStart";

        /// <summary>
        ///   Number of entries currently in the Hot Rod server.
        /// </summary>
        public const String CURRENT_NR_OF_ENTRIES = "currentNumberOfEntries";

        /// <summary>
        ///   Number of entries stored in Hot Rod server since the server started running.
        /// </summary>
        public const String TOTAL_NR_OF_ENTRIES = "totalNumberOfEntries";

        /// <summary>
        ///   Number of put operations.
        /// </summary>
        public const String STORES = "stores";

        /// <summary>
        ///   Number of get operations.
        /// </summary>
        public const String RETRIEVALS = "retrievals";

        /// <summary>
        ///   Number of get hits.
        /// </summary>
        public const String HITS = "hits";

        /// <summary>
        ///   Number of get misses.
        /// </summary>
        public const String MISSES = "misses";

        /// <summary>
        ///   Number of removal hits.
        /// </summary>
        public const String REMOVE_HITS = "removeHits";

        /// <summary>
        ///   Number of removal misses.
        /// </summary>
        public const String REMOVE_MISSES = "removeMisses";

        /// <summary>
        ///   Retrieve the complete list of statistics and their associated value.
        /// </summary>
        public IDictionary<String, String> GetStatsMap()
        {
            return stats;
        }

        /// <summary>
        ///   Retrive the value of the specified statistic.
        /// </summary>
        ///
        /// <param name="statName">name of the statistic to retrieve</param>
        ///
        /// <returns>the value for the specified statistic as a string or null</returns>
        public String GetStatistic(String statsName)
        {
            return stats != null ? stats[statsName] : null;
        }

        /// <summary>
        ///   Retrive the value of the specified statistic.
        /// </summary>
        ///
        /// <param name="statName">name of the statistic to retrieve</param>
        ///
        /// <returns>the value for the specified statistic as an int or -1 if no value is available</returns>
        public int GetIntStatistic(String statsName)
        {
            String value = GetStatistic(statsName);
            return value == null ? -1 : int.Parse(value);
        }
        private IDictionary<String, String> stats;        
   }

}