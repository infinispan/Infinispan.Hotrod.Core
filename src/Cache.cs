using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Org.Infinispan.Protostream;
using Org.Infinispan.Query.Remote.Client;
using System.Threading;

namespace Infinispan.Hotrod.Core
{
    public class CacheBase
    {
        public CacheBase(InfinispanDG ispnCluster, string name)
        {
            _cluster = ispnCluster;
            Name = name;
            NameAsBytes = Encoding.ASCII.GetBytes(Name);
            if (_cluster != null)
            {
                ForceReturnValue = _cluster.ForceReturnValue;
            }
            codec = Codec.getCodec(_cluster.Version);
        }
        public readonly string Name;
        public bool ForceReturnValue;
        public MediaType KeyMediaType;
        public MediaType ValueMediaType;
        public readonly byte[] NameAsBytes;
        private readonly InfinispanDG _cluster;
        public InfinispanDG Cluster { get { return _cluster; } }
        public bool UseCacheDefaultLifespan;
        public bool UseCacheDefaultMaxIdle;
        public readonly Codec30 codec;
        public Int32 Flags { get { return getFlags(); } }
        private int getFlags()
        {
            int retVal = 0;
            if (ForceReturnValue) retVal += 1;
            if (UseCacheDefaultLifespan) retVal += 2;
            if (UseCacheDefaultMaxIdle) retVal += 4;
            return retVal;
        }
    }
    public class Cache<K, V> : CacheBase
    {
        public Cache(InfinispanDG ispnCluster, Marshaller<K> keyM, Marshaller<V> valM, string name) : base(ispnCluster, name)
        {
            KeyMarshaller = keyM;
            ValueMarshaller = valM;
        }
        readonly Marshaller<K> KeyMarshaller;
        readonly Marshaller<V> ValueMarshaller;

        /// <summary>
        /// Get an entry from the cache
        /// </summary>
        /// <param name="key">key of the entry</param>
        /// <returns>the value of the entry or null (async)</returns>
        public async Task<V> Get(K key)
        {
            return await Cluster.Get(KeyMarshaller, ValueMarshaller, (CacheBase)this, key);
        }
        /// <summary>
        /// Get an entry from the cache with its version
        /// </summary>
        /// <param name="key">key of the entry</param>
        /// <returns>the value with version of the entry or null (async)</returns>
        public async Task<ValueWithVersion<V>> GetWithVersion(K key)
        {
            return await Cluster.GetWithVersion(KeyMarshaller, ValueMarshaller, (CacheBase)this, key);
        }
        /// <summary>
        /// Get an entry from the cache with its metadata
        /// </summary>
        /// <param name="key">key of the entry</param>
        /// <returns>the value with metadata of the entry or null (async)</returns>
        public async Task<ValueWithMetadata<V>> GetWithMetadata(K key)
        {
            return await Cluster.GetWithMetadata(KeyMarshaller, ValueMarshaller, (CacheBase)this, key);
        }
        /// <summary>
        /// Put/replace an entry in the cache
        /// </summary>
        /// <param name="key">key of the entry</param>
        /// <param name="value">value of the entry</param>
        /// <param name="lifespan">lifespan</param>
        /// <param name="maxidle">maximum idle time</param>
        /// <returns></returns>
        public async Task<V> Put(K key, V value, ExpirationTime lifespan = null, ExpirationTime maxidle = null)
        {
            return await Cluster.Put(KeyMarshaller, ValueMarshaller, this, key, value, lifespan, maxidle);
        }
        /// <summary>
        /// Put an entry in the cache if absent, does nothing otherwise
        /// </summary>
        /// <param name="key">key of the entry</param>
        /// <param name="value">value of the entry</param>
        /// <param name="lifespan">lifespan</param>
        /// <param name="maxidle">maximum idle time</param>
        /// <returns></returns>
        public async Task<V> PutIfAbsent(K key, V value, ExpirationTime lifespan = null, ExpirationTime maxidle = null)
        {
            return await Cluster.PutIfAbsent(KeyMarshaller, ValueMarshaller, this, key, value, lifespan, maxidle);
        }
        /// <summary>
        /// Return the number of entries in a cache
        /// </summary>
        /// <returns>number of entries</returns>
        public async Task<Int32> Size()
        {
            return await Cluster.Size(this);
        }
        /// <summary>
        /// Check if an entry with the given key is present
        /// </summary>
        /// <param name="key">key of the entry</param>
        /// <returns>true if an entry with the given exists</returns>
        public async Task<Boolean> ContainsKey(K key)
        {
            return await Cluster.ContainsKey(KeyMarshaller, (CacheBase)this, key);
        }
        /// <summary>
        /// Remove an entry from the cache
        /// </summary>
        /// <param name="key">entry's key</param>
        /// <returns>true if the entry has been removed</returns>
        public async Task<(V PrevValue, Boolean Removed)> Remove(K key)
        {
            return await Cluster.Remove(KeyMarshaller, ValueMarshaller, (CacheBase)this, key);
        }
        /// <summary>
        /// Clear the cache
        /// </summary>
        public async Task Clear()
        {
            await Cluster.Clear(this);
        }
        /// <summary>
        /// Return true is the cache is empty
        /// </summary>
        public async Task<Boolean> IsEmpty()
        {
            return await Cluster.Size(this) == 0;
        }
        /// <summary>
        /// Acquire some cache/cluster statistics
        /// </summary>
        /// <returns>some statistics</returns>
        public async Task<ServerStatistics> Stats()
        {
            return await Cluster.Stats(this);
        }
        /// <summary>
        /// Replace an entry value
        /// </summary>
        /// <param name="key">entry key</param>
        /// <param name="value">new value</param>
        /// <param name="lifespan">lifespan for the entry</param>
        /// <param name="maxidle">max idle time</param>
        /// <returns>if replaced (the previous value, true) otherwise (null,false)</returns>
        public async Task<(V PrevValue, Boolean Replaced)> Replace(K key, V value, ExpirationTime lifespan = null, ExpirationTime maxidle = null)
        {
            return await Cluster.Replace(KeyMarshaller, ValueMarshaller, this, key, value, lifespan, maxidle);
        }
        /// <summary>
        /// Replace the value of an entry with the given version
        /// </summary>
        /// <param name="key">entry key</param>
        /// <param name="value">new value</param>
        /// <param name="version">entry version</param>
        /// <param name="lifespan">lifespan for the entry</param>
        /// <param name="maxidle">max idle time</param>
        /// <returns>if replaced true otherwise false</returns>
        public async Task<Boolean> ReplaceWithVersion(K key, V value, Int64 version, ExpirationTime lifeSpan = null, ExpirationTime maxIdle = null)
        {
            return await Cluster.ReplaceWithVersion(KeyMarshaller, ValueMarshaller, (CacheBase)this, key, value, version, lifeSpan, maxIdle);
        }
        /// <summary>
        /// Remove an entry with the given version
        /// </summary>
        /// <param name="key">entry key</param>
        /// <param name="version">entry version</param>
        /// <returns>if replaced (the previous value, true) otherwise (null,false)</returns>
        public async Task<(V V, Boolean Removed)> RemoveWithVersion(K key, Int64 version)
        {
            return await Cluster.RemoveWithVersion(KeyMarshaller, ValueMarshaller, (CacheBase)this, key, version);
        }
        /// <summary>
        /// Run a query on the cache
        /// </summary>
        /// <param name="query">the query request</param>
        /// <returns>the query result</returns>
        public async Task<QueryResponse> Query(QueryRequest query)
        {
            return await Cluster.Query(query, (CacheBase)this);
        }
        /// <summary>
        /// A simplified method to run query
        /// </summary>
        /// This method returns the result set as a list of cache objects if the query has no select projection,
        /// otherwise return a list of tuples
        /// <param name="query">the query string</param>
        /// <returns>the resultSet</returns>
        public async Task<List<Object>> Query(String query)
        {
            var qr = new QueryRequest();
            qr.QueryString = query;
            var queryResponse = await Cluster.Query(qr, (CacheBase)this);
            List<Object> result = new List<Object>();
            if (queryResponse.ProjectionSize > 0)
            {  // Query has select
                return (List<object>)unwrapWithProjection(queryResponse);
            }
            for (int i = 0; i < queryResponse.NumResults; i++)
            {
                WrappedMessage wm = queryResponse.Results[i];

                if (wm.WrappedBytes != null)
                {
                    Object u = ValueMarshaller.unmarshall(wm.WrappedBytes.ToByteArray());
                    result.Add(u);
                }
            }
            return result;
        }
        /// <summary>
        /// Returns the set of all the cache entry keys 
        /// </summary>
        ///
        public async Task<ISet<K>> KeySet()
        {
            return await Cluster.KeySet<K>(KeyMarshaller, (CacheBase)this);
        }
        /// <summary>
        /// Put in the cache all the entries in the map
        /// </summary>
        /// <param name="map">the map of entries to put in the cache</param>
        /// <param name="lifespan">the lifespan for all the entries</param>
        /// <param name="maxidle">the maxidle for all the entries</param>
        /// <returns></returns>
        public async Task PutAll(Dictionary<K, V> map, ExpirationTime lifespan = null, ExpirationTime maxidle = null)
        {
            await Cluster.PutAll(KeyMarshaller, ValueMarshaller, this, map, lifespan, maxidle);
        }
        /// <summary>
        /// Get all the entries matching the keys in the set
        /// </summary>
        /// <param name="keys">the key set</param>
        /// <returns>a map with the found entries</returns>
        public async Task<IDictionary<K, V>> GetAll(ISet<K> keys)
        {
            return await Cluster.GetAll(KeyMarshaller, ValueMarshaller, this, keys);
        }
        /// <summary>
        /// An optimized for speed version of GetAll
        /// </summary>
        /// This splits the given getall is several getall operation each of which contains keys of a specific
        /// owner. Then all the operations are sent to the relative owner. Answers are collected and returned in a single result.
        /// This operation is not atomic and could "partially" fail.
        /// TODO Allow user to await for the result
        /// <param name="keys">the key set</param>
        /// <returns>a map with the found entries</returns>
        public IPartResult<IDictionary<K, V>> GetAllPart(ISet<K> keys)
        {
            var res = Cluster.GetAllPart(KeyMarshaller, ValueMarshaller, this, keys);
            return res != null ? new GetAllPartResult<K, V>(res) : null;
        }
        /// <summary>
        /// An optimized for speed version of GetAll
        /// </summary>
        /// This splits the given putall is several putall operation each of which contains keys of a specific
        /// owner. Then all the operations are sent to the relative owner. Answers are collected and returned in a single result.
        /// This operation is not atomic and could "partially" fail.
        /// TODO Allow user to await for the result
        /// <param name="map">the map of entries to put in the cache</param>
        /// <param name="lifespan">the lifespan for all the entries</param>
        /// <param name="maxidle">the maxidle for all the entries</param>
        /// <returns></returns>
        public IPartResult PutAllPart(IDictionary<K, V> map, ExpirationTime lifespan = null, ExpirationTime maxidle = null)
        {
            var res = Cluster.PutAllPart(KeyMarshaller, ValueMarshaller, this, map, lifespan, maxidle);
            return res != null ? new PutAllPartResult(res) : null;
        }
        /// <summary>
        /// ping operation
        /// </summary>
        /// <returns>a ping result</returns>
        public async Task<PingResult> Ping()
        {
            return await Cluster.Ping(this);
        }
        /// <summary>
        /// Add a listener for events to this cache
        /// </summary>
        /// <param name="listener">the listener</param>
        /// <param name="includeState">wether or not to return the initial cache state</param>
        /// <returns></returns>
        public async Task AddListener(IClientListener listener, bool includeState = false)
        {
            await Cluster.AddListener(this, listener, includeState);
        }
        /// <summary>
        /// Remove the listener from the cache
        /// </summary>
        /// <param name="listener">the listener</param>
        /// <returns></returns>
        public async Task RemoveListener(IClientListener listener)
        {
            await Cluster.RemoveListener(this, listener);
        }

        private static List<Object> unwrapWithProjection(QueryResponse resp)
        {
            List<Object> result = new List<Object>();
            if (resp.ProjectionSize == 0)
            {
                return result;
            }
            for (int i = 0; i < resp.NumResults; i++)
            {
                Object[] projection = new Object[resp.ProjectionSize];
                for (int j = 0; j < resp.ProjectionSize; j++)
                {
                    WrappedMessage wm = resp.Results[i * resp.ProjectionSize + j];
                    switch (wm.ScalarOrMessageCase)
                    {
                        case WrappedMessage.ScalarOrMessageOneofCase.WrappedDouble:
                            projection[j] = wm.WrappedDouble;
                            break;
                        case WrappedMessage.ScalarOrMessageOneofCase.WrappedFloat:
                            projection[j] = wm.WrappedFloat;
                            break;
                        case WrappedMessage.ScalarOrMessageOneofCase.WrappedInt64:
                            projection[j] = wm.WrappedInt64;
                            break;
                        case WrappedMessage.ScalarOrMessageOneofCase.WrappedUInt64:
                            projection[j] = wm.WrappedUInt64;
                            break;
                        case WrappedMessage.ScalarOrMessageOneofCase.WrappedInt32:
                            projection[j] = wm.WrappedInt32;
                            break;
                        case WrappedMessage.ScalarOrMessageOneofCase.WrappedFixed64:
                            projection[j] = wm.WrappedFixed64;
                            break;
                        case WrappedMessage.ScalarOrMessageOneofCase.WrappedFixed32:
                            projection[j] = wm.WrappedFixed32;
                            break;
                        case WrappedMessage.ScalarOrMessageOneofCase.WrappedBool:
                            projection[j] = wm.WrappedBool;
                            break;
                        case WrappedMessage.ScalarOrMessageOneofCase.WrappedString:
                            projection[j] = wm.WrappedString;
                            break;
                        case WrappedMessage.ScalarOrMessageOneofCase.WrappedBytes:
                            projection[j] = wm.WrappedBytes;
                            break;
                        case WrappedMessage.ScalarOrMessageOneofCase.WrappedUInt32:
                            projection[j] = wm.WrappedUInt32;
                            break;
                        case WrappedMessage.ScalarOrMessageOneofCase.WrappedSFixed32:
                            projection[j] = wm.WrappedSFixed32;
                            break;
                        case WrappedMessage.ScalarOrMessageOneofCase.WrappedSFixed64:
                            projection[j] = wm.WrappedSFixed64;
                            break;
                        case WrappedMessage.ScalarOrMessageOneofCase.WrappedSInt32:
                            projection[j] = wm.WrappedSInt32;
                            break;
                        case WrappedMessage.ScalarOrMessageOneofCase.WrappedSInt64:
                            projection[j] = wm.WrappedSInt64;
                            break;
                        case WrappedMessage.ScalarOrMessageOneofCase.WrappedDescriptorFullName:
                            projection[j] = wm.WrappedDescriptorFullName;
                            break;
                        case WrappedMessage.ScalarOrMessageOneofCase.WrappedMessageBytes:
                            projection[j] = wm.WrappedMessageBytes;
                            break;
                    }
                }
                result.Add(projection);
            }
            return result;
        }
    }
    public class ValueWithVersion<V>
    {
        public V Value;
        public Int64 Version;
    }

    public class ValueWithMetadata<V> : ValueWithVersion<V>
    {
        public Int64 Created = -1;
        public Int32 Lifespan = -1;
        public Int64 LastUsed = -1;
        public Int32 MaxIdle = -1;
    }
    public class VersionedResponse<V>
    {
    }

    public class ServerStatistics
    {
        public ServerStatistics(Dictionary<string, string> stats)
        {
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
    public class PingResult
    {
        public MediaType KeyType;
        public MediaType ValueType;
        public int Version;
        public int[] Operations;

    }


    public interface IPartResult
    {
        void WaitAll();

    }
    public interface IPartResult<T> : IPartResult
    {
        T Result();
    }

    internal class GetAllPartResult<K, V> : IPartResult<IDictionary<K, V>>
    {
        internal GetAllPartResult(Task<IDictionary<K, V>>[] ts)
        {
            tasks = ts;
        }
        IDictionary<K, V> result;
        Task<IDictionary<K, V>>[] tasks;
        public IDictionary<K, V> Result()
        {
            result = new Dictionary<K, V>();
            foreach (var t in tasks)
            {
                foreach (var entry in t.Result)
                {
                    result.Add(entry.Key, entry.Value);
                }
            }
            return result;
        }
        public void WaitAll()
        {
            Task.WaitAll(tasks);
        }
    }
    internal class PutAllPartResult : IPartResult
    {
        internal PutAllPartResult(Task[] ts)
        {
            tasks = ts;
        }
        Task[] tasks;
        public void WaitAll()
        {
            Task.WaitAll(tasks);
        }
    }
    public interface IClientListener
    {
        String ListenerID { get; set; }
        void OnEvent(Event e);
        void OnError(Exception ex = null);
    }
    public enum EventType : byte
    {
        CREATED = 0x60,
        MODIFIED = 0x61,
        REMOVED = 0x62,
        EXPIRED = 0x63
    }
    public class Event
    {
        public byte[] Key;
        public byte[] customData;
        public byte CustomMarker;
        public byte Retried;
        public long Version;
        public EventType Type;
        public String ListenerID;

    }

    public abstract class AbstractClientListener : IClientListener
    {
        private Task _task;
        public Task task { set => _task = value; }
        public abstract string ListenerID { get; set; }
        public void Wait()
        {
            try
            {
                _task.Wait();
            }
            catch { }
        }
        public abstract void OnError(Exception ex = null);
        public abstract void OnEvent(Event e);
    }
}