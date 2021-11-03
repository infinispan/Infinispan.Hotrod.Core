using BeetleX.Tracks;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Org.Infinispan.Query.Remote.Client;

namespace Infinispan.Hotrod.Core
{
    public class InfinispanDG : IHostHandler, IDisposable
    {
        public InfinispanDG(int db = 0, IDataFormater dataFormater = null, IHostHandler hostHandler = null)
        {
            DB = db;
            if (hostHandler == null)
            {
                this.HostHandler = this;
            }
            else
            {
                this.HostHandler = hostHandler;
            }
        }

        private static InfinispanDG mDefault = new InfinispanDG();
        internal static InfinispanDG Default => mDefault;
        public bool AutoPing { get; set; } = true;
        public string User { get; set; }
        public string Password { get; set; }
        public string AuthMech { get; set; }
        public byte Version {get; set;} = 0x1f;
        public byte ClientIntelligence {get; set;} = 0x01;
        public UInt32 TopologyId {get; set;} = 0xFFFFFFFFU;
        public bool ForceReturnValue = false;
        public bool UseTLS = false;
        private IHostHandler HostHandler;
        private TopologyInfo topologyInfo;
        private IList<InfinispanHost> mHosts = new List<InfinispanHost>();
        private InfinispanHost[] mActiveHosts = new InfinispanHost[0];
        private bool OnClientPush(InfinispanClient client)
        {
            return true;
        }
        public int DB { get; set; }
        public ulong MAXHASHVALUE { get; private set; } = 0xFFFFFFFFL;

        public InfinispanHost AddHost(string host, int port = 11222)
        {
            return AddHost(host, port, UseTLS);
        }
        public InfinispanHost AddHost(string host, int port, bool ssl)
        {
            if (port == 0)
                port = 11222;
            InfinispanHost ispnHost = new InfinispanHost(ssl, this, host, port);
            ispnHost.User=User;
            ispnHost.Password=Password;
            ispnHost.AuthMech=AuthMech;
            mHosts.Add(ispnHost);
            mActiveHosts = mHosts.ToArray();
            return ispnHost;
        }

  
        InfinispanHost IHostHandler.GetHost()
        {
            var items = mActiveHosts;
            for (int i = 0; i < items.Length; i++)
            {
                if (items[i].Available)
                    return items[i];
            }
          return null;
        }
        InfinispanHost IHostHandler.GetHost(uint hash)
        {
            var items = mActiveHosts;
            var index = hashToIndex(hash);
            foreach(var owner in topologyInfo.OwnersPerSegment[index])
            {
            if (items[owner].Available)
                return items[owner];
            }
          return null;
        }
        private int hashToIndex(uint hash)
        {
            return (int)(((ulong)hash*(ulong)topologyInfo.OwnersPerSegment.Count())/MAXHASHVALUE);
        }
        public async Task<Result> Execute(UntypedCache cache, Command cmd)
        {
            InfinispanHost host;
            if (cmd.isHashAware() && topologyInfo!=null) {
                host = HostHandler.GetHost(this.getIndexFromBytes(cmd.getKeyAsBytes()));
            } else {
                host = HostHandler.GetHost();
            }
            if (host == null)
            {
                return new Result() { ResultType = ResultType.NetError, Messge = "Infinispan server is not available" };
            }
            var client = await host.Pop();
            if (client == null)
                return new Result() { ResultType = ResultType.NetError, Messge = "exceeding maximum number of connections" };
            try
            {
                var result = host.Connect(client);
                if (result.IsError)
                {
                    return result;
                }
                using (var tarck = CodeTrackFactory.Track(cmd.Name, CodeTrackLevel.Module, null, "Redis", client.Host.Name))
                {
                    if (tarck.Enabled)
                    {
                        tarck.Activity?.AddTag("tag", "BeetleX Redis");
                    }
                    cmd.Activity = tarck.Activity;
                    InfinispanRequest request = new InfinispanRequest(host, host.Cluster, cache, client, cmd);
                    request.Activity = tarck.Activity;
                    result = await request.Execute();
                    return result;
                }
            }
            finally
            {
                if (client != null)
                    host.Push(client);
            }
        }

        public static uint getSegmentSize(int numSegments) {
            return (uint)Math.Ceiling((double)(1L << 31) / numSegments);
        }
        private uint getIndexFromBytes(byte[] buf)
        {
            Array arr = (Array)buf;
            Int32 hash = MurmurHash3.hash(((sbyte[])arr));
            return ((UInt32)hash)/getSegmentSize(topologyInfo.servers.Count);
        }

        internal void UpdateTopologyInfo(TopologyInfo topology)
        {
            this.TopologyId = topology.TopologyId;
            this.topologyInfo = topology;
            var newHosts = new List<InfinispanHost>();
            foreach (var node in topology.servers) {
                var hostName = Encoding.ASCII.GetString(node.Item1);
                var port = node.Item2;
                var added = false;
                foreach (var oldHost in mHosts) {
                    // Reuse current working host if available...
                    if (oldHost.Name == hostName && oldHost.Port == port && oldHost.Available) {
                        newHosts.Add(oldHost);
                        added = true;
                        continue;
                    }
                }                
                // ...or create a new one
                if (!added) {
                    newHosts.Add(new InfinispanHost(UseTLS, this, hostName, port));
                }
            }
            mHosts = newHosts; // TODO: concurrency issues?
            mActiveHosts = mHosts.ToArray(); // TODO: concurrency issues?
        }
        public async ValueTask<V> Put<K,V>(Marshaller<K> km, Marshaller<V> vm, UntypedCache cache, K key, V value, ExpirationTime lifespan=null, ExpirationTime maxidle=null)
        {
            Commands.PUT<K,V> cmd = new Commands.PUT<K,V>(km, vm, key, value);
            cmd.Flags = cache.Flags;
            if (lifespan!=null){
                cmd.Lifespan = lifespan;
            }
            if (maxidle!=null) {
                cmd.MaxIdle = maxidle;
            }
            var result = await Execute(cache, cmd);
            if (result.IsError)
                throw new InfinispanException(result.Messge);
            return cmd.PrevValue;
        }
        public async ValueTask<V> Get<K,V>(Marshaller<K> km, Marshaller<V> vm, UntypedCache cache, K key)
        {
            Commands.GET<K,V> cmd = new Commands.GET<K,V>(km, vm, key);
            cmd.Flags = cache.Flags;
            var result = await Execute(cache, cmd);
            if (result.IsError)
                throw new InfinispanException(result.Messge);
            return cmd.Value;
        }
        public async ValueTask<ValueWithVersion<V>> GetWithVersion<K,V>(Marshaller<K> km, Marshaller<V> vm, UntypedCache cache, K key)
        {
            Commands.GETWITHVERSION<K,V> cmd = new Commands.GETWITHVERSION<K,V>(km, vm, key);
            cmd.Flags = cache.Flags;
            var result = await Execute(cache, cmd);
            if (result.IsError)
                throw new InfinispanException(result.Messge);
            return cmd.ValueWithVersion;
        }
        public async ValueTask<ValueWithMetadata<V>> GetWithMetadata<K,V>(Marshaller<K> km, Marshaller<V> vm, UntypedCache cache, K key)
        {
            Commands.GETWITHMETADATA<K,V> cmd = new Commands.GETWITHMETADATA<K,V>(km, vm, key);
            cmd.Flags = cache.Flags;
            var result = await Execute(cache, cmd);
            if (result.IsError)
                throw new InfinispanException(result.Messge);
            return cmd.ValueWithMetadata;
        }
        public async ValueTask<Int32> Size(UntypedCache cache) {
            Commands.SIZE cmd = new Commands.SIZE();
            cmd.Flags = cache.Flags;
            var result = await Execute(cache, cmd);
            if (result.IsError)
                throw new InfinispanException(result.Messge);
            return cmd.Size;
        }
        public async ValueTask<Boolean> ContainsKey<K>(Marshaller<K> km, UntypedCache cache, K key)
        {
            Commands.CONTAINSKEY<K> cmd = new Commands.CONTAINSKEY<K>(km, key);
            if (cache != null) {
                cmd.Flags = cache.Flags;
            }
            var result = await Execute(cache, cmd);
            if (result.IsError)
                throw new InfinispanException(result.Messge);
            return cmd.IsContained;
        }
        public async ValueTask<(V V, Boolean Removed)> Remove<K,V>(Marshaller<K> km, Marshaller<V> vm, UntypedCache cache, K key)
        {
            Commands.REMOVE<K,V> cmd = new Commands.REMOVE<K,V>(km, vm, key);
            cmd.Flags = cache.Flags;
            var result = await Execute(cache, cmd);
            if (result.IsError)
                throw new InfinispanException(result.Messge);
            return (cmd.PrevValue, cmd.Removed);
        }
        public async ValueTask Clear(UntypedCache cache) {
            Commands.CLEAR cmd = new Commands.CLEAR();
            cmd.Flags = cache.Flags;
            var result = await Execute(cache, cmd);
            if (result.IsError)
                throw new InfinispanException(result.Messge);
            return;
        }

        public async ValueTask<ServerStatistics> Stats(UntypedCache cache) {
            Commands.STATS cmd = new Commands.STATS();
            cmd.Flags = cache.Flags;
            var result = await Execute(cache, cmd);
            if (result.IsError)
                throw new InfinispanException(result.Messge);
            return cmd.Stats;
        }
        public async ValueTask<(V V, Boolean Replaced)> Replace<K,V>(Marshaller<K> km, Marshaller<V> vm, UntypedCache cache, K key, V value, ExpirationTime lifespan=null, ExpirationTime maxidle=null)
        {
            Commands.REPLACE<K,V> cmd = new Commands.REPLACE<K,V>(km, vm, key, value);
            cmd.Flags = cache.Flags;
            if (lifespan!=null){
                cmd.Lifespan = lifespan;
            }
            if (maxidle!=null) {
                cmd.MaxIdle = maxidle;
            }
            var result = await Execute(cache, cmd);
            if (result.IsError)
                throw new InfinispanException(result.Messge);
            return (cmd.PrevValue, cmd.Replaced);
        }
        public async ValueTask<Boolean> ReplaceWithVersion<K,V>(Marshaller<K> km, Marshaller<V> vm, UntypedCache cache, K key, V value, Int64 version, ExpirationTime lifespan=null, ExpirationTime maxidle=null)
        {
            Commands.REPLACEWITHVERSION<K,V> cmd = new Commands.REPLACEWITHVERSION<K,V>(km, vm, key, value);
            cmd.Flags = cache.Flags;
            if (lifespan!=null){
                cmd.Lifespan = lifespan;
            }
            if (maxidle!=null) {
                cmd.MaxIdle = maxidle;
            }
            cmd.Version = version;
            var result = await Execute(cache, cmd);
            if (result.IsError)
                throw new InfinispanException(result.Messge);
            return cmd.Replaced;
        }
        public async ValueTask<(V V, Boolean Removed)> RemoveWithVersion<K,V>(Marshaller<K> km, Marshaller<V> vm, UntypedCache cache, K key, Int64 version)
        {
            Commands.REMOVEWITHVERSION<K,V> cmd = new Commands.REMOVEWITHVERSION<K,V>(km, vm, key);
            cmd.Flags = cache.Flags;
            cmd.Version = version;
            var result = await Execute(cache, cmd);
            if (result.IsError)
                throw new InfinispanException(result.Messge);
            return (cmd.PrevValue, cmd.Removed);
        }
        public async ValueTask<QueryResponse> Query(QueryRequest query, UntypedCache cache) {
            Commands.QUERY cmd = new Commands.QUERY(query);
            cmd.Flags = cache.Flags;
            cmd.Query = query;
            var result = await Execute(cache, cmd);
            if (result.IsError)
                throw new InfinispanException(result.Messge);
            return cmd.QueryResponse;
        }
        private bool mIsDisposed = false;

        public void Dispose()
        {
            if (!mIsDisposed)
            {
                mIsDisposed = true;
                foreach (var item in mHosts)
                    item.Dispose();
            }
        }
        public Cache<K,V> newCache<K,V>(Marshaller<K> keyM, Marshaller<V> valM, string name) {
            return new Cache<K,V>(this, keyM, valM, name);
        }
    }
}
