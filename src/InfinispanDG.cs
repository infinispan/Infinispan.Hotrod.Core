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
        private Dictionary<UntypedCache, TopologyInfo> topologyInfoMap = new Dictionary<UntypedCache, TopologyInfo>();
        private IList<InfinispanHost> mHosts = new List<InfinispanHost>();
        private InfinispanHost[] mActiveHosts = new InfinispanHost[0];
        private bool OnClientPush(InfinispanClient client)
        {
            return true;
        }
        public int DB { get; set; }
        public static Int32 MAXHASHVALUE { get; private set; } = 0x7FFFFFFF;

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
        InfinispanHost IHostHandler.GetHost(int index, TopologyInfo topologyInfo)
        {
            var items = mActiveHosts;
            foreach(var owner in topologyInfo.OwnersPerSegment[index])
            {
            if (items[owner].Available)
                return items[owner];
            }
          return null;
        }
        public async Task<Result> Execute(UntypedCache cache, Command cmd)
        {
            TopologyInfo topologyInfo;
            // Get the topology info for this cache. Initial hosts list will be used
            // until a topology record is received for a given cache
            topologyInfoMap.TryGetValue(cache, out topologyInfo);
            return await ExecuteWithRetry(cache, cmd, topologyInfo);
        }

        public async Task<Result> ExecuteWithRetry(UntypedCache cache, Command cmd, TopologyInfo topologyInfo)
        {
            InfinispanHost host;
            var hostHandlerForRetry = new HostHandlerForRetry(this);
            var cmdResultTask = new TaskCompletionSource<Result>();
            Result lastResult = new Result();
            while(true)  {
                if (cmd.isHashAware() && topologyInfo!=null) {
                    var keyIdx = this.getIndexFromBytes(cmd.getKeyAsBytes(), topologyInfo);
                    host = hostHandlerForRetry.GetHost(keyIdx, topologyInfo);
                } else {
                    host = hostHandlerForRetry.GetHost();
                }
                if (host == null)
                {
                    // No (more) hosts available for the execution
                    var cmdResult = new Result() { ResultType = ResultType.NetError, Messge = "Infinispan server is not available" };
                    cmdResultTask.TrySetResult(cmdResult);
                    return cmdResult;
                }
                // First available host will be used even if its clients are all busy
                // caller will have to wait (TODO: better policy can be implemented)
                var client = await host.Pop();
                if (client == null) {
                    // clients for the host are all busy, go ahead with the next host if
                    var ret = new Result() { ResultType = ResultType.NetError, Messge = "exceeding maximum number of connections" };
                    continue;
                }
                try
                {
                    var result = host.Connect(client);
                    if (result.IsError)
                    {
                        // TODO: save the error and then go ahead with retry
                        continue;
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
                        if (result.IsError) {
                            continue;
                        }
                        cmdResultTask.TrySetResult(result);
                        return result;
                    }
                } catch (Exception ) {}
                finally
                {
                    if (client != null)
                        host.Push(client);
                }
            }

        }

        public static uint getSegmentSize(int numSegments) {
            return (uint)( InfinispanDG.MAXHASHVALUE / numSegments );
        }
        private int getIndexFromBytes(byte[] buf, TopologyInfo topologyInfo)
        {
            Array arr = (Array)buf;
            Int32 hash = MurmurHash3.hash(((sbyte[])arr));
            Int32 normalizedHash= hash & MAXHASHVALUE;
            return (int)(normalizedHash/getSegmentSize(topologyInfo.servers.Count));
        }

        /**
        * UpdatedTopologyInfo adds all the new host provided by the topology info
        * structure to the cluster host list.
        * Needs a way to cleanup no more used hosts
        */
        internal void UpdateTopologyInfo(TopologyInfo topology, UntypedCache cache)
        {
            this.TopologyId = topology.TopologyId;
            this.topologyInfoMap[cache] = topology;
            var newHosts = new List<InfinispanHost>();
            for (var i=0; i< topology.servers.Count;  i++) {
                var node = topology.servers[i];
                var hostName = Encoding.ASCII.GetString(node.Item1);
                var port = node.Item2;
                var hostIsNew = true;
                foreach (var host in mHosts) {
                    if (host.Name == hostName && host.Port == port) {
                        // By design if an host is return in a topology struct
                        // it is available by default
                        host.Available = true;
                        hostIsNew = false;
                        topology.hosts[i]=host;
                      break;
                    }
                }
                // If host isn't in the list then add it
                if (hostIsNew) {
                    topology.hosts[i]=this.AddHost(hostName, port, UseTLS);
                }
            }
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
        public async ValueTask<ISet<K>> KeySet<K>(Marshaller<K> km, UntypedCache cache)
        {
            Commands.KEYSET<K> cmd = new Commands.KEYSET<K>(km);
            if (cache != null) {
                cmd.Flags = cache.Flags;
            }
            var result = await Execute(cache, cmd);
            if (result.IsError)
                throw new InfinispanException(result.Messge);
            return cmd.keys;
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

        private class HostHandlerForRetry: IHostHandler
        {
            private InfinispanDG hostHandler;
            private int indexOnInitialList=0;
            private int indexOnSegment=0;
            private int traversedSegments=0;
            public HostHandlerForRetry(InfinispanDG hostHandler)
            {
                this.hostHandler = hostHandler;
            }

            public InfinispanHost AddHost(string host, int port = 11222)
            {
                return this.hostHandler.AddHost(host,port);
            }

            public InfinispanHost AddHost(string host, int port, bool ssl)
            {
                return this.hostHandler.AddHost(host,port,ssl);
            }

            // return the first host available in the initial list of hosts.
            // Following calls will start the search from the index of the last host returned in the last call,
            // this is how the retry policy is implemented here 
            public InfinispanHost GetHost()
            {
                var items = this.hostHandler.mActiveHosts;
                for (; this.indexOnInitialList < items.Length; this.indexOnInitialList++)
                {
                    if (items[this.indexOnInitialList].Available)
                        return items[this.indexOnInitialList++];
                }
                return null;
            }

            // First available host in the owner segment is returned
            // Following calls try to return an available host using this strategy:
            // - search in the owner segment
            // - search in the other segments starting from the segment owner+1
            public InfinispanHost GetHost(int segment, TopologyInfo topologyInfo)
            {
                var items = this.hostHandler.mActiveHosts;
                while (this.traversedSegments<topologyInfo.OwnersPerSegment.Count) {
                    var s = (segment+this.traversedSegments)%topologyInfo.OwnersPerSegment.Count;
                    var owners = topologyInfo.OwnersPerSegment[s];
                    for (; this.indexOnSegment < owners.Count; this.indexOnSegment++) {
                        if (topologyInfo.hosts[owners[this.indexOnSegment]].Available)
                            return topologyInfo.hosts[owners[this.indexOnSegment++]];
                    }
                ++this.traversedSegments;
                this.indexOnSegment=0;
                }
                return null;
            }
        }
    }
}
