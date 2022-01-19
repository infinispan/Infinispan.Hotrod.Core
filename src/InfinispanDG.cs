﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using BeetleX.EventArgs;
using Org.Infinispan.Query.Remote.Client;

namespace Infinispan.Hotrod.Core
{
    /// <summary>
    /// InfinispanDG class describes an Infinispan Cluster and is the main API entry point.
    /// </summary>
    public class InfinispanDG : IDisposable, ILogHandler
    {
        /// <summary>
        /// Constructor for the InfinispanDG class. The returned instance is blank
        /// and needs to be properly setup before any operation can be executed on the Infinispan cluster.
        /// Most of the configuration is exposed as properties or public methods.
        /// </summary>
        public InfinispanDG()
        {
        }
        /// <summary>
        /// Username for the connection credentials
        /// </summary>
        public string User { get; set; }
        /// <summary>
        /// Password for the connection credentials
        /// </summary>
        public string Password { get; set; }
        /// <summary>
        /// SASL authentication mechanism  
        /// </summary>
        public string AuthMech { get; set; }
        /// <summary>
        /// HotRod protocol version
        /// </summary>
        public byte Version { get; set; } = 0x1f;
        /// <summary>
        /// Client intelligence. Supported values are: 0x01 (basic)
        /// and 0x03 (hash-distribution)
        /// </summary>
        public byte ClientIntelligence { get; set; } = 0x02;
        /// <summary>
        /// Force the server must include a return value in the respose
        /// </summary>
        public bool ForceReturnValue = false;
        /// <summary>
        /// Enable TLS communication
        /// </summary>
        public bool UseTLS = false;
        /// <summary>
        /// Add a cluster node to the initial list of nodes.
        /// </summary>
        /// <param name="host">node address</param>
        /// <param name="port">port</param>
        /// <returns></returns>
        public InfinispanHost AddHost(string host, int port = 11222)
        {
            return AddHost(host, port, UseTLS);
        }
        /// <summary>
        /// Add a cluster node to the initial list of nodes.
        /// </summary>
        /// <param name="host">node address</param>
        /// <param name="port">port</param>
        /// <param name="ssl">overrides the cluster TLS setting</param>
        /// <returns></returns>
        public InfinispanHost AddHost(string host, int port, bool ssl)
        {
            if (port == 0)
                port = 11222;
            InfinispanHost ispnHost = new InfinispanHost(ssl, this, host, port);
            ispnHost.User = User;
            ispnHost.Password = Password;
            ispnHost.AuthMech = AuthMech;
            mHosts.Add(ispnHost);
            mActiveHosts = mHosts.ToArray();
            return ispnHost;
        }
        /// <summary>
        /// Returns a proxy to a remote cache on the server
        /// </summary>
        /// <typeparam name="K">Type of the key</typeparam>
        /// <typeparam name="V">Type of the value</typeparam>
        /// <param name="keyM">A marshaller for K. <see>Infinispan.Hotrod.Core.Marshaller</see></param>
        /// <param name="valM">A marshaller for V</param>
        /// <param name="name">Name of the cache</param>
        /// <returns></returns>
        public Cache<K, V> newCache<K, V>(Marshaller<K> keyM, Marshaller<V> valM, string name)
        {
            return new Cache<K, V>(this, keyM, valM, name);
        }
        public void EnableLog(LogType type)
        {
            enabledType = type;
        }
        public void Log(LogType type, string message)
        {
            if (type < enabledType)
            {
                return;
            }
            lock (mLockConsole)
            {
                Console.Write($"[{ DateTime.Now.ToString("HH:mmm:ss")}] ");
                switch (type)
                {
                    case LogType.Error:
                        Console.ForegroundColor = ConsoleColor.DarkRed;
                        break;
                    case LogType.Warring:
                        Console.ForegroundColor = ConsoleColor.Yellow;
                        break;
                    case LogType.Fatal:
                        Console.ForegroundColor = ConsoleColor.Red;
                        break;
                    case LogType.Info:
                        Console.ForegroundColor = ConsoleColor.Green;
                        break;
                    default:
                        Console.ForegroundColor = ConsoleColor.White;
                        break;
                }
                Console.Write($"[{type.ToString()}] ");
                Console.ForegroundColor = ConsoleColor.Gray;
                Console.WriteLine(message);
            }
        }
        internal async ValueTask<V> Put<K, V>(Marshaller<K> km, Marshaller<V> vm, UntypedCache cache, K key, V value, ExpirationTime lifespan = null, ExpirationTime maxidle = null)
        {
            Commands.PUT<K, V> cmd = new Commands.PUT<K, V>(km, vm, key, value);
            cmd.Flags = cache.Flags;
            if (lifespan != null)
            {
                cmd.Lifespan = lifespan;
            }
            if (maxidle != null)
            {
                cmd.MaxIdle = maxidle;
            }
            var result = await Execute(cache, cmd);
            if (result.IsError)
                throw new InfinispanException(result.Messge);
            return cmd.PrevValue;
        }
        internal async ValueTask<V> Get<K, V>(Marshaller<K> km, Marshaller<V> vm, UntypedCache cache, K key)
        {
            Commands.GET<K, V> cmd = new Commands.GET<K, V>(km, vm, key);
            cmd.Flags = cache.Flags;
            var result = await Execute(cache, cmd);
            if (result.IsError)
                throw new InfinispanException(result.Messge);
            return cmd.Value;
        }
        internal async ValueTask<ValueWithVersion<V>> GetWithVersion<K, V>(Marshaller<K> km, Marshaller<V> vm, UntypedCache cache, K key)
        {
            Commands.GETWITHVERSION<K, V> cmd = new Commands.GETWITHVERSION<K, V>(km, vm, key);
            cmd.Flags = cache.Flags;
            var result = await Execute(cache, cmd);
            if (result.IsError)
                throw new InfinispanException(result.Messge);
            return cmd.ValueWithVersion;
        }
        internal async ValueTask<ValueWithMetadata<V>> GetWithMetadata<K, V>(Marshaller<K> km, Marshaller<V> vm, UntypedCache cache, K key)
        {
            Commands.GETWITHMETADATA<K, V> cmd = new Commands.GETWITHMETADATA<K, V>(km, vm, key);
            cmd.Flags = cache.Flags;
            var result = await Execute(cache, cmd);
            if (result.IsError)
                throw new InfinispanException(result.Messge);
            return cmd.ValueWithMetadata;
        }
        internal async ValueTask<Int32> Size(UntypedCache cache)
        {
            Commands.SIZE cmd = new Commands.SIZE();
            cmd.Flags = cache.Flags;
            var result = await Execute(cache, cmd);
            if (result.IsError)
                throw new InfinispanException(result.Messge);
            return cmd.Size;
        }
        internal async ValueTask<Boolean> ContainsKey<K>(Marshaller<K> km, UntypedCache cache, K key)
        {
            Commands.CONTAINSKEY<K> cmd = new Commands.CONTAINSKEY<K>(km, key);
            if (cache != null)
            {
                cmd.Flags = cache.Flags;
            }
            var result = await Execute(cache, cmd);
            if (result.IsError)
                throw new InfinispanException(result.Messge);
            return cmd.IsContained;
        }
        internal async ValueTask<(V V, Boolean Removed)> Remove<K, V>(Marshaller<K> km, Marshaller<V> vm, UntypedCache cache, K key)
        {
            Commands.REMOVE<K, V> cmd = new Commands.REMOVE<K, V>(km, vm, key);
            cmd.Flags = cache.Flags;
            var result = await Execute(cache, cmd);
            if (result.IsError)
                throw new InfinispanException(result.Messge);
            return (cmd.PrevValue, cmd.Removed);
        }
        internal async ValueTask Clear(UntypedCache cache)
        {
            Commands.CLEAR cmd = new Commands.CLEAR();
            cmd.Flags = cache.Flags;
            var result = await Execute(cache, cmd);
            if (result.IsError)
                throw new InfinispanException(result.Messge);
            return;
        }

        internal async ValueTask<ServerStatistics> Stats(UntypedCache cache)
        {
            Commands.STATS cmd = new Commands.STATS();
            cmd.Flags = cache.Flags;
            var result = await Execute(cache, cmd);
            if (result.IsError)
                throw new InfinispanException(result.Messge);
            return cmd.Stats;
        }
        internal async ValueTask<(V V, Boolean Replaced)> Replace<K, V>(Marshaller<K> km, Marshaller<V> vm, UntypedCache cache, K key, V value, ExpirationTime lifespan = null, ExpirationTime maxidle = null)
        {
            Commands.REPLACE<K, V> cmd = new Commands.REPLACE<K, V>(km, vm, key, value);
            cmd.Flags = cache.Flags;
            if (lifespan != null)
            {
                cmd.Lifespan = lifespan;
            }
            if (maxidle != null)
            {
                cmd.MaxIdle = maxidle;
            }
            var result = await Execute(cache, cmd);
            if (result.IsError)
                throw new InfinispanException(result.Messge);
            return (cmd.PrevValue, cmd.Replaced);
        }
        internal async ValueTask<Boolean> ReplaceWithVersion<K, V>(Marshaller<K> km, Marshaller<V> vm, UntypedCache cache, K key, V value, Int64 version, ExpirationTime lifespan = null, ExpirationTime maxidle = null)
        {
            Commands.REPLACEWITHVERSION<K, V> cmd = new Commands.REPLACEWITHVERSION<K, V>(km, vm, key, value);
            cmd.Flags = cache.Flags;
            if (lifespan != null)
            {
                cmd.Lifespan = lifespan;
            }
            if (maxidle != null)
            {
                cmd.MaxIdle = maxidle;
            }
            cmd.Version = version;
            var result = await Execute(cache, cmd);
            if (result.IsError)
                throw new InfinispanException(result.Messge);
            return cmd.Replaced;
        }
        internal async ValueTask<(V V, Boolean Removed)> RemoveWithVersion<K, V>(Marshaller<K> km, Marshaller<V> vm, UntypedCache cache, K key, Int64 version)
        {
            Commands.REMOVEWITHVERSION<K, V> cmd = new Commands.REMOVEWITHVERSION<K, V>(km, vm, key);
            cmd.Flags = cache.Flags;
            cmd.Version = version;
            var result = await Execute(cache, cmd);
            if (result.IsError)
                throw new InfinispanException(result.Messge);
            return (cmd.PrevValue, cmd.Removed);
        }
        internal async ValueTask<QueryResponse> Query(QueryRequest query, UntypedCache cache)
        {
            Commands.QUERY cmd = new Commands.QUERY(query);
            cmd.Flags = cache.Flags;
            cmd.Query = query;
            var result = await Execute(cache, cmd);
            if (result.IsError)
                throw new InfinispanException(result.Messge);
            return cmd.QueryResponse;
        }
        internal async ValueTask<ISet<K>> KeySet<K>(Marshaller<K> km, UntypedCache cache)
        {
            Commands.KEYSET<K> cmd = new Commands.KEYSET<K>(km);
            if (cache != null)
            {
                cmd.Flags = cache.Flags;
            }
            var result = await Execute(cache, cmd);
            if (result.IsError)
                throw new InfinispanException(result.Messge);
            return cmd.keys;
        }
        public void Dispose()
        {
            if (!mIsDisposed)
            {
                mIsDisposed = true;
                foreach (var item in mHosts)
                    item.Dispose();
            }
        }
        public async Task shutdown()
        {  // TODO: is this a correct shutdown?
            await mHosts[0].shutdown();
        }
        // Private stuff below this line
        internal UInt32 TopologyId { get; set; } = 0xFFFFFFFFU;
        private IHostHandler HostHandler;
        private Dictionary<UntypedCache, TopologyInfo> topologyInfoMap = new Dictionary<UntypedCache, TopologyInfo>();
        private IList<InfinispanHost> mHosts = new List<InfinispanHost>();
        private InfinispanHost[] mActiveHosts = new InfinispanHost[0];
        private static Int32 MAXHASHVALUE { get; set; } = 0x7FFFFFFF;
        private async Task<Result> Execute(UntypedCache cache, Command cmd)
        {
            TopologyInfo topologyInfo;
            // Get the topology info for this cache. Initial hosts list will be used
            // until a topology record is received for a given cache
            topologyInfoMap.TryGetValue(cache, out topologyInfo);
            return await ExecuteWithRetry(cache, cmd, topologyInfo);
        }
        private async Task<Result> ExecuteWithRetry(UntypedCache cache, Command cmd, TopologyInfo topologyInfo)
        {
            InfinispanHost host;
            var hostHandlerForRetry = new HostHandlerForRetry(this);
            var cmdResultTask = new TaskCompletionSource<Result>();
            Result lastResult = new Result();
            while (true)
            {
                if (cmd.isHashAware() && topologyInfo != null)
                {
                    var keyIdx = this.getIndexFromBytes(cmd.getKeyAsBytes(), topologyInfo);
                    host = hostHandlerForRetry.GetHost(keyIdx, topologyInfo);
                }
                else
                {
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
                if (client == null)
                {
                    // clients for the host are all busy, go ahead with the next host if
                    var ret = new Result() { ResultType = ResultType.NetError, Messge = "exceeding maximum number of connections" };
                    continue;
                }
                try
                {
                    var result = await host.Connect(client);
                    if (result.IsError)
                    {
                        Console.WriteLine("errCon");
                        // TODO: save the error? and then go ahead with retry
                        continue;
                    }
                    InfinispanRequest request = new InfinispanRequest(host, host.Cluster, cache, client, cmd);
                    result = await request.Execute();
                    if (result.IsError)
                    {
                        if (canRetry(result))
                        {
                            continue;
                        }
                        return result;
                    }
                    cmdResultTask.TrySetResult(result);
                    return result;
                }
                catch (Exception) { }
                finally
                {
                    if (client != null)
                    {
                        host.Push(client);
                    }
                }
            }
        }
        private bool canRetry(Result result)
        {
            // Do not retry by default
            // Recoverable-by-retry errors should be added one by one
            return false;
        }
        private static uint getSegmentSize(int numSegments)
        {
            return (uint)(InfinispanDG.MAXHASHVALUE / numSegments);
        }
        private int getIndexFromBytes(byte[] buf, TopologyInfo topologyInfo)
        {
            Array arr = (Array)buf;
            Int32 hash = MurmurHash3.hash(((sbyte[])arr));
            Int32 normalizedHash = hash & MAXHASHVALUE;
            return (int)(normalizedHash / getSegmentSize(topologyInfo.servers.Count));
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
            for (var i = 0; i < topology.servers.Count; i++)
            {
                var node = topology.servers[i];
                var hostName = Encoding.ASCII.GetString(node.Item1);
                var port = node.Item2;
                var hostIsNew = true;
                foreach (var host in mHosts)
                {
                    if (host.Name == hostName && host.Port == port)
                    {
                        // By design if an host is return in a topology struct
                        // it is available by default
                        host.Available = true;
                        hostIsNew = false;
                        topology.hosts[i] = host;
                        break;
                    }
                }
                // If host isn't in the list then add it
                if (hostIsNew)
                {
                    topology.hosts[i] = this.AddHost(hostName, port, UseTLS);
                }
            }
        }
        private bool mIsDisposed = false;
        private object mLockConsole = new object();
        private LogType enabledType = LogType.Error;
        private class HostHandlerForRetry : IHostHandler
        {
            private InfinispanDG hostHandler;
            private int indexOnInitialList = 0;
            private int indexOnSegment = 0;
            private int traversedSegments = 0;
            public HostHandlerForRetry(InfinispanDG hostHandler)
            {
                this.hostHandler = hostHandler;
            }

            public InfinispanHost AddHost(string host, int port = 11222)
            {
                return this.hostHandler.AddHost(host, port);
            }

            public InfinispanHost AddHost(string host, int port, bool ssl)
            {
                return this.hostHandler.AddHost(host, port, ssl);
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
                while (this.traversedSegments < topologyInfo.OwnersPerSegment.Count)
                {
                    var s = (segment + this.traversedSegments) % topologyInfo.OwnersPerSegment.Count;
                    var owners = topologyInfo.OwnersPerSegment[s];
                    for (; this.indexOnSegment < owners.Count; this.indexOnSegment++)
                    {
                        if (topologyInfo.hosts[owners[this.indexOnSegment]].Available)
                            return topologyInfo.hosts[owners[this.indexOnSegment++]];
                    }
                    ++this.traversedSegments;
                    this.indexOnSegment = 0;
                }
                return null;
            }
        }
    }
}
