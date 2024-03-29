﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using BeetleX.EventArgs;
using Infinispan.Hotrod.Core.Commands;
using Org.Infinispan.Query.Remote.Client;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;

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
        /// SASL domain name  
        /// </summary>
        public string Domain { get; set; } = "infinispan";
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
        /// Certification chain for server certificate verification. Leave it null for no verification.
        /// </summary>
        public X509Chain CACert;
        public string ServiceName = "hotrod";
        public bool SwitchCluster(string clusterName)
        {
            if (mClusters.ContainsKey(clusterName))
            {
                lock (mActiveCluster)
                {
                    mActiveCluster = clusterName;
                    mActiveHosts = mClusters[clusterName].staticHosts.ToArray();
                    return true;
                }
            }
            return false;
        }
        /// <summary>
        /// Add a cluster node to the initial list of nodes for the DEFAULT_CLUSTER.
        /// </summary>
        /// <param name="host">node address</param>
        /// <param name="port">port</param>
        /// <returns></returns>
        public InfinispanHost AddHost(string host, int port = 11222)
        {
            return AddHost("DEFAULT_CLUSTER", host, port);
        }
        /// <summary>
        /// Add a cluster node to the initial list of nodes for the specified cluster.
        /// If applied to the active cluster, the current list of active node is discarded and replace
        /// with the new list. i.e. all the nodes info received via topology update are discarded.
        /// </summary>
        /// <param name="clusterName">name of the owner cluster</param>
        /// <param name="host">node address</param>
        /// <param name="port">port</param>
        /// <param name="ssl">overrides the cluster TLS setting</param>
        /// <returns></returns>
        public InfinispanHost AddHost(string clusterName, string host, int port = 11222)
        {
            InfinispanHost ispnHost = new InfinispanHost(this, host, port)
            {
                User = this.User,
                Password = this.Password,
                AuthMech = this.AuthMech,
                Domain = this.Domain
            };
            if (!mClusters.ContainsKey(clusterName))
            {
                mClusters[clusterName] = new Cluster();
            }
            mClusters[clusterName].staticHosts.Add(ispnHost);
            if (clusterName == mActiveCluster)
            {
                lock (mActiveCluster)
                {
                    mActiveHosts = mClusters[mActiveCluster].staticHosts.ToArray();
                }
            }
            return ispnHost;
        }

        private InfinispanHost AddTopologyHost(string clusterName, string host, int port)
        {
            InfinispanHost ispnHost = new InfinispanHost(this, host, port)
            {
                User = this.User,
                Password = this.Password,
                AuthMech = this.AuthMech,
                Domain = this.Domain

            };
            if (!mClusters.ContainsKey(clusterName))
            {
                mClusters[clusterName] = new Cluster();
            }
            mClusters[clusterName].topologyHosts.Add(ispnHost);
            return ispnHost;
        }


        /// <summary>
        /// Returns a proxy to a remote cache on the server
        /// </summary>
        /// The returned Cache object allows the execution of all the remote operation on the %Infinispan cluster.
        /// <typeparam name="K">Type of the key</typeparam>
        /// <typeparam name="V">Type of the value</typeparam>
        /// <param name="keyM">A marshaller for K. <see>Infinispan.Hotrod.Core.Marshaller</see></param>
        /// <param name="valM">A marshaller for V</param>
        /// <param name="name">Name of the cache</param>
        /// <returns>the cache object</returns>
        public Cache<K, V> NewCache<K, V>(Marshaller<K> keyM, Marshaller<V> valM, string name)
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
                Console.Write($"[{DateTime.Now:HH:mmm:ss}] ");
                Console.ForegroundColor = type switch
                {
                    LogType.Error => ConsoleColor.DarkRed,
                    LogType.Warring => ConsoleColor.Yellow,
                    LogType.Fatal => ConsoleColor.Red,
                    LogType.Info => ConsoleColor.Green,
                    _ => ConsoleColor.White,
                };
                Console.Write($"[{type}] ");
                Console.ForegroundColor = ConsoleColor.Gray;
                Console.WriteLine(message);
            }
        }

        internal async Task AddListener(CacheBase cache, IClientListener listener, bool includeState)
        {
            Commands.ADDCLIENTLISTENER cmd = new Commands.ADDCLIENTLISTENER(this.ListenerMap);
            cmd.Listener = listener;
            if (includeState)
            {
                cmd.IncludeState = 1;
            }
            await Execute(cache, cmd);
        }

        internal async Task RemoveListener(CacheBase cache, IClientListener listener)
        {
            Commands.REMOVECLIENTLISTENER cmd = new Commands.REMOVECLIENTLISTENER(listener);
            await Execute(cache, cmd);
        }

        public void Dispose()
        {
            if (!mIsDisposed)
            {
                mIsDisposed = true;
                foreach (var entry in mClusters)
                {
                    foreach (var item in entry.Value.staticHosts)
                        item.Dispose();
                }
            }
        }
        internal async ValueTask<V> Put<K, V>(Marshaller<K> km, Marshaller<V> vm, CacheBase cache, K key, V value, ExpirationTime lifespan = null, ExpirationTime maxidle = null)
        {
            Commands.PUT<K, V> cmd = new Commands.PUT<K, V>(km, vm, key, value);
            cmd.Flags = cache.Flags;
            PopulateExpirations(lifespan, maxidle, cmd);
            var result = await Execute(cache, cmd);
            return cmd.PrevValue;
        }
        internal async ValueTask<V> PutIfAbsent<K, V>(Marshaller<K> km, Marshaller<V> vm, CacheBase cache, K key, V value, ExpirationTime lifespan = null, ExpirationTime maxidle = null)
        {
            Commands.PUTIFABSENT<K, V> cmd = new Commands.PUTIFABSENT<K, V>(km, vm, key, value);
            cmd.Flags = cache.Flags;
            PopulateExpirations(lifespan, maxidle, cmd);
            var result = await Execute(cache, cmd);
            return cmd.PrevValue;
        }
        internal async ValueTask<V> Get<K, V>(Marshaller<K> km, Marshaller<V> vm, CacheBase cache, K key)
        {
            Commands.GET<K, V> cmd = new Commands.GET<K, V>(km, vm, key);
            cmd.Flags = cache.Flags;
            var result = await Execute(cache, cmd);
            return cmd.Value;
        }
        internal async ValueTask<ValueWithVersion<V>> GetWithVersion<K, V>(Marshaller<K> km, Marshaller<V> vm, CacheBase cache, K key)
        {
            Commands.GETWITHVERSION<K, V> cmd = new Commands.GETWITHVERSION<K, V>(km, vm, key);
            cmd.Flags = cache.Flags;
            var result = await Execute(cache, cmd);
            return cmd.ValueWithVersion;
        }
        internal async ValueTask<ValueWithMetadata<V>> GetWithMetadata<K, V>(Marshaller<K> km, Marshaller<V> vm, CacheBase cache, K key)
        {
            Commands.GETWITHMETADATA<K, V> cmd = new Commands.GETWITHMETADATA<K, V>(km, vm, key);
            cmd.Flags = cache.Flags;
            var result = await Execute(cache, cmd);
            return cmd.ValueWithMetadata;
        }
        internal async ValueTask<Int32> Size(CacheBase cache)
        {
            Commands.SIZE cmd = new Commands.SIZE();
            cmd.Flags = cache.Flags;
            var result = await Execute(cache, cmd);
            return cmd.Size;
        }
        internal async ValueTask<Boolean> ContainsKey<K>(Marshaller<K> km, CacheBase cache, K key)
        {
            Commands.CONTAINSKEY<K> cmd = new Commands.CONTAINSKEY<K>(km, key);
            if (cache != null)
            {
                cmd.Flags = cache.Flags;
            }
            var result = await Execute(cache, cmd);
            return cmd.IsContained;
        }
        internal async ValueTask<(V V, Boolean Removed)> Remove<K, V>(Marshaller<K> km, Marshaller<V> vm, CacheBase cache, K key)
        {
            Commands.REMOVE<K, V> cmd = new Commands.REMOVE<K, V>(km, vm, key);
            cmd.Flags = cache.Flags;
            var result = await Execute(cache, cmd);
            return (cmd.PrevValue, cmd.Removed);
        }
        internal async ValueTask Clear(CacheBase cache)
        {
            Commands.CLEAR cmd = new Commands.CLEAR();
            cmd.Flags = cache.Flags;
            var result = await Execute(cache, cmd);
            return;
        }

        internal async ValueTask<ServerStatistics> Stats(CacheBase cache)
        {
            Commands.STATS cmd = new Commands.STATS();
            cmd.Flags = cache.Flags;
            var result = await Execute(cache, cmd);
            return cmd.Stats;
        }
        internal async ValueTask<(V V, Boolean Replaced)> Replace<K, V>(Marshaller<K> km, Marshaller<V> vm, CacheBase cache, K key, V value, ExpirationTime lifespan = null, ExpirationTime maxidle = null)
        {
            Commands.REPLACE<K, V> cmd = new Commands.REPLACE<K, V>(km, vm, key, value);
            cmd.Flags = cache.Flags;
            PopulateExpirations(lifespan, maxidle, cmd);
            var result = await Execute(cache, cmd);
            return (cmd.PrevValue, cmd.Replaced);
        }
        internal async ValueTask<Boolean> ReplaceWithVersion<K, V>(Marshaller<K> km, Marshaller<V> vm, CacheBase cache, K key, V value, Int64 version, ExpirationTime lifespan = null, ExpirationTime maxidle = null)
        {
            Commands.REPLACEWITHVERSION<K, V> cmd = new Commands.REPLACEWITHVERSION<K, V>(km, vm, key, value);
            cmd.Flags = cache.Flags;
            PopulateExpirations(lifespan, maxidle, cmd);
            cmd.Version = version;
            var result = await Execute(cache, cmd);
            return cmd.Replaced;
        }
        internal async ValueTask<(V V, Boolean Removed)> RemoveWithVersion<K, V>(Marshaller<K> km, Marshaller<V> vm, CacheBase cache, K key, Int64 version)
        {
            Commands.REMOVEWITHVERSION<K, V> cmd = new Commands.REMOVEWITHVERSION<K, V>(km, vm, key);
            cmd.Flags = cache.Flags;
            cmd.Version = version;
            var result = await Execute(cache, cmd);
            return (cmd.PrevValue, cmd.Removed);
        }
        internal async ValueTask<QueryResponse> Query(QueryRequest query, CacheBase cache)
        {
            Commands.QUERY cmd = new Commands.QUERY(query);
            cmd.Flags = cache.Flags;
            cmd.Query = query;
            var result = await Execute(cache, cmd);
            return cmd.QueryResponse;
        }
        internal async ValueTask<ISet<K>> KeySet<K>(Marshaller<K> km, CacheBase cache)
        {
            Commands.KEYSET<K> cmd = new Commands.KEYSET<K>(km);
            if (cache != null)
            {
                cmd.Flags = cache.Flags;
            }
            var result = await Execute(cache, cmd);
            return cmd.keys;
        }
        internal async ValueTask PutAll<K, V>(Marshaller<K> km, Marshaller<V> vm, CacheBase cache, IDictionary<K, V> map, ExpirationTime lifespan = null, ExpirationTime maxidle = null, int segment = -1)
        {
            Commands.PUTALL<K, V> cmd = new Commands.PUTALL<K, V>(km, vm, map);
            cmd.Segment = segment;
            cmd.Flags = cache.Flags;
            PopulateExpirations(lifespan, maxidle, cmd);
            var result = await Execute(cache, cmd);
            return;
        }
        internal async ValueTask<IDictionary<K, V>> GetAll<K, V>(Marshaller<K> km, Marshaller<V> vm, CacheBase cache, ISet<K> keys, int segment = -1)
        {
            Commands.GETALL<K, V> cmd = new Commands.GETALL<K, V>(km, vm, keys);
            cmd.Segment = segment;
            cmd.Flags = cache.Flags;
            var result = await Execute(cache, cmd);
            return cmd.Entries;
        }
        internal Task[] PutAllPart<K, V>(Marshaller<K> km, Marshaller<V> vm, CacheBase cache, IDictionary<K, V> map, ExpirationTime lifespan = null, ExpirationTime maxidle = null)
        {
            var mapBySeg = this.SplitKeyValueBySegment(km, cache, map);
            Task[] ts = new Task[mapBySeg.Count];
            int i = 0;
            foreach (var entry in mapBySeg)
            {
                ts[i++] = Task.Run(async () =>
                {
                    await this.PutAll<K, V>(km, vm, cache, entry.Value, lifespan, maxidle, entry.Key);
                });
            }
            return ts;
        }
        internal Task<IDictionary<K, V>>[] GetAllPart<K, V>(Marshaller<K> km, Marshaller<V> vm, CacheBase cache, ISet<K> keys)
        {
            var map = this.SplitBySegment(km, cache, keys);
            if (map == null)
                return null;
            Task<IDictionary<K, V>>[] ts = new Task<IDictionary<K, V>>[map.Count];
            int i = 0;
            foreach (var entry in map)
            {
                ts[i++] = Task.Run<IDictionary<K, V>>(async () =>
                {
                    return await this.GetAll<K, V>(km, vm, cache, entry.Value, entry.Key);
                });
            }
            return ts;
        }
        internal async ValueTask<PingResult> Ping(CacheBase cache)
        {
            Commands.PING cmd = new Commands.PING();
            cmd.Flags = cache.Flags;
            var result = await Execute(cache, cmd);
            return cmd.Result;
        }

        public IDictionary<int, ISet<K>> SplitBySegment<K>(Marshaller<K> km, CacheBase cache, ICollection<K> keys)
        {
            TopologyInfo topologyInfo;
            this.topologyInfoMap.TryGetValue(cache, out topologyInfo);
            if (topologyInfo == null)
            {
                return null;
            }
            IDictionary<int, ISet<K>> res = new Dictionary<int, ISet<K>>();
            foreach (var k in keys)
            {
                var segment = this.getSegmentFromBytes(km.marshall(k), topologyInfo);
                if (!res.ContainsKey(segment))
                {
                    res[segment] = new HashSet<K>();
                }
                res[segment].Add(k);
            }
            return res;
        }

        public IDictionary<int, IDictionary<K, V>> SplitKeyValueBySegment<K, V>(Marshaller<K> km, CacheBase cache, IDictionary<K, V> map)
        {
            TopologyInfo topologyInfo;
            this.topologyInfoMap.TryGetValue(cache, out topologyInfo);
            if (topologyInfo == null)
            {
                return null;
            }
            IDictionary<int, IDictionary<K, V>> res = new Dictionary<int, IDictionary<K, V>>();
            foreach (var entry in map)
            {
                var segment = this.getSegmentFromBytes(km.marshall(entry.Key), topologyInfo);
                if (!res.ContainsKey(segment))
                {
                    res[segment] = new Dictionary<K, V>();
                }
                res[segment].Add(entry);
            }
            return res;
        }

        // Private stuff below this line
        internal UInt32 TopologyId { get; set; } = 0xFFFFFFFFU;
        private Dictionary<CacheBase, TopologyInfo> topologyInfoMap = new Dictionary<CacheBase, TopologyInfo>();
        private IDictionary<string, Cluster> mClusters = new Dictionary<string, Cluster>();
        internal string mActiveCluster = "DEFAULT_CLUSTER";
        private InfinispanHost[] mActiveHosts = new InfinispanHost[0];
        private static Int32 MAXHASHVALUE { get; set; } = 0x7FFFFFFF;
        internal IDictionary<String, InfinispanRequest> ListenerMap = new Dictionary<String, InfinispanRequest>();
        private async Task<CommandResult> Execute(CacheBase cache, Command cmd)
        {
            TopologyInfo topologyInfo;
            // Get the topology info for this cache. Initial hosts list will be used
            // until a topology record is received for a given cache

            topologyInfoMap.TryGetValue(cache, out topologyInfo);
            var result = await ExecuteWithRetry(cache, cmd, topologyInfo);
            if (result.IsError)
            {
                throw new InfinispanException(result);
            }
            return result;
        }
        private async Task<CommandResult> ExecuteWithRetry(CacheBase cache, Command cmd, TopologyInfo topologyInfo)
        {
            var cmdResult = new CommandResult();
            var hostHandlerForRetry = new HostHandlerForRetry(this);
            InfinispanHost host = null;
            int segment = -1;
            switch (cmd.getTopologyKnowledgeType())
            {
                case Command.TopologyKnoledge.SEGMENT:
                    segment = cmd.getSegment();
                    break;
                case Command.TopologyKnoledge.KEY:
                    segment = this.getSegmentFromBytes(cmd.getKeyAsBytes(), topologyInfo);
                    break;
            }
            while (true)
            {
                host = (segment != -1)
                        ? host = hostHandlerForRetry.GetHostFromTopologyList(segment, topologyInfo)
                        : host = hostHandlerForRetry.GetHostFromStaticList();
                if (host == null)
                {
                    // Mark this cluster as FAULT
                    mClusters[mActiveCluster].status = Cluster.Status.FAULT;
                    hostHandlerForRetry.clusterFault(mActiveCluster);
                    if (this.clusterFaultRecoveryPolicy(hostHandlerForRetry.faultClusterNames))
                    {
                        // Fault recovery has been applied successfully
                        // try execution on the new cluster
                        continue;
                    }
                    // No (more) hosts available for the execution
                    Result res = new Result() { ResultType = ResultType.NetError, Messge = "Infinispan server is not available" };
                    cmdResult.Results.Add(res);
                    cmdResult.IsError = true;
                    cmdResult.ErrorMessage = res.Messge;
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
                Result result = null;
                try
                {
                    result = await host.Connect(client);
                    if (result.IsError)
                    {
                        Console.WriteLine("errCon");
                        hostHandlerForRetry.faultHosts.Add(host);
                        // TODO: save the error? and then go ahead with retry
                        cmdResult.Results.Add(result);
                        continue;
                    }
                    InfinispanRequest request = new InfinispanRequest(cache, client, cmd);
                    result = await request.Execute();
                    cmdResult.Results.Add(result);
                    if (result.IsError)
                    {
                        if (canRetry(result))
                        {
                            continue;
                        }
                        cmdResult.IsError = true;
                        cmdResult.ErrorMessage = result.Messge;
                    }
                    return cmdResult;
                }
                catch (Exception) { }
                finally
                {
                    if (client != null)
                    {
                        if (result?.ResultType != ResultType.Event)
                        {
                            host.Push(client);
                        }
                    }
                }
            }
        }

        private bool clusterFaultRecoveryPolicy(ISet<string> faultClusterNames)
        {
            if (!faultClusterNames.Contains(this.mActiveCluster))
            {
                // Current cluster is working. Continue with it
                return true;
            }
            foreach (var cluster in mClusters)
            {
                if (!faultClusterNames.Contains(cluster.Key))
                {
                    // Found a working cluster. Switch to it.
                    SwitchCluster(cluster.Key);
                    return true;
                }
            }
            return false;
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
        internal int getSegmentFromBytes(byte[] buf, TopologyInfo topologyInfo)
        {
            if (topologyInfo == null)
            {
                return -1;
            }
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
        internal void UpdateTopologyInfo(TopologyInfo topology, CacheBase cache)
        {
            this.TopologyId = topology.TopologyId;
            this.topologyInfoMap[cache] = topology;
            var newHosts = new List<InfinispanHost>();
            for (var i = 0; i < topology.servers.Count; i++)
            {
                var node = topology.servers[i];
                var hostName = Encoding.ASCII.GetString(node.Item1);
                var port = node.Item2;
                var host = getHostByNameAndPort(mClusters[mActiveCluster].staticHosts, hostName, port);
                if (host != null)
                {
                    topology.hosts[i] = host;
                    continue;
                }
                host = getHostByNameAndPort(mClusters[mActiveCluster].topologyHosts, hostName, port);
                if (host != null)
                {
                    topology.hosts[i] = host;
                    continue;
                }

                topology.hosts[i] = this.AddTopologyHost(mActiveCluster, hostName, port);
            }
        }
        private InfinispanHost getHostByNameAndPort(IList<InfinispanHost> hosts, string hostName, int port)
        {
            foreach (var host in mClusters[mActiveCluster].staticHosts)
            {
                if (host.Name == hostName && host.Port == port)
                {
                    // By design if an host is return in a topology struct
                    // it is available by default
                    host.Available = true;
                    return host;
                }
            }
            return null;
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

            internal ISet<string> faultClusterNames = new HashSet<string>();
            internal ISet<InfinispanHost> faultHosts = new HashSet<InfinispanHost>();
            public HostHandlerForRetry(InfinispanDG hostHandler)
            {
                this.hostHandler = hostHandler;
            }

            // return the first host available in the initial list of hosts.
            // Following calls will start the search from the index of the last host returned in the last call,
            // this is how the retry policy is implemented here 
            public InfinispanHost GetHostFromStaticList()
            {
                for (var items = this.hostHandler.mActiveHosts; this.indexOnInitialList < items.Length; this.indexOnInitialList++)
                {
                    if (!this.faultHosts.Contains(items[this.indexOnInitialList]))
                    {
                        return items[this.indexOnInitialList++];
                    }
                }
                return null;
            }

            // First available host in the owner segment is returned
            // Following calls try to return an available host using this strategy:
            // - search in the owner segment
            // - search in the other segments starting from the segment owner+1
            public InfinispanHost GetHostFromTopologyList(int segment, TopologyInfo topologyInfo)
            {
                while (this.traversedSegments < topologyInfo.OwnersPerSegment.Count)
                {
                    var s = (segment + this.traversedSegments) % topologyInfo.OwnersPerSegment.Count;
                    var owners = topologyInfo.OwnersPerSegment[s];
                    for (; this.indexOnSegment < owners.Count; this.indexOnSegment++)
                    {
                        if (!this.faultHosts.Contains(topologyInfo.hosts[owners[this.indexOnSegment]]))
                        {
                            return topologyInfo.hosts[owners[this.indexOnSegment++]];
                        }
                    }
                    ++this.traversedSegments;
                    this.indexOnSegment = 0;
                }
                return null;
            }

            internal void clusterFault(string currentCluster)
            {
                this.faultClusterNames.Add(currentCluster);
                this.faultHosts = new HashSet<InfinispanHost>();
                indexOnInitialList = 0;
                indexOnSegment = 0;
                traversedSegments = 0;
            }
        }
        private static void PopulateExpirations(ExpirationTime lifespan, ExpirationTime maxidle, ICommandWithExpiration cmd)
        {
            if (lifespan != null)
            {
                cmd.Lifespan = lifespan;
            }
            if (maxidle != null)
            {
                cmd.MaxIdle = maxidle;
            }
        }


    }

    internal class Cluster
    {
        internal IList<InfinispanHost> staticHosts = new List<InfinispanHost>();
        internal IList<InfinispanHost> topologyHosts = new List<InfinispanHost>();
        internal enum Status
        {
            OK,
            FAULT
        }
        internal Status status = Status.OK;
    }
}
