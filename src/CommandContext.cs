using BeetleX.Buffers;
using System;
using System.Collections.Generic;
using System.Text;
using System.Collections.Concurrent;

namespace Infinispan.Hotrod.Core
{
    internal class CommandContext
    {
        public MediaType CmdReqMediaType;
        public MediaType CmdResMediaType;
        public bool IsReqResCommand;
        public InfinispanClient Client;
        public CacheBase Cache;
        public byte[] CacheNameAsBytes { get { return (Cache != null) ? Cache.NameAsBytes : new byte[] { }; } }
        public long MessageId;
        public byte ClientIntelligence { get { return Client.Host.Cluster.ClientIntelligence; } }
        public byte Version { get { return Client.Host.Cluster.Version; } }
        public UInt32 TopologyId { get { return Client.Host.Cluster.TopologyId; } }
    }
}