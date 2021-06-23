using BeetleX.Buffers;
using System;
using System.Collections.Generic;
using System.Text;
using System.Collections.Concurrent;
using BeetleX.Tracks;

namespace Infinispan.Hotrod.Core
{
    public class CommandContext
    {
        public long MessageId { get; internal set; }
        public byte Version { get; internal set; }
        public byte[] NameAsBytes { get; internal set; }
        public byte ClientIntelligence { get; internal set; }
        public int TopologyId { get; internal set; }
        public MediaType KeyMediaType { get; internal set; }
        public MediaType ValueMediaType { get; internal set; }
    }
}