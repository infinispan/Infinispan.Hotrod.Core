using BeetleX.Buffers;
using System;
using System.Collections.Generic;
using System.Text;
using System.Collections.Concurrent;

namespace Infinispan.Hotrod.Core
{
    public class CommandContext
    {
        public long MessageId { get; internal set; }
        public byte Version { get; internal set; }
        public byte[] NameAsBytes { get; internal set; }
        public byte ClientIntelligence { get; internal set; }
        public UInt32 TopologyId { get; internal set; }
        public MediaType KeyMediaType { get; internal set; }
        public MediaType ValueMediaType { get; internal set; }
        public MediaType CmdReqMediaType { get; internal set; }
        public MediaType CmdResMediaType { get; internal set; }
        public bool IsReqResCommand { get; internal set; }

    }
}