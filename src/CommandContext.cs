using BeetleX.Buffers;
using System;
using System.Collections.Generic;
using System.Text;
using System.Collections.Concurrent;

namespace Infinispan.Hotrod.Core
{
    internal class CommandContext
    {
        public long MessageId;
        public byte Version;
        public byte[] NameAsBytes;
        public byte ClientIntelligence;
        public UInt32 TopologyId;
        public MediaType KeyMediaType;
        public MediaType ValueMediaType;
        public MediaType CmdReqMediaType;
        public MediaType CmdResMediaType;
        public bool IsReqResCommand;

    }
}