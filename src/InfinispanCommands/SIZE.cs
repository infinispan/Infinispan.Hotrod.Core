using BeetleX.Buffers;
using System;
using System.Collections.Generic;
using System.Text;

namespace Infinispan.Hotrod.Core.Commands
{
    public class SIZE : Command
    {
        public SIZE()
        {
            NetworkReceive = OnReceive;
        }
        public override string Name => "SIZE";

        public override Byte Code => 0x29;
        public UInt32 Size;

        public override void OnExecute(UntypedCache cache)
        {
            // TODO: here the code to build the bytebuffer that will be sent
            base.OnExecute(cache); // Generic code (build header?)
        }

        public override void Execute(UntypedCache cache, InfinispanClient client, PipeStream stream)
        {
            base.Execute(cache, client, stream);
        }

        public override Result OnReceive(InfinispanRequest request, PipeStream stream)
        {
            Size = Codec.readVInt(stream);
            return new Result{ Status =  ResultStatus.Completed, ResultType = ResultType.Object };
        }
    }
}