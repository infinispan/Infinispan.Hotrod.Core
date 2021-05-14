using System;
using System.Collections.Generic;
using System.Text;
using BeetleX.Buffers;

namespace Infinispan.Hotrod.Core.Commands
{
    public class CLEAR : Command
    {
        public CLEAR()
        {
            NetworkReceive = OnReceive;
        }
        public int TimeOut { get; set; }
        public override string Name => "CLEAR";

        public override Byte Code => 0x13;
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
            return new Result{ Status =  ResultStatus.Completed, ResultType = ResultType.Null };
        }
    }
}
