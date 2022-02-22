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
        public Int32 Size;

        public override void OnExecute(CommandContext ctx)
        {
            base.OnExecute(ctx);
        }

        public override void Execute(CommandContext ctx, InfinispanClient client, PipeStream stream)
        {
            base.Execute(ctx, client, stream);
        }

        public override Result OnReceive(InfinispanRequest request, ResponseStream stream)
        {
            Size = Codec.readVInt(stream);
            return new Result{ Status =  ResultStatus.Completed, ResultType = ResultType.Object };
        }
    }
}