using System;
using System.Collections.Generic;
using System.Text;
using BeetleX.Buffers;

namespace Infinispan.Hotrod.Core.Commands
{
    public class AUTH_MECH_LIST : Command
    {
        public AUTH_MECH_LIST()
        {
            NetworkReceive = OnReceive;
        }
        public int TimeOut { get; set; }

        public override string Name => "AUTH_MECH_LIST";
        public override Byte Code => 0x21;
        public string[] availableMechs { get; set; }
        public override void OnExecute(CommandContext ctx)
        {
            base.OnExecute(ctx);
        }

        public override void Execute(CommandContext ctx, InfinispanClient client, PipeStream stream)
        {
            base.Execute(ctx, client, stream);
        }
        public override Result OnReceive(InfinispanRequest request, PipeStream stream)
        {
            var count = Codec.readVInt(stream);
            availableMechs = new string[count];
            for (int i = 0; i < count; i++)
            {
                availableMechs[i] = Encoding.ASCII.GetString(Codec.readArray(stream));
            }
            return new Result { Status = ResultStatus.Completed, ResultType = ResultType.Object };
        }
    }
}
