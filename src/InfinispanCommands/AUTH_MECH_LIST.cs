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
        public override void OnExecute(UntypedCache cache)
        {
            base.OnExecute(cache);
        }

        public override void Execute(UntypedCache cache, InfinispanClient client, PipeStream stream)
        {
            base.Execute(cache, client, stream);
        }
        public override Result OnReceive(InfinispanRequest request, PipeStream stream)
        {
            var count = Codec.readVInt(stream);
            availableMechs = new string[count];
            for (int i=0; i<count; i++) {
                availableMechs[i]= Encoding.ASCII.GetString(Codec.readArray(stream));
            }
            return new Result{ Status =  ResultStatus.Completed, ResultType = ResultType.Object };
        }
    }
}
