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
        public override void OnExecute(Cache cache)
        {
            // TODO: here the code to build the bytebuffer that will be sent
            base.OnExecute(cache); // Generic code (build header?)
        }

        public override void Execute(Cache cache, InfinispanClient client, PipeStream stream)
        {
            base.Execute(cache, client, stream);
            stream.Flush();
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
