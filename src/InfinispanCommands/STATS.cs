using BeetleX.Buffers;
using System;
using System.Collections.Generic;
using System.Text;

namespace Infinispan.Hotrod.Core.Commands
{
    public class STATS : Command
    {
        public STATS()
        {
            NetworkReceive = OnReceive;
        }
        public override string Name => "STATS";

        public override Byte Code => 0x15;
        public ServerStatistics Stats;

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
            if (request.ResponseStatus == InfinispanRequest.NO_ERROR_STATUS) {
                var d = new Dictionary<string,string>();
                var statsNum = Codec.readVInt(stream);
                for (int i=0; i<statsNum; i++) {
                    var name = Encoding.ASCII.GetString(Codec.readArray(stream));
                    var value = Encoding.ASCII.GetString(Codec.readArray(stream));
                    d.Add(name,value);
                }
                this.Stats = new ServerStatistics(d);
                return new Result{ Status =  ResultStatus.Completed, ResultType = ResultType.Object };
            }
            return new Result{ Status =  ResultStatus.Completed, ResultType = ResultType.Error };
        }
    }
}