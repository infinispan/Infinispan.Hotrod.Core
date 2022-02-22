using BeetleX.Buffers;
using System;
using System.Collections.Generic;
using System.Text;

namespace Infinispan.Hotrod.Core.Commands
{
    public class KEYSET<K> : Command
    {
        public KEYSET(Marshaller<K> km, int scope = 0)
        {
            KeyMarshaller = km;
            NetworkReceive = OnReceive;
            Scope = scope;
        }
        public Marshaller<K> KeyMarshaller;
        public int Scope;
        public override string Name => "KEYSET";

        public override Byte Code => 0x1D;
        public ISet<K> keys;

        public override void OnExecute(CommandContext ctx)
        {
            base.OnExecute(ctx);
        }

        public override void Execute(CommandContext ctx, InfinispanClient client, PipeStream stream)
        {
            base.Execute(ctx, client, stream);
            Codec.writeVInt(Scope, stream);
        }

        public override Result OnReceive(InfinispanRequest request, ResponseStream stream)
        {
            keys = new HashSet<K>();
            while (stream.ReadByte() == 1)
            {
                keys.Add(this.KeyMarshaller.unmarshall(Codec.readArray(stream)));
            }
            return new Result { Status = ResultStatus.Completed, ResultType = ResultType.Object };
        }
    }
}