using BeetleX.Buffers;
using System;
using System.Collections.Generic;
using System.Text;

namespace Infinispan.Hotrod.Core.Commands
{
    public class GET_ALL<K, V> : Command
    {
        public GET_ALL(Marshaller<K> km, Marshaller<V> vm, ISet<K> keys)
        {
            KeyMarshaller = km;
            ValueMarshaller = vm;
            Keys = keys;
            NetworkReceive = OnReceive;
        }
        public Marshaller<V> ValueMarshaller;
        public Marshaller<K> KeyMarshaller;

        public ISet<K> Keys { get; set; }

        public IDictionary<K, V> Entries;
        public override string Name => "GET_ALL";

        public override Byte Code => 0x2F;

        public override void OnExecute(CommandContext ctx)
        {
            base.OnExecute(ctx);
        }

        public override void Execute(CommandContext ctx, InfinispanClient client, PipeStream stream)
        {
            base.Execute(ctx, client, stream);
            Codec.writeVInt(Keys.Count, stream);
            foreach (var k in Keys)
            {
                Codec.writeArray(KeyMarshaller.marshall(k), stream);
            }
            stream.Flush();
        }

        public override Result OnReceive(InfinispanRequest request, PipeStream stream)
        {
            if (request.ResponseStatus == Codec30.NO_ERROR_STATUS)
            {

                var resCount = Codec.readVInt(stream);
                Entries = new Dictionary<K, V>();
                for (var i = 0; i < resCount; i++)
                {
                    Codec.readArray(stream, ref request.ras);
                    K k = KeyMarshaller.unmarshall(request.ras.Result);
                    Codec.readArray(stream, ref request.ras);
                    V v = ValueMarshaller.unmarshall(request.ras.Result);
                    Entries.Add(k, v);
                }
                return new Result { Status = ResultStatus.Completed, ResultType = ResultType.Object };
            }
            return new Result { Status = ResultStatus.Completed, ResultType = ResultType.Null };
        }

    }
}