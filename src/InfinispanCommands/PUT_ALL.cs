using System;
using System.Collections.Generic;
using System.Text;
using BeetleX.Buffers;

namespace Infinispan.Hotrod.Core.Commands
{
    public class PUT_ALL<K, V> : Command
    {
        public PUT_ALL(Marshaller<K> km, Marshaller<V> vm, IDictionary<K, V> map)
        {
            Map = map;
            KeyMarshaller = km;
            ValueMarshaller = vm;

            NetworkReceive = OnReceive;
        }
        public Marshaller<K> KeyMarshaller;
        public Marshaller<V> ValueMarshaller;
        public int Segment = -1;
        public int TimeOut { get; set; }

        public ExpirationTime Lifespan = new ExpirationTime { Unit = TimeUnit.DEFAULT, Value = 0 };
        public ExpirationTime MaxIdle = new ExpirationTime { Unit = TimeUnit.DEFAULT, Value = 0 };

        public override string Name => "PUT_ALL";

        public override Byte Code => 0x2D;

        public IDictionary<K, V> Map { get; set; }
        public V PrevValue { get; set; }

        public override void OnExecute(CommandContext ctx)
        {
            base.OnExecute(ctx);
        }

        public override void Execute(CommandContext ctx, InfinispanClient client, PipeStream stream)
        {
            base.Execute(ctx, client, stream);
            Codec.writeExpirations(Lifespan, MaxIdle, stream);
            Codec.writeVInt(Map.Count, stream);
            foreach (var entry in Map)
            {
                Codec.writeArray(KeyMarshaller.marshall(entry.Key), stream);
                Codec.writeArray(ValueMarshaller.marshall(entry.Value), stream);
            }
            stream.Flush();
        }
        public override Result OnReceive(InfinispanRequest request, PipeStream stream)
        {
            return new Result { Status = ResultStatus.Completed, ResultType = ResultType.Null };
        }
    }
}
