using System;
using System.Collections.Generic;
using System.Text;
using BeetleX.Buffers;

namespace Infinispan.Hotrod.Core.Commands
{
    public class PUTIFABSENT<K, V> : CommandWithKey<K>
    {
        public PUTIFABSENT(Marshaller<K> km, Marshaller<V> vm, K key, V data)
        {
            Key = key;
            Value = data;
            KeyMarshaller = km;
            ValueMarshaller = vm;

            NetworkReceive = OnReceive;
        }
        public Marshaller<V> ValueMarshaller;
        public int TimeOut { get; set; }

        public ExpirationTime Lifespan = new ExpirationTime { Unit = TimeUnit.DEFAULT, Value = 0 };
        public ExpirationTime MaxIdle = new ExpirationTime { Unit = TimeUnit.DEFAULT, Value = 0 };

        public override string Name => "PUTIFABSENT";

        public override Byte Code => 0x05;

        public V Value { get; set; }
        public V PrevValue { get; set; }

        public override void OnExecute(CommandContext ctx)
        {
            base.OnExecute(ctx);
        }

        public override void Execute(CommandContext ctx, InfinispanClient client, PipeStream stream)
        {
            base.Execute(ctx, client, stream);
            Codec.writeArray(KeyMarshaller.marshall(Key), stream);
            Codec.writeExpirations(Lifespan, MaxIdle, stream);
            Codec.writeArray(ValueMarshaller.marshall(Value), stream);
            stream.Flush();
        }
        public override Result OnReceive(InfinispanRequest request, ResponseStream stream)
        {
            if ((request.Command.Flags & 0x01) == 1)
            {
                PrevValue = ValueMarshaller.unmarshall(Codec.readArray(stream));
                return new Result { Status = ResultStatus.Completed, ResultType = ResultType.Object };
            }
            return new Result { Status = ResultStatus.Completed, ResultType = ResultType.Null };
        }
    }
}
