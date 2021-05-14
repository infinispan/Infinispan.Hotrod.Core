using System;
using System.Collections.Generic;
using System.Text;
using BeetleX.Buffers;

namespace Infinispan.Hotrod.Core.Commands
{
    public class SET<K,V> : Command
    {
        public SET(Marshaller<K> km, Marshaller<V> vm, K key, V data)
        {
            Key = key;
            Value = data;
            KeyMarshaller = km;
            ValueMarshaller = vm;

            NetworkReceive = OnReceive;
        }
        public Marshaller<K> KeyMarshaller;
        public Marshaller<V> ValueMarshaller;
        public int TimeOut { get; set; }

        public ExpirationTime Lifespan = new ExpirationTime{ Unit = TimeUnit.DEFAULT, Value = 0};
        public ExpirationTime MaxIdle = new ExpirationTime{ Unit = TimeUnit.DEFAULT, Value = 0};

        public override string Name => "SET";

        public override Byte Code => 0x01;

        public K Key { get; set; }

        public V Value { get; set; }
        public V PrevValue { get; set; }

        public override void OnExecute(Cache cache)
        {
            // TODO: here the code to build the bytebuffer that will be sent
            base.OnExecute(cache); // Generic code (build header?)
        }

        public override void Execute(Cache cache, InfinispanClient client, PipeStream stream)
        {
            base.Execute(cache, client, stream);
            Codec.writeArray(KeyMarshaller.marshall(Key), stream);
            Codec.writeExpirations(Lifespan, MaxIdle, stream);
            Codec.writeArray(ValueMarshaller.marshall(Value), stream);
            stream.Flush();
        }
        public override Result OnReceive(InfinispanRequest request, PipeStream stream)
        {
            if ((request.Command.Flags & 0x01) == 1) {
                PrevValue = ValueMarshaller.unmarshall(Codec.readArray(stream));
                return new Result{ Status =  ResultStatus.Completed, ResultType = ResultType.Object };
            }
            return new Result{ Status =  ResultStatus.Completed, ResultType = ResultType.Null };
        }
    }
}
