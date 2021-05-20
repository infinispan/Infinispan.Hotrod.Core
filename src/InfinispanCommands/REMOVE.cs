using System;
using System.Collections.Generic;
using System.Text;
using BeetleX.Buffers;

namespace Infinispan.Hotrod.Core.Commands
{
    public class REMOVE<K,V> : Command
    {
        public REMOVE(Marshaller<K> km, Marshaller<V> vm, K key)
        {
            Key = key;
            KeyMarshaller = km;
            ValueMarshaller = vm;

            NetworkReceive = OnReceive;
        }
        public Marshaller<K> KeyMarshaller;
        public Marshaller<V> ValueMarshaller;
        public int TimeOut { get; set; }

        public ExpirationTime Lifespan = new ExpirationTime{ Unit = TimeUnit.DEFAULT, Value = 0};
        public ExpirationTime MaxIdle = new ExpirationTime{ Unit = TimeUnit.DEFAULT, Value = 0};

        public override string Name => "REMOVE";

        public override Byte Code => 0x0B;

        public K Key { get; set; }
        public V PrevValue { get; set; }
        public Boolean Removed;
        public override void OnExecute(UntypedCache cache)
        {
            base.OnExecute(cache);
        }

        public override void Execute(UntypedCache cache, InfinispanClient client, PipeStream stream)
        {
            base.Execute(cache, client, stream);
            Codec.writeArray(KeyMarshaller.marshall(Key), stream);
        }
        public override Result OnReceive(InfinispanRequest request, PipeStream stream)
        {
            Removed=!Codec30.isNotExecuted(request.ResponseStatus);
            if ((request.Command.Flags & 0x01) == 1 && request.ResponseStatus!=InfinispanRequest.KEY_DOES_NOT_EXIST_STATUS) {
                PrevValue = ValueMarshaller.unmarshall(Codec.readArray(stream));
                return new Result{ Status =  ResultStatus.Completed, ResultType = ResultType.Object };
            }
            return new Result{ Status =  ResultStatus.Completed, ResultType = ResultType.Null };
        }
    }
}
