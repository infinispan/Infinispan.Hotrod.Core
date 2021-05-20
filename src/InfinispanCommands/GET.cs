using BeetleX.Buffers;
using System;
using System.Collections.Generic;
using System.Text;

namespace Infinispan.Hotrod.Core.Commands
{
    public class GET<K,V> : Command
    {
        public GET(Marshaller<K> km, Marshaller<V> vm, K key)
        {
            Key = key;
            KeyMarshaller = km;
            ValueMarshaller = vm;
            NetworkReceive = OnReceive;
        }
        public Marshaller<K> KeyMarshaller;
        public Marshaller<V> ValueMarshaller;

        public K Key { get; set; }
        public V Value { get; set; }
        public override string Name => "GET";

        public override Byte Code => 0x03;

        public override void OnExecute(UntypedCache cache)
        {
            base.OnExecute(cache);
        }

        public override void Execute(UntypedCache cache, InfinispanClient client, PipeStream stream)
        {
            base.Execute(cache, client, stream);
            Codec.writeArray(KeyMarshaller.marshall(Key), stream);
            stream.Flush();
        }

        public override Result OnReceive(InfinispanRequest request, PipeStream stream)
        {
            if (request.ResponseStatus == InfinispanRequest.KEY_DOES_NOT_EXIST_STATUS) {
                return new Result{ Status =  ResultStatus.Completed, ResultType = ResultType.Null };
            }
            Value = ValueMarshaller.unmarshall(Codec.readArray(stream));
            return new Result{ Status =  ResultStatus.Completed, ResultType = ResultType.Object };
        }
    }
}