using BeetleX.Buffers;
using System;
using System.Collections.Generic;
using System.Text;

namespace Infinispan.Hotrod.Core.Commands
{
    public class GETWITHVERSION<K,V> : Command
    {
        public GETWITHVERSION(Marshaller<K> km, Marshaller<V> vm, K key)
        {
            Key = key;
            KeyMarshaller = km;
            ValueMarshaller = vm;
            NetworkReceive = OnReceive;
        }
        public Marshaller<K> KeyMarshaller;
        public Marshaller<V> ValueMarshaller;

        public K Key { get; set; }
        public ValueWithVersion<V> ValueWithVersion { get; set; }
        public override string Name => "GETWITHVERSION";

        public override Byte Code => 0x11;

        public override void OnExecute(UntypedCache cache)
        {
            // TODO: here the code to build the bytebuffer that will be sent
            base.OnExecute(cache); // Generic code (build header?)
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
            ValueWithVersion = new ValueWithVersion<V>();
            ValueWithVersion.Version = Codec.readLong(stream);
            ValueWithVersion.Value = ValueMarshaller.unmarshall(Codec.readArray(stream));
            return new Result{ Status =  ResultStatus.Completed, ResultType = ResultType.Object };
        }
    }
}