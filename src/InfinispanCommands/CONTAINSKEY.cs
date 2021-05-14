using BeetleX.Buffers;
using System;
using System.Collections.Generic;
using System.Text;

namespace Infinispan.Hotrod.Core.Commands
{
    public class CONTAINSKEY<K> : Command
    {
        public CONTAINSKEY(Marshaller<K> km, K key)
        {
            Key = key;
            KeyMarshaller = km;
            NetworkReceive = OnReceive;
        }
        public Marshaller<K> KeyMarshaller;
        public K Key { get; set; }
        public override string Name => "CONTAINSKEY";

        public override Byte Code => 0x0F;
        public Boolean IsContained;

        public override void OnExecute(UntypedCache cache)
        {
            // TODO: here the code to build the bytebuffer that will be sent
            base.OnExecute(cache); // Generic code (build header?)
        }

        public override void Execute(UntypedCache cache, InfinispanClient client, PipeStream stream)
        {
            base.Execute(cache, client, stream);
            Codec.writeArray(KeyMarshaller.marshall(Key), stream);
        }

        public override Result OnReceive(InfinispanRequest request, PipeStream stream)
        {
            IsContained = request.ResponseStatus==InfinispanRequest.NO_ERROR_STATUS;
            return new Result{ Status =  ResultStatus.Completed, ResultType = ResultType.Object };
        }
    }
}