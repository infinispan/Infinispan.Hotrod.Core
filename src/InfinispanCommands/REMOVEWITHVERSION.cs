using System;
using System.Collections.Generic;
using System.Text;
using BeetleX.Buffers;

namespace Infinispan.Hotrod.Core.Commands
{
    public class REMOVEWITHVERSION<K,V> : Command
    {
        public REMOVEWITHVERSION(Marshaller<K> km, Marshaller<V> vm, K key)
        {
            Key = key;
            KeyMarshaller = km;
            ValueMarshaller = vm;

            NetworkReceive = OnReceive;
        }
        public Marshaller<K> KeyMarshaller;
        public Marshaller<V> ValueMarshaller;
        public int TimeOut { get; set; }
        public UInt64 Version;
        public override string Name => "REPLACEWITHVERSION";
        public override Byte Code => 0x0D;
        public K Key { get; set; }
        public V PrevValue { get; private set;}
        public Boolean Removed;
        public override void OnExecute(UntypedCache cache)
        {
            // TODO: here the code to build the bytebuffer that will be sent
            base.OnExecute(cache); // Generic code (build header?)
        }
        public override void Execute(UntypedCache cache, InfinispanClient client, PipeStream stream)
        {
            base.Execute(cache, client, stream);
            Codec.writeArray(KeyMarshaller.marshall(Key), stream);
            Codec.writeLong(Version, stream);
            stream.Flush();
        }
        public override Result OnReceive(InfinispanRequest request, PipeStream stream)
        {
            Removed=Codec30.isSuccess(request.ResponseStatus);
            if ((request.Command.Flags & 0x01) == 1 && Codec30.hasPrevious(request.ResponseStatus))
            {
                var retValAsArray = Codec.readArray(stream);
                if (retValAsArray.Length>0) {
                    PrevValue = ValueMarshaller.unmarshall(retValAsArray);
                    return new Result{ Status =  ResultStatus.Completed, ResultType = ResultType.Object };
                }
            }
            return new Result{ Status =  ResultStatus.Completed, ResultType = ResultType.Null };
        }
    }
}
