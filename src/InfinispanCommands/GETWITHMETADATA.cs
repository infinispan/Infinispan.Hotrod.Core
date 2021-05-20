﻿using BeetleX.Buffers;
using System;
using System.Collections.Generic;
using System.Text;

namespace Infinispan.Hotrod.Core.Commands
{
    public class GETWITHMETADATA<K,V> : Command
    {
        public GETWITHMETADATA(Marshaller<K> km, Marshaller<V> vm, K key)
        {
            Key = key;
            KeyMarshaller = km;
            ValueMarshaller = vm;
            NetworkReceive = OnReceive;
        }
        public Marshaller<K> KeyMarshaller;
        public Marshaller<V> ValueMarshaller;

        public K Key { get; set; }
        public ValueWithMetadata<V> ValueWithMetadata { get; set; }
        public override string Name => "GETWITHMETADATA";

        public override Byte Code => 0x1B;

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
            ValueWithMetadata = new ValueWithMetadata<V>();
            var expirationInfoFlags = (byte)stream.ReadByte();
            if ((expirationInfoFlags & 0x01) == 0)  { // no infinite lifespan
                ValueWithMetadata.Created = (Int64)Codec.readLong(stream);
                ValueWithMetadata.Lifespan = (Int32)Codec.readVInt(stream);
            }
            if ((expirationInfoFlags & 0x02) == 0)  { // no infinite maxidle
                ValueWithMetadata.LastUsed = (Int64)Codec.readLong(stream);
                ValueWithMetadata.MaxIdle = (Int32)Codec.readVInt(stream);
            }
            ValueWithMetadata.Version = Codec.readLong(stream);
            ValueWithMetadata.Value = ValueMarshaller.unmarshall(Codec.readArray(stream));
            return new Result{ Status =  ResultStatus.Completed, ResultType = ResultType.Object };
        }
    }
}