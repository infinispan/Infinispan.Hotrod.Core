﻿using BeetleX.Buffers;
using System;
using System.Collections.Generic;
using System.Text;

namespace Infinispan.Hotrod.Core.Commands
{
    public class GETWITHMETADATA<K, V> : CommandWithKey<K>
    {
        public GETWITHMETADATA(Marshaller<K> km, Marshaller<V> vm, K key)
        {
            Key = key;
            KeyMarshaller = km;
            ValueMarshaller = vm;
            NetworkReceive = OnReceive;
        }
        public Marshaller<V> ValueMarshaller;
        public ValueWithMetadata<V> ValueWithMetadata { get; set; }
        public override string Name => "GETWITHMETADATA";

        public override Byte Code => 0x1B;

        public override void OnExecute(CommandContext ctx)
        {
            base.OnExecute(ctx);
        }

        public override void Execute(CommandContext ctx, InfinispanClient client, PipeStream stream)
        {
            base.Execute(ctx, client, stream);
            Codec.writeArray(KeyMarshaller.marshall(Key), stream);
            stream.Flush();
        }

        public override Result OnReceive(InfinispanRequest request, ResponseStream stream)
        {
            if (request.ResponseStatus == Codec30.KEY_DOES_NOT_EXIST_STATUS)
            {
                return new Result { Status = ResultStatus.Completed, ResultType = ResultType.Null };
            }
            ValueWithMetadata = new ValueWithMetadata<V>();
            var expirationInfoFlags = (byte)stream.ReadByte();
            if ((expirationInfoFlags & 0x01) == 0)
            { // no infinite lifespan
                ValueWithMetadata.Created = (Int64)Codec.readLong(stream);
                ValueWithMetadata.Lifespan = (Int32)Codec.readVInt(stream);
            }
            if ((expirationInfoFlags & 0x02) == 0)
            { // no infinite maxidle
                ValueWithMetadata.LastUsed = (Int64)Codec.readLong(stream);
                ValueWithMetadata.MaxIdle = (Int32)Codec.readVInt(stream);
            }
            ValueWithMetadata.Version = Codec.readLong(stream);
            ValueWithMetadata.Value = ValueMarshaller.unmarshall(Codec.readArray(stream));
            return new Result { Status = ResultStatus.Completed, ResultType = ResultType.Object };
        }
    }
}