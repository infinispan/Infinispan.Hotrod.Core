﻿using System;
using System.Collections.Generic;
using System.Text;
using BeetleX.Buffers;

namespace Infinispan.Hotrod.Core.Commands
{
    public class REPLACE<K, V> : CommandWithKey<K>, ICommandWithExpiration
    {
        public REPLACE(Marshaller<K> km, Marshaller<V> vm, K key, V data)
        {
            Key = key;
            Value = data;
            KeyMarshaller = km;
            ValueMarshaller = vm;

            NetworkReceive = OnReceive;
        }
        public Marshaller<V> ValueMarshaller;
        public int TimeOut { get; set; }
        public ExpirationTime Lifespan { get; set; } = new ExpirationTime { Unit = TimeUnit.DEFAULT, Value = 0 };
        public ExpirationTime MaxIdle { get; set; } = new ExpirationTime { Unit = TimeUnit.DEFAULT, Value = 0 };
        public override string Name => "REPLACE";
        public override Byte Code => 0x07;
        public V Value { get; set; }
        public V PrevValue { get; private set; }
        public Boolean Replaced { get; private set; }


        internal override void OnExecute(CommandContext ctx)
        {
            base.OnExecute(ctx);
        }
        internal override void Execute(CommandContext ctx, InfinispanClient client, PipeStream stream)
        {
            base.Execute(ctx, client, stream);
            Codec.writeArray(KeyMarshaller.marshall(Key), stream);
            Codec.writeExpirations(Lifespan, MaxIdle, stream);
            Codec.writeArray(ValueMarshaller.marshall(Value), stream);
            stream.Flush();
        }
        public override Result OnReceive(InfinispanRequest request, ResponseStream stream)
        {
            Replaced = Codec30.isSuccess(request.ResponseStatus);
            if ((request.Command.Flags & 0x01) == 1 && Codec30.hasPrevious(request.ResponseStatus))
            {
                var retValAsArray = Codec.readArray(stream);
                if (retValAsArray.Length > 0)
                {
                    PrevValue = ValueMarshaller.unmarshall(retValAsArray);
                    return new Result { Status = ResultStatus.Completed, ResultType = ResultType.Object };
                }
            }
            return new Result { Status = ResultStatus.Completed, ResultType = ResultType.Null };
        }
    }
}
