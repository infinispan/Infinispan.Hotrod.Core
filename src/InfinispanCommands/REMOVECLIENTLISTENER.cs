﻿using BeetleX.Buffers;
using BeetleX.Clients;
using System;
using System.Collections.Generic;
using System.Text;

namespace Infinispan.Hotrod.Core.Commands
{
    public class REMOVECLIENTLISTENER : Command
    {
        public REMOVECLIENTLISTENER(IClientListener listener)
        {
            NetworkReceive = OnReceive;
            this.Listener = listener;
        }
        public override string Name => "REMOVECLIENTLISTENER";

        public override Byte Code => 0x27;
        public Byte ResponseCode => 0x28;

        internal override void OnExecute(CommandContext ctx)
        {
            base.OnExecute(ctx);
        }
        internal override void Execute(CommandContext ctx, InfinispanClient client, PipeStream stream)
        {
            base.Execute(ctx, client, stream);
            Codec.writeArray(StringMarshaller._ASCII.marshall(this.Listener.ListenerID), stream);
            stream.Flush();
        }

        public override Result OnReceive(InfinispanRequest request, ResponseStream stream)
        {
            if (request.ResponseStatus == Codec30.NO_ERROR_STATUS)
            {
                InfinispanRequest oldReq;
                if (request.Cluster.ListenerMap.TryGetValue(this.Listener.ListenerID, out oldReq))
                {
                    request.Cluster.ListenerMap.Remove(this.Listener.ListenerID);
                    oldReq.rs.TokenSource.Cancel();
                }
                return new Result { Status = ResultStatus.Completed, ResultType = ResultType.Object };
            }
            return new Result { Status = ResultStatus.Completed, ResultType = ResultType.Error };
        }
    }
}