using BeetleX.Buffers;
using BeetleX.Clients;
using System;
using System.Collections.Generic;
using System.Text;

namespace Infinispan.Hotrod.Core.Commands
{
    public class ADDCLIENTLISTENER : Command
    {
        private String ListenerID;
        public byte IncludeState;
        private String FilterFactoryName = "";
        private Tuple<byte[], byte[]>[] FilterArgs;
        private String ConverterFactoryName = "";
        private Tuple<byte[], byte[]>[] ConverterArgs;
        private int Interests;
        private bool isBinary;
        public IClientListener Listener;
        public ADDCLIENTLISTENER(String uuid)
        {
            NetworkReceive = OnReceive;
            this.ListenerID = uuid;
        }
        public override string Name => "ADDCLIENTLISTENER";

        public override Byte Code => 0x25;
        public Byte ResponseCode => 0x26;

        public override void OnExecute(CommandContext ctx)
        {
            base.OnExecute(ctx);
        }
        public override void Execute(CommandContext ctx, InfinispanClient client, PipeStream stream)
        {
            base.Execute(ctx, client, stream);
            Codec.writeArray(StringMarshaller._ASCII.marshall(this.ListenerID), stream);
            stream.WriteByte(this.IncludeState);
            if (!String.IsNullOrEmpty(this.FilterFactoryName))
            {
                Codec.writeArray(Encoding.ASCII.GetBytes(this.FilterFactoryName), stream);
                var argsLen = (byte)(this.FilterArgs == null ? 0 : this.FilterArgs.Length);
                stream.WriteByte(argsLen);
                for (var i = 0; i < argsLen; i++)
                {
                    Codec.writeArray(this.FilterArgs[i].Item1, stream);
                    Codec.writeArray(this.FilterArgs[i].Item2, stream);
                }
            }
            else
            {
                stream.WriteByte(0);
            }
            if (!String.IsNullOrEmpty(this.ConverterFactoryName))
            {
                Codec.writeArray(Encoding.ASCII.GetBytes(this.ConverterFactoryName), stream);
                var argsLen = (byte)(this.ConverterArgs == null ? 0 : this.ConverterArgs.Length);
                stream.WriteByte(argsLen);
                for (var i = 0; i < argsLen; i++)
                {
                    Codec.writeArray(this.ConverterArgs[i].Item1, stream);
                    Codec.writeArray(this.ConverterArgs[i].Item2, stream);
                }
            }
            else
            {
                stream.WriteByte(0);
            }
            Codec.writeVInt(this.Interests, stream);
            stream.WriteByte(isBinary ? (byte)1 : (byte)0);
            stream.Flush();
        }

        public override Result OnReceive(InfinispanRequest request, ResponseStream stream)
        {
            System.Diagnostics.Debug.WriteLine("OnReceive ADD");
            if (request.ResponseStatus == Codec30.NO_ERROR_STATUS)
            {
                InfinispanRequest oldReq;
                if (request.Cluster.ListenerMap.TryGetValue(this.ListenerID, out oldReq))
                {
                    oldReq.rs.TokenSource.Cancel();
                }
                request.Listener = this.Listener;
                request.Cluster.ListenerMap[this.ListenerID] = request;
                return new Result { Status = ResultStatus.Completed, ResultType = ResultType.Event };
            }
            return new Result { Status = ResultStatus.Completed, ResultType = ResultType.Error };
        }

    }
}