using BeetleX.Buffers;
using System;
using System.Collections.Generic;
using System.Text;
using System.Collections.Concurrent;

namespace Infinispan.Hotrod.Core
{
    public abstract class Command
    {
        private const int MAX_LENGTH_TABLE = 1024 * 32;

        public static List<byte[]> mMsgHeaderLenData = new List<byte[]>();

        public static byte[] GetMsgHeaderLengthData(int length)
        {
            if (length > MAX_LENGTH_TABLE)
                return null;
            return mMsgHeaderLenData[length - 1];
        }

        public static List<byte[]> mBodyHeaderLenData = new List<byte[]>();

        public static byte[] GetBodyHeaderLenData(int length)
        {
            if (length > MAX_LENGTH_TABLE)
                return null;
            return mBodyHeaderLenData[length - 1];
        }

        public Command(Int32 flags = 0)
        {
            Flags = flags;
        }
        public Func<InfinispanRequest, ResponseStream, Result> NetworkReceive { get; set; }
        public abstract string Name { get; }
        public abstract Byte Code { get; }
        public Int32 Flags { get; set; } // TODO: where to store this?
        internal virtual void OnExecute(CommandContext cache)
        {
        }

        internal virtual void Execute(CommandContext ctx, InfinispanClient client, PipeStream stream)
        {
            OnExecute(ctx); // Build the message. But there's no need to build anything for hotrod
            stream.WriteByte(0xA0);
            Codec.writeVLong(ctx.MessageId, stream);
            stream.Write(ctx.Version);
            stream.Write(Code);
            Codec.writeArray(ctx.NameAsBytes, stream);
            Codec.writeVInt(Flags, stream);
            stream.Write(ctx.ClientIntelligence);
            Codec.writeVUInt(ctx.TopologyId, stream);
            if (ctx.IsReqResCommand)
            {
                Codec.writeMediaType(ctx.CmdReqMediaType, stream);
                Codec.writeMediaType(ctx.CmdResMediaType, stream);
            }
            else
            {
                Codec.writeMediaType(ctx.KeyMediaType, stream);
                Codec.writeMediaType(ctx.ValueMediaType, stream);
            }
        }

        public abstract Result OnReceive(InfinispanRequest request, ResponseStream stream);

        internal enum TopologyKnoledge
        {
            NONE,
            KEY,
            SEGMENT
        }
        internal virtual TopologyKnoledge getTopologyKnowledgeType()
        {
            return TopologyKnoledge.NONE;
        }

        internal virtual byte[] getKeyAsBytes()
        {
            throw new NotImplementedException();
        }

        internal virtual int getSegment()
        {
            throw new NotImplementedException();
        }

    }
    public abstract class CommandWithKey<K> : Command
    {
        public Marshaller<K> KeyMarshaller;
        public K Key { get; set; }
        internal override TopologyKnoledge getTopologyKnowledgeType()
        {
            return TopologyKnoledge.KEY;
        }

        internal override byte[] getKeyAsBytes()
        {
            return KeyMarshaller.marshall(this.Key);
        }
    }
}