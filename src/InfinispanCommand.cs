using BeetleX.Buffers;
using System;
using System.Collections.Generic;
using System.Text;
using System.Collections.Concurrent;
using BeetleX.Tracks;

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
        internal XActivity Activity { get; set; }
        public Func<InfinispanRequest, PipeStream, Result> NetworkReceive { get; set; }
        public abstract string Name { get; }
        public abstract Byte Code { get; }
        public Int32 Flags {get; set;} // TODO: where to store this?
        public virtual void OnExecute(UntypedCache cache)
        {
        }

        public virtual void Execute(UntypedCache cache, InfinispanClient client, PipeStream stream)
        {
            using (var track = CodeTrackFactory.Track("Write", CodeTrackLevel.Function, Activity?.Id, "Infinispan", "Protocol"))
            {
                OnExecute(cache); // Build the message. But there's no need to build anything for hotrod
                stream.WriteByte(0xA0);
                Codec.writeVLong(cache.MessageId, stream);
                stream.Write(cache.Version);
                stream.Write(Code);
                Codec.writeArray(cache.NameAsBytes,stream);
                Codec.writeVInt(Flags,stream);
                stream.Write(cache.ClientIntelligence);
                Codec.writeVInt(cache.TopologyId, stream);
                Codec.writeMediaType(cache.KeyMediaType, stream);
                Codec.writeMediaType(cache.ValueMediaType, stream);
            }
        }

        public abstract Result OnReceive(InfinispanRequest request, PipeStream stream);
   }
}