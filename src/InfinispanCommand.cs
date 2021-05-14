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

        public Command(UInt32 flags = 0)
        {
            Flags = flags;
        }

        internal XActivity Activity { get; set; }

        public Func<Result, PipeStream, InfinispanClient, bool> Reader { get; set; }

        public Func<InfinispanRequest, PipeStream, Result> NetworkReceive { get; set; }

        public IDataFormater DataFormater { get; set; }

        public abstract string Name { get; }

        public abstract Byte Code { get; }
        public UInt32 Flags {get; set;} // TODO: where to store this?
        private List<CommandParameter> mParameters = new List<CommandParameter>();

        private ConcurrentDictionary<string, byte[]> mCommandBuffers = new ConcurrentDictionary<string, byte[]>();

        public Command AddText(object text)
        {
            mParameters.Add(new CommandParameter { Value = text });
            return this;
        }

        public Command AddData(object data)
        {
            if (data is ArraySegment<byte> buffer)
            {
                mParameters.Add(new CommandParameter { DataBuffer = buffer });
            }
            else
            {
                mParameters.Add(new CommandParameter { Value = data, DataFormater = this.DataFormater, Serialize = true });
            }
            return this;
        }

        public virtual void OnExecute(Cache cache)
        {
        }

        public virtual void Execute(Cache cache, InfinispanClient client, PipeStream stream)
        {
            // TODO: here where the byte buffer is streamed. Caller will then flush the data
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

        public class CommandParameter
        {
            public object Value { get; set; }

            public IDataFormater DataFormater { get; set; }

            internal byte[] ValueBuffer { get; set; }

            public ArraySegment<byte> DataBuffer { get; set; }

            public bool Serialize
            {
                get; set;
            } = false;
        }
   }
}