using BeetleX;
using BeetleX.Buffers;
using BeetleX.Clients;
using BeetleX.Tracks;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
namespace Infinispan.Hotrod.Core
{
    public class InfinispanRequest
    {
        public InfinispanRequest(InfinispanHost host, InfinispanDG cluster, UntypedCache cache, InfinispanClient client, Command cmd, params Type[] types) {
            Client = client;
            Client.TcpClient.DataReceive = OnReceive;
            Client.TcpClient.ClientError = OnError;
            Command = cmd;
            Host = host;
            Types = types;
            Host = host;
            Cluster = cluster;
            Cache = cache;
            context.MessageId = host.MessageId;
            context.ClientIntelligence = Cluster.ClientIntelligence;
            context.Version = Cluster.Version;
            context.TopologyId = Cluster.TopologyId;
            if (cache != null) {
                context.NameAsBytes = cache.NameAsBytes;
                context.KeyMediaType = cache.KeyMediaType;
                context.ValueMediaType = cache.ValueMediaType;
            } else {
                context.NameAsBytes = new byte[]{};
            }
        }
        internal XActivity Activity { get; set; }
        private byte ResponseOpCode;
        public byte ResponseStatus;

        public CommandContext context = new CommandContext();
        public InfinispanHost Host { get; set; }
        public InfinispanDG Cluster { get; set; }

        public UntypedCache Cache {get; set;}
        private void OnError(IClient c, ClientErrorArgs e)
        {
            if (e.Error is BeetleX.BXException || e.Error is System.Net.Sockets.SocketException ||
                e.Error is System.ObjectDisposedException)
            {
                c.DisConnect();
                OnCompleted(ResultType.NetError, e.Error.Message);
            }
            else
            {
                OnCompleted(ResultType.DataError, e.Error.Message);
            }
        }

        public Type[] Types { get; private set; }

        private int mFreeLength;

        private void FreeLine(PipeStream stream)
        {
            if (stream.Length >= 2)
            {
                stream.ReadFree(2);
                mFreeLength = 0;
            }
            else
            {
                mFreeLength = 2;
            }
        }

        private CodeTrack mReceiveTrack;

        private void OnReceive(IClient c, ClientReceiveArgs reader)
        {
            mReceiveTrack = CodeTrackFactory.Track("Read", CodeTrackLevel.Function, Activity?.Id, "Infinispan", "Protocol");

            var stream = reader.Stream.ToPipeStream();

            if (stream.ReadByte()!=0xA1) {
                Result.ResultType=ResultType.Error;  // TODO: needed some design for errors
                Result.Messge="Bad Magic NUmber";
                OnCompleted(Result.ResultType, Result.Messge);
                return;
            }

            if (Codec.readVLong(stream)!=context.MessageId) {
                Result.ResultType=ResultType.Error;  // TODO: needed some design for errors
                Result.Messge="Message ID mistmatch";
                OnCompleted(Result.ResultType, Result.Messge);
                return;
            }

            ResponseOpCode = (byte) stream.ReadByte();
            ResponseStatus = (byte) stream.ReadByte();
            var topologyChanged = (byte) stream.ReadByte();
            if (topologyChanged!=0) {
                var topology=readNewTopologyInfo(stream);
                Cluster.UpdateTopologyInfo(topology, this.Cache);
            }
            var errMsg = readResponseError(ResponseStatus, stream);
            if (errMsg != null) {
                Result.ResultType=ResultType.Error;  // TODO: needed some design for errors
                Result.Messge=Encoding.ASCII.GetString(errMsg);
                OnCompleted(Result.ResultType, Result.Messge);
                return;
            }
            // Here ends the processing of the response header
            // commmand specific data processed below
            Result = Command.OnReceive(this, stream);
            OnCompleted(Result.ResultType, Result.Messge);
        }

        private TopologyInfo readNewTopologyInfo(PipeStream stream) {
            var t = new TopologyInfo();
            t.TopologyId=Codec.readVUInt(stream);
            var serversNum = Codec.readVInt(stream);
            t.servers = new List<Tuple<byte[], ushort>>();
            t.hosts = new InfinispanHost[serversNum];
            for (int i=0; i< serversNum; i++) {
                var addr = Codec.readArray(stream);
                var port = Codec.readUnsignedShort(stream);
                t.servers.Add(Tuple.Create(addr,port));
            }
            //  TODO: check if   clientIntelligence==CLIENT_INTELLIGENCE_HASH_DISTRIBUTION_AWARE
            t.HashFuncNum = (byte) stream.ReadByte();
            if (t.HashFuncNum > 0) {
                var segmentsNum = Codec.readVInt(stream);
                t.OwnersPerSegment = new List<List<Int32>>();
                for (int i=0; i < segmentsNum; i++) {
                    var ownerNumPerSeg = (byte) stream.ReadByte();
                    var owners = new List<Int32>();
                    for (int j=0; j < ownerNumPerSeg; j++) {
                        owners.Add(Codec.readVInt(stream));
                    }
                    t.OwnersPerSegment.Add(owners);                    
                }
            }
            return t;
        }
        private byte[] readResponseError(byte status, PipeStream stream) {
            if (Codec30.hasError(status)) {
                return Codec.readArray(stream);
            }
            return null;
        }
        public Action<InfinispanRequest> Completed { get; set; }

        protected Result Result { get; set; } = new Result();

        public Command Command { get; private set; }

        public InfinispanClient Client { get; private set; }

        public TaskCompletionSource<Result> TaskCompletionSource { get; protected set; }

        internal void SendCommmand(Command cmd)
        {
            try
            {
                Client.Send(context, cmd);
                if (!Client.TcpClient.IsConnected)
                {
                    OnCompleted(ResultType.NetError, "Connection is closed!");
                }
            }
            catch (Exception e_)
            {
                OnCompleted(ResultType.DataError, e_.Message);
            }
        }

        public Task<Result> Execute()
        {

            TaskCompletionSource = new TaskCompletionSource<Result>();
            SendCommmand(Command);
            return TaskCompletionSource.Task;

        }

        private int mCompletedStatus = 0;

        public virtual void OnCompleted(ResultType type, string message)
        {
            if (System.Threading.Interlocked.CompareExchange(ref mCompletedStatus, 1, 0) == 0)
            {
                Result.Status = ResultStatus.Completed;
                Client.TcpClient.DataReceive = null;
                Client.TcpClient.ClientError = null;
                Result.ResultType = type;
                Result.Messge = message;
                Completed?.Invoke(this);
                mReceiveTrack?.Dispose();
                mReceiveTrack = null;
                // TODO: no SELECT or AUTH command in Infinispan, correct implementation needed here
                // TaskCompletion();
                // if (Command.GetType() == typeof(SELECT) || Command.GetType() == typeof(AUTH))
                // {
                //     Task.Run(() => TaskCompletion());
                // }
                // else
                {
                    //TODO: check if there's something equivalent in Infinispan and reenable it in case
                    // if (ResultDispatch.UseDispatch)
                    //     ResultDispatch.DispatchCenter.Enqueue(this, 3);
                    // else
                        TaskCompletion();
                }

            }

        }

        internal void TaskCompletion()
        {
            TaskCompletionSource.TrySetResult(Result);
        }
    }
    public class TopologyInfo {
        public  UInt32 TopologyId;
        public List<Tuple <byte[], UInt16>> servers;
        public InfinispanHost []hosts;
        public byte HashFuncNum;
        public List<List<Int32>> OwnersPerSegment;
    }
}
