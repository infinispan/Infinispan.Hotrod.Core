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
        // TODO: find the right place for error codes
        public const byte NO_ERROR_STATUS                          = 0x00; ///< No error
        public const byte NOT_PUT_REMOVED_REPLACED_STATUS = 0x01;
        public const byte KEY_DOES_NOT_EXIST_STATUS = 0x02;
        public const byte SUCCESS_WITH_PREVIOUS = 0x03;
        public const byte NOT_EXECUTED_WITH_PREVIOUS = 0x04;
        public const byte INVALID_ITERATION = 0x05;
        public const byte NO_ERROR_STATUS_COMPAT = 0x06;
        public const byte SUCCESS_WITH_PREVIOUS_COMPAT = 0x07;
        public const byte NOT_EXECUTED_WITH_PREVIOUS_COMPAT = 0x08;
        public const byte INVALID_MAGIC_OR_MESSAGE_ID_STATUS = 0x81; ///< Invalid magic or message id
        public const byte UNKNOWN_COMMAND_STATUS             = 0x82; ///< Unknown command
        public const byte UNKNOWN_VERSION_STATUS             = 0x83; ///< Unknown version
        public const byte REQUEST_PARSING_ERROR_STATUS       = 0x84; ///< Request parsing error
        public const byte SERVER_ERROR_STATUS                = 0x85; ///< Server Error
        public const byte COMMAND_TIMEOUT_STATUS             = 0x86; ///< Command timed out
        public InfinispanRequest(UntypedCache cache, InfinispanHost host, InfinispanClient client, Command cmd, params Type[] types)
        {
            Client = client;
            Client.TcpClient.DataReceive = OnReceive;
            Client.TcpClient.ClientError = OnError;
            Command = cmd;
            Host = host;
            Types = types;
            Host = host;
            Cache = cache;
            MessageId = Cache.MessageId;
        }

        internal XActivity Activity { get; set; }
        private UInt64 MessageId;

        private byte ResponseOpCode;
        public byte ResponseStatus;

        public InfinispanHost Host { get; set; }

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

            if (Codec.readVLong(stream)!=MessageId) {
                Result.ResultType=ResultType.Error;  // TODO: needed some design for errors
                Result.Messge="Message ID mistmatch";
                OnCompleted(Result.ResultType, Result.Messge);
                return;
            }

            var ResponseOpCode = (byte) stream.ReadByte();
            var ResponseStatus = (byte) stream.ReadByte();
            var topologyChanged = (byte) stream.ReadByte();
            if (topologyChanged!=0) {
                var topology=readNewTopologyInfo(stream); // TODO: store topology in the righ place and use it to get the owner host for a key
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

        private object readNewTopologyInfo(PipeStream stream) {
            var t = new TopologyInfo();
            t.TopologyId=Codec.readVInt(stream);
            var serversNum = Codec.readVInt(stream);
            t.servers = new List<Tuple<byte[], ushort>>();
            for (int i=0; i< serversNum; i++) {
                var addr = Codec.readArray(stream);
                var port = Codec.readShort(stream);
                t.servers.Add(Tuple.Create(addr,port));
            }
            //  TODO: check if   clientIntelligence==CLIENT_INTELLIGENCE_HASH_DISTRIBUTION_AWARE
            t.HashFuncNum = (byte) stream.ReadByte();
            if (t.HashFuncNum > 0) {
                var segmentsNum = Codec.readVInt(stream);
                t.OwnersPerSegment = new List<List<UInt32>>();
                for (int i=0; i < segmentsNum; i++) {
                    var ownerNumPerSeg = (byte) stream.ReadByte();
                    var owners = new List<UInt32>();
                    for (int j=0; j < ownerNumPerSeg; j++) {
                        owners.Add(Codec.readVInt(stream));
                    }
                    t.OwnersPerSegment.Add(owners);                    
                }
            }
            return t;
        }

        private byte[] readResponseError(byte status, PipeStream stream) {
            switch (status) {
                case INVALID_MAGIC_OR_MESSAGE_ID_STATUS:
                case UNKNOWN_COMMAND_STATUS:
                case UNKNOWN_VERSION_STATUS:
                case REQUEST_PARSING_ERROR_STATUS:
                case SERVER_ERROR_STATUS:
                case COMMAND_TIMEOUT_STATUS:
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
                Client.Send(Cache, cmd);
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
        public byte HashFuncNum;
        public List<List<UInt32>> OwnersPerSegment;
    }
}
