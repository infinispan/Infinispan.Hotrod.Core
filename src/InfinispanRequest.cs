using BeetleX;
using BeetleX.Buffers;
using BeetleX.Clients;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using System.Threading;
using System.Threading.Channels;

namespace Infinispan.Hotrod.Core
{

    public class ResponseStream
    {
        public CancellationTokenSource TokenSource = new CancellationTokenSource();
        private static UnboundedChannelOptions opts = new UnboundedChannelOptions { SingleReader = true, SingleWriter = true };
        public Channel<ClientReceiveArgs> ResponseChannel = Channel.CreateUnbounded<ClientReceiveArgs>(opts);
        private ClientReceiveArgs CurrReader;


        public async Task<byte[]> ReadAsync(int size)
        {
            byte[] buf = new byte[size];
            int offSet = 0;
            do
            {
                if (CurrReader == null)
                {
                    CurrReader = await ResponseChannel.Reader.ReadAsync(TokenSource.Token);
                }
                var read = CurrReader.Stream.Read(buf, offSet, size - offSet);
                if (read == 0)
                {
                    CurrReader = null;
                    continue;
                }
                offSet += read;
            } while (offSet < size);
            return buf;
        }
        public async Task<int> ReadByteAsync()
        {
            int result;
            do
            {
                if (CurrReader == null)
                {
                    CurrReader = await ResponseChannel.Reader.ReadAsync(TokenSource.Token);
                }
                // Stream.ReadByte() should return -1 on empty stream
                // but it actually rises an exception. Checking both ways
                result = -1;
                try
                {
                    result = CurrReader.Stream.ReadByte();
                }
                catch (NullReferenceException)
                { }
                if (result == -1)
                {
                    CurrReader = null;
                }
            } while (result == -1);
            return result;
        }

        public byte[] Read(int size)
        {
            return ReadAsync(size).Result;
        }

        public int ReadByte()
        {
            return ReadByteAsync().Result;
        }
    }

    public class InfinispanRequest
    {
        public InfinispanRequest(CacheBase cache, InfinispanClient client, Command cmd)
        {
            Client = client;
            Client.TcpClient.DataReceive = OnReceive;
            Client.TcpClient.ClientError = OnError;
            Command = cmd;
            context = new CommandContext
            {
                MessageId = client.Host.NewMessageId(),
                Client = client,
                Cache = cache,
            };
        }
        internal byte ResponseOpCode;
        public byte ResponseStatus;
        internal CommandContext context;
        internal InfinispanDG Cluster { get { return Client.Host.Cluster; } }
        public IClientListener Listener;
        private void OnError(IClient c, ClientErrorArgs e)
        {
            this.peerDisconnect = true;
            this.rs?.TokenSource?.Cancel();
            c.DisConnect();
            if (e.Error is BeetleX.BXException || e.Error is System.Net.Sockets.SocketException ||
                e.Error is System.ObjectDisposedException)
            {
                OnCompleted(ResultType.NetError, e.Error.Message);
            }
            else
            {
                OnCompleted(ResultType.DataError, e.Error.Message);
            }
        }
        internal ResponseStream rs = new ResponseStream();
        private void OnReceive(IClient c, ClientReceiveArgs reader)
        {
            rs.ResponseChannel.Writer.WriteAsync(reader).AsTask().Wait();
        }

        private void ProcessResponse()
        {
            try
            {
                do
                {
                    if (ReadHeader())
                    {
                        // Processing commmand/event specific data
                        if (IsEvent(ResponseOpCode))
                        {
                            ProcessEvent();
                        }
                        else
                        {
                            ProcessCommandResponse();
                        }
                    }
                } while (Result.ResultType == ResultType.Event);
            }
            catch (Exception ex)
            {
                if (this.Listener!=null)
                {
                    Cluster.ListenerMap.Remove(this.Listener.ListenerID);
                }
                CompleteWithError(ex.Message);
                // Return Client to the pool only if TaskCompletionSource is already completed
                if (!TaskCompletionSource.TrySetException(ex))
                {
                    this.Client.ReturnToPool();
                }
                // If the exception is not related to a task cancellation by the client, then call the OnError recover function if any
                // and propagate the exception
                if (this.peerDisconnect)
                {
                    if (this.Listener != null)
                    {
                        Task.Run(() => { this.Listener.OnError(ex); });
                    }
                      // Propagate the exception
                       throw;
                }
            }
        }
        // Read the message header from the stream
        // Return false on error
        private bool ReadHeader()
        {
            if (rs.ReadByte() != 0xA1)
            {
                CompleteWithError("Bad Magic Number");
                return false;
            }
            long inMessageId = Codec.readVLong(rs);
            if (inMessageId != 0 && inMessageId != context.MessageId)
            {
                CompleteWithError("Message ID mistmatch");
                return false;
            }
            ResponseOpCode = (byte)rs.ReadByte();
            ResponseStatus = (byte)rs.ReadByte();
            var topologyChanged = (byte)rs.ReadByte();
            if (topologyChanged != 0)
            {
                ProcessTopologyChange();
            }
            var errMsg = ReadResponseError(ResponseStatus, rs);
            if (errMsg != null)
            {
                CompleteWithError(Encoding.ASCII.GetString(errMsg));
                return false;
            }
            return true;
        }

        private void ProcessCommandResponse()
        {
            Result = Command.OnReceive(this, rs);
            if (Result.Status == ResultStatus.Completed)
            {
                OnCompleted(Result.ResultType, Result.Messge);
                if (Result.ResultType != ResultType.Event)
                {
                    rs.ResponseChannel.Writer.Complete();
                }
            }
        }
        private void ProcessEvent()
        {
            var ev = this.OnReceiveEvent();
            InfinispanRequest req;
            if (Cluster.ListenerMap.TryGetValue(ev.ListenerID, out req))
            {
                Task.Run(() => { req.Listener.OnEvent(ev); });
            }
        }
        private bool IsEvent(byte responseOpCode)
        {
            return Enum.IsDefined(typeof(EventType), responseOpCode);
        }
        private void ProcessTopologyChange()
        {
            var topology = ReadNewTopologyInfo();
            // No need to update if monitor can't be taken
            // see InfinispanDG.SwitchCluster
            if (this.context.Cache != null && Monitor.TryEnter(Cluster.mActiveCluster))
            {
                try
                {
                    Cluster.UpdateTopologyInfo(topology, this.context.Cache);
                }
                finally
                {
                    Monitor.Exit(Cluster.mActiveCluster);
                }
            }
        }

        private void CompleteWithError(string msg)
        {
            Result.ResultType = ResultType.Error;  // TODO: needed some design for errors
            Result.Messge = msg;
            OnCompleted(Result.ResultType, Result.Messge);
        }

        internal Event OnReceiveEvent()
        {
            var listenerId = StringMarshaller._ASCII.unmarshall(Codec.readArray(rs));
            Event e = new Event
            {
                ListenerID = listenerId,
                CustomMarker = (byte)rs.ReadByte()
            };
            if (e.CustomMarker == 0)
            {
                e.Retried = (byte)rs.ReadByte();
                e.Key = Codec.readArray(rs);
                e.Type = (EventType)this.ResponseOpCode;
                if (e.Type != EventType.REMOVED && e.Type != EventType.EXPIRED)
                {
                    e.Version = Codec.readLong(rs);
                }
            }
            else
            {
                e.customData = Codec.readArray(rs);
            }
            return e;
        }

        private TopologyInfo ReadNewTopologyInfo()
        {
            var t = new TopologyInfo
            {
                TopologyId = Codec.readVUInt(rs)
            };
            var serversNum = Codec.readVInt(rs);
            t.servers = new List<Tuple<byte[], ushort>>();
            t.hosts = new InfinispanHost[serversNum];
            for (int i = 0; i < serversNum; i++)
            {
                var addr = Codec.readArray(rs);
                var port = Codec.readUnsignedShort(rs);
                t.servers.Add(Tuple.Create(addr, port));
            }
            //  TODO: check if   clientIntelligence==CLIENT_INTELLIGENCE_HASH_DISTRIBUTION_AWARE
            t.HashFuncNum = (byte)rs.ReadByte();
            if (t.HashFuncNum > 0)
            {
                var segmentsNum = Codec.readVInt(rs);
                t.OwnersPerSegment = new List<List<Int32>>();
                for (int i = 0; i < segmentsNum; i++)
                {
                    var ownerNumPerSeg = (byte)rs.ReadByte();
                    var owners = new List<Int32>();
                    for (int j = 0; j < ownerNumPerSeg; j++)
                    {
                        owners.Add(Codec.readVInt(rs));
                    }
                    t.OwnersPerSegment.Add(owners);
                }
            }
            return t;
        }
        private byte[] ReadResponseError(byte status, ResponseStream stream)
        {
            if (Codec30.hasError(status))
            {
                return Codec.readArray(stream);
            }
            return null;
        }
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
            TaskCompletionSource = new TaskCompletionSource<Result>(TaskCreationOptions.RunContinuationsAsynchronously);
            var processResponseTask = Task.Run(() => ProcessResponse());
            SendCommmand(Command);
            return TaskCompletionSource.Task;
        }
        private int mCompletedStatus = 0;
        private bool peerDisconnect;

        public virtual void OnCompleted(ResultType type, string message)
        {
            if (System.Threading.Interlocked.CompareExchange(ref mCompletedStatus, 1, 0) == 0)
            {
                Result.Status = ResultStatus.Completed;
                if (type != ResultType.Event)
                {
                    Client.TcpClient.ClientError = null;
                    Client.TcpClient.DataReceive = null;
                }
                Result.ResultType = type;
                Result.Messge = message;
                TaskCompletionSource.TrySetResult(Result);
            }
        }
    }
    public class TopologyInfo
    {
        public UInt32 TopologyId;
        public List<Tuple<byte[], UInt16>> servers;
        public InfinispanHost[] hosts;
        public byte HashFuncNum;
        public List<List<Int32>> OwnersPerSegment;
    }
}
