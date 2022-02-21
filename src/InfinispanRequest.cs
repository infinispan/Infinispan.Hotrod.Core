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
        static public int count = 0;
        public int ide;
        public InfinispanRequest(InfinispanHost host, InfinispanDG cluster, ICache cache, InfinispanClient client, Command cmd, params Type[] types)
        {
            ide = ++count;
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
            if (cache != null)
            {
                context.NameAsBytes = cache.NameAsBytes;
                context.KeyMediaType = cache.KeyMediaType;
                context.ValueMediaType = cache.ValueMediaType;
            }
            else
            {
                context.NameAsBytes = new byte[] { };
            }
        }
        internal byte ResponseOpCode;
        public byte ResponseStatus;
        public CommandContext context = new CommandContext();
        public InfinispanHost Host { get; set; }
        public InfinispanDG Cluster { get; set; }
        public ICache Cache { get; set; }
        public IClientListener Listener;
        private void OnError(IClient c, ClientErrorArgs e)
        {
            if (this.rs != null)
            {
                this.rs.TokenSource.Cancel();
            }
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
                    if (rs.ReadByte() != 0xA1)
                    {
                        Result.ResultType = ResultType.Error;  // TODO: needed some design for errors
                        Result.Messge = "Bad Magic Number";
                        OnCompleted(Result.ResultType, Result.Messge);
                        return;
                    }
                    long inMessageId = Codec.readVLong(rs);

                    if (inMessageId != 0 && inMessageId != context.MessageId)
                    {
                        Result.ResultType = ResultType.Error;  // TODO: needed some design for errors
                        Result.Messge = "Message ID mistmatch";
                        OnCompleted(Result.ResultType, Result.Messge);
                        return;
                    }
                    ResponseOpCode = (byte)rs.ReadByte();
                    ResponseStatus = (byte)rs.ReadByte();
                    var topologyChanged = (byte)rs.ReadByte();
                    if (topologyChanged != 0)
                    {
                        var topology = readNewTopologyInfo(rs);
                        // No need to update if monitor can't be taken
                        // see InfinispanDG.SwitchCluster
                        if (Monitor.TryEnter(Cluster.mActiveCluster))
                        {
                            try
                            {
                                Cluster.UpdateTopologyInfo(topology, this.Cache);
                            }
                            finally
                            {
                                Monitor.Exit(Cluster.mActiveCluster);
                            }
                        }
                    }
                    var errMsg = readResponseError(ResponseStatus, rs);
                    if (errMsg != null)
                    {
                        Result.ResultType = ResultType.Error; // TODO: needed some design for errors
                        Result.Messge = Encoding.ASCII.GetString(errMsg);
                        OnCompleted(Result.ResultType, Result.Messge);
                        return;
                    }
                    // Here ends the processing of the response header
                    // commmand specific data processed below
                    if (ResponseOpCode == 0x60 || ResponseOpCode == 0x61 || ResponseOpCode == 0x62 || ResponseOpCode == 0x63)
                    {
                        var ev = this.OnReceiveEvent(this, rs);
                        this.Cluster.ListenerMap[ev.ListenerID].Listener.OnEvent(ev);
                        continue;
                    }
                    Result = Command.OnReceive(this, rs);
                    if (Result.Status == ResultStatus.Completed)
                    {
                        OnCompleted(Result.ResultType, Result.Messge);
                        if (Result.ResultType != ResultType.Event)
                        {
                            rs.ResponseChannel.Writer.Complete();
                        }
                    }
                } while (Result.ResultType == ResultType.Event);
            }
            catch (Exception ex)
            {
                if (Cluster.ListenerMap.ContainsKey(this.Listener?.ListenerID))
                {
                    Cluster.ListenerMap.Remove(this.Listener.ListenerID);
                    this.Listener.OnError(ex);
                    this.Host.Push(this.Client);
                    if (!(ex is TaskCanceledException))
                    {
                        if (!(ex is AggregateException) && !(((AggregateException)ex).InnerException is TaskCanceledException))
                        {
                            // TODO: unexpected exception. log something
                            throw;
                        }
                    }
                }
            }
        }
        internal Event OnReceiveEvent(InfinispanRequest request, ResponseStream stream)
        {
            var listenerId = StringMarshaller._ASCII.unmarshall(Codec.readArray(stream));
            Event e = new Event();
            e.ListenerID = listenerId;
            e.CustomMarker = (byte)stream.ReadByte();
            if (e.CustomMarker == 0)
            {
                e.Retried = (byte)stream.ReadByte();
                e.Key = Codec.readArray(stream);
                if (Enum.IsDefined(typeof(EventType), (Int32)request.ResponseOpCode))
                {
                    e.Type = (EventType)request.ResponseOpCode;
                }
                if (e.Type != EventType.REMOVED && e.Type != EventType.EXPIRED)
                {
                    e.Version = Codec.readLong(stream);
                }
            }
            else
            {
                e.customData = Codec.readArray(stream);
            }
            return e;
        }

        private TopologyInfo readNewTopologyInfo(ResponseStream stream)
        {
            var t = new TopologyInfo();
            t.TopologyId = Codec.readVUInt(stream);
            var serversNum = Codec.readVInt(stream);
            t.servers = new List<Tuple<byte[], ushort>>();
            t.hosts = new InfinispanHost[serversNum];
            for (int i = 0; i < serversNum; i++)
            {
                var addr = Codec.readArray(stream);
                var port = Codec.readUnsignedShort(stream);
                t.servers.Add(Tuple.Create(addr, port));
            }
            //  TODO: check if   clientIntelligence==CLIENT_INTELLIGENCE_HASH_DISTRIBUTION_AWARE
            t.HashFuncNum = (byte)stream.ReadByte();
            if (t.HashFuncNum > 0)
            {
                var segmentsNum = Codec.readVInt(stream);
                t.OwnersPerSegment = new List<List<Int32>>();
                for (int i = 0; i < segmentsNum; i++)
                {
                    var ownerNumPerSeg = (byte)stream.ReadByte();
                    var owners = new List<Int32>();
                    for (int j = 0; j < ownerNumPerSeg; j++)
                    {
                        owners.Add(Codec.readVInt(stream));
                    }
                    t.OwnersPerSegment.Add(owners);
                }
            }
            return t;
        }
        private byte[] readResponseError(byte status, ResponseStream stream)
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
            TaskCompletionSource = new TaskCompletionSource<Result>();
            var processResponseTask = Task.Run(() => ProcessResponse());
            SendCommmand(Command);
            return TaskCompletionSource.Task;
        }
        private int mCompletedStatus = 0;
        private int mEventLoopStatus = 0;
        public virtual void OnCompleted(ResultType type, string message)
        {
            // if (type == ResultType.NetError && this.Listener != null)
            // {
            //     this.Listener.OnError();
            // }
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
    public class TopologyInfo
    {
        public UInt32 TopologyId;
        public List<Tuple<byte[], UInt16>> servers;
        public InfinispanHost[] hosts;
        public byte HashFuncNum;
        public List<List<Int32>> OwnersPerSegment;
    }
}
