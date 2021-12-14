using System;
using System.Collections;
using System.Collections.Generic;
using System.Threading.Tasks;
namespace Infinispan.Hotrod.Core
{
    // Describes an Infinispan node
    public class InfinispanHost : IDisposable
    {
        public InfinispanHost(bool ssl, InfinispanDG cluster, string host, int port = 6379)
        {
            SSL = ssl;
            Name = host;
            Port = port;
            Cluster = cluster;
            Available = true;
            Master = true;
            mPingClient = new InfinispanClient(SSL, this);
        }

        private InfinispanClient mPingClient;

        private int mDisposed = 0;

        private int mCount = 0;
        public int getMCount()
        {
            return mCount;
        }
        private int mServed = 0;
        public int getMServed()
        {
            return mServed;
        }
        private int mRequest = 0;
        public int getMRequest()
        {
            return mRequest;
        }
        public int getQueueSize()
        {
            return mQueue.Count;
        }

        public int getPoolSize()
        {
            return mPool.Count;
        }
        private long messageId = 1;
        public long MessageId { get { return messageId++; } set { messageId = value; } }
        private Queue<TaskCompletionSource<InfinispanClient>> mQueue = new Queue<TaskCompletionSource<InfinispanClient>>();

        private Stack<InfinispanClient> mPool = new Stack<InfinispanClient>();

        public int QueueMaxLength { get; set; } = 256;

        public int MaxConnections { get; set; } = 30;

        public InfinispanDG Cluster { get; set; }

        public string Name { get; set; }

        public int Port { get; set; }

        public string Password { get; set; }
        public string User { get; set; }
        public string AuthMech { get; set; }

        public bool Master { get; set; }

        public bool SSL { get; set; } = false;

        // Pop a client for usage
        public Task<InfinispanClient> Pop()
        {
            lock (mPool)
            {
                TaskCompletionSource<InfinispanClient> result = new TaskCompletionSource<InfinispanClient>();
                mRequest++;
                // If no client is available...
                if (!mPool.TryPop(out InfinispanClient client))
                {
                    mCount++;
                    //  ...and clients are not too much...
                    if (mCount <= MaxConnections)
                    {
                        // ... create a new one ...
                        client = new InfinispanClient(SSL, this);
                        result.SetResult(client);
                    }
                    else
                    {
                        // ... otherwise put the request in queue (see Push below)
                        if (mQueue.Count > QueueMaxLength)
                        {
                            result.SetResult(null);
                        }
                        else
                        {
                            mQueue.Enqueue(result);
                        }
                    }
                }
                else
                {
                    result.SetResult(client);
                }
                return result.Task;
            }
        }
        public bool Available { get; set; }
        public Result Connect(InfinispanClient client)
        {
            if (!client.TcpClient.IsConnected)
            {
                if (client.TcpClient.Connect().Result.Connected)
                {
                    this.Available = true;
                    if (!string.IsNullOrEmpty(Password))
                    {
                        Commands.AUTH_MECH_LIST authMechList = new Commands.AUTH_MECH_LIST();
                        InfinispanRequest request = new InfinispanRequest(this, this.Cluster, null, client, authMechList, typeof(string));
                        var task = request.Execute();
                        task.Wait();
                        if (task.Result.ResultType == ResultType.DataError ||
                            task.Result.ResultType == ResultType.Error
                            || task.Result.ResultType == ResultType.NetError)
                        {
                            return task.Result;
                        }
                        bool found = false;
                        if (this.AuthMech != null)
                        {
                            for (int i = 0; i < authMechList.availableMechs.Length; i++)
                            {
                                if (this.AuthMech.Equals(authMechList.availableMechs[i]))
                                {
                                    found = true;
                                    break;
                                }
                            }
                        }
                        if (!found)
                        {
                            // TODO: check if error is handled correctly
                            task.Result.Messge = "SASL mech: " + this.AuthMech + " not available server side";
                            task.Result.ResultType = ResultType.NetError;
                            return task.Result;
                        }
                        Commands.AUTH auth = new Commands.AUTH(this.AuthMech, new System.Net.NetworkCredential(User, Password));
                        while (auth.Completed == 0)
                        {
                            request = new InfinispanRequest(this, this.Cluster, null, client, auth, typeof(string));
                            task = request.Execute();
                            task.Wait();
                            if (task.Result.ResultType == ResultType.DataError ||
                                task.Result.ResultType == ResultType.Error
                                || task.Result.ResultType == ResultType.NetError)
                            {
                                return task.Result;
                            }
                        }
                    }
                    var temp = new TaskCompletionSource<Result>();
                    temp.SetResult(new Result());
                    return temp.Task.Result;
                }
                else
                {
                    this.Available = false;
                    return new Result { ResultType = ResultType.NetError, Messge = client.TcpClient.LastError.Message };
                }
            }
            return new Result { ResultType = ResultType.Simple, Messge = "Connected" };
        }

        // Return a client for others
        public void Push(InfinispanClient client)
        {
            TaskCompletionSource<InfinispanClient> item = null;
            lock (mPool)
            {
                mServed++;
                if (mDisposed > 0)
                {
                    // If this host has been disposed clean up...
                    client.TcpClient.DisConnect();
                }
                else
                {
                    // ... otherwise see if someone is in queue waiting for client
                    // set item to this client
                    if (!mQueue.TryDequeue(out item))
                    {
                        mPool.Push(client);
                    }

                }
            }
            if (item != null)
            {
                Task.Run(() => item.SetResult(client));
            }
        }
        public async Task shutdown()
        {
            var tasks = new List<Task>();
            foreach (var tcs in mQueue)
            {
                tasks.Add(tcs.Task);
            }
            await Task.WhenAll(tasks.ToArray());
        }
        // Disposing this host. Disconnecting the clients
        public void Dispose()
        {
            if (System.Threading.Interlocked.CompareExchange(ref mDisposed, 1, 0) == 0)
            {
                while (mPool.TryPop(out InfinispanClient item))
                {
                    item.TcpClient.DisConnect();
                }
                while (mQueue.TryDequeue(out TaskCompletionSource<InfinispanClient> t))
                {
                    t.SetCanceled();
                }
            }
        }
    }
}
