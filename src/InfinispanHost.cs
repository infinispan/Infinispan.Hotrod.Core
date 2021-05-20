using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
namespace Infinispan.Hotrod.Core
{
    public class InfinispanHost : IDisposable
    {
        public InfinispanHost(bool ssl, int db, string host, int port = 6379)
        {
            SSL = ssl;
            Host = host;
            Port = port;
            DB = db;
            Available = true;
            Master = true;
            mPingClient = new InfinispanClient(SSL, Host, Port);
        }

        private InfinispanClient mPingClient;

        private int mDisposed = 0;

        private int mCount = 0;


        private Queue<TaskCompletionSource<InfinispanClient>> mQueue = new Queue<TaskCompletionSource<InfinispanClient>>();

        private Stack<InfinispanClient> mPool = new Stack<InfinispanClient>();

        public int QueueMaxLength { get; set; } = 256;

        public int MaxConnections { get; set; } = 30;

        public int DB { get; set; }

        public string Host { get; set; }

        public int Port { get; set; }

        public string Password { get; set; }
        public string User { get; set; }
        public string AuthMech { get; set; }

        public bool Master { get; set; }

        public bool SSL { get; set; } = false;

        public Task<InfinispanClient> Pop()
        {
            lock (mPool)
            {
                TaskCompletionSource<InfinispanClient> result = new TaskCompletionSource<InfinispanClient>();
                if (!mPool.TryPop(out InfinispanClient client))
                {
                    mCount++;
                    if (mCount <= MaxConnections)
                    {
                        client = new InfinispanClient(SSL, Host, Port);
                        result.SetResult(client);
                    }
                    else
                    {
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

        public InfinispanClient Create()
        {
            var client = new InfinispanClient(SSL, Host, Port);
            var result = Connect(client);
            if (result.IsError)
            {
                client.TcpClient.DisConnect();
                throw new InfinispanException(result.Messge);
            }
            return client;
        }

        public bool Available { get; set; }

        public Result Connect(InfinispanClient client)
        {
            if (!client.TcpClient.IsConnected)
            {
                bool isNew;
                if (client.TcpClient.Connect(out isNew))
                {
                    this.Available = true;
                    if (!string.IsNullOrEmpty(Password))
                    {
                        Commands.AUTH_MECH_LIST authMechList = new Commands.AUTH_MECH_LIST();
                        InfinispanRequest request = new InfinispanRequest(UntypedCache.NullCache, this, client, authMechList, typeof(string));
                        var task = request.Execute();
                        task.Wait();
                        if (task.Result.ResultType == ResultType.DataError ||
                            task.Result.ResultType == ResultType.Error
                            || task.Result.ResultType == ResultType.NetError) {
                            return task.Result;
                        }
                        bool found = false;
                        if (this.AuthMech!=null) {
                            for (int i=0; i<authMechList.availableMechs.Length; i++) {
                                if (this.AuthMech.Equals(authMechList.availableMechs[i])) {
                                    found=true;
                                    break;
                                }
                            }
                        }
                        if (!found) {
                            // TODO: check if error is handled correctly
                            task.Result.Messge="SASL mech: "+this.AuthMech+" not available server side";
                            task.Result.ResultType = ResultType.NetError;
                            return task.Result;
                        }
                        Commands.AUTH auth = new Commands.AUTH(this.AuthMech, new System.Net.NetworkCredential(User, Password));
                        while (auth.Completed==0) {
                            request = new InfinispanRequest(UntypedCache.NullCache, this, client, auth, typeof(string));
                            task = request.Execute();
                            task.Wait();
                            if (task.Result.ResultType == ResultType.DataError ||
                                task.Result.ResultType == ResultType.Error
                                || task.Result.ResultType == ResultType.NetError) {
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

        public void Push(InfinispanClient client)
        {
            TaskCompletionSource<InfinispanClient> item = null;
            lock (mPool)
            {
                if (mDisposed > 0)
                {
                    client.TcpClient.DisConnect();
                }
                else
                {
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
