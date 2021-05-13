using BeetleX.Tracks;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BeetleX.Infinispan
{
    public class InfinispanDG : IHostHandler, IDisposable
    {
        public InfinispanDG(int db = 0, IDataFormater dataFormater = null, IHostHandler hostHandler = null)
        {
            DB = db;
            if (hostHandler == null)
            {
                mDetectionTime = new System.Threading.Timer(OnDetection, null, 1000, 1000);
                this.Host = this;
            }
            else
            {
                this.Host = hostHandler;
            }
        }

        private System.Threading.Timer mDetectionTime;

        private static InfinispanDG mDefault = new InfinispanDG();

        internal static InfinispanDG Default => mDefault;

        public bool AutoPing { get; set; } = true;

        public IHostHandler Host { get; set; }

        private void OnDetection(object state)
        {
            mDetectionTime?.Change(-1, -1);
            if (AutoPing)
            {
                var rHost = mActiveHosts;
                foreach (var item in rHost)
                    item.Ping();
            }
            mDetectionTime?.Change(1000, 1000);

        }

        private List<InfinispanHost> mHosts = new List<InfinispanHost>();

        private InfinispanHost[] mActiveHosts = new InfinispanHost[0];

        private bool OnClientPush(InfinispanClient client)
        {
            return true;
        }

        public int DB { get; set; }

        InfinispanHost IHostHandler.AddHost(string host, int port = 11222)
        {
            return ((IHostHandler)this).AddHost(host, port, false);
        }

        InfinispanHost IHostHandler.AddHost(string host, int port, bool ssl)
        {
            if (port == 0)
                port = 6379;
            InfinispanHost redisHost = new InfinispanHost(ssl, DB, host, port);
            mHosts.Add(redisHost);
            mActiveHosts = mHosts.ToArray();
            return redisHost;
        }

  
        InfinispanHost IHostHandler.GetHost()
        {
            var items = mActiveHosts;
            for (int i = 0; i < items.Length; i++)
            {
                if (items[i].Available)
                    return items[i];
            }
          return null;
        }
        public async Task<Result> Execute(Cache cache, Command cmd, params Type[] types)
        {
            var host = Host.GetHost();
            if (host == null)
            {
                return new Result() { ResultType = ResultType.NetError, Messge = "redis server is not available" };
            }
            var client = await host.Pop();
            if (client == null)
                return new Result() { ResultType = ResultType.NetError, Messge = "exceeding maximum number of connections" };
            try
            {
                var result = host.Connect(client);
                if (result.IsError)
                {
                    return result;
                }
                using (var tarck = CodeTrackFactory.Track(cmd.Name, CodeTrackLevel.Module, null, "Redis", client.Host))
                {
                    if (tarck.Enabled)
                    {
                        tarck.Activity?.AddTag("tag", "BeetleX Redis");
                    }
                    cmd.Activity = tarck.Activity;
                    InfinispanRequest request = new InfinispanRequest(cache, host, client, cmd, types);
                    request.Activity = tarck.Activity;
                    result = await request.Execute();
                    return result;
                }
            }
            finally
            {
                if (client != null)
                    host.Push(client);
            }
        }

        public async ValueTask<V> Set<K,V>(Marshaller<K> km, Marshaller<V> vm, Cache cache, K key, V value)
        {
            return await Set(km, vm, cache, key, value, null, null);
        }

        public async ValueTask<V> Set<K,V>(Marshaller<K> km, Marshaller<V> vm, Cache cache, K key, V value, int? seconds, bool? nx)
        {
            Commands.SET<K,V> set = new Commands.SET<K,V>(km, vm, key, value);
            if (cache.ForceReturnValue) {
                set.Flags |= 0x01;
            }
            var result = await Execute(cache, set, typeof(string));
            if (result.IsError)
                throw new InfinispanException(result.Messge);
            return set.PrevValue;
        }
        public async ValueTask<V> Get<K,V>(Marshaller<K> km, Marshaller<V> vm, Cache cache, K key)
        {
            Commands.GET<K,V> cmd = new Commands.GET<K,V>(km, vm, key);
            var result = await Execute(cache, cmd, typeof(V));
            if (result.IsError)
                throw new InfinispanException(result.Messge);
            return cmd.Value;
        }
        private bool mIsDisposed = false;

        public void Dispose()
        {
            if (!mIsDisposed)
            {
                mIsDisposed = true;
                if (mDetectionTime != null)
                {
                    mDetectionTime.Dispose();
                    mDetectionTime = null;
                }
                foreach (var item in mHosts)
                    item.Dispose();
            }
        }
        public Cache newCache(string name) {
            return new Cache(name);
        }
    }
}
