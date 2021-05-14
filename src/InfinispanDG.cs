using BeetleX.Tracks;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Infinispan.Hotrod.Core
{
    public class InfinispanDG : IHostHandler, IDisposable
    {
        public InfinispanDG(int db = 0, IDataFormater dataFormater = null, IHostHandler hostHandler = null)
        {
            DB = db;
            if (hostHandler == null)
            {
                this.Host = this;
            }
            else
            {
                this.Host = hostHandler;
            }
        }

        private static InfinispanDG mDefault = new InfinispanDG();
        internal static InfinispanDG Default => mDefault;
        public bool AutoPing { get; set; } = true;
        public string User { get; set; }
        public string Password { get; set; }
        public string AuthMech { get; set; }
        public byte Version {get; set;} = 0x30;
        public byte ClientIntelligence {get; set;} = 0x01;
        public UInt32 TopologyId {get; set;} = 0x01;
        public bool ForceReturnValue = false;
        private IHostHandler Host;
        private List<InfinispanHost> mHosts = new List<InfinispanHost>();
        private InfinispanHost[] mActiveHosts = new InfinispanHost[0];
        private bool OnClientPush(InfinispanClient client)
        {
            return true;
        }
        public int DB { get; set; }
        public InfinispanHost AddHost(string host, int port = 11222)
        {
            return AddHost(host, port, false);
        }
        public InfinispanHost AddHost(string host, int port, bool ssl)
        {
            if (port == 0)
                port = 11222;
            InfinispanHost ispnHost = new InfinispanHost(ssl, DB, host, port);
            ispnHost.User=User;
            ispnHost.Password=Password;
            ispnHost.AuthMech=AuthMech;
            mHosts.Add(ispnHost);
            mActiveHosts = mHosts.ToArray();
            return ispnHost;
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
        public async Task<Result> Execute(UntypedCache cache, Command cmd)
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
                    InfinispanRequest request = new InfinispanRequest(cache, host, client, cmd);
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

        public async ValueTask<V> Set<K,V>(Marshaller<K> km, Marshaller<V> vm, UntypedCache cache, K key, V value)
        {
            return await Set(km, vm, cache, key, value, null, null);
        }

        public async ValueTask<V> Set<K,V>(Marshaller<K> km, Marshaller<V> vm, UntypedCache cache, K key, V value, int? seconds, bool? nx)
        {
            Commands.SET<K,V> set = new Commands.SET<K,V>(km, vm, key, value);
            if (cache.ForceReturnValue) {
                set.Flags |= 0x01;
            }
            var result = await Execute(cache, set);
            if (result.IsError)
                throw new InfinispanException(result.Messge);
            return set.PrevValue;
        }
        public async ValueTask<V> Get<K,V>(Marshaller<K> km, Marshaller<V> vm, UntypedCache cache, K key)
        {
            Commands.GET<K,V> cmd = new Commands.GET<K,V>(km, vm, key);
            var result = await Execute(cache, cmd);
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
                foreach (var item in mHosts)
                    item.Dispose();
            }
        }
        public Cache<K,V> newCache<K,V>(Marshaller<K> keyM, Marshaller<V> valM, string name) {
            return new Cache<K,V>(this, keyM, valM, name);
        }
    }
}
