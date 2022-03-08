using Infinispan.Hotrod.Core;
using Infinispan.Hotrod.Core.Tests.Util;
using Xunit;
using System;
using System.IO;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Infinispan.Hotrod.Core.XUnitTest
{
    [Collection("Sequential")]
    public class ClusterCacheTestFixture : IDisposable
    {
        public HotRodServer hotRodServer1 { get; private set; }
        public HotRodServer hotRodServer2 { get; private set; }
        public Cache<string, string> distributedCache;
        public Cache<string, string> localCache;
        public InfinispanDG infinispan1 = new InfinispanDG();
        public ClusterCacheTestFixture()
        {
            hotRodServer1 = new HotRodServer("infinispan.xml", "-Dinfinispan.cluster.name=name  -Djgroups.tcp.address=127.0.0.1");
            hotRodServer1.StartHotRodServer();
            infinispan1.AddHost("127.0.0.1", 11222);
            infinispan1.Version = 0x1f;
            infinispan1.ForceReturnValue = false;
            infinispan1.ClientIntelligence = 0x03;
            string jbossHome = System.Environment.GetEnvironmentVariable("JBOSS_HOME");
            hotRodServer2 = new HotRodServer("infinispan.xml", "-o 100 -s " + jbossHome + "/server1 -Dinfinispan.cluster.name=name -Djgroups.tcp.address=127.0.0.1", "server1", 11322);
            hotRodServer2.StartHotRodServer();
            distributedCache = infinispan1.NewCache(new StringMarshaller(), new StringMarshaller(), "distributed");
            localCache = infinispan1.NewCache(new StringMarshaller(), new StringMarshaller(), "namedCache");
        }

        public void Dispose()
        {
            hotRodServer1.Dispose();
            hotRodServer2.Dispose();
        }
    }
    [Collection("MainSequence")]
    public class ClusterCacheTest : IClassFixture<ClusterCacheTestFixture>
    {
        private readonly ClusterCacheTestFixture _fixture;
        private Cache<string, string> _distributedCache;
        private Cache<string, string> _localCache;
        private InfinispanDG _infinispan1;
        public ClusterCacheTest(ClusterCacheTestFixture fixture)
        {
            _fixture = fixture;
            _infinispan1 = _fixture.infinispan1;
            _distributedCache = _fixture.distributedCache;
            _localCache = _fixture.localCache;
            // Ensure servers are up
            if (!_fixture.hotRodServer1.started)
            {
                _fixture.hotRodServer1.StartHotRodServer();
            }
            if (!_fixture.hotRodServer2.started)
            {
                _fixture.hotRodServer2.StartHotRodServer();
            }
            // Ensure current cluster is default
        }

        [Fact]
        public void verifyHotRodServersTest()
        {
            Assert.True(_fixture.hotRodServer1.IsRunning(), "Server is not running");
            Assert.True(_fixture.hotRodServer2.IsRunning(), "Server is not running");
        }

        [Fact]
        public async void distributedCacheTest()
        {
            String key = UniqueKey.NextKey();
            await _distributedCache.Put(key, "value");
            Assert.Equal("value", await _distributedCache.Get(key));
            _fixture.hotRodServer1.ShutDownHotrodServer();
            Assert.Equal("value", await _distributedCache.Get(key));
        }
        [Fact]
        public async void localCacheTest()
        {
            String key = UniqueKey.NextKey();
            await _localCache.Put(key, "value");
            Assert.Equal("value", await _localCache.Get(key));
            _fixture.hotRodServer1.ShutDownHotrodServer();
            await Assert.ThrowsAsync<InfinispanException>(() => _localCache.Get(key));
        }
        [Fact]
        public async void localAndDistributedCacheTest()
        {
            String key = UniqueKey.NextKey();
            await _localCache.Put(key, "valueLocal");
            await _distributedCache.Put(key, "valueDistributed");
            Assert.Equal("valueLocal", await _localCache.Get(key));
            _fixture.hotRodServer1.ShutDownHotrodServer();
            Assert.Equal("valueDistributed", await _distributedCache.Get(key));
            await Assert.ThrowsAsync<InfinispanException>(() => _localCache.Get(key));
        }
        [Fact]
        public async void distributedCachePutGetAllByOwner()
        {
            var keyVals = new Dictionary<String, String>();
            var keys = new HashSet<String>();

            for (var i = 0; i < 20; i++)
            {
                var k = UniqueKey.NextKey();
                keys.Add(k);
                keyVals.Add(k, k + "value");
            }
            // Getting the topology
            var pr = await _distributedCache.Ping();
            await _distributedCache.PutAll(keyVals);
            var res = await _distributedCache.GetAll(keys);
            var partResult = _distributedCache.GetAllPart(keys);
            try
            {
                partResult.WaitAll();
            }
            catch (AggregateException aEx)
            {
                Assert.Null("Should not reach this point: " + aEx.Message);
            }
            await _distributedCache.Clear();

            try
            {
                _distributedCache.PutAllPart(keyVals).WaitAll();
            }
            catch (AggregateException aEx)
            {
                Assert.Null("Should not reach this point: " + aEx.Message);
            }
            var res1 = await _distributedCache.GetAll(keys);
            var partResult1 = _distributedCache.GetAllPart(keys);
            try
            {
                partResult1.WaitAll();
            }
            catch (AggregateException aEx)
            {
                Assert.Null("Should not reach this point: " + aEx.Message);
            }
            var d = partResult.Result();
            Assert.Equal(d, res);
            var d1 = partResult1.Result();
            Assert.Equal(d1, res1);
            Assert.Equal(d, d1);
        }
    }
}