using Infinispan.Hotrod.Core;
using Infinispan.Hotrod.Core.Tests.Util;
using Xunit;
using System;

namespace Infinispan.Hotrod.Core.XUnitTest
{
    [Collection("Sequential")]
    public class FailoverCacheTestFixture : IDisposable
    {
        public HotRodServer hotRodServer1 { get; private set; }
        public HotRodServer hotRodServer2 { get; private set; }
        public Cache<string, string> cache1;
        public Cache<string, string> cache2;
        public InfinispanDG infinispan1 = new InfinispanDG();
        public InfinispanDG infinispan2 = new InfinispanDG();
        public FailoverCacheTestFixture()
        {
            hotRodServer1 = new HotRodServer("infinispan-noauth.xml");
            hotRodServer1.StartHotRodServer();
            infinispan1.AddHost("127.0.0.1", 11222);
            infinispan1.AddHost("nyc", "127.0.0.1", 11322);
            infinispan1.Version = 0x1f;
            infinispan1.ForceReturnValue = false;
            infinispan1.ClientIntelligence = 0x01;
            cache1 = infinispan1.newCache(new StringMarshaller(), new StringMarshaller(), "default");

            string jbossHome = System.Environment.GetEnvironmentVariable("JBOSS_HOME");
            hotRodServer2 = new HotRodServer("infinispan-noauth.xml", "-o 100 -s " + jbossHome + "/server1", "server1", 11322);
            hotRodServer2.StartHotRodServer();
            infinispan2.AddHost("127.0.0.1", 11322);
            infinispan2.AddHost("lon", "127.0.0.1", 11222);
            infinispan2.Version = 0x1f;
            infinispan2.ForceReturnValue = false;
            infinispan2.ClientIntelligence = 0x01;
            cache2 = infinispan2.newCache(new StringMarshaller(), new StringMarshaller(), "default");
        }

        public void Dispose()
        {
            hotRodServer1.Dispose();
            hotRodServer2.Dispose();
        }
    }
    [Collection("MainSequence")]
    public class FailoverCacheTest : IClassFixture<FailoverCacheTestFixture>
    {
        private readonly FailoverCacheTestFixture _fixture;
        private Cache<string, string> _cache1;
        private Cache<string, string> _cache2;
        private InfinispanDG _infinispan1;
        private InfinispanDG _infinispan2;
        public FailoverCacheTest(FailoverCacheTestFixture fixture)
        {
            _fixture = fixture;
            _cache1 = _fixture.cache1;
            _cache2 = _fixture.cache2;
            _infinispan1 = _fixture.infinispan1;
            _infinispan2 = _fixture.infinispan2;
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
            _fixture.infinispan1.SwitchCluster("DEFAULT_CLUSTER");
            _fixture.infinispan2.SwitchCluster("DEFAULT_CLUSTER");
        }

        [Fact]
        public void verifyHotRodServersTest()
        {
            Assert.True(_fixture.hotRodServer1.IsRunning(), "Server is not running");
            Assert.True(_fixture.hotRodServer2.IsRunning(), "Server is not running");
        }

        [Fact]
        public async void multipleClustersTest()
        {
            String key = UniqueKey.NextKey();
            await _cache1.Put(key, "valueCache1");
            await _cache2.Put(key, "valueCache2");
            Assert.Equal("valueCache1", await _cache1.Get(key));
            Assert.Equal("valueCache2", await _cache2.Get(key));
        }
        [Fact]
        public async void manualClusterSwitchTest()
        {
            String key = UniqueKey.NextKey();
            await _cache1.Put(key, "valueCache1");
            await _cache2.Put(key, "valueCache2");
            Assert.Equal("valueCache1", await _cache1.Get(key));
            _infinispan1.SwitchCluster("nyc");
            Assert.Equal("valueCache2", await _cache1.Get(key));
            Assert.Equal("valueCache2", await _cache2.Get(key));
            _infinispan2.SwitchCluster("lon");
            Assert.Equal("valueCache1", await _cache2.Get(key));
        }

        [Fact]
        public async void ClusterSwitchOnFaultTest()
        {
            String key = UniqueKey.NextKey();
            await _cache1.Put(key, "valueCache1");
            await _cache2.Put(key, "valueCache2");
            Assert.Equal("valueCache1", await _cache1.Get(key));
            _fixture.hotRodServer1.ShutDownHotrodServer();
            Assert.Equal("valueCache2", await _cache1.Get(key));
            _fixture.hotRodServer1.StartHotRodServer();
            Assert.Equal("valueCache2", await _cache1.Get(key));
            _fixture.hotRodServer2.ShutDownHotrodServer();
            Assert.Null(await _cache1.Get(key));
            _fixture.hotRodServer1.ShutDownHotrodServer();
            await Assert.ThrowsAsync<InfinispanException>(() => _cache1.Get(key));
            _fixture.hotRodServer1.StartHotRodServer();
            _fixture.hotRodServer2.StartHotRodServer();
        }

        public async void ClusterSwitchOnFaultTes()
        {
            String key = UniqueKey.NextKey();
            await _cache1.Put(key, "valueCache1");
            await _cache2.Put(key, "valueCache2");
            string res1 = "", res2 = "", res3 = "";
            Assert.Equal("valueCache1", await _cache1.Get(key));
            _fixture.hotRodServer1.ShutDownHotrodServer();
            try
            {
                res1 = await _cache1.Get(key);
            }
            catch (Exception)
            {
                // go ahead...
            }
            _fixture.hotRodServer1.StartHotRodServer();
            try
            {
                res2 = await _cache1.Get(key);
            }
            catch (Exception)
            {
                // go ahead...
            }
            _fixture.hotRodServer2.ShutDownHotrodServer();
            try
            {
                res3 = await _cache1.Get(key);
            }
            catch (Exception)
            {
                // go ahead...
            }
            _fixture.hotRodServer1.ShutDownHotrodServer();
            Exception resEx = null;
            try
            {
                await _cache1.Get(key);
            }
            catch (Exception ex)
            {
                resEx = ex;
            }
            _fixture.hotRodServer1.StartHotRodServer();
            _fixture.hotRodServer2.StartHotRodServer();
            Assert.Equal("valueCache2", res1);
            Assert.Equal("valueCache2", res2);
            Assert.Null(res3);
            Assert.IsType<InfinispanException>(resEx);
        }


        //     [OneTimeSetUp]
        //     public void BeforeClass()
        //     {
        //         conf1 = new ConfigurationBuilder();
        //         conf1.AddServer().Host("127.0.0.1").Port(11222);
        //         ccb = conf1.AddCluster("nyc");
        //         ccb1 = ccb.AddClusterNode("127.0.0.1", 11322);
        //         conf1.BalancingStrategyProducer(d);
        //         configu1 = conf1.Build();
        //         manager1 = new RemoteCacheManager(configu1, true);
        //         cache1 = manager1.GetCache<String, String>();

        //         conf2 = new ConfigurationBuilder();
        //         conf2.AddServer().Host("127.0.0.1").Port(11322);
        //         conf2.AddCluster("lon").AddClusterNode("127.0.0.1", 11222);
        //         conf2.BalancingStrategyProducer(d2);
        //         configu2 = conf2.Build();
        //         remoteManager = new RemoteCacheManager(configu2, true);
        //         cache2 = remoteManager.GetCache<String, String>();
        //     }

        //     [Test]
        //     public void FailoverTest()
        //     {
        //         Assert.IsNull(cache1.Put("k1", "v1"));
        //         Assert.AreEqual("v1", cache1.Get("k1"), "Expected v1 from cache1");
        //         Assert.AreEqual("v1", cache2.Get("k1"), "Expected v1 from cache2");
        //         XSiteTestSuite.server1.ShutDownHotrodServer();
        //         //client1 should failover
        //         Assert.AreEqual("v1", cache1.Get("k1"), "Expected v1 from cache1 after failover");
        //         Assert.AreEqual("v1", cache2.Get("k1"), "Expected v1 from cache2 after failover");
        //         XSiteTestSuite.server1.StartHotRodServer();
        //         manager1.SwitchToDefaultCluster();
        //         //client1 should get null as state transfer is not enabled
        //         Assert.IsNull(cache1.Get("k1"));
        //         Assert.IsNull(cache1.Put("k2", "v2"));
        //         Assert.AreEqual("v2", cache1.Get("k2"));
        //         //double check client2
        //         Assert.AreEqual("v1", cache2.Get("k1"), "Expected v1 from cache2 after starting LON back again");
        //     }
        // }
    }
}