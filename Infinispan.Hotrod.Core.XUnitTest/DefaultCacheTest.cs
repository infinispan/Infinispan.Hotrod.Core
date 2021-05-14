using System;
using Infinispan.Hotrod.Core.Tests.Util;
using Xunit;

namespace Infinispan.Hotrod.Core.XUnitTest
{

    public class DefaultCacheTestFixture : IDisposable
    {
        public HotRodServer hotRodServer {get; private set;}
        public Cache<string,string> cache;
        public DefaultCacheTestFixture() {
            hotRodServer = new HotRodServer("infinispan.xml");
            hotRodServer.StartHotRodServer();
            DefaultInfinispan.Instance.AddHost("127.0.0.1");
            DefaultInfinispan.Instance.Version=0x30;
            DefaultInfinispan.Instance.ForceReturnValue=false;
            DefaultInfinispan.Instance.ClientIntelligence=0x01;
            cache = DefaultInfinispan.Instance.newCache(new StringMarshaller(), new StringMarshaller(), "default");
        }
         
        public void Dispose()   
        {
            hotRodServer.Dispose();
        }
    }
    public class DefaultCacheTest : IClassFixture<DefaultCacheTestFixture>
    {
        private readonly DefaultCacheTestFixture _fixture;
        private Cache<string,string> _cache;
        public DefaultCacheTest(DefaultCacheTestFixture fixture) {
            _fixture = fixture;
            _cache = _fixture.cache;
        }

        [Fact]
        public void startHotRodServerTest() {
            Console.WriteLine("Running test");
            Assert.True(_fixture.hotRodServer.IsRunning(), "Server is not running");
        }

        [Fact]
        public void NameTest()
        {
            Assert.NotNull(_cache.Name);
        }

        [Fact]
        public void VersionTest()
        {
            Assert.NotNull(_cache.Version);
        }

        // TODO: Verify if GetProtocolVersion method is needed
        // [Fact]
        // public void ProtocolVersionTest()
        // {
        //     Assert.NotNull(_cache.GetProtocolVersion());
        // }

        [Fact]
        public async void GetTest()
        {
            String key = UniqueKey.NextKey();

            Assert.Null(await _cache.Get(key));
            await _cache.Put(key, "carbon");
            Assert.Equal("carbon", await _cache.Get(key));
        }

        [Fact]
        public async void PutTest()
        {
            String key1 = UniqueKey.NextKey();
            String key2 = UniqueKey.NextKey();
            UInt32 initialSize = await _cache.Size();

            await _cache.Put(key1, "boron");
            Assert.Equal(initialSize + 1, await _cache.Size());
            Assert.Equal("boron", await _cache.Get(key1));

            await _cache.Put(key2, "chlorine");
            Assert.Equal(initialSize + 2, await _cache.Size());
            Assert.Equal("chlorine", await _cache.Get(key2));
        }

        [Fact]
        public async void ContainsKeyTest()
        {
            String key = UniqueKey.NextKey();
            Assert.False(await _cache.ContainsKey(key));
            await _cache.Put(key, "oxygen");
            Assert.True(await _cache.ContainsKey(key));
        }


        [Fact]
        public async void RemoveTest()
        {
            String key = UniqueKey.NextKey();
            await _cache.Put(key, "bromine");
            Assert.True(await _cache.ContainsKey(key));
            await _cache.Remove(key);
            Assert.False(await _cache.ContainsKey(key));
        }



        // public void NameTest()
        // public void VersionTest()
        // public void GetTest()
        // public void PutTest()
        // public void ContainsKeyTest()
        // public void RemoveTest()


        // [Test]
        // public void ProtocolVersionTest()
        // {
        //     Assert.IsNotNull(cache.GetProtocolVersion());
        // }

        // [Test]
        // public void GetAllTest()
        // {
        //     String key1 = UniqueKey.NextKey();
        //     String key2 = UniqueKey.NextKey();
        //     cache.Clear();
        //     Assert.IsNull(cache.Get(key1));
        //     Assert.IsNull(cache.Get(key2));
        //     cache.Put(key1, "carbon");
        //     cache.Put(key2, "oxygen");
        //     ISet<String> keySet = new HashSet<String>();
        //     keySet.Add(key1);
        //     keySet.Add(key2);
        //     IDictionary<String,String> d = cache.GetAll(keySet);
        //     Assert.AreEqual(d[key1], cache.Get(key1));
        //     Assert.AreEqual(d[key2], cache.Get(key2));
        //     Assert.AreEqual(d[key1], "carbon");
        //     Assert.AreEqual(d[key2], "oxygen");
        // }

        // [Test]
        // public void GetVersionedTest()
        // {
        //     String key = UniqueKey.NextKey();

        //     cache.Put(key, "uranium");
        //     IVersionedValue<String> previous = cache.GetVersioned(key);
        //     cache.Put(key, "rubidium");

        //     IVersionedValue<String> current = cache.GetVersioned(key);
        //     Assert.AreEqual("rubidium", current.GetValue());

        //     Assert.AreNotEqual(previous.GetVersion(), current.GetVersion());
        // }

        // [Test]
        // public void GetWithMetadataImmortalTest()
        // {
        //     String key = UniqueKey.NextKey();

        //     /* Created immortal entry. */
        //     cache.Put(key, "uranium");
        //     IMetadataValue<String> metadata = cache.GetWithMetadata(key);
        //     Assert.AreEqual("uranium", metadata.GetValue());

        //     Assert.IsTrue(metadata.GetLifespan() < 0);
        //     Assert.AreEqual(-1, metadata.GetCreated());

        //     Assert.IsTrue(metadata.GetMaxIdle() < 0);
        //     Assert.AreEqual(-1, metadata.GetLastUsed());
        // }

        // [Test]
        // public void GetWithMetadataTest()
        // {
        //     String key = UniqueKey.NextKey();

        //     /* Created with lifespan/maxidle. */
        //     cache.Put(key, "rubidium", 60, TimeUnit.MINUTES, 30, TimeUnit.MINUTES);

        //     IMetadataValue<String> metadata = cache.GetWithMetadata(key);
        //     Assert.AreEqual("rubidium", metadata.GetValue());

        //     Assert.AreEqual(3600, metadata.GetLifespan());
        //     Assert.AreNotEqual(0, metadata.GetCreated());

        //     Assert.AreEqual(1800, metadata.GetMaxIdle());
        //     Assert.AreNotEqual(0, metadata.GetLastUsed());
        // }

        // [Test]
        // public void GetBulkTest()
        // {
        //     String key1 = UniqueKey.NextKey();
        //     String key2 = UniqueKey.NextKey();
        //     String key3 = UniqueKey.NextKey();

        //     cache.Put(key1, "hydrogen");
        //     cache.Put(key2, "helium");
        //     cache.Put(key3, "lithium");

        //     IDictionary<String, String> data;

        //     data = cache.GetBulk();
        //     Assert.AreEqual("hydrogen", data[key1]);
        //     Assert.AreEqual("helium", data[key2]);
        //     Assert.AreEqual("lithium", data[key3]);

        //     data = cache.GetBulk(2);
        //     Assert.AreEqual(data.Count, 2);
        // }

        // [Test]
        // public void PutAllTest()
        // {
        //     ulong initialSize = cache.Size();

        //     String key1 = UniqueKey.NextKey();
        //     String key2 = UniqueKey.NextKey();
        //     String key3 = UniqueKey.NextKey();

        //     Dictionary<String, String> map = new Dictionary<String, String>();
        //     map.Add(key1, "v1");
        //     map.Add(key2, "v2");
        //     map.Add(key3, "v3");

        //     cache.PutAll(map);
        //     Assert.AreEqual(initialSize + 3, cache.Size());
        //     Assert.AreEqual("v1", cache.Get(key1));
        //     Assert.AreEqual("v2", cache.Get(key2));
        //     Assert.AreEqual("v3", cache.Get(key3));
        // }

        // [Test]
        // public void ContainsValueTest()
        // {
        //     try
        //     {
        //         cache.ContainsValue("key");
        //         Assert.Fail("Should throw an unsupported op exception for now.");
        //     }
        //     catch (Infinispan.Hotrod.Exceptions.UnsupportedOperationException)
        //     {
        //     }
        // }

        // [Test]
        // public void ReplaceWithVersionTest()
        // {
        //     String key = UniqueKey.NextKey();

        //     cache.Put(key, "bromine");
        //     ulong version = (ulong)cache.GetVersioned(key).GetVersion();
        //     cache.Put(key, "hexane");
        //     bool response = cache.ReplaceWithVersion(key, "barium", version);
        //     Assert.IsFalse(response);
        //     Assert.AreEqual("hexane", cache.Get(key));

        //     cache.Put(key, "oxygen");
        //     ulong newVersion = (ulong)cache.GetVersioned(key).GetVersion();
        //     Assert.AreNotEqual(newVersion, version);
        //     Assert.IsTrue(cache.ReplaceWithVersion(key, "barium", newVersion));
        //     Assert.AreEqual("barium", cache.Get(key));
        // }

        // [Test]
        // public void RemoveWithVersionTest()
        // {
        //     String key = UniqueKey.NextKey();

        //     cache.Put(key, "bromine");
        //     ulong version = (ulong)cache.GetVersioned(key).GetVersion();

        //     cache.Put(key, "hexane");
        //     Assert.IsFalse(cache.RemoveWithVersion(key, version));

        //     version = (ulong)cache.GetVersioned(key).GetVersion();
        //     Assert.IsTrue(cache.RemoveWithVersion(key, version));
        //     Assert.IsNull(cache.Get(key));
        // }

        // [Test]
        // public void ClearTest()
        // {
        //     String key1 = UniqueKey.NextKey();
        //     String key2 = UniqueKey.NextKey();

        //     cache.Put(key1, "hydrogen");
        //     cache.Put(key2, "helium");
        //     Assert.IsFalse(cache.IsEmpty());

        //     cache.Clear();

        //     Assert.IsNull(cache.Get(key1));
        //     Assert.IsNull(cache.Get(key2));

        //     Assert.AreEqual(0, cache.Size());
        //     Assert.IsTrue(cache.IsEmpty());
        // }

        // [Test]
        // public void DefaultValueForForceReturnValueTest()
        // {
        //     String key = UniqueKey.NextKey();

        //     Assert.IsNull(cache.Put(key, "v1"));
        //     Assert.IsNull(cache.Put(key, "v2"));

        //     Assert.IsNull(cache.Remove(key));

        //     Assert.IsNull(cache.Replace(key, "v3"));
        // }

        // [Test]
        // public void EntrySetTest()
        // {
        //     try
        //     {
        //         cache.EntrySet();
        //         Assert.Fail("Should throw an unsupported op exception for now.");
        //     }
        //     catch (Infinispan.Hotrod.Exceptions.UnsupportedOperationException)
        //     {
        //     }
        // }

        // [Test]
        // public void KeySetTest()
        // {
        //     String key1 = UniqueKey.NextKey();
        //     String key2 = UniqueKey.NextKey();
        //     String key3 = UniqueKey.NextKey();

        //     cache.Clear();
        //     cache.Put(key1, "v1");
        //     cache.Put(key2, "v2");
        //     cache.Put(key3, "v3");

        //     ISet<String> keys = cache.KeySet();
        //     Assert.AreEqual(3, keys.Count);
        //     Assert.IsTrue(keys.Contains(key1));
        //     Assert.IsTrue(keys.Contains(key2));
        //     Assert.IsTrue(keys.Contains(key3));
        // }

        // [Test]
        // public void ValuesTest()
        // {
        //     try
        //     {
        //         cache.Values();
        //         Assert.Fail("Should throw an unsupported op exception for now.");
        //     }
        //     catch (Infinispan.Hotrod.Exceptions.UnsupportedOperationException)
        //     {
        //     }
        // }

        // [Test]
        // public void WithFlagsTest()
        // {
        //     String key1 = UniqueKey.NextKey();

        //     Assert.IsNull(cache.Put(key1, "v1"));
        //     Assert.IsNull(cache.Put(key1, "v2"));

        //     Flags flags = Flags.FORCE_RETURN_VALUE | Flags.DEFAULT_LIFESPAN | Flags.DEFAULT_MAXIDLE;
        //     Assert.AreEqual("v2", cache.WithFlags(flags).Put(key1, "v3"));
        //     Assert.IsNull(cache.Put(key1, "v4"));

        //     //TODO: requires changes to the server configuration file to configure default expiration
        //     // IMetadataValue<String> metadata = cache.GetWithMetadata(key1);
        //     // Assert.IsTrue(metadata.GetLifespan() > 0);
        //     // Assert.IsTrue(metadata.GetMaxIdle() > 0);
        // }

        // [Test]
        // public void StatTest()
        // {
        //     ServerStatistics stats;

        //     /* Gather the initial stats. */
        //     stats = cache.Stats();
        //     int initialTimeSinceStart = stats.GetIntStatistic(ServerStatistics.TIME_SINCE_START);
        //     int initialEntries = stats.GetIntStatistic(ServerStatistics.CURRENT_NR_OF_ENTRIES);
        //     int initialTotalEntries = stats.GetIntStatistic(ServerStatistics.TOTAL_NR_OF_ENTRIES);
        //     int initialStores = stats.GetIntStatistic(ServerStatistics.STORES);
        //     int initialRetrievals = stats.GetIntStatistic(ServerStatistics.RETRIEVALS);
        //     int initialHits = stats.GetIntStatistic(ServerStatistics.HITS);
        //     int initialMisses = stats.GetIntStatistic(ServerStatistics.MISSES);
        //     int initialRemoveHits = stats.GetIntStatistic(ServerStatistics.REMOVE_HITS);
        //     int initialRemoveMisses = stats.GetIntStatistic(ServerStatistics.REMOVE_MISSES);

        //     /* Check that all are present. */
        //     Assert.IsTrue(initialTimeSinceStart >= 0);
        //     Assert.IsTrue(initialEntries >= 0);
        //     Assert.IsTrue(initialTotalEntries >= 0);
        //     Assert.IsTrue(initialStores >= 0);
        //     Assert.IsTrue(initialRetrievals >= 0);
        //     Assert.IsTrue(initialHits >= 0);
        //     Assert.IsTrue(initialMisses >= 0);
        //     Assert.IsTrue(initialRemoveHits >= 0);
        //     Assert.IsTrue(initialRemoveMisses >= 0);

        //     /* Add 3 key/value pairs. */
        //     String key1 = UniqueKey.NextKey();
        //     String key2 = UniqueKey.NextKey();
        //     String key3 = UniqueKey.NextKey();

        //     cache.Put(key1, "v");
        //     cache.Put(key2, "v");
        //     cache.Put(key3, "v");

        //     stats = cache.Stats();
        //     Assert.AreEqual(initialEntries + 3, stats.GetIntStatistic(ServerStatistics.CURRENT_NR_OF_ENTRIES));
        //     Assert.AreEqual(initialTotalEntries + 3, stats.GetIntStatistic(ServerStatistics.TOTAL_NR_OF_ENTRIES));
        //     Assert.AreEqual(initialStores + 3, stats.GetIntStatistic(ServerStatistics.STORES));

        //     /* Get hit/misses. */
        //     cache.Get(key1);
        //     cache.Get(key2);
        //     cache.Get(UniqueKey.NextKey());

        //     stats = cache.Stats();
        //     Assert.AreEqual(initialRetrievals + 3, stats.GetIntStatistic(ServerStatistics.RETRIEVALS));
        //     Assert.AreEqual(initialHits + 2, stats.GetIntStatistic(ServerStatistics.HITS));
        //     Assert.AreEqual(initialMisses + 1, stats.GetIntStatistic(ServerStatistics.MISSES));

        //     /* Remove hit/misses. */
        //     cache.Remove(key3);
        //     cache.Remove(UniqueKey.NextKey());
        //     cache.Remove(UniqueKey.NextKey());
        //     cache.Remove(UniqueKey.NextKey());

        //     stats = cache.Stats();
        //     Assert.AreEqual(initialRemoveHits + 1, stats.GetIntStatistic(ServerStatistics.REMOVE_HITS));
        //     Assert.AreEqual(initialRemoveMisses + 3, stats.GetIntStatistic(ServerStatistics.REMOVE_MISSES));

        //     /* Clear the cache. */
        //     cache.Clear();

        //     stats = cache.Stats();
        //     Assert.AreEqual(0, stats.GetIntStatistic(ServerStatistics.CURRENT_NR_OF_ENTRIES));
        //     Assert.AreEqual(initialTotalEntries + 3, stats.GetIntStatistic(ServerStatistics.TOTAL_NR_OF_ENTRIES));
        // }

    }
}
