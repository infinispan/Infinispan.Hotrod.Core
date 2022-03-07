using System;
using Infinispan.Hotrod.Core.Tests.Util;
using Xunit;
using System.Collections.Generic;
using System.Threading.Tasks;

[assembly: CollectionBehavior(DisableTestParallelization = true)]

namespace Infinispan.Hotrod.Core.XUnitTest
{
    public class RemoteEventTestFixture : IDisposable
    {
        public HotRodServer hotRodServer { get; private set; }
        public Cache<string, string> cache;
        public InfinispanDG infinispan = new InfinispanDG();
        public Marshaller<string> marshaller;
        public RemoteEventTestFixture()
        {
            hotRodServer = new HotRodServer("infinispan-noauth.xml");
            hotRodServer.StartHotRodServer();
            infinispan.AddHost("127.0.0.1");
            infinispan.Version = 0x1f;
            infinispan.ForceReturnValue = false;
            infinispan.ClientIntelligence = 0x01;
            marshaller = new StringMarshaller();
            cache = infinispan.newCache(marshaller, marshaller, "default");
        }

        public void Dispose()
        {
            hotRodServer.Dispose();
        }
    }

    [Collection("MainSequence")]
    public class RemoteEventTest : IClassFixture<RemoteEventTestFixture>
    {
        public HotRodServer _hotRodServer;
        private readonly RemoteEventTestFixture _fixture;
        private Cache<string, string> _cache;
        private InfinispanDG _infinispan;
        private Marshaller<string> _marshaller;
        public RemoteEventTest(RemoteEventTestFixture fixture)
        {
            _fixture = fixture;
            _cache = _fixture.cache;
            _infinispan = _fixture.infinispan;
            _marshaller = _fixture.marshaller;
            _hotRodServer = _fixture.hotRodServer;
        }
        const string ERRORS_KEY_SUFFIX = ".errors";
        const string PROTOBUF_SCRIPT_CACHE_NAME = "___script_cache";

        [Fact]
        public async void BasicEventsTest()
        {
            LoggingEventListener listener = new LoggingEventListener(_cache, "Basic");
            try
            {
                await _cache.Clear();
                await _cache.AddListener(listener);
                AssertNoEvents(listener);
                await _cache.Put("key1", "value1");
                AssertOnly("key1", listener, EventType.CREATED);
                await _cache.Put("key1", "value1bis");
                AssertOnly("key1", listener, EventType.MODIFIED);
                await _cache.Remove("key1");
                AssertOnly("key1", listener, EventType.REMOVED);
                var expire = new ExpirationTime { Unit = TimeUnit.MILLISECONDS, Value = 100 };
                await _cache.Put("key1", "value1", expire);
                AssertOnly("key1", listener, EventType.CREATED);
                TimeUtils.WaitFor(() => { return _cache.Get("key1").Result == null; });
                AssertOnly("key1", listener, EventType.EXPIRED);
            }
            catch (Exception)
            {
            }
            finally
            {
                AssertErrorCount(listener,0);
                await _cache.RemoveListener(listener);
                AssertErrorCount(listener,0);
            }
        }

        [Fact]
        public async void IncludeCurrentStateEventTest()
        {
            LoggingEventListener listener = new LoggingEventListener(_cache, "IncludeCurrentState");
            try
            {
                await _cache.Clear();
                await _cache.Put("key1", "value1");
                AssertNoEvents(listener);
                await _cache.AddListener(listener, true);
                AssertOnly("key1", listener, EventType.CREATED);
            }
            finally
            {
                AssertErrorCount(listener,0);
                await _cache.RemoveListener(listener);
                AssertErrorCount(listener,0);
            }
        }

        [Fact]
        public async void ConditionalEventsTest()
        {
            LoggingEventListener listener = new LoggingEventListener(_cache, "Conditional");
            try
            {
                await _cache.Clear();
                await _cache.AddListener(listener);
                AssertNoEvents(listener);
                await _cache.PutIfAbsent("key1", "value1");
                AssertOnly("key1", listener, EventType.CREATED);
                await _cache.PutIfAbsent("key1", "value1again");
                AssertNoEvents(listener);
                await _cache.Replace("key1", "modified");
                AssertOnly("key1", listener, EventType.MODIFIED);
                await _cache.ReplaceWithVersion("key1", "modified", 0);
                AssertNoEvents(listener);
                ValueWithVersion<string> versioned = await _cache.GetWithVersion("key1");
                await _cache.ReplaceWithVersion("key1", "modified", versioned.Version);
                AssertOnly("key1", listener, EventType.MODIFIED);
                await _cache.RemoveWithVersion("key1", 0);
                AssertNoEvents(listener);
                versioned = await _cache.GetWithVersion("key1");
                await _cache.RemoveWithVersion("key1", versioned.Version);
                AssertOnly("key1", listener, EventType.REMOVED);
            }
            finally
            {
                AssertErrorCount(listener, 0);
                await _cache.RemoveListener(listener);
                AssertErrorCount(listener,0);
            }
        }
        [Fact]
        public async void RecoverOnErrorTest()
        {
            LoggingEventListener listener = new LoggingEventListener(_cache, "RecoverOnError");
            try
            {
                await _cache.Clear();
                await _cache.AddListener(listener);
                AssertNoEvents(listener);
                await _cache.Put("key1", "value1");
                AssertOnly("key1", listener, EventType.CREATED);
                _hotRodServer.ShutDownHotrodServer();
                Assert.True(_hotRodServer.IsStopped());
                _hotRodServer.StartHotRodServer();
                Assert.True(_hotRodServer.IsRunning());
                listener.serverIsRunning.SetResult(true);
                listener.ListenerIsAdded.Task.Wait();
                await _cache.Put("key1", "value1");
                AssertOnly("key1", listener, EventType.CREATED);
            }
            finally
            {
                AssertErrorCount(listener, 1);
                await _cache.RemoveListener(listener);
                AssertErrorCount(listener, 1);
                listener.CleanErrors();
            }
        }
        //     [Test]
        //     [Ignore("ISPN-9409")]
        //     public void CustomEventsTest()
        //     {
        //         LoggingEventListener<string> listener = new LoggingEventListener<string>();
        //         IRemoteCache<string, string> cache = remoteManager.GetCache<string, string>();
        //         Event.ClientListener<string, string> cl = new Event.ClientListener<string, string>();
        //         try
        //         {
        //             cache.Clear();
        //             cl.filterFactoryName = "";
        //             cl.converterFactoryName = "";
        //             cl.converterFactoryName = "to-string-converter-factory";
        //             cl.AddListener(listener.CreatedEventAction);
        //             cl.AddListener(listener.ModifiedEventAction);
        //             cl.AddListener(listener.RemovedEventAction);
        //             cl.AddListener(listener.ExpiredEventAction);
        //             cl.AddListener(listener.CustomEventAction);
        //             cache.AddClientListener(cl, new string[] { }, new string[] { }, null);
        //             cache.Put("key1", "value1");
        //             AssertOnlyCustom("custom event: key1 value1", listener);
        //         }
        //         finally
        //         {
        //             if (cl.listenerId != null)
        //             {
        //                 cache.RemoveClientListener(cl);
        //             }
        //         }
        //     }

        //     [Test]
        //     [Ignore("ISPN-9409")]
        //     public void FilterEventsTest()
        //     {
        //         LoggingEventListener<string> listener = new LoggingEventListener<string>();
        //         IRemoteCache<string, string> cache = remoteManager.GetCache<string, string>();
        //         Event.ClientListener<string, string> cl = new Event.ClientListener<string, string>();
        //         try
        //         {
        //             cache.Clear();
        //             cl.filterFactoryName = "string-is-equal-filter-factory";
        //             cl.converterFactoryName = "";
        //             cl.AddListener(listener.CreatedEventAction);
        //             cl.AddListener(listener.ModifiedEventAction);
        //             cl.AddListener(listener.RemovedEventAction);
        //             cl.AddListener(listener.ExpiredEventAction);
        //             cl.AddListener(listener.CustomEventAction);
        //             cache.AddClientListener(cl, new string[] { "wantedkeyprefix" }, new string[] { }, null);
        //             AssertNoEvents(listener);
        //             cache.Put("key1", "value1");
        //             cache.Put("wantedkeyprefix_key1", "value2");
        //             //only one received; one is ignored
        //             AssertOnlyCreated("wantedkeyprefix_key1", listener);
        //             AssertNoEvents(listener);
        //             cache.Replace("key1", "modified");
        //             cache.Replace("wantedkeyprefix_key1", "modified");
        //             AssertOnlyModified("wantedkeyprefix_key1", listener);
        //             AssertNoEvents(listener);
        //             cache.Remove("key1");
        //             cache.Remove("wantedkeyprefix_key1");
        //             AssertOnlyRemoved("wantedkeyprefix_key1", listener);
        //             AssertNoEvents(listener);
        //         }
        //         finally
        //         {
        //             if (cl.listenerId != null)
        //             {
        //                 cache.RemoveClientListener(cl);
        //             }
        //         }
        //     }

        private void AssertNoEvents(LoggingEventListener listener)
        {
            Assert.Empty(listener.createdEvents);
            Assert.Empty(listener.removedEvents);
            Assert.Empty(listener.modifiedEvents);
            Assert.Empty(listener.expiredEvents);
            Assert.Empty(listener.customEvents);
        }

        private void AssertOnly(string key, LoggingEventListener listener, EventType et, bool isCustom = false)
        {
            var remoteEvent = listener.PollEvent(et);
            Assert.Equal(key, _marshaller.unmarshall(remoteEvent.Key));
            if (et != EventType.CREATED || isCustom)
            {
                Assert.Empty(listener.createdEvents);
            }
            if (et != EventType.REMOVED || isCustom)
            {
                Assert.Empty(listener.removedEvents);
            }
            if (et != EventType.MODIFIED || isCustom)
            {
                Assert.Empty(listener.modifiedEvents);
            }
            if (et != EventType.EXPIRED || isCustom)
            {
                Assert.Empty(listener.expiredEvents);
            }
            if (isCustom)
            {
                Assert.Empty(listener.customEvents);
            }
        }
        private void AssertErrorCount(LoggingEventListener listener, int expected)
        {
            Assert.Equal(expected, listener.ErrorEvents.Count);
        }
    }
}
