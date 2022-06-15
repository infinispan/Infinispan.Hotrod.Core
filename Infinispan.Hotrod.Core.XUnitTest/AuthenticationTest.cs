using System;
using Infinispan.Hotrod.Core.Tests.Util;
using Xunit;
namespace Infinispan.Hotrod.Core.XUnitTest
{
    public class AuthenticationTestFixture : IDisposable
    {
        public HotRodServer server1 { get; private set; }
        public HotRodServer server2 { get; private set; }
        public AuthenticationTestFixture()
        {
            server1 = new HotRodServer("infinispan-sasl.xml");
            server1.StartHotRodServer();
            string jbossHome = System.Environment.GetEnvironmentVariable("JBOSS_HOME");
            server2 = new HotRodServer("infinispan-sasl.xml", "-o 100 -s " + jbossHome + "/server1", "server1", 11322);
            server2.StartHotRodServer();
        }
        public void Dispose()
        {
            server1.Dispose();
            server2.Dispose();
        }
    }

    [Collection("MainSequence")]
    public class AuthenticationTest : IClassFixture<AuthenticationTestFixture>
    {
        AuthenticationTestFixture fixture;
        public AuthenticationTest(AuthenticationTestFixture fixture)
        {
            this.fixture = fixture;
        }
        public const string HOTROD_HOST = "127.0.0.1";
        public const int HOTROD_PORT = 11222;
        public const string REALM = "ApplicationRealm";
        private const string USER = "supervisor";
        private const string PASS = "lessStrongPassword";
        public const string AUTH_CACHE = "authCache";
        Marshaller<string> marshaller;

        [Fact]
        public void PlainAutheticationTest()
        {
            Cache<string, string> testCache = InitCache("PLAIN", USER, PASS);
            TestPut(testCache);
        }

        [Fact]
        public void MD5AutheticationTest()
        {
            Cache<string, string> testCache = InitCache("DIGEST-MD5", USER, PASS);
            TestPut(testCache);
        }
        [Fact]
        public void PlainAutheticationWithEasySaslSetupTest()
        {
            var ispnCluster = new InfinispanDG();
            ispnCluster.User = "supervisor";
            ispnCluster.Password = "lessStrongPassword";
            ispnCluster.AuthMech = "PLAIN";
            ispnCluster.Domain = "node0";
            ispnCluster.Version = 0x1f;
            ispnCluster.ClientIntelligence = 0x03;
            ispnCluster.ForceReturnValue = false;
            var host = ispnCluster.AddHost("127.0.0.1", 11222);

            marshaller = new StringMarshaller();
            var cache = ispnCluster.NewCache(marshaller, marshaller, "authCache");

            TestPut(cache);
        }

        [Fact]
        public void MD5AutheticationWithEasySaslSetupTest()
        {
            var ispnCluster = new InfinispanDG();
            ispnCluster.User = "supervisor";
            ispnCluster.Password = "lessStrongPassword";
            ispnCluster.AuthMech = "DIGEST-MD5";
            ispnCluster.Domain = "node0";
            ispnCluster.Version = 0x1f;
            ispnCluster.ClientIntelligence = 0x03;
            ispnCluster.ForceReturnValue = false;
            var host = ispnCluster.AddHost("127.0.0.1", 11222);

            marshaller = new StringMarshaller();
            var cache = ispnCluster.NewCache(marshaller, marshaller, "authCache");

            TestPut(cache);
        }

        private void TestPut(Cache<string, string> testCache)
        {
            string k1 = "key13";
            string v1 = "boron";
            testCache.Put(k1, v1).Wait();
            Assert.Equal(v1, testCache.Get(k1).Result);
        }
        private Cache<String, String> InitCache(string mech, string user, string password, string cacheName = AUTH_CACHE)
        {
            var ispnCluster = new InfinispanDG();
            ispnCluster.User = user;
            ispnCluster.Password = password;
            ispnCluster.AuthMech = mech;
            ispnCluster.Domain = "node0";
            ispnCluster.Version = 0x1f;
            ispnCluster.ClientIntelligence = 0x03;
            ispnCluster.ForceReturnValue = false;
            var host = ispnCluster.AddHost("127.0.0.1", 11222);

            marshaller = new StringMarshaller();
            var cache = ispnCluster.NewCache(marshaller, marshaller, cacheName);

            return cache;
        }
    }
}
