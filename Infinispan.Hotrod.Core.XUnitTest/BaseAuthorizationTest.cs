using System;
using Infinispan.Hotrod.Core.Tests.Util;
using Xunit;
namespace Infinispan.Hotrod.Core.XUnitTest
{
        public class AuthorizationCacheTestFixture : IDisposable
    {
        public HotRodServer hotRodServer {get; private set;}
        public AuthorizationCacheTestFixture() {
            hotRodServer = new HotRodServer("infinispan-sasl.xml");
            hotRodServer.StartHotRodServer();
        }
        public void Dispose()   
        {
            hotRodServer.Dispose();
        }
    }

    [Collection("MainSequence")]
    public abstract class BaseAuthorizationTest : IClassFixture<AuthorizationCacheTestFixture>
    {
        AuthorizationCacheTestFixture fixture;
        public BaseAuthorizationTest(AuthorizationCacheTestFixture fixture) {
            this.fixture = fixture;
            BeforeClass();
        }
        public const string HOTROD_HOST = "127.0.0.1";
        public const int HOTROD_PORT = 11222;
        public const string REALM = "ApplicationRealm";


        protected AuthorizationTester tester = new AuthorizationTester();


        public abstract string GetMech(); //  { return "PLAIN";}
        public Cache<String, String> readerCache;
        public Cache<String, String> writerCache;
        public Cache<String, String> supervisorCache;
        public Cache<String, String> adminCache;
        public Cache<String, String> scriptCache;
        public const string PROTOBUF_SCRIPT_CACHE_NAME = "___script_cache";
        public const string AUTH_CACHE = "authCache";
        Marshaller<string> marshaller;

        private Cache<String, String> InitCache(string user, string password, string cacheName = AUTH_CACHE)
        {
            var ispnCluster = new InfinispanDG();
            ispnCluster.User=user;
            ispnCluster.Password=password;
            ispnCluster.AuthMech= GetMech();
            ispnCluster.Version = 0x1f;
            ispnCluster.ClientIntelligence = 0x01;
            ispnCluster.ForceReturnValue = false;
            var host = ispnCluster.AddHost("127.0.0.1", 11222);

            marshaller= new StringMarshaller();
            var cache = ispnCluster.NewCache(marshaller, marshaller, cacheName);

            // marshaller = new JBasicMarshaller();
            // conf.Marshaller(marshaller);
            return cache;
        }
        private void BeforeClass()
        {
            readerCache = InitCache("reader", "password");
            writerCache = InitCache("writer", "somePassword");
            supervisorCache = InitCache("supervisor", "lessStrongPassword");
            adminCache = InitCache("admin", "strongPassword");
            scriptCache = InitCache("admin", "strongPassword", PROTOBUF_SCRIPT_CACHE_NAME);
        }


         


    }
}
