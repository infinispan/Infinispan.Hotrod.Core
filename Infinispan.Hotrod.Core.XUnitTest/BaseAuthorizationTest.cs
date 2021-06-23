using System;
using Infinispan.Hotrod.Core.Tests.Util;
using Xunit;
namespace Infinispan.Hotrod.Core.XUnitTest
{
        public class AuthorizationCacheTestFixture : IDisposable
    {
        public HotRodServer hotRodServer {get; private set;}
        public Cache<String, String> readerCache;
        public Cache<String, String> writerCache;
        public Cache<String, String> supervisorCache;
        public Cache<String, String> adminCache;
        public Cache<String, String> scriptCache;
        public const string PROTOBUF_SCRIPT_CACHE_NAME = "___script_cache";
        public const string AUTH_CACHE = "authCache";
        Marshaller<string> marshaller;
        public AuthorizationCacheTestFixture() {
            hotRodServer = new HotRodServer("clustered-sasl-cs.xml");
            hotRodServer.StartHotRodServer();
            DefaultInfinispan.Instance.AddHost("127.0.0.1");
            DefaultInfinispan.Instance.Version=0x30;
            DefaultInfinispan.Instance.ForceReturnValue=false;
            DefaultInfinispan.Instance.ClientIntelligence=0x01;

            BeforeClass();
        }
        public void BeforeClass()
        {

            readerCache = InitCache("reader", "password");
            writerCache = InitCache("writer", "somePassword");
            supervisorCache = InitCache("supervisor", "lessStrongPassword");
            adminCache = InitCache("admin", "strongPassword");
            scriptCache = InitCache("admin", "strongPassword", PROTOBUF_SCRIPT_CACHE_NAME);
        }

        private Cache<String, String> InitCache(string user, string password, string cacheName = AUTH_CACHE)
        {
            var ispnCluster = new InfinispanDG();
            ispnCluster.User=user;
            ispnCluster.Password=password;
            ispnCluster.AuthMech= "PLAIN";
            ispnCluster.Version = 0x1e;
            ispnCluster.ClientIntelligence = 0x01;
            ispnCluster.ForceReturnValue = false;

            var host = ispnCluster.AddHost("127.0.0.1", 11222, false);

            marshaller= new StringMarshaller();
            var cache = ispnCluster.newCache(marshaller, marshaller, cacheName);

            // marshaller = new JBasicMarshaller();
            // conf.Marshaller(marshaller);
            return cache;
        }

         
        public void Dispose()   
        {
            hotRodServer.Dispose();
        }
    }

    public abstract class BaseAuthorizationTest : IClassFixture<AuthorizationCacheTestFixture>
    {
        AuthorizationCacheTestFixture fixture;
        public BaseAuthorizationTest(AuthorizationCacheTestFixture fixture) {
            this.fixture = fixture;
        }
        public const string HOTROD_HOST = "127.0.0.1";
        public const int HOTROD_PORT = 11222;
        public const string REALM = "ApplicationRealm";


        private AuthorizationTester tester = new AuthorizationTester();


        public abstract string GetMech(); //  { return "PLAIN";}


        [Fact]
        public void ReaderSuccessTest()
        {
            tester.TestReaderSuccess(fixture.readerCache);
        }

        [Fact]
        public void ReaderPerformsWritesTest()
        {
            tester.TestReaderPerformsWrites(fixture.readerCache);
        }

        [Fact]
        public void WriterSuccessTest()
        {
            tester.TestWriterSuccess(fixture.writerCache);
        }

        [Fact]
        public void WriterPerformsReadsTest()
        {
            tester.TestWriterPerformsReads(fixture.writerCache);
        }

        [Fact]
        public void WriterPerformsSupervisorOpsTest()
        {
            tester.TestWriterPerformsSupervisorOps(fixture.writerCache, fixture.scriptCache);//, marshaller);
        }

        [Fact]
        public void SupervisorSuccessTest()
        {
            tester.TestSupervisorSuccess(fixture.supervisorCache, fixture.scriptCache); // , marshaller);
        }

        [Fact]
        public void SupervisorPerformsAdminOpsTest()
        {
            // TODO: fix STATS OPCODE hangs on 11.0.x tester.TestSupervisorPerformsAdminOps(fixture.supervisorCache);
        }

        [Fact]
        public void AdminSuccessTest()
        {
            tester.TestAdminSuccess(fixture.adminCache, fixture.scriptCache); //, marshaller);
        }
        [Fact]
        public void ReaderAccessStatsTest()
        {
            // TODO: this call hangs on 11.0.x tester.TestReaderAccessStats(fixture.readerCache, fixture.scriptCache); //, marshaller);
        }

    }
}
