using System;
using Infinispan.Hotrod.Core.Tests.Util;
using Xunit;
namespace Infinispan.Hotrod.Core.XUnitTest
{
        public class AuthorizationCacheTestFixture : IDisposable
    {
        public HotRodServer hotRodServer {get; private set;}
        public AuthorizationCacheTestFixture() {
            hotRodServer = new HotRodServer("clustered-sasl-cs.xml");
            hotRodServer.StartHotRodServer();
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
            System.Console.WriteLine("BaseAuthTest construct");
            BeforeClass();
        }
        public const string HOTROD_HOST = "127.0.0.1";
        public const int HOTROD_PORT = 11222;
        public const string REALM = "ApplicationRealm";


        private AuthorizationTester tester = new AuthorizationTester();


        public abstract string GetMech(); //  { return "PLAIN";}


        [Fact]
        public void ReaderSuccessTest()
        {
            tester.TestReaderSuccess(readerCache);
        }

        [Fact]
        public void ReaderPerformsWritesTest()
        {
            tester.TestReaderPerformsWrites(readerCache);
        }

        [Fact]
        public void WriterSuccessTest()
        {
            tester.TestWriterSuccess(writerCache);
        }

        [Fact]
        public void WriterPerformsReadsTest()
        {
            tester.TestWriterPerformsReads(writerCache);
        }

        [Fact]
        public void WriterPerformsSupervisorOpsTest()
        {
            tester.TestWriterPerformsSupervisorOps(writerCache, scriptCache);//, marshaller);
        }

        [Fact]
        public void SupervisorSuccessTest()
        {
            tester.TestSupervisorSuccess(supervisorCache, scriptCache); // , marshaller);
        }

        [Fact]
        public void SupervisorPerformsAdminOpsTest()
        {
            // TODO: fix STATS OPCODE hangs on 11.0.x tester.TestSupervisorPerformsAdminOps(fixture.supervisorCache);
        }

        [Fact]
        public void AdminSuccessTest()
        {
            tester.TestAdminSuccess(adminCache, scriptCache); //, marshaller);
        }
        [Fact]
        public void ReaderAccessStatsTest()
        {
            // TODO: this call hangs on 11.0.x tester.TestReaderAccessStats(fixture.readerCache, fixture.scriptCache); //, marshaller);
        }

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
        public void BeforeClass()
        {
            readerCache = InitCache("reader", "password");
            writerCache = InitCache("writer", "somePassword");
            supervisorCache = InitCache("supervisor", "lessStrongPassword");
            adminCache = InitCache("admin", "strongPassword");
            scriptCache = InitCache("admin", "strongPassword", PROTOBUF_SCRIPT_CACHE_NAME);
        }


         


    }
}
