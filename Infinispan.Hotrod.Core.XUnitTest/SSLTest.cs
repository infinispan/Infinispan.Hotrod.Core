using System;
using Infinispan.Hotrod.Core.Tests.Util;
using Xunit;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.IO;
using System.Reflection;

namespace Infinispan.Hotrod.Core.XUnitTest
{
    public class SSLTestFixture : IDisposable
    {
        public HotRodServer hotRodServer { get; private set; }
        public Cache<string, string> cache_verified;
        public InfinispanDG infinispan_verified = new InfinispanDG();
        public Cache<string, string> cache_bad_verified;
        public InfinispanDG infinispan_bad_verified = new InfinispanDG();
        public Cache<string, string> cache;
        public InfinispanDG infinispan = new InfinispanDG();
        private string fcacert;
        private string bad_fcacert;
        public SSLTestFixture()
        {
            try
            {
                hotRodServer = new HotRodServer("infinispan-ssl.xml");
                hotRodServer.StartHotRodServer();

                var resourceName = "Infinispan.Hotrod.Core.XUnitTest.resources.client.infinispan-ca.pem";
                var assembly = Assembly.GetExecutingAssembly();
                using (Stream stream = assembly.GetManifestResourceStream(resourceName))
                using (StreamReader reader = new StreamReader(stream))
                {
                    fcacert = reader.ReadToEnd();
                }
                X509Chain chain = new X509Chain();
                X509Certificate2 caCert = new X509Certificate2(StringMarshaller._ASCII.marshall(fcacert));
                chain.ChainPolicy.CustomTrustStore.Add(caCert);
                chain.ChainPolicy.RevocationMode = X509RevocationMode.NoCheck;
                chain.ChainPolicy.TrustMode = X509ChainTrustMode.CustomRootTrust;
                infinispan_verified.UseTLS = true;
                infinispan_verified.CACert = chain;
                infinispan_verified.AddHost("127.0.0.1");
                infinispan_verified.Version = 0x1f;
                infinispan_verified.ForceReturnValue = false;
                infinispan_verified.ClientIntelligence = 0x01;
                cache_verified = infinispan_verified.NewCache(new StringMarshaller(), new StringMarshaller(), "default");

                var bad_resourceName = "Infinispan.Hotrod.Core.XUnitTest.resources.client.bad-infinispan-ca.pem";
                var bad_assembly = Assembly.GetExecutingAssembly();
                using (Stream stream = bad_assembly.GetManifestResourceStream(bad_resourceName))
                using (StreamReader reader = new StreamReader(stream))
                {
                    bad_fcacert = reader.ReadToEnd();
                }
                X509Chain bad_chain = new X509Chain();
                X509Certificate2 bad_caCert = new X509Certificate2(StringMarshaller._ASCII.marshall(bad_fcacert));
                bad_chain.ChainPolicy.CustomTrustStore.Add(bad_caCert);
                bad_chain.ChainPolicy.RevocationMode = X509RevocationMode.NoCheck;
                bad_chain.ChainPolicy.TrustMode = X509ChainTrustMode.CustomRootTrust;
                infinispan_bad_verified.UseTLS = true;
                infinispan_bad_verified.CACert = bad_chain;
                infinispan_bad_verified.AddHost("127.0.0.1");
                infinispan_bad_verified.Version = 0x1f;
                infinispan_bad_verified.ForceReturnValue = false;
                infinispan_bad_verified.ClientIntelligence = 0x01;
                cache_bad_verified = infinispan_bad_verified.NewCache(new StringMarshaller(), new StringMarshaller(), "default");


                infinispan.UseTLS = true;
                infinispan.AddHost("127.0.0.1");
                infinispan.Version = 0x1f;
                infinispan.ForceReturnValue = false;
                infinispan.ClientIntelligence = 0x01;
                cache = infinispan.NewCache(new StringMarshaller(), new StringMarshaller(), "default");
            }
            catch (Exception)
            {
                hotRodServer?.ShutDownHotrodServer();
            }
        }

        public void Dispose()
        {
            hotRodServer.Dispose();
        }
    }
    [Collection("MainSequence")]

    public class SSLTest : IClassFixture<SSLTestFixture>
    {
        private readonly SSLTestFixture _fixture;
        private Cache<string, string> _cache_verified;
        private InfinispanDG _infinispan_verified;
        private Cache<string, string> _cache_bad_verified;
        private InfinispanDG _infinispan_bad_verified;
        private Cache<string, string> _cache;
        private InfinispanDG _infinispan;
        public SSLTest(SSLTestFixture fixture)
        {
            _fixture = fixture;
            _cache_verified = _fixture.cache_verified;
            _infinispan_verified = _fixture.infinispan_verified;
            _cache_bad_verified = _fixture.cache_bad_verified;
            _infinispan_bad_verified = _fixture.infinispan_bad_verified;
            _cache = _fixture.cache;
            _infinispan = _fixture.infinispan;
        }

        // private AuthorizationTester tester = new AuthorizationTester();
        // private IRemoteCache<string, string> testCache;
        // private IRemoteCache<string, string> scriptCache;


        [Fact]
        public async void SimpleTLSVerifiedGetTest()
        {
            String key = UniqueKey.NextKey();

            Assert.Null(await _cache_verified.Get(key));
            await _cache_verified.Put(key, "carbon");
            Assert.Equal("carbon", await _cache_verified.Get(key));
        }

        [Fact]
        public async void SimpleTLSGetTest()
        {
            String key = UniqueKey.NextKey();

            Assert.Null(await _cache.Get(key));
            await _cache.Put(key, "carbon");
            Assert.Equal("carbon", await _cache.Get(key));
        }

        [Fact]
        public async void SimpleTLSBadVerifiedGetTest()
        {
            String key = UniqueKey.NextKey();
            var excpt = await Assert.ThrowsAsync<InfinispanException>(() => _cache_bad_verified.Get(key));
        }

        // [Fact(Skip = "ALPN setup on Windows doesn't work")]
        // public void WriterSuccessTest()
        // {
        //     ConfigureSecuredCaches("infinispan-ca.pem", "keystore_client.p12");
        //     tester.TestWriterSuccess(_cache);
        // }

        // [Fact(Skip = "ALPN setup on Windows doesn't work")]
        // public void WriterPerformsReadsTest()
        // {
        //     ConfigureSecuredCaches("infinispan-ca.pem", "keystore_client.p12");
        //     tester.TestWriterPerformsReads(_cache);
        // }

        // [Fact(Skip = "ALPN setup on Windows doesn't work")]
        // public void WriterPerformsSupervisorOpsTest()
        // {
        //     ConfigureSecuredCaches("infinispan-ca.pem", "keystore_client.p12");
        //     tester.TestWriterPerformsSupervisorOps(_cache, scriptCache, marshaller);
        // }

        // [Fact(Skip = "https://issues.jboss.org/browse/HRCPP-434")]
        // public void ClientAuthFailureTest()
        // {
        //     ConfigureSecuredCaches("infinispan-ca.pem", "malicious_client.p12");
        //     tester.TestWriterSuccess(_cache);
        //     Assert.Fail("Should not get here");
        // }

        // [Fact(Skip = "ALPN setup on Windows doesn't work")]
        // public void SNI1CorrectCredentialsTest()
        // {
        //     ConfigureSecuredCaches("keystore_server_sni1_rsa.pem", "keystore_client.p12", "sni1");
        //     tester.TestWriterSuccess(_cache);
        // }

        // [Fact(Skip = "ALPN setup on Windows doesn't work")]
        // public void SNI2CorrectCredentialsTest()
        // {
        //     ConfigureSecuredCaches("keystore_server_sni2_rsa.pem", "keystore_client.p12", "sni2");
        //     tester.TestWriterSuccess(_cache);
        // }

        // [Fact(Skip = "ALPN setup on Windows doesn't work")]
        // public void SNIUntrustedTest()
        // {
        //     Assert.Throws<Infinispan.HotRod.Exceptions.TransportException>(() => ConfigureSecuredCaches("malicious.pem", "keystore_client.p12", "sni3-untrusted"));
        //     var ex = Assert.Throws<Infinispan.HotRod.Exceptions.TransportException>(() => tester.TestWriterSuccess(_cache));
        //     Assert.AreEqual("**** The server certificate did not validate correctly.\n", ex.Message);
        // }

        // private void ConfigureSecuredCaches(string serverCAFile, string clientCertFile, string sni = "")
        // {
        //     ConfigurationBuilder conf = new ConfigurationBuilder();
        //     conf.AddServer().Host("127.0.0.1").Port(11222).ConnectionTimeout(90000).SocketTimeout(900);
        //     marshaller = new JBasicMarshaller();
        //     conf.Marshaller(marshaller);
        //     SslConfigurationBuilder sslConf = conf.Ssl();
        //     conf.Security().Authentication()
        //                         .Enable()
        //                         .SaslMechanism("EXTERNAL")
        //                         .ServerFQDN("node0");

        //     RegisterServerCAFile(sslConf, serverCAFile, sni);
        //     RegisterClientCertificateFile(sslConf, clientCertFile);

        //     RemoteCacheManager remoteManager = new RemoteCacheManager(conf.Build(), true);

        //     _cache = remoteManager.GetCache<string, string>();
        //     scriptCache = remoteManager.GetCache<string, string>("___script_cache");
        // }

        // private void RegisterServerCAFile(SslConfigurationBuilder conf, string filename = "", string sni = "")
        // {
        //     if (filename != "")
        //     {
        //         CheckFileExists(filename);
        //         conf.Enable().ServerCAFile(filename);
        //         if (sni != "")
        //         {
        //             conf.SniHostName(sni);
        //         }
        //     }
        // }

        //     private void RegisterClientCertificateFile(SslConfigurationBuilder conf, string filename = "")
        //     {
        //         if (filename != "")
        //         {
        //             CheckFileExists(filename);
        //             conf.Enable().ClientCertificateFile(filename);
        //         }
        //     }

        //     private void CheckFileExists(string filename)
        //     {
        //         Assert.IsTrue(filename != "" && System.IO.File.Exists(filename));
        //     }
        // }
    }
}