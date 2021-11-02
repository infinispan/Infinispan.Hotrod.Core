using Xunit;
namespace Infinispan.Hotrod.Core.XUnitTest
{
    [Collection("MainSequence")]
    public class AuthorizationDigestTest : BaseAuthorizationTest
    {
        public AuthorizationDigestTest(AuthorizationCacheTestFixture fixture): base(fixture)
        {
        }
        public override string GetMech()
        {
            return "DIGEST-MD5";
        }

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

        [Fact(Skip="this test hangs on 11.0.x")]
        public void SupervisorPerformsAdminOpsTest()
        {
            tester.TestSupervisorPerformsAdminOps(supervisorCache);
        }

        [Fact]
        public void AdminSuccessTest()
        {
            tester.TestAdminSuccess(adminCache, scriptCache); //, marshaller);
        }
        [Fact(Skip="this test hangs on 11.0.x")]
        public void ReaderAccessStatsTest()
        {
            tester.TestReaderAccessStats(readerCache, scriptCache); //, marshaller);
        }

    }
}
