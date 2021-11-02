using Xunit;
namespace Infinispan.Hotrod.Core.XUnitTest
{
    [Collection("MainSequence")]
    public class AuthorizationScramSha256Test : BaseAuthorizationTest
    {
        public AuthorizationScramSha256Test(AuthorizationCacheTestFixture fixture): base(fixture)
        {
        }
        public override string GetMech()
        {
            return "SCRAM-SHA-256";
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

    }
    public class AuthorizationScramSha1Test : BaseAuthorizationTest
    {
        public AuthorizationScramSha1Test(AuthorizationCacheTestFixture fixture): base(fixture)
        {
        }
        public override string GetMech()
        {
            return "SCRAM-SHA-1";
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

        [Fact (Skip="this test hangs on 11.0.x")]
        public void SupervisorPerformsAdminOpsTest()
        {
            tester.TestSupervisorPerformsAdminOps(supervisorCache);
        }

        [Fact]
        public void AdminSuccessTest()
        {
            tester.TestAdminSuccess(adminCache, scriptCache); //, marshaller);
        }
        [Fact (Skip="this test hangs on 11.0.x")]
        public void ReaderAccessStatsTest()
        {
            tester.TestReaderAccessStats(readerCache, scriptCache); //, marshaller);
        }

    }
}
