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
    }
}
