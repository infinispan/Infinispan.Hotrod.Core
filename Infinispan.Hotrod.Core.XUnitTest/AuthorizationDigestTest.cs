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
    }
}
