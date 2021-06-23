using Xunit;
namespace Infinispan.Hotrod.Core.XUnitTest
{
    [Collection("MainSequence")]
    public class AuthorizationPlainTest : BaseAuthorizationTest
    {
        public AuthorizationPlainTest(AuthorizationCacheTestFixture fixture): base(fixture)
        {
        }
        public override string GetMech()
        {
            return "PLAIN";
        }
    }
}
