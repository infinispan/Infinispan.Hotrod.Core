namespace Infinispan.Hotrod.Core.XUnitTest
{
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
