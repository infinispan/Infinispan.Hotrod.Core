namespace Infinispan.Hotrod.Core.XUnitTest
{
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
