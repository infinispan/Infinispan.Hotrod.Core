using System;
using System.Collections.Generic;
using System.Text;

namespace Infinispan.Hotrod.Core
{
    public class InfinispanException : Exception
    {
        static int counter = 0;
        public InfinispanException(string msg) : base(msg)
        {
        }
        public InfinispanException(string msg, Exception innerError) : base(msg, innerError) { }
    }
}
