using System;
using System.Collections.Generic;
using System.Text;

namespace Infinispan.Hotrod.Core
{
    public class InfinispanException : Exception
    {
        public InfinispanException(string msg) : base(msg)
        {
        }
        public InfinispanException(string msg, Exception innerError) : base(msg, innerError) { }
    }
    public class InfinispanOperationException<K> : InfinispanException
    {
        public K Args;
        public InfinispanOperationException(K args, string msg) : base(msg)
        {
            Args = args;
        }
        public InfinispanOperationException(K args, string msg, Exception innerError) : base(msg, innerError)
        {
            Args = args;
        }
    }
}
