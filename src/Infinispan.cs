using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace Infinispan.Hotrod.Core
{
    public class DefaultInfinispan
    {
        public static InfinispanDG Instance
        {
            get
            {
                return InfinispanDG.Default;
            }

        }
    }
}
