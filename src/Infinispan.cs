using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace Infinispan.Hotrod.Core
{
    public static class InfinispanDGEx
    {
        public static InfinispanDG Instance(this InfinispanDG dg)
        {
            return dg ?? InfinispanDG.Default;
        }
    }


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
