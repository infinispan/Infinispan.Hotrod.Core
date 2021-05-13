﻿using System;
using System.Collections.Generic;
using System.Text;

namespace BeetleX.Infinispan
{
    public interface IHostHandler
    {
        InfinispanHost AddHost(string host, int port = 11222);

        InfinispanHost AddHost(string host, int port, bool ssl);

        InfinispanHost GetHost();

    }
}
