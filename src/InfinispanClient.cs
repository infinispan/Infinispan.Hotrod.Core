using BeetleX;
using BeetleX.Buffers;
using BeetleX.Clients;
using MessagePack;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace Infinispan.Hotrod.Core
{
    public class InfinispanClient
    {
        public InfinispanClient(bool ssl, string hostname, int hostport)
        {
            if (ssl)
            {

                TcpClient = BeetleX.SocketFactory.CreateSslClient<AsyncTcpClient>(hostname, hostport, "beetlex");
                TcpClient.CertificateValidationCallback = (o, e, f, d) =>
                {
                    return true;
                };
            }
            else
            {
                TcpClient = BeetleX.SocketFactory.CreateClient<AsyncTcpClient>(hostname, hostport);
            }
        }

        public AsyncTcpClient TcpClient { get; private set; }

        public void Send(CommandContext cmdCtx, Command cmd)
        {
            PipeStream stream = TcpClient.Stream.ToPipeStream();

            cmd.Execute(cmdCtx, this, stream);

            TcpClient.Stream.Flush();
        }

    }
}
