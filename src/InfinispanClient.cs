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
        public InfinispanClient(InfinispanHost host)
        {
            this.Host = host;
            if (this.Host.Cluster.UseTLS)
            {

                TcpClient = BeetleX.SocketFactory.CreateSslClient<AsyncTcpClient>(this.Host.Name, this.Host.Port, "hotrod");
                TcpClient.CertificateValidationCallback = (o, e, f, d) =>
                {
                    if (host.Cluster.CACert == null)
                        return true;
                    bool result = host.Cluster.CACert.Build(new System.Security.Cryptography.X509Certificates.X509Certificate2(e));
                    if (!result)
                    {
                        System.Diagnostics.Debug.WriteLine("{0}", host.Cluster.CACert.ChainStatus);
                    }
                    return result;
                };
            }
            else
            {
                TcpClient = BeetleX.SocketFactory.CreateClient<AsyncTcpClient>(this.Host.Name, this.Host.Port);
            }
        }

        public readonly InfinispanHost Host;
        public AsyncTcpClient TcpClient { get; private set; }

        internal void Send(CommandContext cmdCtx, Command cmd)
        {
            PipeStream stream = TcpClient.Stream.ToPipeStream();

            cmd.Execute(cmdCtx, this, stream);

            TcpClient.Stream.Flush();
        }

        public void ReturnToPool()
        {
            this.Host.Push(this);
        }

    }
}
