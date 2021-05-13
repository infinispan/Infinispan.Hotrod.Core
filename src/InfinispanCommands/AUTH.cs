using System;
using System.Collections.Generic;
using System.Text;
using BeetleX.Buffers;
using System.Net;
using MailKit.Security;

namespace BeetleX.Infinispan.Commands
{
    public class AUTH : Command
    {
        public AUTH(string mech, NetworkCredential c)
        {
            Credential = c;
            NetworkReceive = OnReceive;
            SaslMechName = mech;
            c.Domain="hotrod/node0";
            SaslMech = SaslMechanism.Create(mech, new Uri("hotrod://node0"), c);
        }
        public int TimeOut { get; set; }

        public override string Name => "AUTH";
        public override Byte Code => 0x23;
        public NetworkCredential Credential;
        public SaslMechanism SaslMech;
        public string SaslMechName;
        public byte Completed;
        public byte[] Challenge= new byte[0];
        private int Step = 0;
        public override void OnExecute(Cache cache)
        {
            // TODO: here the code to build the bytebuffer that will be sent
            base.OnExecute(cache); // Generic code (build header?)
        }

        public override void Execute(Cache cache, InfinispanClient client, PipeStream stream)
        {
            switch (Step) {
                case 0:
                    if (this.SaslMechName.Equals("DIGEST-MD5")) {
                    base.Execute(cache, client, stream);
                    Codec.writeArray(Encoding.ASCII.GetBytes(SaslMechName), stream);
                    Codec.writeArray(Challenge, stream);
                    stream.Flush();
                    Step++;
                    break;
                    }
                    Step++;
                    goto case 1;
                case 1:
                    base.Execute(cache, client, stream);
                    Codec.writeArray(Encoding.ASCII.GetBytes(SaslMechName), stream);
                    var s = Convert.ToBase64String(Challenge);
                    s = SaslMech.Challenge(s);
                    Challenge = Convert.FromBase64String(s);
                    Codec.writeArray(Challenge, stream);
                    Completed=1;
                    stream.Flush();
                    Step++;
                    break;
                default:
                    Completed=1;
                    break;

            }
        }
        public override Result OnReceive(InfinispanRequest request, PipeStream stream)
        {
            var completed = (byte)stream.ReadByte();
            Challenge = Codec.readArray(stream);
            return new Result{ Status =  ResultStatus.Completed, ResultType = ResultType.Object };
        }
    }
}
