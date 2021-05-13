using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace BeetleX.Infinispan
{
    public class Cache {
        public Cache(string name) {
            Name = name;
            MessageId=1;
            NameAsBytes = Encoding.ASCII.GetBytes(Name);
            Version = 30; // TODO: parametrize
            ClientIntelligence = 0x01; // TODO: parametrize
            TopologyId = 0x01;
            codec = Codec30.getCodec(Version);
            ForceReturnValue = false;
        }

        public static Cache NullCache = new Cache("");
        public string Name {get;}
        public byte[] NameAsBytes {get;}
        public byte Version {get;}
        public UInt64 MessageId {get;}
        public byte ClientIntelligence {get;}
        public UInt32 TopologyId {get; set;}
        public MediaType KeyMediaType {get; set;}
        public MediaType ValueMediaType {get; set;}
        public Codec30 codec;
        public bool ForceReturnValue;
    }
}