using BeetleX.Buffers;
using MessagePack;
using System;
using System.Collections.Generic;
using System.Text;

namespace Infinispan.Hotrod.Core
{
    public abstract class Marshaller<T> {
        public abstract byte[] marshall(T t);
        public abstract T unmarshall(byte[] buff);
    }

    public class StringMarshaller: Marshaller<string> {

        public Encoding Encoding;
        public StringMarshaller(Encoding enc = null) {
            Encoding = (enc == null) ? Encoding.ASCII : Encoding = enc;
        }
        public override byte[] marshall(string t) {
            return t==null ? null : Encoding.GetBytes(t);
        }
        public override string unmarshall(byte[] buff) {
            return buff==null ? null : Encoding.GetString(buff);
        }
    }
}
