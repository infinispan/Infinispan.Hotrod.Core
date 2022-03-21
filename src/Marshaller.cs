using BeetleX.Buffers;
using MessagePack;
using System;
using System.Collections.Generic;
using System.Text;

namespace Infinispan.Hotrod.Core
{
    /// <summary>
    /// Marshaller knows how to convert a type T into byte[] and viceversa.
    /// </summary>
    /// Before being sent in an hotrod request,
    /// keys and values must be turned into byte[]. In the other way byte[], need to be turned back into object when an
    /// hotrod response is received. Marshallers for key and value must be provided by the user at the cache creation. See InfinispanDG.NewCache<K,V>().
    /// <typeparam name="T">The type handled by the Marshaller</typeparam>
    public abstract class Marshaller<T>
    {
        public abstract byte[] marshall(T t);
        public abstract T unmarshall(byte[] buff);
    }

    /// <summary>
    /// An untility Marshaller that works on strings using ASCII encoding
    /// by default
    /// </summary>
    public class StringMarshaller : Marshaller<string>
    {

        public static StringMarshaller _ASCII = new StringMarshaller();
        public Encoding Encoding;
        /// <summary>
        /// Creates a StringMarshaller
        /// </summary>
        /// <param name="enc">The chars encoder. ASCII by default</param>
        public StringMarshaller(Encoding enc = null)
        {
            Encoding = (enc == null) ? Encoding.ASCII : Encoding = enc;
        }
        public override byte[] marshall(string t)
        {
            return t == null ? null : Encoding.GetBytes(t);
        }
        public override string unmarshall(byte[] buff)
        {
            return buff == null ? null : Encoding.GetString(buff);
        }
    }
}
