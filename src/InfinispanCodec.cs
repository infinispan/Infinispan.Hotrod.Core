using BeetleX.Buffers;
using BeetleX.Clients;
using MessagePack;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace BeetleX.Infinispan
{
    public class Codec {
        public static UInt64 readVLong(PipeStream stream) {
            byte b = (byte)stream.ReadByte();
            UInt64 i = (UInt64)(b & 0x7F);
            for (int shift = 7; (b & 0x80) != 0; shift += 7) {
                b = (byte)stream.ReadByte();
                i |= (UInt64)((b & 0x7FL) << shift);
            }
            return i;
        }
        public static UInt32 readVInt(PipeStream stream) {
            byte b = (byte)stream.ReadByte();
            UInt32 i = (UInt32) (b & 0x7F);
            for (int shift = 7; (b & 0x80) != 0; shift += 7) {
                b = (byte)stream.ReadByte();
                i |= (UInt32)((b & 0x7F) << shift);
            }
            return i;
        }

        public static byte[] readArray(PipeStream stream) {
            UInt32 size = readVInt(stream);
            byte[] ret = new byte[size];
            for (int i=0; i<size; i++) {
                ret[i]= (byte)stream.ReadByte();
            }
            return ret;
        }

        public static UInt16 readShort(PipeStream stream) {
            UInt16 val;
            var b = (byte) stream.ReadByte();
            val = (UInt16)(b<<8);
            val += (byte) stream.ReadByte();
            return val;
        }

        public static void writeVLong(UInt64 val, PipeStream stream) {
            while (val>0x7f) {
                byte b = (byte)(val & 0x7fL | 0x80);
                stream.Write(b);
                val >>=7;
            }
            stream.Write((byte)val);
        }
        public static void writeVInt(UInt32 val, PipeStream stream) {
            while (val>0x7f) {
                byte b = (byte)(val & 0x7fL | 0x80);
                stream.Write(b);
                val >>=7;
            }
            stream.Write((byte)val);
        }

        public static void writeArray(byte[] arr, PipeStream stream) {
            writeVInt((UInt32)arr.Length, stream);
            if (arr.Length>0) {
                stream.Write(arr,0,arr.Length);
            }
        }
        public static void writeMediaType(MediaType mt, PipeStream stream) {
            if (mt == null) {
                stream.WriteByte(0x00);
                return;
            }
            switch (mt.InfoType) {
                case 0:
                    stream.WriteByte(0x00);
                break;
                case 1:
                    writeVInt(mt.PredefinedMediaType, stream);
                break;
                case 2:
                    writeArray(mt.CustomMediaType, stream);
                    writeVInt((UInt32) mt.Params.Count, stream);
                    foreach  (var par in  mt.Params) {
                        writeArray(par.Item1, stream); // write parameter key
                        writeArray(par.Item2, stream); // and par value
                    }
                break;
            }
        }
        
        public static void writeExpirations(ExpirationTime Lifespan, ExpirationTime MaxIdle, PipeStream stream) {
            byte units = (byte)(((int)Lifespan.Unit << 4)+(int)MaxIdle.Unit);
            stream.WriteByte(units);
            if (Lifespan.hasValue()) {
                writeVLong(Lifespan.Value, stream);
            }
            if (MaxIdle.hasValue()) {
                writeVLong(Lifespan.Value, stream);
            }
        }
    }

    public enum TimeUnit : byte {
        SECONDS = 0x00,
        MILLISECONDS = 0x01,
        NANOSECONDS = 0x02,
        MICROSECONDS = 0x03,
        MINUTES = 0x04,
        HOURS = 0x05,
        DAYS = 0x06,
        DEFAULT = 0x07,
        INFINITE = 0x08
    }
    public class ExpirationTime {
        public TimeUnit Unit { get; set; }
        public UInt64 Value { get; set; }
        public bool hasValue() {
            return this!=null && this.Unit!=TimeUnit.DEFAULT && this.Unit!=TimeUnit.INFINITE; 
        }
    }
 
    public class MediaType {
        public byte InfoType;
        public UInt32 PredefinedMediaType;
        public byte[] CustomMediaType;
        public UInt32 ParamsNum;
        public List<Tuple<byte[],byte[]>> Params;
    }

    public class Codec30 {
        private Codec30() {}

        public static Codec30 getCodec(byte version = 30) {
            switch (version) {
                default:
                    return new Codec30();
            }
        }
    }
}

