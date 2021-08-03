using BeetleX.Buffers;
using BeetleX.Clients;
using MessagePack;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace Infinispan.Hotrod.Core
{
    public class Codec {

        public static Codec30 getCodec(byte version = 30) {
            switch (version) {
                default:
                    return new Codec30();
            }
        }

        public static Int64 readVLong(PipeStream stream) {
            byte b = (byte)stream.ReadByte();
            Int64 i = (Int64)(b & 0x7F);
            for (int shift = 7; (b & 0x80) != 0; shift += 7) {
                b = (byte)stream.ReadByte();
                i |= (Int64)((b & 0x7FL) << shift);
            }
            return i;
        }
        public static Int32 readVInt(PipeStream stream) {
            byte b = (byte)stream.ReadByte();
            Int32 i = (Int32) (b & 0x7F);
            for (int shift = 7; (b & 0x80) != 0; shift += 7) {
                b = (byte)stream.ReadByte();
                i |= (Int32)((b & 0x7F) << shift);
            }
            return i;
        }

        public static UInt32 readVUInt(PipeStream stream) {
            byte b = (byte)stream.ReadByte();
            UInt32 i = (UInt32) (b & 0x7F);
            for (int shift = 7; (b & 0x80) != 0; shift += 7) {
                b = (byte)stream.ReadByte();
                i |= (UInt32)((b & 0x7F) << shift);
            }
            return i;
        }

        public static byte[] readArray(PipeStream stream) {
            Int32 size = readVInt(stream);
            byte[] ret = new byte[size];
            for (int i=0; i<size; i++) {
                ret[i]= (byte)stream.ReadByte();
            }
            return ret;
        }

        public static Int16 readShort(PipeStream stream) {
            Int16 val;
            var b = (byte) stream.ReadByte();
            val = (Int16)(b<<8);
            val += (byte) (stream.ReadByte() & 0xff);
            return val;
        }
        public static UInt16 readUnsignedShort(PipeStream stream) {
            UInt16 val;
            var b = (byte) stream.ReadByte();
            val = (UInt16)(b<<8);
            val |= (byte) (stream.ReadByte() & 0xff);
            return val;
        }


        public static Int32 readInt(PipeStream stream) {
            Int32 val = readShort(stream);
            val = (val << 16) + readShort(stream);
            return val;
        }
        public static Int64 readLong(PipeStream stream) {
            Int64 val = readInt(stream);
            val = (val << 32) + readInt(stream);
            return val;
        }
        public static void writeVLong(Int64 val, PipeStream stream) {
            while (val>0x7f) {
                byte b = (byte)(val & 0x7fL | 0x80);
                stream.Write(b);
                val >>=7;
            }
            stream.Write((byte)val);
        }
        public static void writeVInt(Int32 val, PipeStream stream) {
            while (val>0x7f) {
                byte b = (byte)(val & 0x7fL | 0x80);
                stream.Write(b);
                val >>=7;
            }
            stream.Write((byte)val);
        }

        public static void writeVUInt(UInt32 val, PipeStream stream) {
            while (val>0x7f) {
                byte b = (byte)(val & 0x7fL | 0x80);
                stream.Write(b);
                val >>=7;
            }
            stream.Write((byte)val);
        }

        public static void writeInt(Int32 v, PipeStream stream) {
            stream.Write((byte) (v >> 24));
            stream.Write((byte) (v >> 16));
            stream.Write((byte) (v >> 8));
            stream.Write((byte) v);
        }

        public static void writeLong(Int64 v,PipeStream stream) {
            stream.Write((byte) (v >> 56));
            stream.Write((byte) (v >> 48));
            stream.Write((byte) (v >> 40));
            stream.Write((byte) (v >> 32));
            stream.Write((byte) (v >> 24));
            stream.Write((byte) (v >> 16));
            stream.Write((byte) (v >> 8));
            stream.Write((byte) v);
        }

        public static void writeArray(byte[] arr, PipeStream stream) {
            writeVInt((Int32)arr.Length, stream);
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
                    writeVInt((Int32) mt.Params.Count, stream);
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
                writeVLong(MaxIdle.Value, stream);
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
        public Int64 Value { get; set; }
        public bool hasValue() {
            return this!=null && this.Unit!=TimeUnit.DEFAULT && this.Unit!=TimeUnit.INFINITE; 
        }
    }
 
    public class MediaType {
        public byte InfoType;
        public Int32 PredefinedMediaType;
        public byte[] CustomMediaType;
        public UInt32 ParamsNum;
        public List<Tuple<byte[],byte[]>> Params;
    }

    public class Codec30 {
        public Codec30() {}

        public static Boolean isSuccess(int status) {
            return status == NO_ERROR_STATUS
                || status == NO_ERROR_STATUS_OBJ_STORAGE
                || status == SUCCESS_WITH_PREVIOUS
                || status == SUCCESS_WITH_PREVIOUS_OBJ_STORAGE;
        }
        public static Boolean isNotExecuted(int status) {
            return status == NOT_PUT_REMOVED_REPLACED_STATUS
                || status == NOT_EXECUTED_WITH_PREVIOUS
                || status == NOT_EXECUTED_WITH_PREVIOUS_OBJ_STORAGE;
        }

        public static Boolean isNotExist(int status) {
            return status == KEY_DOES_NOT_EXIST_STATUS;
        }

        public static Boolean hasPrevious(int status) {
            return status == SUCCESS_WITH_PREVIOUS
                    || status == SUCCESS_WITH_PREVIOUS_OBJ_STORAGE
                    || status == NOT_EXECUTED_WITH_PREVIOUS
                    || status == NOT_EXECUTED_WITH_PREVIOUS_OBJ_STORAGE;
        }

        public static Boolean hasError(int status) {
            switch (status) {
                case INVALID_MAGIC_OR_MESSAGE_ID_STATUS:
                case UNKNOWN_COMMAND_STATUS:
                case UNKNOWN_VERSION_STATUS:
                case REQUEST_PARSING_ERROR_STATUS:
                case SERVER_ERROR_STATUS:
                case COMMAND_TIMEOUT_STATUS:
                return true;
            }
            return false;
        }
      //response status
   public const byte NO_ERROR_STATUS = 0x00;
   public const byte NOT_PUT_REMOVED_REPLACED_STATUS = 0x01;
   public const byte KEY_DOES_NOT_EXIST_STATUS = 0x02;
   public const byte SUCCESS_WITH_PREVIOUS = 0x03;
   public const byte NOT_EXECUTED_WITH_PREVIOUS = 0x04;
   public const byte INVALID_ITERATION = 0x05;
   public const byte NO_ERROR_STATUS_OBJ_STORAGE = 0x06;
   public const byte SUCCESS_WITH_PREVIOUS_OBJ_STORAGE = 0x07;
   public const byte NOT_EXECUTED_WITH_PREVIOUS_OBJ_STORAGE = 0x08;
   public const byte INVALID_MAGIC_OR_MESSAGE_ID_STATUS = 0x81;
   public const byte REQUEST_PARSING_ERROR_STATUS = 0x84;
   public const byte UNKNOWN_COMMAND_STATUS = 0x82;
   public const byte UNKNOWN_VERSION_STATUS = 0x83;
   public const byte SERVER_ERROR_STATUS = 0x85;
   public const byte COMMAND_TIMEOUT_STATUS = 0x86;
   public const byte NODE_SUSPECTED = 0x87;
   public const byte ILLEGAL_LIFECYCLE_STATE = 0x88;

    }
}

