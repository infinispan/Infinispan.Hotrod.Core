﻿using System;
using SampleBankAccount;
using Google.Protobuf;
using Org.Infinispan.Protostream;
using Infinispan.Hotrod.Core;
namespace Infinispan.Hotrod.Core.XUnitTest
{
    class BasicTypesProtoStreamMarshaller : Marshaller<Object>
    {
        private byte[] ObjectToByteBuffer(int descriptorId, object obj)
        {
            IMessage u = (IMessage)obj;

            int size = u.CalculateSize();
            byte[] bytes = new byte[size];
            CodedOutputStream cos = new CodedOutputStream(bytes);
            u.WriteTo(cos);
            
            cos.Flush();
            WrappedMessage wm = new WrappedMessage();
            wm.WrappedMessageBytes = ByteString.CopyFrom(bytes);
            wm.WrappedDescriptorId = descriptorId;

            byte[] msgBytes = new byte[wm.CalculateSize()];
            CodedOutputStream msgCos = new CodedOutputStream(msgBytes);
            wm.WriteTo(msgCos);
            msgCos.Flush();
            return msgBytes;
        }

        private byte[] StringToByteBuffer(string str)
        {
            int t = CodedOutputStream.ComputeTagSize(9);
            int s = CodedOutputStream.ComputeStringSize(str);

            s += t;
            byte[] bytes = new byte[s];
            CodedOutputStream cos = new CodedOutputStream(bytes);
            cos.WriteTag((9 << 3) + 2);
            cos.WriteString(str);
            cos.Flush();
            return bytes;
        }

        private byte[] IntToByteBuffer(int i)
        {
            int t = CodedOutputStream.ComputeTagSize(5);
            int s = CodedOutputStream.ComputeInt32Size(i);

            s += t;
            byte[] bytes = new byte[s];
            CodedOutputStream cos = new CodedOutputStream(bytes);
            cos.WriteTag((5 << 3) + 0);
            cos.WriteInt32(i);
            cos.Flush();
            return bytes;
        }

        public override byte[] marshall(Object obj)
        {
                        if (obj is String)
            {
                return StringToByteBuffer((String)obj);
            }
            if (obj is int)
            {
                return IntToByteBuffer((int)obj);
            }
            if (obj is User)
            {
                return ObjectToByteBuffer(1000042, obj);
            }
            if (obj is Account)
            {
                return ObjectToByteBuffer(1000044, obj);
            }
            if (obj is Transaction)
            {
                return ObjectToByteBuffer(1000045, obj);
            }
            throw new NotImplementedException();
        }

        public override Object unmarshall(byte[] buff)
        {
            base_types bt = base_types.Parser.ParseFrom(buff);
            if (bt.I32 != 0)
            {
                return bt.I32;
            }
            else if (bt.I64 != 0)
            {
                return bt.I64;
            }
            else return bt.Str;

        }
    }
}
