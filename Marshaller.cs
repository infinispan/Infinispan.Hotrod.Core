using System;
using Google.Protobuf;
using Org.Infinispan.Protostream;
using Infinispan.Hotrod.Core;
using AppDB;
namespace Query
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
            if (obj == null)
            {
                return null;
            }
            if (obj is Application)
            {
                return ObjectToByteBuffer(1000043, obj);
            }
            if (obj is Review)
            {
                return ObjectToByteBuffer(1000042, obj);
            }
            throw new NotImplementedException(obj.ToString());
        }

        public override Object unmarshall(byte[] buff)
        {
            var w = WrappedMessage.Parser.ParseFrom(buff);
            switch (w.WrappedDescriptorId)
            {
                case 1000042:
                    return Review.Parser.ParseFrom(w.WrappedMessageBytes.ToByteArray());
                case 1000043:
                    return Application.Parser.ParseFrom(w.WrappedMessageBytes.ToByteArray());
            }
            throw new NotSupportedException("Unsupported DescriptionId: " + w.WrappedDescriptorId);
        }
    }
}
