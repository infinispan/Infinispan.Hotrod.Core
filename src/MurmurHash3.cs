using System;
namespace Infinispan.Hotrod.Core {
public class MurmurHash3 {
   private static MurmurHash3 instance = new MurmurHash3();
//   public static byte INVALID_CHAR = (byte) '?';

   // public static MurmurHash3 getInstance() {
   //    return instance;
   // }

   private MurmurHash3() {
   }

    class State {
      public Int64 h1;
      public Int64 h2;

      public Int64 k1;
      public Int64 k2;

      public Int64 c1;
      public Int64 c2;
   }

   static long getblock(sbyte[] key, int i) {
      return
           ((key[i + 0] & 0x00000000000000FFL))
         | ((key[i + 1] & 0x00000000000000FFL) << 8)
         | ((key[i + 2] & 0x00000000000000FFL) << 16)
         | ((key[i + 3] & 0x00000000000000FFL) << 24)
         | ((key[i + 4] & 0x00000000000000FFL) << 32)
         | ((key[i + 5] & 0x00000000000000FFL) << 40)
         | ((key[i + 6] & 0x00000000000000FFL) << 48)
         | ((key[i + 7] & 0x00000000000000FFL) << 56);
   }

   static void bmix(State state) {
      state.k1 *= state.c1;
      state.k1 = (state.k1 << 23) | uRightShift(state.k1, 64 - 23);
      state.k1 *= state.c2;
      state.h1 ^= state.k1;
      state.h1 += state.h2;

      state.h2 = (state.h2 << 41) | uRightShift(state.h2, 64 - 41);

      state.k2 *= state.c2;
      state.k2 = (state.k2 << 23) | uRightShift(state.k2, 64 - 23);
      state.k2 *= state.c1;
      state.h2 ^= state.k2;
      state.h2 += state.h1;

      state.h1 = state.h1 * 3 + 0x52dce729;
      state.h2 = state.h2 * 3 + 0x38495ab5;

      state.c1 = state.c1 * 5 + 0x7b7d159c;
      state.c2 = state.c2 * 5 + 0x6bce6396;
   }

   static long fmix(long k) {
      k ^= uRightShift(k,33);
      k *= unchecked((long)0xff51afd7ed558ccdL);
      k ^= uRightShift(k,33);;
      k *= unchecked((long)0xc4ceb9fe1a85ec53L);
      k ^= uRightShift(k,33);;
      return k;
   }

   static long uRightShift(long v, int s) {
      return unchecked((long)unchecked ((ulong)v >> s));
   }

   public static Int64 getblock64(Int64[] p, int i) {
      return p[i];
   }

   /**
    * Hash a value using the x64 64 bit variant of MurmurHash3
    *
    * @param key value to hash
    * @param seed random value
    * @return 64 bit hashed key
    */
   public static long MurmurHash3_x64_64(sbyte[] key, uint seed) {
      // Exactly the same as MurmurHash3_x64_128, except it only returns state.h1
      State state = new State();

      state.h1 = (Int64)(0x9368e53c2f6af274UL ^ seed);
      state.h2 = (Int64)(0x586dcd208f7cd3fdUL ^ seed);

      state.c1 = unchecked ((long)0x87c37b91114253d5UL);
      state.c2 = unchecked ((long)0x4cf5ad432745937fUL);

      for (int i = 0; i < key.Length / 16; i++) {
         state.k1 = getblock(key, i * 2 * 8);
         state.k2 = getblock(key, (i * 2 + 1) * 8);

         bmix(state);
      }

      state.k1 = 0;
      state.k2 = 0;

      int tail = (key.Length >> 4) << 4;

      switch (key.Length & 15) {
         case 15: state.k2 ^= ((Int64)key[tail + 14]) << 48;
         goto case 14;
         case 14: state.k2 ^= ((Int64)key[tail + 13]) << 40;
         goto case 13;
         case 13: state.k2 ^= ((Int64)key[tail + 12]) << 32;
         goto case 12;
         case 12: state.k2 ^= ((Int64)key[tail + 11]) << 24;
         goto case 11;
         case 11: state.k2 ^= ((Int64)key[tail + 10]) << 16;
         goto case 10;
         case 10: state.k2 ^= ((Int64)key[tail + 9]) << 8;
         goto case 9;
         case 9:  state.k2 ^= ((Int64)key[tail + 8]);
         goto case 8;
         case 8:  state.k1 ^= ((Int64)key[tail + 7]) << 56;
         goto case 7;
         case 7:  state.k1 ^= ((Int64)key[tail + 6]) << 48;
         goto case 6;
         case 6:  state.k1 ^= ((Int64)key[tail + 5]) << 40;
         goto case 5;
         case 5:  state.k1 ^= ((Int64)key[tail + 4]) << 32;
         goto case 4;
         case 4:  state.k1 ^= ((Int64)key[tail + 3]) << 24;
         goto case 3;
         case 3:  state.k1 ^= ((Int64)key[tail + 2]) << 16;
         goto case 2;
         case 2:  state.k1 ^= ((Int64)key[tail + 1]) << 8;
         goto case 1;
         case 1:  state.k1 ^= ((Int64)key[tail + 0]);
            bmix(state);
            break;
      }

      state.h2 ^= (uint)key.Length;

      state.h1 += state.h2;
      state.h2 += state.h1;

      state.h1 = fmix(state.h1);
      state.h2 = fmix(state.h2);

      state.h1 += state.h2;
      state.h2 += state.h1;

      return state.h1;
   }

   // /**
   //  * Hash a value using the x64 32 bit variant of MurmurHash3
   //  *
   //  * @param key value to hash
   //  * @param seed random value
   //  * @return 32 bit hashed key
   //  */
   public static Int32 MurmurHash3_x64_32(sbyte[] key, uint seed) {
      return  ((Int32)(uRightShift(MurmurHash3_x64_64(key, seed), 32)));
   }

   // /**
   //  * Hash a value using the x64 64 bit variant of MurmurHash3
   //  *
   //  * @param key value to hash
   //  * @param seed random value
   //  * @return 64 bit hashed key
   //  */
   // public static ulong MurmurHash3_x64_64(ulong[] key, uint seed) {
   //    // Exactly the same as MurmurHash3_x64_128, except it only returns state.h1
   //    State state = new State();

   //    state.h1 = 0x9368e53c2f6af274UL ^ seed;
   //    state.h2 = 0x586dcd208f7cd3fdUL ^ seed;

   //    state.c1 = 0x87c37b91114253d5UL;
   //    state.c2 = 0x4cf5ad432745937fUL;

   //    for (int i = 0; i < key.Length / 2; i++) {
   //       state.k1 = key[i * 2];
   //       state.k2 = key[i * 2 + 1];

   //       bmix(state);
   //    }

   //    ulong tail = key[key.Length - 1];

   //    if (key.Length % 2 != 0) {
   //       state.k1 ^= tail;
   //       bmix(state);
   //    }

   //    state.h2 ^= (uint)(key.Length * 8);

   //    state.h1 += state.h2;
   //    state.h2 += state.h1;

   //    state.h1 = fmix(state.h1);
   //    state.h2 = fmix(state.h2);

   //    state.h1 += state.h2;
   //    state.h2 += state.h1;

   //    return state.h1;
   // }

   /**
    * Hash a value using the x64 32 bit variant of MurmurHash3
    *
    * @param key value to hash
    * @param seed random value
    * @return 32 bit hashed key
    */
//    public static int MurmurHash3_x64_32(ulong[] key, uint seed) {
//       return (int) (MurmurHash3_x64_64(key, seed) >> 32);
//    }

   public static Int32 hash(sbyte[] payload) {
      return MurmurHash3_x64_32(payload, 9001);
   }

//    /**
//     * Hashes a byte array efficiently.
//     *
//     * @param payload a byte array to hash
//     * @return a hash code for the byte array
//     */
//    public static int hash(ulong[] payload) {
//       return MurmurHash3_x64_32(payload, 9001);
//    }
   public static Int32 hash(sbyte[] key, int size) {
     return MurmurHash3_x64_32(key, 9001);
   }
   public static Int32 hash(UInt32 hashcode) {
      // Obtained by inlining MurmurHash3_x64_32(byte[], 9001) and removing all the unused code
      // (since we know the input is always 4 bytes and we only need 4 bytes of output)
      sbyte b0 = (sbyte) hashcode;
      sbyte b1 = (sbyte) (hashcode >> 8);
      sbyte b2 = (sbyte) (hashcode >> 16);
      sbyte b3 = (sbyte) (hashcode >> 24);
      State state = new State();

      state.h1 = unchecked((long)0x9368e53c2f6af274UL ^ 9001);
      state.h2 = unchecked((long)0x586dcd208f7cd3fdUL ^ 9001);

      state.c1 = unchecked ((long)0x87c37b91114253d5UL);
      state.c2 = unchecked ((long)0x4cf5ad432745937fUL);

      state.k1 = 0;
      state.k2 = 0;

      state.k1 ^=  b3 << 24;
      state.k1 ^=  b2 << 16;
      state.k1 ^=  b1 << 8;
      state.k1 ^=  b0;
      bmix(state);

      state.h2 ^= 4;

      state.h1 += state.h2;
      state.h2 += state.h1;

      state.h1 = fmix(state.h1);
      state.h2 = fmix(state.h2);

      state.h1 += state.h2;
      state.h2 += state.h1;

      return ((Int32)(state.h1 >> 32));
   }

//    public int hash(Object o) {
//       if (o instanceof byte[])
//          return hash((byte[]) o);
//       else if (o instanceof WrappedBytes) {
//          return hash(((WrappedBytes) o).getBytes());
//       }
//       else if (o instanceof long[])
//          return hash((long[]) o);
//       else if (o instanceof String)
//          return hashString((String) o);
//       else
//          return hash(o.hashCode());
//    }

   // private uint hashString(string s) {
   //    return (uint) (MurmurHash3_x64_64_String(s, 9001) >> 32);
   // }

   // ulong MurmurHash3_x64_64_String(string s, ulong seed) {
   //    // Exactly the same as MurmurHash3_x64_64, except it works directly on a String's chars
   //    MurmurHash3.State state = new MurmurHash3.State();

   //    state.h1 = 0x9368e53c2f6af274UL ^ seed;
   //    state.h2 = 0x586dcd208f7cd3fdUL ^ seed;

   //    state.c1 = 0x87c37b91114253d5UL;
   //    state.c2 = 0x4cf5ad432745937fUL;

   //    uint byteLen = 0;
   //    uint stringLen = (uint)s.Length;
   //    for (int i = 0; i < stringLen; i++) {
   //       char c1 = s[i];
   //       int cp;
   //       if (!Char.IsSurrogate(c1)) {
   //          cp = c1;
   //       } else if (Char.IsHighSurrogate(c1)){
   //          if (i + 1 < stringLen) {
   //             char c2 = s[i + 1];
   //             if (Char.IsLowSurrogate(c2)) {
   //                i++;
   //                cp = Char.ConvertToUtf32(c1, c2);
   //             } else {
   //                cp = INVALID_CHAR;
   //             }
   //          } else {
   //             cp = INVALID_CHAR;
   //          }
   //       } else {
   //          cp = INVALID_CHAR;
   //       }

   //       if (cp <= 0x7f) {
   //          addByte(state, (byte) cp, byteLen++);
   //       } else if (cp <= 0x07ff) {
   //          byte b1 = (byte) (0xc0 | (0x1f & (cp >> 6)));
   //          byte b2 = (byte) (0x80 | (0x3f & cp));
   //          addByte(state, b1, byteLen++);
   //          addByte(state, b2, byteLen++);
   //       } else if (cp <= 0xffff) {
   //          byte b1 = (byte) (0xe0 | (0x0f & (cp >> 12)));
   //          byte b2 = (byte) (0x80 | (0x3f & (cp >> 6)));
   //          byte b3 = (byte) (0x80 | (0x3f & cp));
   //          addByte(state, b1, byteLen++);
   //          addByte(state, b2, byteLen++);
   //          addByte(state, b3, byteLen++);
   //       } else {
   //          byte b1 = (byte) (0xf0 | (0x07 & (cp >> 18)));
   //          byte b2 = (byte) (0x80 | (0x3f & (cp >> 12)));
   //          byte b3 = (byte) (0x80 | (0x3f & (cp >> 6)));
   //          byte b4 = (byte) (0x80 | (0x3f & cp));
   //          addByte(state, b1, byteLen++);
   //          addByte(state, b2, byteLen++);
   //          addByte(state, b3, byteLen++);
   //          addByte(state, b4, byteLen++);
   //       }
   //    }

   //    ulong savedK1 = state.k1;
   //    ulong savedK2 = state.k2;
   //    state.k1 = 0;
   //    state.k2 = 0;
   //    switch (byteLen & 15) {
   //       case 15:
   //          state.k2 ^= (ulong) ((byte)(savedK2 >> 48)) << 48;
   //          goto case 14;
   //       case 14:
   //          state.k2 ^= (ulong) ((byte) (savedK2 >> 40)) << 40;
   //          goto case 13;
   //       case 13:
   //          state.k2 ^= (ulong) ((byte) (savedK2 >> 32)) << 32;
   //          goto case 12;
   //       case 12:
   //          state.k2 ^= (ulong) ((byte) (savedK2 >> 24)) << 24;
   //          goto case 11;
   //       case 11:
   //          state.k2 ^= (ulong) ((byte) (savedK2 >> 16)) << 16;
   //          goto case 10;
   //       case 10:
   //          state.k2 ^= (ulong) ((byte) (savedK2 >> 8)) << 8;
   //          goto case 9;
   //       case 9:
   //          state.k2 ^= ((byte) savedK2);
   //          goto case 8;

   //       case 8:
   //          state.k1 ^= (ulong) ((byte) (savedK1 >> 56)) << 56;
   //          goto case 7;
   //       case 7:
   //          state.k1 ^= (ulong) ((byte) (savedK1 >> 48)) << 48;
   //          goto case 6;
   //       case 6:
   //          state.k1 ^= (ulong) ((byte) (savedK1 >> 40)) << 40;
   //          goto case 5;
   //       case 5:
   //          state.k1 ^= (ulong) ((byte) (savedK1 >> 32)) << 32;
   //          goto case 4;
   //       case 4:
   //          state.k1 ^= (ulong) ((byte) (savedK1 >> 24)) << 24;
   //          goto case 3;
   //       case 3:
   //          state.k1 ^= (ulong) ((byte) (savedK1 >> 16)) << 16;
   //          goto case 2;
   //       case 2:
   //          state.k1 ^= (ulong) ((byte) (savedK1 >> 8)) << 8;
   //          goto case 1;
   //       case 1:
   //          state.k1 ^= ((byte) savedK1);
   //          bmix(state);
   //          break;
   //    }

   //    state.h2 ^= byteLen;

   //    state.h1 += state.h2;
   //    state.h2 += state.h1;

   //    state.h1 = fmix(state.h1);
   //    state.h2 = fmix(state.h2);

   //    state.h1 += state.h2;
   //    state.h2 += state.h1;

   //    return state.h1;
   // }

   // private void addByte(State state, byte b, uint len) {
   //    int shift = (int)(len & 0x7) * 8;
   //    ulong bb = (b & 0xffUL) << shift;
   //    if ((len & 0x8) == 0) {
   //       state.k1 |= bb;
   //    } else {
   //       state.k2 |= bb;
   //       if ((len & 0xf) == 0xf) {
   //          bmix(state);
   //          state.k1 = 0;
   //          state.k2 = 0;
   //       }
   //    }
   // }

//    public boolean equals(Object other) {
//       return other != null && other.getClass() == getClass();
//    }

   public int hashCode() {
      return 0;
   }

   public String toString() {
      return "MurmurHash3";
   }

//    public static class Externalizer extends NoStateExternalizer<MurmurHash3> {
//       @Override
//       public Set<Class<? extends MurmurHash3>> getTypeClasses() {
//          return Collections.singleton(MurmurHash3.class);
//       }

//       @Override
//       public MurmurHash3 readObject(ObjectInput input) {
//          return instance;
//       }

//       @Override
//       public Integer getId() {
//          return Ids.MURMURHASH_3;
//       }
//    }
}
}