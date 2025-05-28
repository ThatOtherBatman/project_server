package com.teza.common.tardis;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * User: tom
 * Date: 1/5/17
 * Time: 1:36 PM
 */
public class UUID5 implements Comparable<UUID5>
{
    public static final UUID5 NIL = new UUID5(0L, 0L);
    public static final String NIL_UUID = NIL.toString();
    private static final char[] hexMap = "0123456789abcdef".toCharArray();

    private final long mostSigBits, leastSigBits;
    private transient int hashCode = -1;

    public UUID5(String value)
    {
        this(getUuid5Bytes(value.getBytes(), NIL.getBytes()));
    }

    public UUID5(String value, UUID5 namespace)
    {
        this(getUuid5Bytes(value.getBytes(), namespace.getBytes()));
    }

    private UUID5(long msb, long lsb)
    {
        mostSigBits = msb;
        leastSigBits = lsb;
    }

    private UUID5(byte[] data) {
        long msb = 0;
        long lsb = 0;
        assert data.length == 16;
        for (int i=0; i<8; i++)
            msb = (msb << 8) | (data[i] & 0xff);
        for (int i=8; i<16; i++)
            lsb = (lsb << 8) | (data[i] & 0xff);
        this.mostSigBits = msb;
        this.leastSigBits = lsb;
    }

    public int hashCode() {
        if (hashCode == -1) {
            hashCode = (int)((mostSigBits >> 32) ^
                    mostSigBits ^
                    (leastSigBits >> 32) ^
                    leastSigBits);
        }
        return hashCode;
    }

    public boolean equals(Object obj) {
        if (obj instanceof String)
        {
            return toString().equals(obj);
        }
        else if (!(obj instanceof UUID5))
            return false;
        UUID5 id = (UUID5) obj;
        return (mostSigBits == id.mostSigBits &&
                leastSigBits == id.leastSigBits);
    }

    public byte[] getBytes()
    {
        byte[] bytes = new byte[16];
        long msb = mostSigBits, lsb = leastSigBits;
        for (int i = 7; i >= 0; i--)
        {
            bytes[i] = (byte) (msb & 0xff);
            msb = msb >> 8;
        }
        for (int i = 15; i >= 8; i--)
        {
            bytes[i] = (byte) (lsb & 0xff);
            lsb = lsb >> 8;
        }
        return bytes;
    }

//    public String toString()
//    {
//        return bytesToUuid(getBytes());
//    }

    public String toString()
    {
        return (digits(mostSigBits >> 32, 8) + "-" +
                digits(mostSigBits >> 16, 4) + "-" +
                digits(mostSigBits, 4) + "-" +
                digits(leastSigBits >> 48, 4) + "-" +
                digits(leastSigBits, 12));
    }

    /** Returns val represented by the specified number of hex digits. */
    private static String digits(long val, int digits)
    {
        long hi = 1L << (digits * 4);
        return Long.toHexString(hi | (val & (hi - 1))).substring(1);
    }


    public static UUID5 fromString(String name)
    {
        return new UUID5(uuidToBytes(name));
    }

    public static String getUuid5(String value, String namespace)
    {
        return bytesToUuid(getUuid5Bytes(value.getBytes(), uuidToBytes(namespace)));
    }

    private static byte[] getUuid5Bytes(final byte[] v, final byte[] ns)
    {
        if (ns == null || ns.length != 16 || v == null || v.length == 0)
        {
            throw new RuntimeException("invalid empty namespace or value");
        }

        final MessageDigest sha1;

        try
        {
            sha1 = MessageDigest.getInstance("SHA-1");
        }
        catch (NoSuchAlgorithmException e)
        {
            throw new RuntimeException(e);
        }
        byte[] combined = new byte[ns.length + v.length];
        for (int i = 0; i < combined.length; i++)
        {
            combined[i] = i < ns.length ? ns[i] : v[i - ns.length];
        }
        combined = sha1.digest(combined);
        byte[] ret = new byte[16];
        System.arraycopy(combined, 0, ret, 0, 16);
        ret[6] &= 0x0F;
        ret[6] |= 0x50;
        ret[8] &= 0x3F;
        ret[8] |= 0x80;
        return ret;
    }

    private static byte[] uuidToBytes(String uuid)
    {
        byte[] value = new byte[16];
        uuid = uuid.replaceAll("-", "");
        int j = 0;
        for (int i = 0; i < 16; i++)
        {
            value[i] = (byte) Integer.parseInt(uuid.substring(j, j + 2), 16);
            j += 2;
        }
        return value;
    }

    private static String bytesToUuid(byte[] value)
    {
        char[] hex = new char[36];
        int j = 0, c;
        for (int i = 0; i < 16; i++)
        {
            if (i == 4 || i == 6 || i == 8 || i == 10)
            {
                hex[i * 2 + j] = '-';
                j++;
            }
            c = value[i] & 0xFF;
            hex[i * 2 + j] = hexMap[c >>> 4];
            hex[i * 2 + j + 1] = hexMap[c & 0x0F];
        }
        return new String(hex);
    }

    @Override
    public int compareTo(UUID5 o)
    {
        return (this.mostSigBits < o.mostSigBits ? -1 :
                (this.mostSigBits > o.mostSigBits ? 1 :
                        (this.leastSigBits < o.leastSigBits ? -1 :
                                (this.leastSigBits > o.leastSigBits ? 1 :
                                        0))));
    }
}
