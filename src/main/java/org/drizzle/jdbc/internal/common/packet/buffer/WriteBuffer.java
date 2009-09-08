/*
 * Drizzle JDBC
 *
 * Copyright (C) 2009 Marcus Eriksson (krummas@gmail.com)
 * All rights reserved.
 *
 * Use and distribution licensed under the BSD license.
 */

package org.drizzle.jdbc.internal.common.packet.buffer;

import java.util.ArrayList;
import java.util.List;

/**
 * . User: marcuse Date: Jan 16, 2009 Time: 8:10:24 PM
 */
public class WriteBuffer {
    private final List<Byte> buffer = new ArrayList<Byte>();

    public WriteBuffer writeByte(final byte theByte) {
        buffer.add(theByte);
        return this;
    }

    public WriteBuffer writeByteArray(final byte[] bytes) {
        for (final byte b : bytes) {
            this.writeByte(b);
        }
        return this;
    }

    public WriteBuffer writeBytes(final byte theByte, final int count) {
        for (int i = 0; i < count; i++) {
            this.writeByte(theByte);
        }
        return this;
    }

    public WriteBuffer writeShort(final short theInt) {
        final byte[] b = shortToByteArray(theInt);
        buffer.add(b[0]);
        buffer.add(b[1]);
        return this;
    }

    public WriteBuffer writeInt(final int theLong) {
        final byte[] b = intToByteArray(theLong);
        for (final byte aB : b) {
            buffer.add(aB);
        }
        return this;
    }

    public WriteBuffer writeString(final String str) {
        final byte[] strBytes = str.getBytes();
        for (final byte aByte : strBytes) {
            buffer.add(aByte);
        }
        return this;
    }

    public byte[] toByteArray() {
        final byte[] returnArray = new byte[buffer.size()];
        int i = 0;
        for (final Byte b : buffer) {
            returnArray[i++] = b;
        }
        return returnArray;
    }

    public byte[] toByteArrayWithLength(final byte packetNumber) {
        final int length = buffer.size();
        final byte[] bufferBytes = new byte[buffer.size() + 4];
        final byte[] lengthBytes = intToByteArray(length);
        lengthBytes[3] = packetNumber;
        int i = 0;
        for (final byte aB : lengthBytes) {
            bufferBytes[i++] = aB;
        }
        for (final Byte aB : buffer) {
            bufferBytes[i++] = aB;
        }
        return bufferBytes;
    }

    public static byte[] shortToByteArray(final short i) {
        final byte[] returnArray = new byte[2];
        returnArray[0] = (byte) (i & 0xff);
        returnArray[1] = (byte) (i >>> 8);
        return returnArray;
    }

    public static byte[] intToByteArray(final int l) {
        final byte[] returnArray = new byte[4];
        returnArray[0] = (byte) (l & 0xff);
        returnArray[1] = (byte) (l >>> 8);
        returnArray[2] = (byte) (l >>> 16);
        returnArray[3] = (byte) (l >>> 24);
        return returnArray;
    }

    public static byte[] longToByteArray(final long l) {
        final byte[] returnArray = new byte[8];
        returnArray[0] = (byte) (l & 0xff);
        returnArray[1] = (byte) (l >>> 8);
        returnArray[2] = (byte) (l >>> 16);
        returnArray[3] = (byte) (l >>> 24);
        returnArray[0] = (byte) (l >>> 32);
        returnArray[1] = (byte) (l >>> 40);
        returnArray[2] = (byte) (l >>> 48);
        returnArray[3] = (byte) (l >>> 56);
        return returnArray;
    }
}
