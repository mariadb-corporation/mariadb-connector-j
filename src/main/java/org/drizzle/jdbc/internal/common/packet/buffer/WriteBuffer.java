/*
 * Drizzle JDBC
 *
 * Copyright (C) 2009 Marcus Eriksson (krummas@gmail.com)
 * All rights reserved.
 *
 * Use and distribution licensed under the BSD license.
 */

package org.drizzle.jdbc.internal.common.packet.buffer;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * . User: marcuse Date: Jan 16, 2009 Time: 8:10:24 PM
 */
public class WriteBuffer {
    private final ByteBuffer byteBuffer;

    public WriteBuffer() {
         byteBuffer = ByteBuffer.allocate(1000).order(ByteOrder.LITTLE_ENDIAN);
    }
    
    public WriteBuffer(int bufferSize) {
         byteBuffer = ByteBuffer.allocate(bufferSize).order(ByteOrder.LITTLE_ENDIAN);
    }

    private void assureBufferCapacity(final int len) {

        if(byteBuffer.remaining()<len) {
            byteBuffer.limit(byteBuffer.capacity()*2);
        }
    }
    public WriteBuffer writeByte(final byte theByte) {
        assureBufferCapacity(1);
        byteBuffer.put(theByte);
        return this;
    }

    public WriteBuffer writeByteArray(final byte[] bytes) {
        assureBufferCapacity(bytes.length);
        byteBuffer.put(bytes);
        return this;
    }

    public WriteBuffer writeBytes(final byte theByte, final int count) {
        for (int i = 0; i < count; i++) {
            this.writeByte(theByte);
        }
        return this;
    }

    public WriteBuffer writeShort(final short theShort) {
        assureBufferCapacity(2);
        byteBuffer.putShort(theShort);
        return this;
    }

    public WriteBuffer writeInt(final int theInt) {
        assureBufferCapacity(4);
        byteBuffer.putInt(theInt);
        return this;
    }

    public WriteBuffer writeString(final String str) {
        final byte[] strBytes;
        try {
            strBytes = str.getBytes("UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException("UTF-8 not supported", e);
        }
        return writeByteArray(strBytes);

    }
    public byte [] getLengthWithPacketSeq(byte packetNumber) {
        int length = getLength();
        byte [] lenArr = intToByteArray(length);
        lenArr[3] = packetNumber;
        return lenArr;
    }

    public byte[] getBuffer() {
        return byteBuffer.array();
    }

    public byte[] toByteArrayWithLength(final byte packetNumber) {
        final int length = byteBuffer.capacity() - byteBuffer.remaining();
        final ByteBuffer returnBuffer = ByteBuffer.allocate(length + 4);
        final byte [] lenArr = intToByteArray(length);
        lenArr[3] = packetNumber;
        returnBuffer.put(lenArr);
        returnBuffer.put(byteBuffer.array(), 0,length);
        return returnBuffer.array();
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

    public int getLength() {
        return byteBuffer.capacity() - byteBuffer.remaining();
    }
}
