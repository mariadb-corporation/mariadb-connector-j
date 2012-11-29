/*
MariaDB Client for Java

Copyright (c) 2012 Monty Program Ab. All Rights Reserved.

This library is free software; you can redistribute it and/or modify it under
the terms of the GNU Lesser General Public License as published by the Free
Software Foundation; either version 2.1 of the License, or (at your option)
any later version.

This library is distributed in the hope that it will be useful, but
WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
for more details.

You should have received a copy of the GNU Lesser General Public License along
with this library; if not, write to Monty Program Ab info@montyprogram.com.

This particular MariaDB Client for Java file is work
derived from a Drizzle-JDBC. Drizzle-JDBC file which is covered by subject to
the following copyright and notice provisions:

Copyright (c) 2009-2011, Marcus Eriksson

All rights reserved.

Redistribution and use in source and binary forms, with or without modification,
are permitted provided that the following conditions are met:
Redistributions of source code must retain the above copyright notice, this list
of conditions and the following disclaimer.

Redistributions in binary form must reproduce the above copyright notice, this
list of conditions and the following disclaimer in the documentation and/or
other materials provided with the distribution.

Neither the name of the driver nor the names of its contributors may not be
used to endorse or promote products derived from this software without specific
prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS  AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY
OF SUCH DAMAGE.
*/

package org.mariadb.jdbc.internal.common.packet.buffer;

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
