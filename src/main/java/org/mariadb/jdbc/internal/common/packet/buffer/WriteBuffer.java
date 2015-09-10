/*
MariaDB Client for Java

Copyright (c) 2012 Monty Program Ab.

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

import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.Timestamp;
import java.util.Calendar;

/**
 * . User: marcuse Date: Jan 16, 2009 Time: 8:10:24 PM
 */
public class WriteBuffer {
    private ByteBuffer byteBuffer;

    public WriteBuffer() {
         byteBuffer = ByteBuffer.allocate(1000).order(ByteOrder.LITTLE_ENDIAN);
    }
    
    public WriteBuffer(int bufferSize) {
         byteBuffer = ByteBuffer.allocate(bufferSize).order(ByteOrder.LITTLE_ENDIAN);
    }

    private void assureBufferCapacity(final int len) {
        while (byteBuffer.remaining()<len) {
            ByteBuffer newByteBuffer = ByteBuffer.allocate(2*(len + byteBuffer.capacity())).order(ByteOrder.LITTLE_ENDIAN);
            System.arraycopy(byteBuffer, 0, newByteBuffer, 0, byteBuffer.position());
            byteBuffer = newByteBuffer;
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

    public WriteBuffer writeByteArrayLength(final byte[] bytes) {
        assureBufferCapacity(bytes.length + 9);
        writeFieldLength(bytes.length);
        byteBuffer.put(bytes);
        return this;
    }


    public WriteBuffer writeTimestampLength(final Calendar calendar, Timestamp ts) {
        assureBufferCapacity(12);
        byteBuffer.put((byte) 11);//length

        byteBuffer.putShort((short) calendar.get(Calendar.YEAR));
        byteBuffer.put((byte) ((calendar.get(Calendar.MONTH) + 1) & 0xff));
        byteBuffer.put((byte) (calendar.get(Calendar.DAY_OF_MONTH) & 0xff));
        byteBuffer.put((byte) calendar.get(Calendar.HOUR_OF_DAY));
        byteBuffer.put((byte) calendar.get(Calendar.MINUTE));
        byteBuffer.put((byte) calendar.get(Calendar.SECOND));
        byteBuffer.putInt(ts.getNanos() / 1000);

        return this;
    }

    public WriteBuffer writeDateLength(final Calendar calendar) {
        assureBufferCapacity(8);
        byteBuffer.put((byte) 7);//length

        byteBuffer.putShort((short) calendar.get(Calendar.YEAR));
        byteBuffer.put((byte) ((calendar.get(Calendar.MONTH) + 1) & 0xff));
        byteBuffer.put((byte) (calendar.get(Calendar.DAY_OF_MONTH) & 0xff));
        byteBuffer.put((byte) 0);
        byteBuffer.put((byte) 0);
        byteBuffer.put((byte) 0);
        return this;
    }

    public WriteBuffer writeTimeLength(final Calendar calendar, final boolean fractionalSeconds) {
        if (fractionalSeconds) {
            assureBufferCapacity(13);
            byteBuffer.put((byte) 12);
            byteBuffer.put((byte) 0);
            byteBuffer.putInt(0);
            byteBuffer.put((byte) calendar.get(Calendar.HOUR_OF_DAY));
            byteBuffer.put((byte) calendar.get(Calendar.MINUTE));
            byteBuffer.put((byte) calendar.get(Calendar.SECOND));
            byteBuffer.putInt(calendar.get(Calendar.MILLISECOND) * 1000);
        } else {
            assureBufferCapacity(9);
            byteBuffer.put((byte) 8);//length
            byteBuffer.put((byte) 0);
            byteBuffer.putInt(0);
            byteBuffer.put((byte) calendar.get(Calendar.HOUR_OF_DAY));
            byteBuffer.put((byte) calendar.get(Calendar.MINUTE));
            byteBuffer.put((byte) calendar.get(Calendar.SECOND));
        }

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

    public WriteBuffer writeLong(final long theLong) {
        assureBufferCapacity(8);
        byteBuffer.putLong(theLong);
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

    public WriteBuffer writeStringLength(final String str) {
        final byte[] strBytes;
        try {
            strBytes = str.getBytes("UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException("UTF-8 not supported", e);
        }
        assureBufferCapacity(strBytes.length + 9);
        writeFieldLength(strBytes.length);
        byteBuffer.put(strBytes);
        return this;
    }

    public WriteBuffer writeFieldLength(long length) {
        if (length < 251) {
            byteBuffer.put((byte) length);
        } else if (length < 65536) {
            assureBufferCapacity(3);
            byteBuffer.put((byte)0xfc);
            writeShort((short) length);
        } else if (length < 16777216) {
            assureBufferCapacity(4);
            byteBuffer.put((byte)0xfd);
            byteBuffer.put((byte) (length & 0xff) );
            byteBuffer.put((byte) (length >>> 8) );
            byteBuffer.put((byte) (length >>> 16));
        } else {
            assureBufferCapacity(9);
            byteBuffer.put((byte)0xfe);
            writeLong(length);
        }
        return this;
    }


    public byte[] getBuffer() {
        return byteBuffer.array();
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
