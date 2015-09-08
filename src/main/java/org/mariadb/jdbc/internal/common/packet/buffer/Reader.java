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

import org.mariadb.jdbc.internal.common.packet.RawPacket;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.Charset;


public class Reader {
    private final byte packetSeq;
    private ByteBuffer byteBuffer;


    public Reader(final RawPacket rawPacket) {
        this.packetSeq = 0;
        byteBuffer = rawPacket.getByteBuffer();
    }

    /**
     * Reads a string from the buffer, looks for a 0 to end the string
     *
     * @param charset the charset to use, for example ASCII
     * @return the read string
     */
    public String readString(final String charset) {
        byte ch;
        int cnt = 0;
        final byte [] byteArrBuff = new byte[byteBuffer.remaining()];
        while (byteBuffer.remaining() > 0 && ((ch = byteBuffer.get()) != 0)) {
            byteArrBuff[cnt++] = ch;
        }
        return new String(byteArrBuff,0,cnt,Charset.forName(charset));
    }

    /**
     * read a short (2 bytes) from the buffer;
     *
     * @return an short
     */
    public short readShort() {
        return byteBuffer.getShort();
    }

    /**
     * read a int (4 bytes) from the buffer;
     *
     * @return a int
     */
    public int readInt() {
        return byteBuffer.getInt();
    }

    /**
     * read a long (8 bytes) from the buffer;
     *
     * @return a long
     */
    public long readLong() {
        return byteBuffer.getLong();
    }


    /**
     * reads a byte from the buffer
     *
     * @return the byte
     */
    public byte readByte()  {
        return byteBuffer.get();
    }

    public byte[] readRawBytes(final int numberOfBytes) {
        final byte [] tmpArr = new byte[numberOfBytes];
        byteBuffer.get(tmpArr, 0,numberOfBytes);
        return tmpArr;
    }

    public void skipByte() throws IOException {
        byteBuffer.get();
    }

    public long skipBytes(final int bytesToSkip) {
        byteBuffer.position(byteBuffer.position()+bytesToSkip);
        return bytesToSkip;
    }

    public Reader skipLengthEncodedBytes() {
        long encLength = getLengthEncodedBinary();
        if (encLength == -1) {
               return null;
        }
        skipBytes((int)encLength);
        return this;
    }
    
    public int read24bitword(){
        final byte[] tmpArr = new byte[3];
        for (int i = 0; i < 3; i++) {
            tmpArr[i] = byteBuffer.get();
        }

        return (tmpArr[0] & 0xff) + ((tmpArr[1] & 0xff) << 8) + ((tmpArr[2] & 0xff) << 16);
    }

    public long getLengthEncodedBinary() {
        if(byteBuffer.remaining() == 0) {
            return 0;
        }
        final byte type = byteBuffer.get();
        if ((type & 0xff) == 251) {
            return -1;
        }
        if ((type & 0xff) == 252) {
            return (long) 0xffff & readShort();
        }
        if ((type & 0xff) == 253) {
            return 0xffffff&read24bitword();
        }
        if ((type & 0xff) == 254) {
            return readLong();
        }
        if ((type & 0xff) <= 250) {
            return (long) 0xff & type;
        }

        return 0;
    }

    public String getLengthEncodedString() {
        final long encLength = getLengthEncodedBinary();
        if (encLength == -1) {
            return null;
        }
        final byte[] tmpBuf = new byte[(int) encLength];
        byteBuffer.get(tmpBuf); 
        try {
            return new String(tmpBuf, "UTF-8");
        } catch (UnsupportedEncodingException uee) {
            throw new AssertionError("UTF-8 not supported");
        }
    }

    public byte[] getLengthEncodedBytes() throws IOException {
        final long encLength = getLengthEncodedBinary();
        if (encLength == -1) {
            return null;
        }
        final byte[] tmpBuf = new byte[(int) encLength];
        byteBuffer.get(tmpBuf);
        return tmpBuf;
    }

    public byte[] getLengthEncodedBytesWithLength(long length) {
        byte [] tmpBuf = new byte[(int) length];
        byteBuffer.get(tmpBuf);
        return tmpBuf;
    }

    public byte getByteAt(final int i) throws IOException {
        return byteBuffer.get(i);
    }

    public byte getPacketSeq() {
        return packetSeq;
    }

    public int getRemainingSize() {
        return byteBuffer.remaining();
    }

    public void appendPacket(RawPacket rawPacket) {
        ByteBuffer newBuffer = ByteBuffer.allocate(byteBuffer.capacity() + rawPacket.getByteBuffer().capacity()).order(ByteOrder.LITTLE_ENDIAN);
        int pos = byteBuffer.position();
        byteBuffer.rewind();
        newBuffer.put(byteBuffer);
        newBuffer.put(rawPacket.getByteBuffer());
        newBuffer.position(pos);
        byteBuffer = newBuffer;
    }

    public long getSilentLengthEncodedBinary(){
        if (byteBuffer.remaining() == 0)
            return 0;
        int pos1 = byteBuffer.position();
        byteBuffer.mark();
        long valueLen = getLengthEncodedBinary();
        int pos2 = byteBuffer.position();
        byteBuffer.reset();
        return valueLen + (pos2 - pos1);
    }
}