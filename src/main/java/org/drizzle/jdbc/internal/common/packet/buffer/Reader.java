/*
 * Drizzle-JDBC
 *
 * Copyright (c) 2009-2011, Marcus Eriksson
 *
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following
 * conditions are met:
 *
 *  Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.
 *  Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following
 *   disclaimer in the documentation and/or other materials provided with the distribution.
 *  Neither the name of the driver nor the names of its contributors may be used to endorse or promote products derived
 *   from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING,
 * BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO
 * EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
 * PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR
 * TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package org.drizzle.jdbc.internal.common.packet.buffer;

import org.drizzle.jdbc.internal.common.packet.RawPacket;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * . User: marcuse Date: Jan 16, 2009 Time: 8:27:38 PM
 */
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
     * @throws java.io.IOException if it is not possible to create the string from the buffer
     */
    public String readString(final String charset) throws IOException {
        byte ch;
        int cnt = 0;
        final byte [] byteArrBuff = new byte[byteBuffer.remaining()];
        while (byteBuffer.remaining() > 0 && ((ch = byteBuffer.get()) != 0)) {
            byteArrBuff[cnt++] = ch;
        }
        return new String(byteArrBuff,0,cnt);
    }

    /**
     * read a short (2 bytes) from the buffer;
     *
     * @return an short
     * @throws java.io.IOException if there are not 2 bytes left in the buffer
     */
    public short readShort() throws IOException {
        return byteBuffer.getShort();
    }

    /**
     * read a int (4 bytes) from the buffer;
     *
     * @return a int
     * @throws java.io.IOException if there are not 4 bytes left in the buffer
     */
    public int readInt() throws IOException {
        return byteBuffer.getInt();
    }

    /**
     * read a long (8 bytes) from the buffer;
     *
     * @return a long
     * @throws java.io.IOException if there are not 8 bytes left in the buffer
     */
    public long readLong() throws IOException {
        return byteBuffer.getLong();
    }


    /**
     * reads a byte from the buffer
     *
     * @return the byte
     * @throws java.io.IOException if bufferPointer exceeds the length of the buffer
     */
    public byte readByte() throws IOException {
        return byteBuffer.get();
    }

    public byte[] readRawBytes(final int numberOfBytes) throws IOException {
        final byte [] tmpArr = new byte[numberOfBytes];
        byteBuffer.get(tmpArr, 0,numberOfBytes);
        return tmpArr;
    }

    public void skipByte() throws IOException {
        byteBuffer.get();
    }

    public long skipBytes(final int bytesToSkip) throws IOException {
        byteBuffer.position(byteBuffer.position()+bytesToSkip);
        return bytesToSkip;
    }

    public int read24bitword() throws IOException {
        final byte[] tmpArr = new byte[3];
        for (int i = 0; i < 3; i++) {
            tmpArr[i] = byteBuffer.get();
        }

        return (tmpArr[0] & 0xff) + ((tmpArr[1] & 0xff) << 8) + ((tmpArr[2] & 0xff) << 16);
    }

    public long getLengthEncodedBinary() throws IOException {
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

    public String getLengthEncodedString() throws IOException {
        final long encLength = getLengthEncodedBinary();
        if (encLength == -1) {
            return null;
        }
        final byte[] tmpBuf = new byte[(int) encLength];
        byteBuffer.get(tmpBuf);
        return new String(tmpBuf, "UTF-8");
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

    public long getSilentLengthEncodedBinary() throws IOException {
        byteBuffer.mark();
        long retVal = getLengthEncodedBinary();
        byteBuffer.reset();
        return retVal;
    }
}