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
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.Charset;


public class Reader {
    public ByteBuffer byteBuffer;


    public Reader(final ByteBuffer byteBuffer) {
        this.byteBuffer = byteBuffer;
    }

    /**
     * Reads a string from the buffer, looks for a 0 to end the string.
     *
     * @param charset the charset to use, for example ASCII
     * @return the read string
     */
    public String readString(final Charset charset) {
        byte ch;
        int cnt = 0;
        final byte[] byteArrBuff = new byte[byteBuffer.remaining()];
        while (byteBuffer.remaining() > 0 && ((ch = byteBuffer.get()) != 0)) {
            byteArrBuff[cnt++] = ch;
        }
        return new String(byteArrBuff, 0, cnt, charset);
    }

    /**
     * Read a short (2 bytes) from the buffer.
     *
     * @return an short
     */
    public short readShort() {
        return byteBuffer.getShort();
    }

    /**
     * Read a int (4 bytes) from the buffer.
     *
     * @return a int
     */
    public int readInt() {
        return byteBuffer.getInt();
    }

    /**
     * Read a long (8 bytes) from the buffer.
     *
     * @return a long
     */
    public long readLong() {
        return byteBuffer.getLong();
    }


    /**
     * Reads a byte from the buffer.
     *
     * @return the byte
     */
    public byte readByte() {
        return byteBuffer.get();
    }

    /**
     * Read raw data.
     * @param numberOfBytes raw data length.
     * @return raw data
     */
    public byte[] readRawBytes(final int numberOfBytes) {
        final byte[] tmpArr = new byte[numberOfBytes];
        byteBuffer.get(tmpArr, 0, numberOfBytes);
        return tmpArr;
    }

    public void skipByte() {
        byteBuffer.get();
    }

    public long skipBytes(final int bytesToSkip) {
        byteBuffer.position(byteBuffer.position() + bytesToSkip);
        return bytesToSkip;
    }

    /**
     * Skip next length encode binary data.
     * @return this.
     */
    public Reader skipLengthEncodedBytes() {
        long encLength = getLengthEncodedBinary();
        if (encLength == -1) {
            return null;
        }
        skipBytes((int) encLength);
        return this;
    }

    /**
     * Read 24bit encoded length.
     * @return length.
     */
    public int read24bitword() {
        final byte[] tmpArr = new byte[3];
        byteBuffer.get(tmpArr);
        return (tmpArr[0] & 0xff) + ((tmpArr[1] & 0xff) << 8) + ((tmpArr[2] & 0xff) << 16);
    }

    /**
     * Get next binary data length.
     * @return length of next binary data
     */
    public long getLengthEncodedBinary() {
        final byte type = byteBuffer.get();
        if (type < (byte) 0xfb) {
            return (long) 0xff & type;
        }
        switch (type) {
            case (byte) 0xfb: //251
                return -1;
            case (byte) 0xfc: //252
                return (long) 0xffff & readShort();
            case (byte) 0xfd: //253
                return 0xffffff & read24bitword();
            case (byte) 0xfe: //254
                return readLong();
            default:
                return (long) 0xff & type;
        }

    }

    /**
     * Get next data bytes with unknown length.
     * @return the raw binary data
     * @throws IOException if connection problem occur
     */
    public byte[] getLengthEncodedBytes() throws IOException {
        if (byteBuffer.remaining() == 0) {
            return new byte[0];
        }
        final long encLength = getLengthEncodedBinary();
        if (encLength == -1) {
            return null;
        }
        final byte[] tmpBuf = new byte[(int) encLength];
        byteBuffer.get(tmpBuf);
        return tmpBuf;
    }

    /**
     * Get next String encoded binary from byteBuffer.
     * @return String data
     * @throws IOException if connection problem occur
     */
    public String getStringLengthEncodedBytes() throws IOException {
        if (byteBuffer.remaining() == 0) {
            return null;
        }
        final long encLength = getLengthEncodedBinary();
        if (encLength == 0) {
            return "";
        }
        if (encLength != -1) {
            final byte[] tmpBuf = new byte[(int) encLength];
            byteBuffer.get(tmpBuf);
            return new String(tmpBuf);
        }
        return null;
    }

    /**
     * Get next data bytes with known length.
     * @param length binary data length
     * @return the raw binary data
     */
    public byte[] getLengthEncodedBytesWithLength(long length) {
        byte[] tmpBuf = new byte[(int) length];
        byteBuffer.get(tmpBuf);
        return tmpBuf;
    }

    public byte getByteAt(final int position) throws IOException {
        return byteBuffer.get(position);
    }

    public int getRemainingSize() {
        return byteBuffer.remaining();
    }


    /**
     * Add packet to bytebuffer.
     * @param rawPacket packet to add if needed
     */
    public void appendPacket(RawPacket rawPacket) {
        ByteBuffer newBuffer = ByteBuffer.allocate(byteBuffer.capacity() + rawPacket.getByteBuffer().capacity()).order(ByteOrder.LITTLE_ENDIAN);
        final int pos = byteBuffer.position();
        byteBuffer.rewind();
        newBuffer.put(byteBuffer);
        newBuffer.put(rawPacket.getByteBuffer());
        newBuffer.position(pos);
        byteBuffer = newBuffer;
    }

    /**
     * If next data length is not contain in byteBuffer, add packet.
     * @param rawPacket packet to add if needed
     * @param encLength next data remaining length to fetch
     */
    public void appendPacket(RawPacket rawPacket, long encLength) {
        if (encLength < byteBuffer.capacity()) {
            byteBuffer.rewind();
            byteBuffer.put(rawPacket.getByteBuffer());
        } else {
            ByteBuffer newBuffer = ByteBuffer.allocate(byteBuffer.capacity() + rawPacket.getByteBuffer().capacity()).order(ByteOrder.LITTLE_ENDIAN);
            final int pos = byteBuffer.position();
            byteBuffer.rewind();
            newBuffer.put(byteBuffer);
            newBuffer.put(rawPacket.getByteBuffer());
            newBuffer.position(pos);
            byteBuffer = newBuffer;
        }
    }

    /**
     * Return next binary field length without moving cursor position.
     * @return next binary field length
     */
    public long getSilentLengthEncodedBinary() {
        if (byteBuffer.remaining() == 0) {
            return 0;
        }
        int pos1 = byteBuffer.position();
        byteBuffer.mark();
        long valueLen = getLengthEncodedBinary();
        int pos2 = byteBuffer.position();
        byteBuffer.reset();
        return valueLen + (pos2 - pos1);
    }
}