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

package org.mariadb.jdbc.internal.util.buffer;


import java.io.IOException;
import java.nio.charset.Charset;


public class Buffer {
    public byte[] buf;
    public int position;
    public int limit;

    public Buffer(final byte[] buf, int limit) {
        this.buf = buf;
        this.limit = limit;
    }

    public Buffer(final byte[] buf) {
        this.buf = buf;
        this.limit = this.buf.length;
    }

    public int remaining() {
        return limit - position;
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
        final byte[] byteArrBuff = new byte[remaining()];
        while (remaining() > 0 && ((ch = buf[position++]) != 0)) {
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
        return (short) ((buf[position++] & 0xff)
                + ((buf[position++] & 0xff) << 8));
    }

    /**
     * Read 24 bit integer.
     * @return length
     */
    public int read24bitword() {
        return (buf[position++] & 0xff)
                + ((buf[position++] & 0xff) << 8)
                + ((buf[position++] & 0xff) << 16);
    }

    /**
     * Read a int (4 bytes) from the buffer.
     *
     * @return a int
     */
    public int readInt() {
        return ((buf[position++] & 0xff)
                | ((buf[position++] & 0xff) << 8)
                | ((buf[position++] & 0xff) << 16)
                | ((buf[position++] & 0xff) << 24));
    }

    /**
     * Read a long (8 bytes) from the buffer.
     *
     * @return a long
     */
    public long readLong() {
        return ((buf[position++] & 0xff)
                | ((long) (buf[position++] & 0xff) << 8)
                | ((long) (buf[position++] & 0xff) << 16)
                | ((long) (buf[position++] & 0xff) << 24)
                | ((long) (buf[position++] & 0xff) << 32)
                | ((long) (buf[position++] & 0xff) << 40)
                | ((long) (buf[position++] & 0xff) << 48)
                | ((long) (buf[position++] & 0xff) << 56));
    }

    /**
     * Reads a byte from the buffer.
     *
     * @return the byte
     */
    public byte readByte() {
        return buf[position++];
    }

    /**
     * Read raw data.
     * @param numberOfBytes raw data length.
     * @return raw data
     */
    public byte[] readRawBytes(final int numberOfBytes) {
        final byte[] tmpArr = new byte[numberOfBytes];
        for (int i = 0; i < numberOfBytes; i++) {
            tmpArr[i] = buf[position++];
        }
        return tmpArr;
    }

    public void skipByte() {
        position++;
    }

    public void skipBytes(final int bytesToSkip) {
        position += bytesToSkip;
    }

    /**
     * Skip next length encode binary data.
     * @return this.
     */
    public Buffer skipLengthEncodedBytes() {
        long encLength = getLengthEncodedBinary();
        if (encLength == -1) {
            return null;
        }
        skipBytes((int) encLength);
        return this;
    }

    /**
     * Get next binary data length.
     * @return length of next binary data
     */
    public long getLengthEncodedBinary() {
        final byte type = buf[position++];
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
        if (remaining() <= 0) {
            return new byte[0];
        }
        final long length = getLengthEncodedBinary();
        return getLengthEncodedBytesWithLength(length);
    }

    /**
     * Get next data bytes with known length.
     * @param length binary data length
     * @return the raw binary data
     */
    public byte[] getLengthEncodedBytesWithLength(long length) {
        if (length < 0) {
            return null;
        }
        final byte[] tmpBuf = new byte[(int) length];
        for (int i = 0; i < length; i++) {
            tmpBuf[i] = buf[position++];
        }
        return tmpBuf;
    }

    public byte getByteAt(final int position) {
        return buf[position];
    }

    /**
     * Add stream to bytebuffer.
     * @param otherBuffer stream to add if needed
     */
    public void appendPacket(Buffer otherBuffer) {
        byte[] newBuffer = new byte[limit - position + otherBuffer.limit];
        System.arraycopy(buf, position, newBuffer, 0, remaining());
        System.arraycopy(otherBuffer.buf, 0, newBuffer, limit - position, otherBuffer.limit);
        buf = newBuffer;
        limit = limit - position + otherBuffer.limit;
        position = 0;
    }

    /**
     * Return next binary field length without moving cursor position.
     * @return next binary field length
     */
    public long getSilentLengthEncodedBinary() {
        if (remaining() <= 0) {
            return 0;
        }
        final byte type = buf[position];
        if (type < (byte) 0xfb) {
            return (long) 0xff & type;
        }
        switch (type) {
            case (byte) 0xfb: //251
                return -1;
            case (byte) 0xfc: //252
                return (long) (0xffff & ((buf[position + 1] & 0xff) + ((buf[position + 2] & 0xff) << 8)));
            case (byte) 0xfd: //253
                return 0xffffff & (buf[position + 1] & 0xff)
                        + ((buf[position + 2] & 0xff) << 8)
                        + ((buf[position + 3] & 0xff) << 16);
            case (byte) 0xfe: //254
                return 0xffffff & (buf[position + 1] & 0xff)
                        + ((buf[position + 2] & 0xff) << 8)
                        + ((buf[position + 3] & 0xff) << 16)
                        + ((buf[position + 4] & 0xff) << 24);
            default:
                return (long) 0xff & type;
        }
    }
}