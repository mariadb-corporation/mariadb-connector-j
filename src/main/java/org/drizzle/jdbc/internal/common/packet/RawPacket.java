/*
 * Drizzle-JDBC
 *
 * Copyright (c) 2009-2011, Marcus Eriksson, Trond Norbye
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

package org.drizzle.jdbc.internal.common.packet;

import org.drizzle.jdbc.internal.common.packet.buffer.ReadUtil;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;


/**
 * Class to represent a raw packet as transferred over the wire. First we got 3 bytes specifying the actual length, then
 * one byte packet sequence number and then n bytes with user data.
 */
public final class RawPacket {
    private final ByteBuffer byteBuffer;
    private final int packetSeq;

    /**
     * Get the next packet from the stream
     *
     * @param is the input stream to read the next packet from
     * @return The next packet from the stream, or NULL if the stream is closed
     * @throws java.io.IOException if an error occurs while reading data
     */
    static RawPacket nextPacket(final InputStream is) throws IOException {
        byte[] lengthBuffer = readLengthSeq(is);
        int length = (lengthBuffer[0] & 0xff) + ((lengthBuffer[1] & 0xff) << 8) + ((lengthBuffer[2] & 0xff) << 16);
        if (length == -1) {
            return null;
        }

        if (length < 0) {
            throw new IOException("Got negative packet size: " + length);
        }

        final int packetSeq = lengthBuffer[3];

        final byte[] rawBytes = new byte[length];

        final int nr = ReadUtil.safeRead(is, rawBytes);
        if (nr != length) {
            throw new IOException("EOF. Expected " + length + ", got " + nr);
        }

        return new RawPacket(ByteBuffer.wrap(rawBytes).order(ByteOrder.LITTLE_ENDIAN),
                             packetSeq);
    }

    /**
     * create a raw packet.
     * @param byteBuffer the byte buffer containing the packet
     * @param packetSeq the packet sequence
     */
    private RawPacket(final ByteBuffer byteBuffer, final int packetSeq) {
        this.byteBuffer = byteBuffer;
        this.packetSeq = packetSeq;
    }

    private static byte readPacketSeq(final InputStream reader) throws IOException {
        final int val = reader.read();
        if (val == -1) {
            throw new IOException("EOF");
        }

        return (byte) val;
    }
    private static byte[] readLengthSeq(final InputStream reader) throws IOException {
        final byte[] lengthBuffer = new byte[4];

        final int nr = ReadUtil.safeRead(reader, lengthBuffer);
        if (nr != 4) {
            throw new IOException("Incomplete read! Expected 4, got " + nr);
        }

        return lengthBuffer;
    }
    private static int readLength(final InputStream reader) throws IOException {
        final byte[] lengthBuffer = new byte[3];

        final int nr = ReadUtil.safeRead(reader, lengthBuffer);
        if (nr == -1) {
            return -1;
        } else if (nr != 3) {
            throw new IOException("Incomplete read! Expected 3, got " + nr);
        }

        return (lengthBuffer[0] & 0xff) + ((lengthBuffer[1] & 0xff) << 8) + ((lengthBuffer[2] & 0xff) << 16);
    }

    /**
     * Get the byte buffer backing this packet
     *
     * @return a read only byte buffer
     */
    public ByteBuffer getByteBuffer() {
        return byteBuffer;
    }

    /**
     * Get the package sequence number
     *
     * @return the sequence number of the package
     */
    public int getPacketSeq() {
        return packetSeq;
    }
}
