/*
MariaDB Client for Java

Copyright (c) 2012-2014 Monty Program Ab.

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

Copyright (c) 2009-2011, Marcus Eriksson, Trond Norbye

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


package org.mariadb.jdbc.internal.packet.read;

import org.mariadb.jdbc.internal.logging.Logger;
import org.mariadb.jdbc.internal.logging.LoggerFactory;
import org.mariadb.jdbc.internal.stream.MariaDbInputStream;
import org.mariadb.jdbc.internal.util.Utils;
import org.mariadb.jdbc.internal.util.buffer.Buffer;

import java.io.EOFException;
import java.io.IOException;

public class ReadPacketFetcher {

    public static final int AVOID_CREATE_BUFFER_LENGTH = 4096;
    private static Logger logger = LoggerFactory.getLogger(ReadPacketFetcher.class);
    private static int maxQuerySizeToLog;
    private final MariaDbInputStream inputStream;

    private byte[] headerBuffer = new byte[4];
    private byte[] reusableBuffer = new byte[AVOID_CREATE_BUFFER_LENGTH];

    /**
     * Reader utility to fetch mysql packet.
     *
     * @param is                inputStream
     * @param maxQuerySizeToLog max query size to log
     */
    public ReadPacketFetcher(final MariaDbInputStream is, int maxQuerySizeToLog) {
        this.inputStream = is;
        this.maxQuerySizeToLog = maxQuerySizeToLog;
    }

    /**
     * Get next packet length.
     *
     * @return the length of the next packet
     * @throws IOException if any
     */
    public int getPacketLength() throws IOException {
        return inputStream.readHeader();
    }

    /**
     * Get buffer packet.
     *
     * @return Buffer the buffer
     * @throws IOException if any
     */
    public Buffer getPacket() throws IOException {
        int remaining = 4;
        int off = 0;
        do {
            int count = inputStream.read(headerBuffer, off, remaining);
            if (count <= 0) {
                throw new EOFException("unexpected end of stream, read " + (4 - remaining) + " bytes from " + 4);
            }
            remaining -= count;
            off += count;
        } while (remaining > 0);
        inputStream.setLastPacketSeq(headerBuffer[3]);
        int length = (headerBuffer[0] & 0xff) + ((headerBuffer[1] & 0xff) << 8) + ((headerBuffer[2] & 0xff) << 16);
        byte[] rawBytes = new byte[length];
        remaining = length;
        off = 0;
        do {
            int count = inputStream.read(rawBytes, off, remaining);
            if (count < 0) {
                throw new EOFException("unexpected end of stream, read " + (length - remaining) + " bytes from " + length);
            }
            remaining -= count;
            off += count;
        } while (remaining > 0);

        if (logger.isTraceEnabled()) {
            logger.trace("read packet seq:" + inputStream.getLastPacketSeq() + " length:" + length + " data:"
                    + Utils.hexdump(rawBytes, maxQuerySizeToLog, 0, length));
        }
        return new Buffer(rawBytes, length);
    }

    /**
     * Get buffer with shared array of designated length.
     *
     * @param length            length to read
     * @param lastReusableArray (optional) lastReusableArray to avoid create new array if possible
     * @return Buffer the buffer
     * @throws IOException if any
     */
    public Buffer getReusableBuffer(int length, byte[] lastReusableArray) throws IOException {

        byte[] rawBytes;

        if (length < ReadPacketFetcher.AVOID_CREATE_BUFFER_LENGTH) {
            rawBytes = reusableBuffer;
        } else {
            if (lastReusableArray != null && lastReusableArray.length > length) {
                rawBytes = lastReusableArray;
            } else {
                rawBytes = new byte[length];
            }
        }

        int reads = 0;
        do {
            int count = inputStream.read(rawBytes, reads, length - reads);
            if (count < 0) {
                throw new EOFException("unexpected end of stream, read " + reads + " bytes from " + length);
            }
            reads += count;
        } while (reads < length);

        if (logger.isTraceEnabled()) {
            logger.trace("read packet data(part):" + Utils.hexdump(rawBytes, maxQuerySizeToLog, 0, length));
        }
        return new Buffer(rawBytes, length);
    }

    /**
     * Get buffer with shared array.
     *
     * @return Buffer the buffer
     * @throws IOException if any
     */
    public Buffer getReusableBuffer() throws IOException {
        int remaining = 4;
        int off = 0;
        do {
            int count = inputStream.read(headerBuffer, off, remaining);
            if (count <= 0) {
                throw new EOFException("unexpected end of stream, read " + (4 - remaining) + " bytes from " + 4);
            }
            remaining -= count;
            off += count;
        } while (remaining > 0);
        inputStream.setLastPacketSeq(headerBuffer[3]);
        int length = (headerBuffer[0] & 0xff) + ((headerBuffer[1] & 0xff) << 8) + ((headerBuffer[2] & 0xff) << 16);
        byte[] rawBytes;

        if (length < ReadPacketFetcher.AVOID_CREATE_BUFFER_LENGTH) {
            rawBytes = reusableBuffer;
        } else {
            rawBytes = new byte[length];
        }

        remaining = length;
        off = 0;
        do {
            int count = inputStream.read(rawBytes, off, remaining);
            if (count < 0) {
                throw new EOFException("unexpected end of stream, read " + (length - remaining) + " bytes from " + length);
            }
            remaining -= count;
            off += count;
        } while (remaining > 0);

        if (logger.isTraceEnabled()) {
            logger.trace("read packet seq:" + inputStream.getLastPacketSeq() + " length:" + length + " data:"
                    + Utils.hexdump(rawBytes, maxQuerySizeToLog, 0, length));
        }
        return new Buffer(rawBytes, length);
    }


    public int getLastPacketSeq() {
        return inputStream.getLastPacketSeq();
    }


    public void close() throws IOException {
        inputStream.close();
    }

    public MariaDbInputStream getInputStream() {
        return inputStream;
    }

    /**
     * Read buffer without reading the length packet first.
     *
     * @param length data to read
     * @return byte array the de desired length
     * @throws IOException if any error occur
     */
    public byte[] readLength(int length) throws IOException {
        byte[] valueBuffer = new byte[length];
        int remainingToRead = length;
        int off = 0;
        do {
            int count = inputStream.read(valueBuffer, off, remainingToRead);
            if (count <= 0) {
                throw new EOFException("unexpected end of stream, read " + (length - remainingToRead) + " bytes from " + length);
            }
            remainingToRead -= count;
            off += count;
        } while (remainingToRead > 0);
        if (logger.isTraceEnabled()) {
            logger.trace("read packet data(part):" + Utils.hexdump(valueBuffer, maxQuerySizeToLog));
        }
        return valueBuffer;
    }

    public void skipLength(long length) throws IOException {
        long remainingToRead = length;
        do {
            long count = inputStream.skip(remainingToRead);
            if (count <= 0) {
                throw new EOFException("unexpected end of stream, read " + (length - remainingToRead) + " bytes from " + length);
            }
            remainingToRead -= count;
        } while (remainingToRead > 0);

        if (logger.isTraceEnabled()) {
            logger.trace("skip " + length + " bytes packet");
        }
    }

}
