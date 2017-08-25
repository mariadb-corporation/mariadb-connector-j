/*
 *
 * MariaDB Client for Java
 *
 * Copyright (c) 2012-2014 Monty Program Ab.
 * Copyright (c) 2015-2017 MariaDB Ab.
 *
 * This library is free software; you can redistribute it and/or modify it under
 * the terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation; either version 2.1 of the License, or (at your option)
 * any later version.
 *
 * This library is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
 * for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License along
 * with this library; if not, write to Monty Program Ab info@montyprogram.com.
 *
 * This particular MariaDB Client for Java file is work
 * derived from a Drizzle-JDBC. Drizzle-JDBC file which is covered by subject to
 * the following copyright and notice provisions:
 *
 * Copyright (c) 2009-2011, Marcus Eriksson
 *
 * Redistribution and use in source and binary forms, with or without modification,
 * are permitted provided that the following conditions are met:
 * Redistributions of source code must retain the above copyright notice, this list
 * of conditions and the following disclaimer.
 *
 * Redistributions in binary form must reproduce the above copyright notice, this
 * list of conditions and the following disclaimer in the documentation and/or
 * other materials provided with the distribution.
 *
 * Neither the name of the driver nor the names of its contributors may not be
 * used to endorse or promote products derived from this software without specific
 * prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS  AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
 * IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
 * NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
 * PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY
 * OF SUCH DAMAGE.
 *
 */

package org.mariadb.jdbc.internal.io.input;

import org.mariadb.jdbc.internal.ColumnType;
import org.mariadb.jdbc.internal.com.read.Buffer;
import org.mariadb.jdbc.internal.io.LruTraceCache;
import org.mariadb.jdbc.internal.io.TraceObject;
import org.mariadb.jdbc.internal.logging.Logger;
import org.mariadb.jdbc.internal.logging.LoggerFactory;
import org.mariadb.jdbc.internal.util.Utils;

import java.io.BufferedInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;

import static org.mariadb.jdbc.internal.io.TraceObject.NOT_COMPRESSED;

public class StandardPacketInputStream implements PacketInputStream {

    private static final int REUSABLE_BUFFER_LENGTH = 1024;
    private static final int MAX_PACKET_SIZE = 0xffffff;
    private static final Logger logger = LoggerFactory.getLogger(StandardPacketInputStream.class);
    private final byte[] header = new byte[4];
    private final byte[] reusableArray = new byte[REUSABLE_BUFFER_LENGTH];

    private final BufferedInputStream inputStream;

    private int packetSeq;
    private final int maxQuerySizeToLog;
    private int lastPacketLength;
    private String serverThreadLog = "";

    private LruTraceCache traceCache = null;

    public StandardPacketInputStream(InputStream in, int maxQuerySizeToLog) {
        inputStream = new BufferedInputStream(in, 64 * 1024);
        this.maxQuerySizeToLog = maxQuerySizeToLog;
    }

    /**
     * Constructor for single Data (using text format).
     *
     * @param value value
     * @return Buffer
     */
    public static byte[] create(byte[] value) {
        if (value == null) return new byte[]{(byte) 251};

        int length = value.length;
        if (length < 251) {

            byte[] buf = new byte[length + 1];
            buf[0] = (byte) length;
            System.arraycopy(value, 0, buf, 1, length);
            return buf;

        } else if (length < 65536) {

            byte[] buf = new byte[length + 3];
            buf[0] = (byte) 0xfc;
            buf[1] = (byte) length;
            buf[2] = (byte) (length >>> 8);
            System.arraycopy(value, 0, buf, 3, length);
            return buf;

        } else if (length < 16777216) {

            byte[] buf = new byte[length + 4];
            buf[0] = (byte) 0xfd;
            buf[1] = (byte) length;
            buf[2] = (byte) (length >>> 8);
            buf[3] = (byte) (length >>> 16);
            System.arraycopy(value, 0, buf, 4, length);
            return buf;

        } else {

            byte[] buf = new byte[length + 9];
            buf[0] = (byte) 0xfe;
            buf[1] = (byte) length;
            buf[2] = (byte) (length >>> 8);
            buf[3] = (byte) (length >>> 16);
            buf[4] = (byte) (length >>> 24);
            //byte[] cannot have a more than 4 byte length size, so buf[5] -> buf[8] = 0x00;
            System.arraycopy(value, 0, buf, 9, length);
            return buf;
        }

    }

    /**
     * Create Buffer with Text protocol values.
     *
     * @param rowDatas    datas
     * @param columnTypes column types
     * @return Buffer
     */
    public static byte[] create(byte[][] rowDatas, ColumnType[] columnTypes) {

        int totalLength = 0;
        for (byte[] rowData : rowDatas) {
            if (rowData == null) {
                totalLength++;
            } else {
                int length = rowData.length;
                if (length < 251) {
                    totalLength += length + 1;
                } else if (length < 65536) {
                    totalLength += length + 3;
                } else if (length < 16777216) {
                    totalLength += length + 4;
                } else {
                    totalLength += length + 9;
                }
            }
        }
        byte[] buf = new byte[totalLength];

        int pos = 0;
        for (byte[] arr : rowDatas) {
            if (arr == null) {
                buf[pos++] = (byte) 251;
            } else {
                int length = arr.length;
                if (length < 251) {
                    buf[pos++] = (byte) length;
                } else if (length < 65536) {
                    buf[pos++] = (byte) 0xfc;
                    buf[pos++] = (byte) length;
                    buf[pos++] = (byte) (length >>> 8);
                } else if (length < 16777216) {
                    buf[pos++] = (byte) 0xfd;
                    buf[pos++] = (byte) length;
                    buf[pos++] = (byte) (length >>> 8);
                    buf[pos++] = (byte) (length >>> 16);
                } else {
                    buf[pos++] = (byte) 0xfe;
                    buf[pos++] = (byte) length;
                    buf[pos++] = (byte) (length >>> 8);
                    buf[pos++] = (byte) (length >>> 16);
                    buf[pos++] = (byte) (length >>> 24);
                    //byte[] cannot have more than 4 byte length size, so buf[pos+5] -> buf[pos+8] = 0x00;
                    pos += 4;
                }
                System.arraycopy(arr, 0, buf, pos, length);
                pos += length;
            }
        }
        return buf;
    }

    @Override
    public Buffer getPacket(boolean reUsable) throws IOException {
        return new Buffer(getPacketArray(reUsable), lastPacketLength);
    }

    /**
     * Get current Buffered input stream for creating compress input stream,
     * to avoid losing already read bytes in case of pipelining.
     *
     * @return buffer input stream.
     */
    public BufferedInputStream getBufferedInputStream() {
        return inputStream;
    }

    /**
     * Get next packet.
     * If packet is more than 16M, read as many packet needed to finish packet.
     * (first that has not length = 16Mb)
     *
     * @param reUsable if can use existing reusable buffer to avoid creating array
     * @return array packet.
     * @throws IOException if socket exception occur.
     */
    public byte[] getPacketArray(boolean reUsable) throws IOException {

        //***************************************************
        //Read 4 byte header
        //***************************************************
        int remaining = 4;
        int off = 0;
        do {
            int count = inputStream.read(header, off, remaining);
            if (count < 0) {
                throw new EOFException("unexpected end of stream, read " + off + " bytes from 4");
            }
            remaining -= count;
            off += count;
        } while (remaining > 0);

        lastPacketLength = (header[0] & 0xff) + ((header[1] & 0xff) << 8) + ((header[2] & 0xff) << 16);
        packetSeq = header[3];

        //prepare array
        byte[] rawBytes;
        if (reUsable && lastPacketLength < REUSABLE_BUFFER_LENGTH) {
            rawBytes = reusableArray;
        } else rawBytes = new byte[lastPacketLength];

        //***************************************************
        //Read content
        //***************************************************
        remaining = lastPacketLength;
        off = 0;
        do {
            int count = inputStream.read(rawBytes, off, remaining);
            if (count < 0) {
                throw new EOFException("unexpected end of stream, read " + (lastPacketLength - remaining) + " bytes from " + lastPacketLength);
            }
            remaining -= count;
            off += count;
        } while (remaining > 0);

        if (traceCache != null) {
            traceCache.put(System.currentTimeMillis(), new TraceObject(false, NOT_COMPRESSED, Arrays.copyOfRange(header, 0, 4),
                    Arrays.copyOfRange(rawBytes, 0, off > 1000 ? 1000 : off)));
        }

        if (logger.isTraceEnabled()) {
            logger.trace("read:"
                    + serverThreadLog
                    + Utils.hexdump(maxQuerySizeToLog - 4, 0, lastPacketLength, header, rawBytes));
        }

        //***************************************************
        //In case content length is big, content will be separate in many 16Mb packets
        //***************************************************
        if (lastPacketLength == MAX_PACKET_SIZE) {
            int packetLength;
            do {
                remaining = 4;
                off = 0;
                do {
                    int count = inputStream.read(header, off, remaining);
                    if (count < 0) {
                        throw new EOFException("unexpected end of stream, read " + off + " bytes from 4");
                    }
                    remaining -= count;
                    off += count;
                } while (remaining > 0);

                packetLength = (header[0] & 0xff) + ((header[1] & 0xff) << 8) + ((header[2] & 0xff) << 16);
                packetSeq = header[3];

                int currentBufferLength = rawBytes.length;
                byte[] newRawBytes = new byte[currentBufferLength + packetLength];
                System.arraycopy(rawBytes, 0, newRawBytes, 0, currentBufferLength);
                rawBytes = newRawBytes;

                //***************************************************
                //Read content
                //***************************************************
                remaining = packetLength;
                off = currentBufferLength;
                do {
                    int count = inputStream.read(rawBytes, off, remaining);
                    if (count < 0) {
                        throw new EOFException("unexpected end of stream, read " + (packetLength - remaining) + " bytes from " + packetLength);
                    }
                    remaining -= count;
                    off += count;
                } while (remaining > 0);

                if (traceCache != null) {
                    traceCache.put(System.currentTimeMillis(), new TraceObject(false, NOT_COMPRESSED, Arrays.copyOfRange(header, 0, 4),
                            Arrays.copyOfRange(rawBytes, 0, off > 1000 ? 1000 : off)));
                }

                if (logger.isTraceEnabled()) {
                    logger.trace("read:"
                            + serverThreadLog
                            + Utils.hexdump(maxQuerySizeToLog - 4, currentBufferLength, packetLength, header, rawBytes));
                }

                lastPacketLength += packetLength;
            } while (packetLength == MAX_PACKET_SIZE);
        }

        return rawBytes;
    }

    @Override
    public int getLastPacketSeq() {
        return packetSeq;
    }

    @Override
    public int getCompressLastPacketSeq() {
        return 0;
    }

    @Override
    public void close() throws IOException {
        inputStream.close();
    }

    /**
     * Set server thread id.
     *
     * @param serverThreadId current server thread id.
     * @param isMaster       is server master
     */
    public void setServerThreadId(long serverThreadId, Boolean isMaster) {
        this.serverThreadLog = " conn:" + serverThreadId + ((isMaster != null) ? "(" + (isMaster ? "M" : "S") + ")" : "");
    }


    public void setTraceCache(LruTraceCache traceCache) {
        this.traceCache = traceCache;
    }
}
